import os

import numpy as np
import pandas as pd
from common.db_utils import (
    get_latency_dict,
    ConnFactory,
    PostgresInterface,
)
from common.loggers import get_logger
from common.orm.repository import PoktInfoRepository
from common.orm.schema import LatencyCache, ServicesStateRange
from sqlalchemy.orm import Session

from db_utils import BASE_PATH

BLOCKS_INTERVAL = 4
LATENCY_DB_CREDS = f"{BASE_PATH}db_creds_latency.json"
SERVICE_CLASS = LatencyCache
SERVICE_NAME = SERVICE_CLASS.__tablename__

path = os.path.dirname(os.path.realpath(__file__))
logger = get_logger(path, SERVICE_NAME, SERVICE_NAME)
perf_logger = get_logger(path, SERVICE_NAME, f"{SERVICE_NAME}_profiler")
stuck_logger = get_logger(path, SERVICE_NAME, f"{SERVICE_NAME}_stuck")
logger.info("Running latency cache")


def cache_latency(from_height: int, to_height: int, session: Session):
    now = pd.Timestamp.now()
    perf_logger.info(f"{now}: Caching latency for {from_height, to_height}")

    with ConnFactory.latency_conn(
        interface_type=PostgresInterface.PSYCOPG2_DRIVER
    ) as conn:
        latency_dict = get_latency_dict(conn, from_height, to_height)

    latency_df = pd.DataFrame(
        data=latency_dict,
        columns=[
            "public_key",
            "chain",
            "session_key",
            "session_height",
            "region",
            "address",
            "total_success",
            "total_failure",
            "median_success_latency",
            "weighted_success_latency",
            "avg_success_latency",
            "avg_weighted_success_latency",
            "p90_latency",
            "attempts",
            "success_rate",
            "failure",
            "application_public_key",
        ],
    )
    address_groups = latency_df.groupby([latency_df["address"]])
    latency_caches = []
    for address, address_group in address_groups:
        address = str(address)
        region_groups = address_group.groupby([address_group["region"]])
        for region, region_group in region_groups:
            region = str(region)
            chain_groups = region_group.groupby([region_group["chain"]])
            for chain, chain_group in chain_groups:
                total_relays = int(chain_group["total_success"].sum())
                if total_relays > 0:
                    avg_latency = float(
                        (
                            chain_group["avg_success_latency"]
                            * chain_group["total_success"]
                        ).sum()
                        / total_relays
                    )
                    avg_weighted_latency = float(
                        (
                            chain_group["avg_weighted_success_latency"]
                            * chain_group["total_success"]
                        ).sum()
                        / total_relays
                    )
                    # TODO what is attempts? should use total_success?
                    avg_p90_latency = float(
                        chain_group.apply(
                            lambda x: sum(
                                np.array(x["p90_latency"]) * np.array(x["attempts"])
                            )
                            / sum(x["attempts"]),
                            axis=1,
                        ).mean()
                    )
                    latency_caches.append(
                        LatencyCache(
                            address=address,
                            region=region,
                            chain=chain,
                            total_relays=total_relays,
                            avg_latency=avg_latency,
                            avg_weighted_latency=avg_weighted_latency,
                            avg_p90_latency=avg_p90_latency,
                            start_height=from_height,
                            end_height=to_height,
                        )
                    )

    has_added = PoktInfoRepository.save_many(session, latency_caches)
    status = "success" if has_added else "failure"
    PoktInfoRepository.upsert(
        session,
        ServicesStateRange(
            service=SERVICE_NAME,
            start_height=from_height,
            end_height=to_height,
            status=status,
        ),
    )

    perf_logger.info(
        f"{pd.Timestamp.now()}: Finished caching latency for "
        f"{from_height, to_height}, took {pd.Timestamp.now() - now}"
    )


def cache_latency_historical(start_height: int, end_height: int):
    with ConnFactory.poktinfo_conn() as session:
        while start_height + BLOCKS_INTERVAL < end_height:
            cache_latency(start_height, start_height + BLOCKS_INTERVAL, session)
            start_height += BLOCKS_INTERVAL
