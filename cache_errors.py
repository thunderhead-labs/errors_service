import os

import pandas as pd
from common.db_utils import ConnFactory
from common.loggers import get_logger
from common.orm.repository import PoktInfoRepository
from common.orm.schema import ErrorsCache, ServicesStateRange
from common.utils import get_block_ts, get_address_from_pubkey

from db_utils import get_errors_between_dates

BLOCKS_INTERVAL = 4
SERVICE_CLASS = ErrorsCache
SERVICE_NAME = SERVICE_CLASS.__tablename__

path = os.path.dirname(os.path.realpath(__file__))
logger = get_logger(path, SERVICE_NAME, SERVICE_NAME)
perf_logger = get_logger(path, SERVICE_NAME, f"{SERVICE_NAME}_profiler")
stuck_logger = get_logger(path, SERVICE_NAME, f"{SERVICE_NAME}_stuck")
logger.info("Running errors cache")


def get_errors_df(start_height, end_height):
    start_date, end_date = get_block_ts(start_height), get_block_ts(end_height)
    now = pd.Timestamp.now()
    perf_logger.info(f"Started getting errors df of {str(start_date), str(end_date)}")
    errors = pd.DataFrame(
        get_errors_between_dates(pd.Timestamp(start_date), pd.Timestamp(end_date)),
        columns=[
            "timestamp",
            "apppk",
            "blockchain",
            "nodepk",
            "elapsedtime",
            "bytes",
            "method",
            "message",
            "code",
        ],
    )
    errors.index = pd.DatetimeIndex(errors["timestamp"])
    errors.drop("timestamp", axis=1, inplace=True)
    perf_logger.info(
        f"Finished getting errors df of "
        f"{str(start_date), str(end_date)}, took {pd.Timestamp.now() - now}"
    )
    return errors


def get_errors_dict(start_height, end_height, public_keys_dict):
    errors_df = get_errors_df(start_height, end_height)
    errors_dict = {}
    for provider in public_keys_dict:
        errors_dict[provider] = errors_df.loc[
            errors_df["nodepk"].isin(public_keys_dict[provider])
        ]

    return errors_dict


def cache_errors(start_height: int, end_height: int):
    now = pd.Timestamp.now()
    perf_logger.info(f"Caching between {start_height, end_height}")
    try:
        errors_df = get_errors_df(start_height, end_height)
        errors_df.code.fillna(-123, inplace=True)
        error_groups = errors_df.groupby(
            [
                errors_df["nodepk"],
                errors_df["blockchain"],
                errors_df["code"],
                errors_df["message"],
            ]
        )
        now2 = pd.Timestamp.now()
        perf_logger.info(f"Saving between {start_height, end_height}")
        errors_caches = []
        i = 0
        for identifiers, group in error_groups:
            pk = identifiers[0]
            chain = identifiers[1]
            error_type = identifiers[2]
            msg = identifiers[3]
            try:
                address = get_address_from_pubkey(pk)
                errors_count = group["nodepk"].count()
                print(address, errors_count, chain)
                errors_caches.append(
                    ErrorsCache(
                        address=address,
                        chain=chain,
                        errors_count=int(errors_count),
                        error_type=str(error_type),
                        msg=msg,
                        start_height=int(start_height),
                        end_height=int(end_height),
                    )
                )
            except Exception as e:
                print(f"invalid address for: {pk} - {e}")

            if i % 10000 == 0:
                with ConnFactory.poktinfo_conn() as session:
                    has_added = PoktInfoRepository.save_many(session, errors_caches)
                    errors_caches = []

            i += 1

        perf_logger.info(
            f"Finished saving between "
            f"{start_height, end_height}, took {pd.Timestamp.now() - now2}"
        )
    except Exception as e:
        logger.error(f"Error at block {start_height, end_height}: ", exc_info=e)
        has_added = False

    status = "success" if has_added else "failed"
    with ConnFactory.poktinfo_conn() as session:
        PoktInfoRepository.upsert(
            session,
            ServicesStateRange(
                service=SERVICE_NAME,
                start_height=start_height,
                end_height=end_height,
                status=status,
            ),
        )

    perf_logger.info(
        f"Finished caching between "
        f"{start_height, end_height}, took {pd.Timestamp.now() - now}"
    )


def cache_errors_historical(start_height: int, end_height: int):
    while start_height + BLOCKS_INTERVAL < end_height:
        cache_errors(
            start_height,
            start_height + BLOCKS_INTERVAL,
        )
        start_height += BLOCKS_INTERVAL
