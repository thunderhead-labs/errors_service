import sys
from time import sleep

from common.db_utils import (
    ConnFactory,
)
from common.orm.repository import PoktInfoRepository
from common.utils import get_last_block_height

from cache_latency import (
    BLOCKS_INTERVAL,
    cache_latency,
    cache_latency_historical,
    SERVICE_CLASS,
    stuck_logger,
    logger,
)

IS_TEST = False
mode = str(sys.argv[1])

# python3 run_latency_cache.py history 500 50000
if mode == "history":
    # Historical mode - gets rewards for addresses between heights.
    from_height, to_height = int(sys.argv[2]), int(sys.argv[3])
    # Force start_block to be divisible by BLOCKS_INTERVAL
    leftover = from_height % BLOCKS_INTERVAL if not IS_TEST else 0
    cache_latency_historical(from_height - leftover, to_height)
# python3 run_latency_cache.py live (optional to add height to start from)
elif mode == "live":
    stuck_height_count = (0, 0)
    while True:
        try:
            last_height = get_last_block_height()
            leftover = last_height % BLOCKS_INTERVAL if not IS_TEST else 0

            with ConnFactory.poktinfo_conn() as session:
                last_recorded_height = (
                    PoktInfoRepository.get_last_recorded_latency_height(session)
                )

            recorded_leftover = (
                last_recorded_height % BLOCKS_INTERVAL if not IS_TEST else 0
            )
            last_recorded_height -= recorded_leftover
            last_recorded_height = (
                last_recorded_height
                if last_recorded_height > 0
                else last_height - BLOCKS_INTERVAL - leftover
            )

            if last_recorded_height == stuck_height_count[0]:
                stuck_height_count = (last_recorded_height, stuck_height_count[1] + 1)
            else:
                stuck_height_count = (last_recorded_height, 0)

            max_stuck_allowed = BLOCKS_INTERVAL * 15 * 2
            if stuck_height_count[1] > max_stuck_allowed:
                last_recorded_height += BLOCKS_INTERVAL * (
                    stuck_height_count[1] // max_stuck_allowed
                )
                stuck_logger.info(
                    f"Stuck at height {stuck_height_count[0]} "
                    f"for {stuck_height_count[1]} minutes, skipping to {last_recorded_height}."
                )

            # Force start_block to be divisible by BLOCKS_INTERVAL
            if IS_TEST or last_recorded_height % BLOCKS_INTERVAL == 0:
                if last_recorded_height + BLOCKS_INTERVAL <= last_height:
                    # Sleep for 1 block to make sure no delayed input
                    sleep(15 * 60)
                    with ConnFactory.poktinfo_conn() as session:
                        cache_latency(
                            last_recorded_height,
                            last_recorded_height + BLOCKS_INTERVAL,
                            session,
                        )
                    logger.info(
                        f"Completed {last_recorded_height, last_recorded_height + BLOCKS_INTERVAL}"
                    )
                    sleep(60)
        except Exception as e:
            logger.error(e)
        sleep(60)
# python3 run_latency_cache.py complete
elif mode == "complete":
    # Retrieves from db failed blocks and reruns them
    with ConnFactory.poktinfo_conn() as session:
        height_ranges = PoktInfoRepository.get_failed_ranges_of_service(
            session, SERVICE_CLASS
        )
    for height_range in zip(height_ranges):
        start_height, end_height = height_range[0]
        with ConnFactory.poktinfo_conn() as session:
            cache_latency(start_height, end_height, session)
