import sys
from time import sleep

from common.db_utils import (
    ConnFactory,
)
from common.orm.repository import PoktInfoRepository
from common.utils import get_last_block_height

from cache_errors import (
    BLOCKS_INTERVAL,
    cache_errors,
    cache_errors_historical,
    SERVICE_CLASS,
    logger,
)

IS_TEST = False
mode = str(sys.argv[1])

# python3 run_errors_cache.py history 500 50000
if mode == "history":
    # Historical mode - gets rewards for addresses between heights.
    from_height, to_height = int(sys.argv[2]), int(sys.argv[3])
    # Force start_block to be divisible by BLOCKS_INTERVAL
    leftover = from_height % BLOCKS_INTERVAL if not IS_TEST else 0
    cache_errors_historical(from_height - leftover, to_height)
# python3 run_errors_cache.py live (optional to add height to start from)
elif mode == "live":
    while True:
        try:
            last_height = get_last_block_height()

            # Force start_block to be divisible by BLOCKS_INTERVAL
            if IS_TEST or last_height % BLOCKS_INTERVAL == 0:
                cache_errors(last_height - BLOCKS_INTERVAL, last_height)
                logger.info(f"Completed {last_height - BLOCKS_INTERVAL, last_height}")
                sleep(60)
        except Exception as e:
            logger.error(e)
        sleep(60)
# python3 run_errors_cache.py complete
elif mode == "complete":
    # Retrieves from db failed blocks and reruns them
    with ConnFactory.poktinfo_conn() as session:
        height_ranges = PoktInfoRepository.get_failed_ranges_of_service(
            session, SERVICE_CLASS
        )
    for height_range in zip(height_ranges):
        start_height, end_height = height_range[0]
        cache_errors(start_height, end_height)
