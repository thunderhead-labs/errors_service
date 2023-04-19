# Summary

This repository contains the [PoktInfo](https://beta.pokt.info) services that
depend on Pocket's internal data they expose to us (latency and errors).
Latency info indexes node performance each session by grouping and transforming the data.
Errors Info indexes the network's errors of each node per block.


# Installation
1. Follow the Installation steps of [common](https://github.com/thunderhead-labs/common-os#readme), a requisite for PoktInfo which contains the ORM, generic interactions, and more.
2. Clone this repository
3. Follow the steps for the desired service below
#### Change `BASE_PATH` at `db_utils.py` if needed

# Latency Info

To run this service and gather historical data:

`python run_latency_cache.py history <START_HEIGHT> <END_HEIGHT>`

To run this service "live" and collect current and future data:

`python run_latency_cache.py live`

`cache_latency.py` is the main file which invokes the logic in `run_latency_cache.py`.

## Notes

#### Caching interval is by default 4
* Can be changed to a higher value (24, 96) by changing `BLOCKS_INTERVAL` at `cache_latency.py`
* Minimum interval is 4 because a session is 4 blocks
* A higher interval will result in smaller data set but will limit minimum poktinfo interval to that interval

## Logic

### Historical:
1. `run_latency_cache.py` invokes `cache_latency_historical()` where `cache_latency()` is called for each block in skips of `BLOCK_INTERVAL`.
   1. `cache_latency()` queries latency records of session from db that is exposed to us by pokt network
   2. Group records by address, region, and chain
   3. For each group, calculate the average latency among other [stats](https://github.com/thunderhead-labs/common-os/blob/master/common/orm/schema/poktinfo.py#L112) and save it to db

### Live:
Performs the same process as historical mode, but calls `cache_latency()`
starting at the last cached block sequentially. If the last cached block is
equal to the current height, the process will sleep until `height % BLOCK_INTERVAL == 0`.
We separate `historical` and `live` processes because `historical` allows for indexing
on custom ranges, saving storage space and time.

### Schema

Please see [here](https://github.com/thunderhead-labs/common-os/blob/master/common/orm/schema/poktinfo.py#L112) for the latency info schema definition.


# Errors Info

To run this service and gather historical data:

`python run_errors_cache.py history <START_HEIGHT> <END_HEIGHT>`

To run this service "live" and collect current and future data:

`python run_errors_cache.py live`

`cache_errors.py` is the main file which invokes the logic in `run_errors_cache.py`.

## Notes

#### Caching interval is by default 4
* Can be changed to a higher value (24, 96) by changing `BLOCKS_INTERVAL` at `cache_errors.py`
* Minimum interval is 1 but that would just be a copy of the errors db
* A higher interval will result in smaller data set but will limit minimum poktinfo interval to that interval

## Logic

### Historical:
1. `run_errors_cache.py` invokes `cache_errors_historical()` where `cache_errors()` is called for each block in skips of `BLOCK_INTERVAL`.
   1. `cache_errors()` queries error records of each block from db that is exposed to us by pokt network
   2. Group records by nodepk (later converted to address), chain, error type, and error message
   3. For each group, save to db

### Live:
Performs the same process as historical mode, but calls `cache_errors()`
starting live block sequentially (no completion due to how slow each cache batch is).
The process will sleep until `height % BLOCK_INTERVAL == 0`.
We separate `historical` and `live` processes because `historical` allows for indexing
on custom ranges, saving storage space and time.

### Schema

Please see [here](https://github.com/thunderhead-labs/common-os/blob/master/common/orm/schema/poktinfo.py#L79) for the errors info schema definition.
