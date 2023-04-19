import os

import pandas as pd

from common.db_utils import (
    fetch_all_raw,
    fetch_all,
    execute_stmt,
    ConnFactory,
    PostgresInterface,
)

BASE_PATH = os.path.dirname(os.path.realpath(__file__))


def get_errors_between_dates(start_date: pd.Timestamp, end_date: pd.Timestamp):
    select_stmt = f"""SELECT * FROM public.error WHERE timestamp > TIMESTAMPTZ
            '{start_date}' AND timestamp < TIMESTAMPTZ '{end_date}'"""
    with ConnFactory.errors_conn(
        interface_type=PostgresInterface.PSYCOPG2_DRIVER
    ) as conn:
        errors = fetch_all_raw(conn, select_stmt)
    return errors


def get_public_keys_of_provider(provider: str):
    select_stmt = f"""SELECT public_key FROM public.node
    WHERE service_url = '{provider}'"""
    with ConnFactory.errors_conn(
        interface_type=PostgresInterface.PSYCOPG2_DRIVER
    ) as conn:
        public_keys = fetch_all(conn, select_stmt)
    return public_keys


def add_errors_cache_entry(
    conn,
    start_height: int,
    end_height: int,
    provider: str,
    errors_count: int,
    error_type: str,
    msg: str,
    chain: str,
):
    insert_stmt = f"""INSERT INTO public.errors_cache(
    start_height, end_height, provider, errors_count,
    error_type, msg, date_created, chain)
    VALUES ({start_height}, {end_height}, '{provider}',
    {errors_count}, '{error_type}', '{msg}', current_timestamp, '{chain}')"""
    return execute_stmt(conn, insert_stmt)


def get_error_msgs(conn, start_height: int, end_height: int):
    select_stmt = f"""SELECT msg FROM public.errors_cache
 WHERE start_height > {start_height} AND end_height < {end_height}"""
    error_msgs = fetch_all(conn, select_stmt)
    return error_msgs
