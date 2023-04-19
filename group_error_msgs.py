import re

from common.db_utils import ConnFactory, PostgresInterface
from common.utils import get_last_block_height

from db_utils import get_error_msgs


# Potentially problematic phrases: [{"error", Too Many Requests, [code


def filter_msg_for_mode(msg: str, mode: str, phrase: str = None):
    split_msg = msg.split(":")
    if phrase is not None:
        split_msg[0] = phrase
    if mode == "first":
        return split_msg[0]
    elif mode == "first_last":
        return f"{split_msg[0]}:{split_msg[-1]}"
    elif mode == "second":
        return split_msg[1]
    return None


def is_saved_phrase_in_msg(saved_phrase: str, msg: str, mode: str):
    if mode == "cut_middle":
        split_msg = msg.split(" ")
        split_msg = [split_msg[0]] + split_msg[2:]
        return saved_phrase in " ".join(split_msg)
    return saved_phrase in msg


def filter_error_msg(msg: str, mode: str = "first"):
    numbers_re = r"[0-9]"
    msg = re.sub(numbers_re, "", msg).lower()
    for saved_phrase in SAVED_PHRASES:
        phrase_mode = SAVED_PHRASES[saved_phrase]
        saved_phrase = saved_phrase.lower()
        if is_saved_phrase_in_msg(saved_phrase, msg.split(":")[0], phrase_mode):
            return filter_msg_for_mode(msg, phrase_mode, saved_phrase)
    return filter_msg_for_mode(msg, mode)


MODE = "first_last"
SAVED_PHRASES = {
    "missing trie node": MODE,
    "No state available for block": MODE,
    "Reverted": MODE,
    "Bad Gateway": MODE,
    "Service Unavailable": MODE,
    "Bad Request": MODE,
    "Not Found": MODE,
    "Not Allowed": MODE,
    "Gateway Time-out": MODE,
    "Internal Server Error": MODE,
    "Service Temporarily Unavailable": MODE,
    "error": MODE,
    "Block could not be found": "cut_middle",
    "tx.origin is not authorized to deploy a contract": "cut_middle",
    "getdeletestateobject error": "cut_middle",
    '{"response"': "second",
}
BLOCKS_INTERVAL = 100
end_height = get_last_block_height()
start_height = end_height - 300

msg_groups = {}

with ConnFactory.poktinfo_conn(
    interface_type=PostgresInterface.PSYCOPG2_DRIVER
) as conn:
    for height in range(start_height, end_height, BLOCKS_INTERVAL):
        error_msgs = get_error_msgs(conn, height, height + BLOCKS_INTERVAL)
        for error_msg in error_msgs:
            error_msg = filter_error_msg(error_msg, MODE)
            if error_msg is not None:
                if error_msg in msg_groups:
                    msg_groups[error_msg] += 1
                else:
                    msg_groups[error_msg] = 1
