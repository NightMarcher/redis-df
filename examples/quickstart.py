"""
run directory: redis-df
command: python -m examples.quickstart
"""
from json import dumps, loads
from random import randrange, sample

from faker import Faker
from pandas import to_datetime
from redis import Redis
from redis_df import Column, Table
from redis_df import Hash, Set, Zset


def init_redis(conn, other_conn, locale_fakers, key_dict):
    offset = 10000
    other_conn.hset(key_dict["student"], mapping={
        aid: dumps({
            "name": faker.name(),
            "country": faker.current_country(),
            "phone_number": faker.phone_number(),
        }) for (aid, faker) in enumerate(locale_fakers, offset)
    })
    conn.zadd(key_dict["start_ts"], {
        aid: randrange(1638417600, 1638450720)
        for (aid, _) in enumerate(locale_fakers, offset)
    })
    conn.hset(key_dict["duration"], mapping={
        aid: randrange(60 * 60 * 3)
        for (aid, _) in enumerate(locale_fakers, offset)
    })
    conn.sadd(key_dict["has_coupon"],
        *sample(range(offset, offset + len(locale_fakers)), 5)
    )


def demo(conn, other_conn, class_id):
    table = Table(
        "live_class_student",
        Column("student", Hash(loads), "lv_cls:{}:student", client=other_conn),
        Column("start_ts", Zset(0, 1638450720),
               "lv_cls:{}:start_ts", converter=to_datetime, apply_kwargs={"unit": "s"}),
        Column("duration", Hash(), "lv_cls:{}:duration", converter=int),
        Column("has_coupon", Set(), "lv_cls:{}:has_coupon", default=False),
        client=conn
    )
    df = table.get(class_id)
    print(df.sort_index())


def clear_redis(conn, other_conn, key_dict):
    for (alias, key) in key_dict.items():
        if alias == "student":
            other_conn.delete(key)
        else:
            conn.delete(key)


if __name__ == "__main__":
    conn = Redis(decode_responses=True)
    other_conn = Redis(db=1, decode_responses=True)
    fakes = [
        Faker(locale) for locale in (
            "en_US", "ru_RU", "zh_CN", "en_GB",
            "fr_FR", "de_DE", "ja_JP", "en_IN",
        )
    ]
    class_id = 1024
    key_dict = {
        "student": f"lv_cls:{class_id}:student",
        "start_ts": f"lv_cls:{class_id}:start_ts",
        "duration": f"lv_cls:{class_id}:duration",
        "has_coupon": f"lv_cls:{class_id}:has_coupon",
    }

    init_redis(conn, other_conn, fakes, key_dict)
    demo(conn, other_conn, class_id)
    clear_redis(conn, other_conn, key_dict)
