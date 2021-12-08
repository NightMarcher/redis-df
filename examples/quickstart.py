"""
run directory: redis-df
command: python -m examples.quickstart
"""
from random import randrange, sample
from faker import Faker
from redis import Redis
from ujson import dumps

from redis_df import Hash, Set, Zset
from redis_df import Column, Table


def init_redis(conn, other_conn, locale_fakes, key_dict):
    other_conn.hmset(key_dict["student"], {
        aid: dumps({
            "name": faker.name(),
            "phone_number": faker.phone_number(),
            "address": faker.address(),
        }) for (aid, faker) in enumerate(locale_fakes, 10000)
    })
    conn.zadd(key_dict["start_ts"], {
        aid: randrange(1638417600, 1638450720) for (aid, _) in enumerate(locale_fakes, 10000)
    })
    conn.hmset(key_dict["duration"], {
        aid: randrange(60 * 60 * 3) for (aid, _) in enumerate(locale_fakes, 10000)
    })
    conn.sadd(key_dict["has_coupon"], *sample(range(10000, 9999 + len(locale_fakes)), 5)
              )


def demo(conn, other_conn):
    table = Table(
        "live_class_student",
        Column("student", Hash(), "lv_cls:{}:student", other_conn),
        Column("start_ts", Zset(0, 1638450720), "lv_cls:{}:start_ts"),
        Column("duration", Hash(), "lv_cls:{}:duration"),
        Column("has_coupon", Set(), "lv_cls:{}:has_coupon"),
        client=conn
    )
    table.get(1024)


def clear_redis(conn, other_conn, key_dict):
    for (alias, key) in key_dict.items():
        if alias == "student":
            other_conn.delete(key)
            continue
        conn.delete(key)


if __name__ == "__main__":
    conn = Redis(decode_responses=True)
    other_conn = Redis(db=1, decode_responses=True)
    fakes = [Faker(locale) for locale in ("en_US", "ru_RU",
                                          "zh_CN", "en_GB", "fr_FR", "de_DE", "ja_JP", "id_ID")]
    key_dict = {
        "student": "lv_cls:1024:student",
        "start_ts": "lv_cls:1024:start_ts",
        "duration": "lv_cls:1024:duration",
        "has_coupon": "lv_cls:1024:has_coupon",
    }

    init_redis(conn, other_conn, fakes, key_dict)
    demo(conn, other_conn)
    clear_redis(conn, other_conn, key_dict)
