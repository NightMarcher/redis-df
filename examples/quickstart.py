from redis import Redis

from redis_df import Hash, Set, Zset
from redis_df import Column, Table


def main():
    conn = Redis(decode_responses=True)
    other_conn = Redis(db=1, decode_responses=True)

    table = Table(
        "live_class_student",
        Column("student", Hash(), "lv_cls:{}:student", other_conn),
        Column("start_ts", Zset(0, 1638450720), "lv_cls:{}:start_ts"),
        Column("duration", Hash(), "lv_cls:{}:duration"),
        Column("has_coupon", Set(), "lv_cls:{}:has_coupon"),
        client=conn
    )
    table.get(1024)


if __name__ == "__main__":
    main()
