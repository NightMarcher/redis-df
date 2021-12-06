from redis import Redis

from redis_df import Hash, Set, Column, Table


def main():
    table = Table(
        "redis_df",
        Column("account_id", Set, "cls:{}:aid"),
        Column("start_time", Hash, "cls:{}:st"),
        client=Redis(decode_responses=True)
    )
    table.read(1024)


if __name__ == "__main__":
    main()
