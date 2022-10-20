# redis-df

Manager redis data with DataFrame(Pandas core data structure).

## The problem we are facing
Assuming that you store data with "lv_cls:1024:start_ts" (zset), "lv_cls:1024:duration" (hash), "lv_cls:1024:has_coupon" (set) in `conn`,
and "lv_cls:1024:student" (hash) in `other_conn`. And you want to read and merge them to a DataFrame.

Here are what they are like:
```shell
127.0.0.1:6379> zrange lv_cls:1024:start_ts 0 -1 WITHSCORES
 1) "10003"
 2) "1638420644"
 3) "10001"
 4) "1638427154"
 5) "10006"
 6) "1638429249"
 7) "10004"
 8) "1638434427"
 9) "10007"
10) "1638434875"
11) "10002"
12) "1638435711"
13) "10005"
14) "1638442177"
15) "10000"
16) "1638449292"

127.0.0.1:6379> hgetall lv_cls:1024:duration
 1) "10000"
 2) "5783"
 3) "10001"
 4) "3026"
 5) "10002"
 6) "5953"
 7) "10003"
 8) "10286"
 9) "10004"
10) "4091"
11) "10005"
12) "6517"
13) "10006"
14) "7698"
15) "10007"
16) "9769"

127.0.0.1:6379> smembers lv_cls:1024:has_coupon
1) "10000"
2) "10002"
3) "10003"
4) "10004"
5) "10006"

127.0.0.1:6379[1]> hgetall lv_cls:1024:student
 1) "10006"
 2) "{\"name\": \"\\u9060\\u85e4 \\u7a14\", \"country\": \"Japan\", \"phone_number\": \"090-1225-7870\"}"
 3) "10000"
 4) "{\"name\": \"Matthew Stevens\", \"country\": \"United States\", \"phone_number\": \"001-828-200-2426x20996\"}"
 5) "10003"
 6) "{\"name\": \"Harriet Hunt-O'Brien\", \"country\": \"United Kingdom\", \"phone_number\": \"028 9018784\"}"
 7) "10001"
 8) "{\"name\": \"\\u0421\\u0443\\u0445\\u0430\\u043d\\u043e\\u0432 \\u0414\\u043e\\u0431\\u0440\\u043e\\u0441\\u043b\\u0430\\u0432 \\u0418\\u0433\\u043d\\u0430\\u0442\\u043e\\u0432\\u0438\\u0447\", \"country\": \"Russia\", \"phone_number\": \"80057759346\"}"
 9) "10007"
10) "{\"name\": \"Baiju Ramachandran\", \"country\": \"India\", \"phone_number\": \"+919436859624\"}"
11) "10002"
12) "{\"name\": \"\\u7eaa\\u6668\", \"country\": \"People's Republic of China\", \"phone_number\": \"13343547086\"}"
13) "10005"
14) "{\"name\": \"Frau Marcella Stoll B.Eng.\", \"country\": \"Germany\", \"phone_number\": \"07264567691\"}"
15) "10004"
16) "{\"name\": \"Claire Caron\", \"country\": \"France\", \"phone_number\": \"+33 (0)3 62 91 94 63\"}"
```

## Define the table which contains these keys
```python
from json import loads

from pandas import to_datetime
from redis_df import Column, Table
from redis_df import Hash, Set, Zset


table = Table(
    "live_class_student",
    Column("student", Hash(loads), "lv_cls:{}:student", client=other_conn),
    Column("start_ts", Zset(0, 1638450720),
            "lv_cls:{}:start_ts", converter=to_datetime, apply_kwargs={"unit": "s"}),
    Column("duration", Hash(), "lv_cls:{}:duration", converter=int),
    Column("has_coupon", Set(), "lv_cls:{}:has_coupon", default=False),
    client=conn
)
```

## Get the DataFrame you want
```python
    table.get(class_id)
    """
                                  name                     country            phone_number            start_ts  duration  has_coupon
    10000              Matthew Stevens               United States  001-828-200-2426x20996 2021-12-02 12:48:12      5783        True
    10001  Суханов Доброслав Игнатович                      Russia             80057759346 2021-12-02 06:39:14      3026       False
    10002                           纪晨  People's Republic of China             13343547086 2021-12-02 09:01:51      5953        True
    10003         Harriet Hunt-O'Brien              United Kingdom             028 9018784 2021-12-02 04:50:44     10286        True
    10004                 Claire Caron                      France    +33 (0)3 62 91 94 63 2021-12-02 08:40:27      4091        True
    10005   Frau Marcella Stoll B.Eng.                     Germany             07264567691 2021-12-02 10:49:37      6517       False
    10006                         遠藤 稔                       Japan           090-1225-7870 2021-12-02 07:14:09      7698        True
    10007           Baiju Ramachandran                       India           +919436859624 2021-12-02 08:47:55      9769       False
    """
```
