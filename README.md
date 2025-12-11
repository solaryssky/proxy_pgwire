**Проект прокси, исспользующего postgres wire protocol для связки клиент - база Postgres**


Запуск: ```source ./env.sh && cargo run``` 

Пример выборки: ```psql -h 127.0.0.1 -p 55432 -U postgres -d postgres_key -c "select * from test where id = 100;"```



**Результат тестов pgbench:**

```
transaction type: /tmp/pgbench_test.sql
scaling factor: 1
query mode: simple
number of clients: 100
number of threads: 1
maximum number of tries: 1
duration: 300 s
number of transactions actually processed: 1313704
number of failed transactions: 0 (0.000%)
latency average = 21.249 ms
latency stddev = 49.597 ms
initial connection time = 68.691 ms
tps = 4379.893502 (without initial connection time)
statement latencies in milliseconds and failures:
        21.249           0  SELECT * FROM test WHERE id = 100;```

