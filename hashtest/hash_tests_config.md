# Hash Tests

This is to create some test to compare range scans on a hash index vs a nonhash index.

## Schema for HASH index tests

Created two tables to test.  Both tables have a hash index and a regular index.  The hash index on `measure2` however uses the `STORING` clause to include data needed for the queries and avoid the join with the main table PK.  

```sql

CREATE DATABASE hash;
USE hash;

SET experimental_enable_hash_sharded_indexes=true;

CREATE TABLE measure (
    id UUID DEFAULT gen_random_uuid(),
    ts TIMESTAMP NOT NULL,
    txtblob TEXT DEFAULT LPAD('',1000,'Z'),
    m100 TEXT DEFAULT FLOOR(RANDOM()*100)::TEXT,
    v100 INT as (m100::INT) STORED,
    PRIMARY KEY (id),
    INDEX idx_ts (ts ASC),
    INDEX idx_tshash (ts ASC) USING HASH WITH BUCKET_COUNT = 9
);
ALTER TABLE measure SPLIT AT select gen_random_uuid() from generate_series(1,9);
ALTER INDEX measure@idx_ts SPLIT AT SELECT ('2000-01-01 00:00:00.000000'::timestamp::int + 3600*24*365*i)::timestamp
  FROM generate_series(1,5) as i;

CREATE TABLE measure2 (
    id UUID DEFAULT gen_random_uuid(),
    ts TIMESTAMP NOT NULL,
    txtblob TEXT DEFAULT LPAD('',1000,'Z'),
    m100 TEXT DEFAULT FLOOR(RANDOM()*100)::TEXT,
    v100 INT as (m100::INT) STORED,
    PRIMARY KEY (id),
    INDEX idx_ts (ts ASC) STORING (m100, v100),
    INDEX idx_tshash (ts ASC) USING HASH WITH BUCKET_COUNT = 9 STORING (m100, v100)
);

ALTER TABLE measure2 SPLIT AT select gen_random_uuid() from generate_series(1,9);
ALTER INDEX measure@idx_ts SPLIT AT SELECT ('2000-01-01 00:00:00.000000'::timestamp::int + 3600*24*365*i)::timestamp
  FROM generate_series(1,5) as i;

ALTER RANGE default CONFIGURE ZONE USING
  range_min_bytes = 134217728,
  range_max_bytes = 536870912,
  gc.ttlseconds = 3600,
  num_replicas = 3,
  constraints = '[]',
  lease_preferences = '[]';


-- Handy Statements to generate series
-- 
select generate_series('2000-01-01 00:00:00.000000', '2000-01-02 00:00:00.000000', '0.1s');
select generate_series(now()::timestamp, (now() + interval '1d')::timestamp, '1s');

```

## QUERIES use in test

### Query Templates used by Python

```sql
-- QUERY Templates for Table and timespan
--
SELECT m100, count(*), sum(v100) 
FROM {}@idx_ts
WHERE ts BETWEEN {} and {}
GROUP BY m100
ORDER BY 3 desc
LIMIT 10;

SELECT m100, count(*), sum(v100) 
FROM {}@idx_tshash
WHERE ts BETWEEN {} and {}
GROUP BY m100
ORDER BY 3 desc
LIMIT 10;
```

### Misc Test Queries

```sql
-- RANDOM QUERY SPANS to compare
--
SELECT m100, count(*), sum(v100) 
FROM measure@idx_ts
WHERE ts BETWEEN '2000-06-01 00:00:00.000000' and '2000-06-02 00:00:00.000000'
GROUP BY m100
ORDER BY 3 desc
LIMIT 10;

SELECT m100, count(*), sum(v100) 
FROM measure2@idx_ts
WHERE ts BETWEEN '2000-06-01 00:00:00.000000' and '2000-06-02 00:00:00.000000'
GROUP BY m100
ORDER BY 3 desc
LIMIT 10;

SELECT m100, count(*), sum(v100) 
FROM measure@idx_tshash
WHERE ts BETWEEN '2000-06-01 00:00:00.000000' and '2000-06-02 00:00:00.000000'
GROUP BY m100
ORDER BY 3 desc
LIMIT 10;

SELECT m100, count(*), sum(v100) 
FROM measure2@idx_tshash
WHERE ts BETWEEN '2000-06-01 00:00:00.000000' and '2000-06-02 00:00:00.000000'
GROUP BY m100
ORDER BY 3 desc
LIMIT 10;

---RANDOM DAY top10 summary (but still full scan)
---
WITH day as (select ('2000-01-01 00:00:00.000000'::timestamp::int + 3600*24*FLOOR(RANDOM()*364)::int)::timestamp as dts)
SELECT m100, count(*), sum(v100), min(ts), max(ts)
FROM measure@idx_tshash
WHERE ts BETWEEN (select dts from day) and (select dts + INTERVAL '1d' from day)
GROUP BY m100
ORDER BY 3 desc
LIMIT 10;

WITH day as (select ('2000-01-01 00:00:00.000000'::timestamp::int + 3600*24*FLOOR(RANDOM()*364)::int)::timestamp as dts1)
SELECT dts1, (dts1 + INTERVAL '1d') as dts2
FROM day;
```

## Hash Test Cluster Creation

```bash
roachprod create `whoami`-hash --gce-machine-type 'n1-standard-16' --nodes 9 --lifetime 48h
roachprod stage `whoami`-hash release v21.1.3
roachprod start `whoami`-hash
roachprod pgurl `whoami`-hash:1
roachprod adminurl `whoami`-hash:1
        http://glenn-hash-0001.roachprod.crdb.io:26258/

## -- configure driver machine
##
roachprod create `whoami`-drive -n 1 --lifetime 48h
roachprod stage `whoami`-drive release v21.1.3
roachprod ssh `whoami`-drive:1
sudo mv ./cockroach /usr/local/bin
sudo apt-get update -y
sudo apt install htop

sudo apt-get update -y
sudo apt-get install haproxy -y
cockroach gen haproxy --insecure   --host=10.142.0.100   --port=26257 
nohup haproxy -f haproxy.cfg &

## Setup Python if needed
sudo apt-get update
sudo apt install python3-pip -y
sudo pip3 install --upgrade pip
sudo apt-get install libpq-dev python-dev python3-psycopg2 python3-numpy -y


## Put Python Script on Driver
roachprod put glenn-drive:1 ./insert_ts_data.py
```

## RAW RESULTS

### Run #1 with Indexes on to join with PK on "measure" table

Limited to 1000 QPS but with the JOIN to PK it never reached the max and pegged CPU on Both!!

```json
Total Q1 idx Run : 22011
Total Q2 hashidx Run : 22446
Q1 idx QPS : 73.37
Q2 hashidx QPS : 74.82
Q1 idx respP90 : 0.36695337295532227
Q2 hashidx respP90 : 0.3399900197982788
```

### Run #2 with STORED/COVERED indexes using "measures2" table QPS limited

Throughput limited to 1000 QPS using COVERED indexes.  
Hash is able to keep up but uses more resources with higher response time.

```json
Total Q1 idx Run : 298534
Total Q2 hashidx Run : 295743
Q1 idx QPS : 995.1133333333333
Q2 hashidx QPS : 985.81
Q1 idx respP90 : 0.009857583045959475
Q2 hashidx respP90 : 0.01623578071594238
```

### Run #3 with STORED/COVERED and NO limiter... 18 threasd

No limiter 18 threads...

```json
Total Q1 idx Run : 676023
Total Q2 hashidx Run : 415418
Q1 idx QPS : 2253.41
Q2 hashidx QPS : 1384.7266666666667
Q1 idx respP90 : 0.010843276977539062
Q2 hashidx respP90 : 0.01953799724578857
```


### Run #4 with STORED/COVERED and NO limiter... 36 threads

No limiter 36 threads...

```json
Total Q1 idx Run : 1149313
Total Q2 hashidx Run : 461941
Q1 idx QPS : 3831.0433333333335
Q2 hashidx QPS : 1539.8033333333333
Q1 idx respP90 : 0.01582789421081543
Q2 hashidx respP90 : 0.035750627517700195
```

### Run #5 with JOIN /w PK... 18 threads... NO LIMIT

* JOIN with PK
* 18 threads NO LIMIT
* 1 DAY RANGE not ONE HOUR

```json
Total Q1 idx Run : 938
Total Q2 hashidx Run : 1061
Q1 idx QPS : 3.1266666666666665
Q2 hashidx QPS : 3.5366666666666666
Q1 idx respP90 : 10.545476341247559
Q2 hashidx respP90 : 8.666908025741577
```

### Run #6 with JOIN /w PK... 9 threads... NO LIMIT

* JOIN with PK
* 9 threads NO LIMIT
* 1 DAY RANGE not ONE HOUR

```json
Total Q1 idx Run : 709
Total Q2 hashidx Run : 1179
Q1 idx QPS : 2.3633333333333333
Q2 hashidx QPS : 3.93
Q1 idx respP90 : 5.0618795394897464
Q2 hashidx respP90 : 3.5239457130432132
```

### Run #7 with JOIN /w PK... 18 threads... Throttle... 1hr... 

* JOIN with PK
* 18 threads throttle 50 QPS
* 1 HOUR RANGE 

```json
Total Q1 idx Run : 15000
Total Q2 hashidx Run : 15012
Q1 idx QPS : 50.0
Q2 hashidx QPS : 50.04
Q1 idx respP90 : 0.2605083227157593
Q2 hashidx respP90 : 0.2714336395263672
```

### Run #8 with STORED noJOIN... 18 threads... Throttle... 1hr... 

* STORED/COVERED index
* 18 threads throttle 50 QPS
* 1 HOUR RANGE 

```json
Q1 idx QPS : 50.04
Q2 hashidx QPS : 50.04
Q1 idx respP90 : 0.01097574234008789
Q2 hashidx respP90 : 0.021715378761291503
```


```sql
explain SELECT m100, count(*), sum(v100)
FROM measure@idx_tshash
WHERE ts BETWEEN '2000-06-01 00:00:00.000000' and '2000-06-02 00:00:00.000000'
GROUP BY m100
ORDER BY 3 desc
LIMIT 10;