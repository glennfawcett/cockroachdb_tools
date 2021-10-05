# Bulk Update Batching with CockroachDB
Bulk operations are not ideal with CockroachDB.  Creating a copy of data before performing a bulk update is a common practice for database operations.  This allows you to revert changes if things are not working as expected.

## Stash Table with CTE Update

```sql
CREATE TABLE mytable (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    c1 STRING DEFAULT 
)



root@localhost:26257/hashtest> 

explain select count(*) from measure2 where ts >'2019-12-25';
                                                                                         info
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  distribution: full
  vectorized: true

  • group (scalar)
  │ estimated row count: 1
  │
  └── • scan
        estimated row count: 155,894 (0.02% of the table; stats collected 2 days ago)
        table: measure2@idx_tshash
        spans: [/0/'2019-12-25 00:00:00.000001' - /0] [/1/'2019-12-25 00:00:00.000001' - /1] [/2/'2019-12-25 00:00:00.000001' - /2] [/3/'2019-12-25 00:00:00.000001' - /3] … (5 more)

root@localhost:26257/hashtest> select count(*) from measure2 where ts >'2019-12-25';
  count
----------
  172799

-- ORIGINAL
update measure2 set m100='42' where ts >'2019-12-26' limit 100 returning (id);

-- NEW

-- m100 is NOT '44' initially for this range
--
CREATE TABLE backup_measure2 (
    id UUID PRIMARY KEY,
    _m100 STRING, 
    done BOOLEAN DEFAULT FALSE
);

INSERT INTO backup_measure2(id, _m100) 
SELECT id, m100 FROM measure2 
WHERE measure2.ts > '2019-12-26';

WITH upd as (
UPDATE measure2 SET m100='44'
FROM backup_measure2
WHERE measure2.id = backup_measure2.id AND
      backup_measure2.done is FALSE
    LIMIT 100
RETURNING measure2.id
)
UPDATE backup_measure2
SET done = TRUE
WHERE backup_measure2.id in (SELECT id from upd);


-- Batch Insert update set
--
CREATE TABLE backup_measure2 (
    id UUID PRIMARY KEY,
    _m100 STRING,
    ts TIMESTAMP, 
    done BOOLEAN DEFAULT FALSE, 
    INDEX tsidx (ts)
);

-- Run until INSERT 0
--
UPSERT INTO backup_measure2(id, _m100, ts) 
SELECT measure2.id, measure2.m100, measure2.ts 
FROM measure2 
LEFT OUTER JOIN backup_measure2 on (backup_measure2.id=measure2.id)
WHERE backup_measure2.id is NULL and
      measure2.ts >= '2019-12-26'
LIMIT 1000;

ANALYZE backup_measure22;

-- Run until UPDATE 0
--
WITH upd as (
UPDATE measure2 SET m100='44'
FROM backup_measure2
WHERE measure2.id = backup_measure2.id AND
      backup_measure2.done is FALSE
LIMIT 1000
RETURNING measure2.id
)
UPDATE backup_measure2
SET done = TRUE
WHERE backup_measure2.id in (SELECT id from upd);

-- Check to see if done
-- 
SELECT done, count(*)
FROM backup_measure2
GROUP by 1;

-- EXAMPLE PART WAY
--
root@localhost:26257/hashtest> SELECT done, count(*)
FROM backup_measure2
GROUP by 1;
  done  | count
--------+--------
  true  | 50000
  false | 36400
