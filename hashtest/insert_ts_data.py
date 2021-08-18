# Import the driver.
import psycopg2
import psycopg2.errorcodes
import threading
import time
import random
from datetime import datetime


usleep = lambda x: time.sleep(x/1000000.0)
msleep = lambda x: time.sleep(x/1000.0)

class dbstr:
    def __init__(self, database, user, host, port):
        self.database = database
        self.user = user
        # self.sslmode = sslmode
        self.host = host
        self.port = port

def onestmt(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)

def getcon(dc):
    myconn = psycopg2.connect(
        database=dc.database,
        user=dc.user,
        sslmode='disable',
        port=dc.port,
        host=dc.host
    )
    return myconn

def makeBatch(beginEpochTime, bsize):
    s = ''
    for v in range(bsize):
        if v == bsize-1:
            s = s + "('" + getTStr(beginEpochTime+v) + "')"
        else:
            s = s + "('" + getTStr(beginEpochTime+v) + "'),"
    return s

def getTStr(epoch):
    datetime.utcnow()
    date_time = datetime.fromtimestamp(epoch)
    return date_time.strftime("%Y-%m-%d %H:%M:%S.%f")

def worker_steady(num, beginEpochOffset, insPerThread, bsize, targetTable, dbstr):
    """ingest worker:: Lookup valid session and then account"""
    print("Worker Steady State")

    mycon = getcon(dbstr)
    mycon.set_session(autocommit=True)

    # Insert into Table with AutoPK
    #
    iTemplate = """
    INSERT INTO {}(ts)
    VALUES {}
    """

    with mycon:
        with mycon.cursor() as cur:
            # for r in range(insPerThread): 
            for i in range(beginEpochOffset,insPerThread+beginEpochOffset,bsize):
                # print(iTemplate.format(targetTable, getTStr(btimestamp+r)))
                # cur.execute(iTemplate.format(targetTable, getTStr(beginEpochOffset+r)))
                # cur.execute(iTemplate.format(targetTable, makeBatch(beginEpochOffset+i,bsize)))
                # print(i)
                # print(beginEpochOffset)
                # print(iTemplate.format(targetTable, makeBatch(i,bsize)))
                cur.execute(iTemplate.format(targetTable, makeBatch(i,bsize)))


            
        print("Worker: {} Finished!!!".format(num))


## Main
##

# year 2000 in UTC
#
timestamp = 946684800.000000
# datetime.utcnow()
# date_time = datetime.fromtimestamp(timestamp)
# d = date_time.strftime("%Y-%m-%d %H:%M:%S.%f")
print(getTStr(timestamp))
print(getTStr(timestamp+1000))

print(makeBatch(timestamp,10))

# exit()

# TODO make command-line options
#
create_ddl = """
CREATE DATABASE hashtest;
USE hashtest;
SET experimental_enable_hash_sharded_indexes=true;

--CREATE TABLE measure (
--    id UUID DEFAULT gen_random_uuid(),
--    ts TIMESTAMP NOT NULL,
--    txtblob TEXT DEFAULT LPAD('',1000,'Z'),
--    m100 TEXT DEFAULT FLOOR(RANDOM()*100)::TEXT,
--    v100 INT as (m100::INT) STORED,
--    PRIMARY KEY (id),
--    INDEX idx_ts (ts ASC),
--    INDEX idx_tshash (ts ASC) USING HASH WITH BUCKET_COUNT = 9
--);
--ALTER TABLE measure SPLIT AT select gen_random_uuid() from generate_series(1,9);
--ALTER INDEX measure@idx_ts SPLIT AT SELECT ('2000-01-01 00:00:00.000000'::timestamp::int + 3600*24*365*i)::timestamp
--  FROM generate_series(1,5) as i;

CREATE TABLE measure2 (
    id UUID DEFAULT gen_random_uuid(),
    ts TIMESTAMP NOT NULL,
    txtblob TEXT DEFAULT LPAD('',1000,'Z'),
    m100 TEXT DEFAULT FLOOR(RANDOM()*100)::TEXT,
    v100 INT as (m100::INT) STORED,
    PRIMARY KEY (id),
    -- INDEX idx_ts (ts ASC) STORING (m100, v100),
    INDEX idx_tshash (ts ASC) USING HASH WITH BUCKET_COUNT = 9 STORING (m100, v100)
);

ALTER TABLE measure2 SPLIT AT select gen_random_uuid() from generate_series(1,18);

--ALTER INDEX measure2@idx_ts SPLIT AT SELECT ('2000-01-01 00:00:00.000000'::timestamp::int + 3600*24*365*i)::timestamp
--  FROM generate_series(1,5) as i;
"""
btimestamp = 946684800.000000
# etimestamp = 365*3600*24 + btimestamp

numThreads = 36
days = 365*20
bsize = 20
insPerThread = int(days*3600*24/numThreads)

threads = []

# Define Host:Port / Injection endpoints
#
dbcons = []
dbcons.append(dbstr("hashtest", "root", "127.0.0.1", 26257))

#test connection
mycon = getcon(dbstr("defaultdb", "root", "127.0.0.1", 26257))
mycon.set_session(autocommit=True)
with mycon:
    with mycon.cursor() as cur:
        cur.execute('Select 1')
        cur.execute(create_ddl)

# exit()


# Define Tables to Test
#
tables = []
tables.append('measure2')
# tables.append('measure')



for tab in tables:
    print ("Inserting to table: {}".format(tab))
    for c in dbcons:
        for i in range(numThreads):
            t = threading.Thread(target=worker_steady, args=((i+1), int(btimestamp+i*insPerThread), insPerThread, bsize, tab, c))
            threads.append(t)
            t.start()

    # Wait for all of them to finish
    for x in threads:
        x.join()
        
    time.sleep(60)

exit()