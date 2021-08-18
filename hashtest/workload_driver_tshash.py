# Workload Driver for testing HASH INDEX range scans
#
import psycopg2
import psycopg2.errorcodes
import threading
from threading import Thread
import time
import random
import numpy
import uuid
import math

usleep = lambda x: time.sleep(x/1000000.0)
msleep = lambda x: time.sleep(x/1000.0)

class dbstr:
    def __init__(self, database, user, host, port):
        self.database = database
        self.user = user
        # self.sslmode = sslmode
        self.host = host
        self.port = port

class ThreadWithReturnValue(Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}, Verbose=None):
        Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None
    def run(self):
        print(type(self._target))
        if self._target is not None:
            self._return = self._target(*self._args,
                                                **self._kwargs)
    def join(self, *args):
        Thread.join(self, *args)
        return self._return

def onestmt(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)

def getcon(dc):
    myconn = psycopg2.connect(
        # database=dc.database,
        # user=dc.user,
        # sslmode='disable',
        # port=dc.port,
        # host=dc.host
        "postgresql://root:@localhost:26257/hashtest?sslmode=disable"
    )
    return myconn

def boolDistro(dval):
    if dval >= random.random():
        return True
    else:
        return False

def getIds(mycon, idSQL):
    # Retrieve Valid Session IDs
    #
    with mycon:
        with mycon.cursor() as cur:
            cur.execute(idSQL)
            rows = cur.fetchall()
            # Create Data Frame for vaild Session IDs
            valid_ids = []
            for row in rows:
                valid_ids.append([str(cell) for cell in row])
    return valid_ids

def perform(fun, *args):
    return(fun(*args))

def q0(val):
    return ("SELECT PG_SLEEP({})".format(val))

def q1_ts(dateArray):
    qTemplate = """
        SELECT m100, count(*), sum(v100), min(ts), max(ts)
        FROM hashtest.measure@idx_ts
        WHERE ts BETWEEN '{}' and '{}'
        GROUP BY m100
        ORDER BY 3 desc
        LIMIT 10
    """
    idLen=len(dateArray)
    ridx=random.randint(0,(idLen-2))
    return (qTemplate.format(dateArray[ridx],dateArray[ridx+1]))

def q2_tshash(dateArray):
    qTemplate = """
        SELECT m100, count(*), sum(v100), min(ts), max(ts)
        FROM hashtest.measure@idx_tshash
        WHERE ts BETWEEN '{}' and '{}'
        GROUP BY m100
        ORDER BY 3 desc
        LIMIT 10
    """
    idLen=len(dateArray)
    ridx=random.randint(0,(idLen-2))
    return (qTemplate.format(dateArray[ridx],dateArray[ridx+1]))

def q99template_in(idArray):
    qTemplate = """
    SELECT *
    FROM test
    WHERE id IN ({}))
    """
    idLen=len(idArray)
    inarr = []

    ## Range for Values of IN clause 4,11
    for i in range(random.randint(4,11)):
        if i <  1:
            inarr = idArray[random.randint(0,(idLen-1))] 
        else:
            inarr = inarr + ',' + idArray[random.randint(0,(idLen-1))]        
    return (qTemplate.format(inarr))

def q99template_value(idArray):
    qTemplate = """
    SELECT '{}'
    """
    idLen=len(idArray)
    return (qTemplate.format(idArray[random.randint(0,(idLen-1))]))


def worker_steady(num, tpsPerThread, dbstr, runtime, qFunc, varray):
    """ingest worker:: Lookup valid session and then account"""
    print("Worker Steady State")

    # mycon = getcon(dbstr)
    mycon = psycopg2.connect(dbstr)
    mycon.set_session(autocommit=True)

    ## Configure Rate Limiter
    if tpsPerThread == 0:
        Limit=False
        arrivaleRateSec = 0
    else:
        Limit=True
        arrivaleRateSec = 1.0/tpsPerThread
    
    threadBeginTime = time.time()
    etime=threadBeginTime

    execute_count = 0
    resp = []

    ## Decorate the session with application name
    with mycon:
        with mycon.cursor() as cur:
            cur.execute("set application_name='hashtest';")

    with mycon:
        with mycon.cursor() as cur:
            while etime < (threadBeginTime + runtime):
                # begin time
                btime = time.time()

                # Run the query from qFunc
                cur.execute(qFunc(varray))
                execute_count += 1

                etime = time.time()
                resp.append(etime-btime)

                sleepTime = arrivaleRateSec - (etime - btime)

                if Limit and sleepTime > 0:
                    time.sleep(sleepTime)

            print("Worker_{}:  Queries={}, QPS={}, P90={}!!!".format(num, execute_count, (execute_count/(time.time()-threadBeginTime)), numpy.percentile(resp,90)))

    return (execute_count, resp)


## Main
##

# TODO make command-line options
#

connStr = "postgres://root@127.0.0.1:26257?sslmode=disable"
mycon = psycopg2.connect(connStr)


## Test Connection and getIds
idrows = getIds(mycon, "select generate_series(1,2)")
idarray = []
for row in idrows:
    print(row[0])
    with mycon.cursor() as cur:
        cur.execute(q0(row[0]))
        idarray.append(row[0])
print(idarray)

## Configure Array for Queries
##   One Second per Sample
##   Five Years of data
##
## one day in a year
# dtsRows = getIds(mycon, "SELECT ('2000-01-01 00:00:00.000000'::timestamp::int + 3600*24*i)::timestamp FROM generate_series(0,365) as i")

## One hour over five years  24*365*5 = 43800 hours in 5 years
## One hour over five years  24*365*20 = 175200 hours in 20 years
# dtsRows = getIds(mycon, "SELECT ('2000-01-01 00:00:00.000000'::timestamp::int + 3600*i)::timestamp FROM generate_series(0,43800) as i")
dtsRows = getIds(mycon, "SELECT ('2000-01-01 00:00:00.000000'::timestamp::int + 3600*i)::timestamp FROM generate_series(0,175200) as i")

## One day over five years  365*5 = 1825 days in 5 years
# dtsRows = getIds(mycon, "SELECT ('2000-01-01 00:00:00.000000'::timestamp::int + 3600*24*i)::timestamp FROM generate_series(0,1825) as i")

## Populate array to feed queries
dtsArray = []
for row in dtsRows:
    dtsArray.append(row[0])
# print(dtsArray)

## Threads Array
threads1 = []
threads2 = []

## Query Counters
tq1 = 0
tq2 = 0

## Response Time Distribution Array
tq1resp = []
tq2resp = []

## Runtime Parameters
runtime = 300
QPS = 1000
#numThreads = 18, 27, 36  ## multiple of number of nodes
numThreads = 18
qpsPerThread = QPS/numThreads

################################
## Launch Run on 1st test table
################################
for i in range(numThreads):
    t1 = ThreadWithReturnValue(target=worker_steady, args=((i+1),qpsPerThread,connStr,runtime, q1_ts, dtsArray))
    threads1.append(t1)
    t1.start()

## Wait for threads to finish
for x in threads1:
    qc, ra = x.join()
    tq1 = tq1 + qc
    tq1resp.extend(ra)

time.sleep(30)

################################
## Launch Run on 2nd test table
################################
for i in range(numThreads):
    t2 = ThreadWithReturnValue(target=worker_steady, args=((i+1),qpsPerThread,connStr,runtime, q2_tshash, dtsArray))
    threads2.append(t2)
    t2.start()

## Wait for threads to finish
for x in threads2:
    qc, ra = x.join()
    tq2 = tq2 + qc
    tq2resp.extend(ra)


## Print results
print("Total Q1 idx Run : {}".format(tq1))
print("Total Q2 hashidx Run : {}".format(tq2))

print("Q1 idx QPS : {}".format(tq1/runtime))
print("Q2 hashidx QPS : {}".format(tq2/runtime))

print("Q1 idx respP90 : {}".format(numpy.percentile(tq1resp,90)))
print("Q2 hashidx respP90 : {}".format(numpy.percentile(tq2resp,90)))

time.sleep(1)

exit()