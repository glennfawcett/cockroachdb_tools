import logging
import argparse
import sys
from os import path as osPath, makedirs, rename, listdir, rmdir, remove, environ, getcwd
import os
import json
import sys
import requests
import psycopg2
import psycopg2.errorcodes
import datetime
import threading
from threading import Thread
import time


__appname__ = 'bulk_update_by'
__version__ = '0.5.0'
__authors__ = ['Glenn Fawcett']
__credits__ = ['Cockroach Toolbox']

# Globals and Constants
################################
class dbstr:
    def __init__(self, database, user, sslmode, host, port):
        self.database = database
        self.user = user
        self.sslmode = sslmode
        self.host = host
        self.port = port

class G:
    """Globals and Constants"""
    LOG_DIRECTORY_NAME = 'logs'
    threads = 16
    deleteBatch = 1000
    cleanupInterval = '10hr'

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

# Helper Functions
################################

def makeDirIfNeeded(d):
    """ create if doesn't exhist """
    if not osPath.exists(d):
        makedirs(d)
        logging.debug('Create needed directory at: {}'.format(d))

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

def prepDelRuntime(conn, pkType): #
#
    onestmt(conn, "DROP TABLE IF EXISTS delthreads")
    onestmt(conn, "DROP TABLE IF EXISTS delruntime")

    if pkType == 'UUID':
        crDelThreadsSQL = """
        CREATE TABLE IF NOT EXISTS delthreads (
            id INT PRIMARY KEY,
            bval UUID,
            eval UUID
        );
        """
    else:
        crDelThreadsSQL = """
        CREATE TABLE IF NOT EXISTS delthreads (
            id INT PRIMARY KEY,
            bval INT,
            eval INT
        );
        """

    if pkType == 'UUID':    
        crDelRuntimeSQL = """
        CREATE TABLE IF NOT EXISTS delruntime (
            id INT, 
            ts TIMESTAMP DEFAULT now(),
            lastval UUID,
            rowsdeleted INT,
            delpersec INT,
            PRIMARY KEY (id, ts)
        );
        """ 
    else:
        crDelRuntimeSQL = """
        CREATE TABLE IF NOT EXISTS delruntime (
            id INT, 
            ts TIMESTAMP DEFAULT now(),
            lastval INT,
            rowsdeleted INT,
            delpersec INT,
            PRIMARY KEY (id, ts)
        );
        """ 

    onestmt(conn, "DROP TABLE IF EXISTS delthreads")
    onestmt(conn, "DROP TABLE IF EXISTS delruntime")   
    onestmt(conn, crDelThreadsSQL)
    onestmt(conn, crDelRuntimeSQL)

    return

def getLatestHistoINT(mycon, tableToTrim, _pkName):
    # 
    statSQL = """
    select histogram_id 
    from [show statistics for table {}] 
    where column_names = '{}' and histogram_id is NOT NULL
    order by created DESC LIMIT 1;
    """
    bucketSQL = """
    select upper_bound 
    from [show histogram {}];
    """

    # Fix pkName for Statistics table
    pkName = '{' + _pkName + '}'

    # Retrieve Valid Session IDs
    #
    with mycon:
        with mycon.cursor() as cur:
            cur.execute(statSQL.format(tableToTrim, pkName))
            rows = cur.fetchall()
            for row in rows:
                histoID = [str(cell) for cell in row]
                # print(histoID[0])

            # Create Data Frame for Histograms
            cur.execute(bucketSQL.format(histoID[0]))
            rows = cur.fetchall()
            valid_histogram = []
            for row in rows:
                valid_histogram.append([str(cell) for cell in row])
            # Set the Begin/End values for the histograms of INT
            valid_histogram[0][0] = "0"
            valid_histogram[len(valid_histogram)-1][0] = "9223372036854775807"

    return valid_histogram

def getLatestHistoUUID(mycon, tableToTrim, _pkName):
    # 
    statSQL = """
    select histogram_id 
    from [show statistics for table {}] 
    where column_names = '{}' and histogram_id is NOT NULL
    order by created DESC LIMIT 1;
    """
    bucketSQL = """
    select upper_bound 
    from [show histogram {}];
    """

    # Fix pkName for Statistics table
    pkName = '{' + _pkName + '}'

    # Retrieve Valid Session IDs
    #
    with mycon:
        with mycon.cursor() as cur:
            cur.execute(statSQL.format(tableToTrim, pkName))
            rows = cur.fetchall()
            for row in rows:
                histoID = [str(cell) for cell in row]
                # print(histoID[0])

            # Create Data Frame for Histograms
            cur.execute(bucketSQL.format(histoID[0]))
            rows = cur.fetchall()
            valid_histogram = []
            for row in rows:
                valid_histogram.append([str(cell) for cell in row])
            
            # Set the Begin/End values for the histograms
            valid_histogram[0][0] = "'00000000-0000-0000-0000-000000000000'"
            valid_histogram[len(valid_histogram)-1][0] = "'ffffffff-ffff-ffff-ffff-ffffffffffff'"

    return valid_histogram

def rptRowsToDelete(mycon, rptSQL):
    #
    are_there_stale_rows = False

    with mycon:
        with mycon.cursor() as cur:
            cur.execute(rptSQL)
            rows = cur.fetchall()
            for row in rows:
                print("{} : {}".format(row[0], row[1]))
                if row[0] == 'stale' : are_there_stale_rows = True

    return are_there_stale_rows

def activeAdjust(mycon, targetDeleteRPS, batchSize, targetArrivalRatePerThread):
    #
    sampleRpsSQL = """
    SELECT 
    CASE  
        WHEN sum(rowsdeleted)::float/extract(epoch from max(ts)-min(ts)) is NULL THEN 0
        ELSE sum(rowsdeleted)::float/extract(epoch from max(ts)-min(ts))
    END as rps, 
    count(distinct id) as active_threads
    from delruntime 
    where id > -1 and
    ts between (now() - INTERVAL '1m10s') and (now() - INTERVAL '10s');
    """
    with mycon:
        with mycon.cursor() as cur:
            cur.execute(sampleRpsSQL)
            rows = cur.fetchall()
            for row in rows:
                # print("     RPS (lastMinute) : {}".format(row[0]))
                # print("       active_threads : {}".format(row[1]))
                if int(row[0])/targetDeleteRPS < 0.95:
                    diffRatio = int(row[0])*0.8/targetDeleteRPS
                else: 
                    diffRatio = 0.99

                targetArrivalRatePerThread[0] = ((batchSize)*(int(row[1]))*diffRatio)/(targetDeleteRPS)

    return targetArrivalRatePerThread[0]

def rptFinal(mycon):
    #
    rptSQL = """
    select 
        min(ts) as begin_ts, 
        max(ts) as done_ts,
        extract(epoch from max(ts)-min(ts)) as runtime_sec, 
        sum(rowsdeleted) as total_rows_deleted,
        round(sum(rowsdeleted)::float/extract(epoch from max(ts)-min(ts))) as rows_deleted_per_second
    from delruntime;   
    """
    print("\n--------------------------------------------")
    print("--- Final Report")
    print("--------------------------------------------")
    with mycon:
        with mycon.cursor() as cur:
            cur.execute(rptSQL)
            rows = cur.fetchall()
            for row in rows:
                print("     BeginDeleteProcess : {}".format(row[0]))
                print("       EndDeleteProcess : {}".format(row[1]))
                print("            runtime_sec : {}".format(row[2]))
                print("     total_rows_deleted : {}".format(row[3]))
                print("rows_deleted_per_second : {}".format(row[4]))
    return 

def populateThreadsTable(mycon, vhist):
    # 
    threadsDataStruct = []
    popthreadsSQL = """
    INSERT INTO delthreads
    VALUES ({},{},{});
    """
    # Retrieve Valid Session IDs
    #
    for i in range(len(vhist)-1):
        if i < len(vhist)-1:
            onestmt(mycon, popthreadsSQL.format(i, vhist[i][0], vhist[i+1][0]))
            # print("{}, {}, {}".format(i, vhist[i][0], vhist[i+1][0]))
            threadsDataStruct.append([i, vhist[i][0], vhist[i+1][0]])

    return threadsDataStruct    

def split(a, n):
    # Split a data structure
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))

def worker_steady(num, dbstr, bkey, ekey, trimTable, delete_timestamp_barrier, pkType, pkName, batchSize, rpsPerThread, arrivalRateSec):
    """Update Worker Thread"""
    # print("Delete Thread : {}".format(num))

    mycon = getcon(dbstr)
    mycon.set_session(autocommit=True)

    deleteSQL = """
    WITH update_thread as (
        UPDATE {}
        SET created_at = now()
        WHERE             
            {} BETWEEN {} AND {}
            --AND created_at between '2021-11-08' AND '2021-11-11'
        LIMIT {}
        RETURNING ({})
    )
    INSERT INTO delruntime ({}, lastval, rowsdeleted)
    SELECT {}, max({}), count(*) FROM update_thread
    RETURNING lastval, rowsdeleted;
    """

    delnum = -1

    # Configure Rate Limiter
    if rpsPerThread == 0:
        Limit=False
        # arrivalRateSec = 0
    else:
        Limit=True
        # arrivalRateSec = batchSize/(rpsPerThread)
    
    threadBeginTime = time.time()
    etime=threadBeginTime

    execute_count = 0
    resp = []

    while delnum != 0:
        with mycon:
            with mycon.cursor() as cur:
                # print(deleteSQL.format(bkey, ekey, num))

                # begin time
                btime = time.time()

                cur.execute(deleteSQL.format(trimTable, pkName, bkey, ekey, batchSize, pkName, pkName, num, pkName))
                rows = cur.fetchall()

                # end time
                execute_count += 1
                etime = time.time()
                resp.append(etime-btime)

                sleepTime = arrivalRateSec[0] - (etime - btime)

                if Limit and sleepTime > 0.01:
                    time.sleep(sleepTime)

                for row in rows:
                    # print("{} : {}".format(row[0], row[1]))
                    delnum = row[1]
                    if delnum == 0:
                        return
                    if pkType == 'UUID':
                        bkey = "'"+ row[0] + "'"
                    else:
                        bkey = row[0]
                    # print("bkey : {}, delnum : {}".format(bkey, delnum))

    # print("Worker: {} Finished!!!".format(num))


####
#### MAIN
####
def getArgs():
    """ Get command line args """
    desc = 'Trim table by PK for CockroachDB based on MVCC timestamp'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("-v", "--verbose", action="store_true", default=False, help='Verbose logging')
    parser.add_argument("-H", "--host", dest='host', default='localhost', help='Host AdminUI')
    parser.add_argument("-d", "--db", dest='database', default='defaultdb', help='Database Name')
    parser.add_argument("-p", "--dbport", dest='dbport', default='26257', help='Database Port')
    parser.add_argument("-U", "--user", dest='user', default='root', help='Datbase User')
    parser.add_argument("-u", "--url", dest='url', default='', help='example... postgres://username@clustername.gcp-us-central1.com:26257/defaultdb?sslmode=verify-full&sslrootcert=~/certs/ca.crt')
    parser.add_argument("-b", "--batch", dest='batch', default=100, help='Number Rows to delete per Batch')
    parser.add_argument("-t", "--table", dest='table', default='bigfast', type=str, required=True, help='Name of Table to Trim')
    parser.add_argument("-k", "--pktype", dest='pktype', default='UUID', type=str, choices=['UUID', 'INT'], required=True, help='Primary Key Type')
    parser.add_argument("-i", "--pkname", dest='pkname', default='id', type=str,  required=True, help='Primary Column Name')
    parser.add_argument("-s", "--rps", dest='rps', default=0, type=int, help='Target Delete RPS throttle... 0 disables')
    parser.add_argument("-z", "--barrierTimestamp", dest='tsb', default='2021-09-28 18:30:00.000000', type=str,  required=True, help='TimeStamp Barrier... 2021-09-28 18:30:00.000000')
    parser.add_argument("-c", "--noCheckBarrier", dest='nocheck', action="store_true", default=False, help='Disable Precheck Rows Timestamp Barrier')

    options = parser.parse_args()
    return options

def main():

    # get command line args
    options = getArgs()

    # Security... 
    # postgresql://host1:123,host2:456/somedb?target_session_attrs=any&application_name=myapp
    # connStr = “postgresql://root@127.0.0.1:26257/defaultdb?sslmode=require&sslrootcert=/home/glenn/crdb/ca.crt&sslcert=/home/glenn/crdb/client.root.crt&sslkey=/glenn/crdb/delivery/client.root.key”
    # mycon = psycopg2.connect(connStr)

    # DB connect string
    dbconnstr = dbstr(options.database, options.user, "disable", options.host, options.dbport)

    # Connect to the cluster.
    conn = getcon(dbconnstr)
    conn.set_session(autocommit=True)

    # Table to Trim
    trimTable = options.table 
    # PKType "UUID" and "INT" are the only supported at this time
    pkType = options.pktype
    # PK for trimTable... UUID only for first version... will do INT as well
    trimPK = options.pkname
    # TargetDeleteRPS... 0 is unlimited
    targetDeleteRPS = int(options.rps)
    # BatchSize = 10, 20, 40, 100, 1000, 2000, 10000
    BatchSize = int(options.batch)
    # timestamp barrier
    delete_timestamp_barrier = options.tsb

    # Prepare Delete Tables for tracking
    prepDelRuntime(conn, pkType)

    # Delete Report Before
    delRptSQL = """
    WITH dstate as (
        SELECT 
            CASE WHEN (crdb_internal_mvcc_timestamp/10^9)::int::timestamptz < '{}'::timestamptz 
                    THEN 'stale'
                ELSE 'current'
            END as data_state
    FROM {}
    )
    SELECT data_state, count(*)
    FROM dstate
    GROUP BY 1;
    """
    print("-------------------------------------------------------------------------------------------")
    print("--- UPDATE Rows in table '{}' ".format(trimTable))
    print("---      BatchSize: {} rows per thread".format(BatchSize))
    print("---      Target Update rowsPerSec: {}".format(targetDeleteRPS if targetDeleteRPS > 0 else "UNLIMITED"))
    print("-------------------------------------------------------------------------------------------")

    # Check Rows To Be deleted ahead of time unless disabled
    if options.nocheck is False: 
        if rptRowsToDelete(conn, delRptSQL.format(delete_timestamp_barrier, trimTable)) is False:
            print("No rows to Delete in table '{}' < '{}'".format(trimTable, delete_timestamp_barrier))
            exit()

    # Find Histograms for Table to Trim for UUID PK
    if pkType == 'UUID':
        histo = getLatestHistoUUID(conn, trimTable, trimPK)
    elif pkType == 'INT':
        histo = getLatestHistoINT(conn, trimTable, trimPK)
    else:
        print("Invalid PK type... only UUID and INT are supported")
        exit()

    # print("{} : {}".format(histo[0][0], histo[1][0]))
    time.sleep(5)
    # exit()
    
    # Extract threads structure and populate table
    threadsDataStruct = populateThreadsTable(conn, histo)
    rpsPerThread = (targetDeleteRPS/len(threadsDataStruct))

    # Configure initial rate to be adjusted dynamically after startup
    targetArrivalRatePerThread = []
    if rpsPerThread == 0: 
        targetArrivalRatePerThread.append(0)
    else: 
        targetArrivalRatePerThread.append(BatchSize/rpsPerThread)

    # Start delete threads
    conn.close()
    threads = []
    for i in range(len(threadsDataStruct)):
        bkey = threadsDataStruct[i][1]
        ekey = threadsDataStruct[i][2]
        # print("i: {}, b: {}, e: {}".format(i, bkey, ekey))
        t = threading.Thread(target=worker_steady, args=(i, dbconnstr, bkey, ekey, trimTable, delete_timestamp_barrier, pkType, trimPK, BatchSize, rpsPerThread, targetArrivalRatePerThread))
        threads.append(t)
        t.start()

    print("\n{} Threads Spawned".format(len(threadsDataStruct)))

    time.sleep(2)

    # Connect to the cluster
    conn = getcon(dbconnstr)
    conn.set_session(autocommit=True)

    while True: 
        if targetDeleteRPS == 0:  
            break
        arrRate = activeAdjust(conn, targetDeleteRPS, BatchSize, targetArrivalRatePerThread)
        # print("ArrivalRate : {}".format(arrRate))
        if arrRate == 0:
            break
        time.sleep(1)

    # Wait for all of them to finish
    for x in threads:
        x.join()

    time.sleep(2)

    # Connect to the cluster
    conn = getcon(dbconnstr)
    conn.set_session(autocommit=True)

    # Delete Report After
    rptFinal(conn)


#  Run Main
####################################
if __name__ == '__main__':
    main()