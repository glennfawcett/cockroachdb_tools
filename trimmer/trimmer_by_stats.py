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
import time


__appname__ = 'trimmer_by_stats'
__version__ = '0.5.0'
__authors__ = ['Glenn Fawcett']
__credits__ = ['Cockroach Toolbox']

# Globals and Constants
################################
class dbstr:
  def __init__(self, database, user, host, port):
    self.database = database
    self.user = user
    # self.sslmode = sslmode
    self.host = host
    self.port = port

class G:
    """Globals and Constants"""
    LOG_DIRECTORY_NAME = 'logs'
    threads = 16
    deleteBatch = 1000
    cleanupInterval = '10hr'

# Helper Functions
################################

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

def prepDelRuntime(conn): #
#
    crDelThreadsSQL = """
    CREATE TABLE IF NOT EXISTS delthreads (
        id INT PRIMARY KEY,
        bval UUID,
        eval UUID
    );
    """

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

    onestmt(conn, crDelThreadsSQL)
    onestmt(conn, crDelRuntimeSQL)
    onestmt(conn, "TRUNCATE TABLE delthreads")
    onestmt(conn, "TRUNCATE TABLE delruntime")

    return

def getLatestHistoUUID(mycon, tableToTrim, colNames):
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
    # Retrieve Valid Session IDs
    #
    with mycon:
        with mycon.cursor() as cur:
            cur.execute(statSQL.format(tableToTrim, colNames))
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
    with mycon:
        with mycon.cursor() as cur:
            cur.execute(rptSQL)
            rows = cur.fetchall()
            valid_histogram = []
            for row in rows:
                print("{} : {} count".format(row[0], row[1]))
    return 

def rptFinal(mycon):
    #
    rptSQL = """
    select 
        min(ts) as begin_ts, 
        max(ts) as done_ts,
        extract(seconds from max(ts)-min(ts)) as runtime_sec, 
        round(sum(rowsdeleted)::float/extract(seconds from max(ts)-min(ts))) as rows_deleted_per_second
    from delruntime as of system time follower_read_timestamp();   
    """
    print("\n--------------------------------------------")
    print("--- Final Report")
    print("--------------------------------------------")
    with mycon:
        with mycon.cursor() as cur:
            cur.execute(rptSQL)
            rows = cur.fetchall()
            valid_histogram = []
            for row in rows:
                print("         BeginTimestamp : {}".format(row[0]))
                print("           EndTimestamp : {}".format(row[1]))
                print("            runtime_sec : {}".format(row[2]))
                print("rows_deleted_per_second : {}".format(row[3]))
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

def getDashers(mycon, cleanupTS):
    # 
    dasherSQL = """
    SELECT dasher_id 
    FROM dasher
    WHERE oldest_server_timestamp < '{}';
    """
    # Retrieve Valid Session IDs
    #
    with mycon:
        with mycon.cursor() as cur:
            cur.execute(dasherSQL.format(cleanupTS[0]))
            rows = cur.fetchall()
            # Create Data Frame for vaild Session IDs
            valid_dashers = []
            for row in rows:
                valid_dashers.append([str(cell) for cell in row])

    return valid_dashers

def minDasherServerTS(mycon, myid):
    # Return min(server_timestamp) for a particuliar id
    #
    minSQL = """
    select min(server_timestamp) 
    from dasher_location as of system time follower_read_timestamp() 
    where dasher_id={}
    """

    # Retrieve Valid Session IDs
    #
    with mycon:
        with mycon.cursor() as cur:
            cur.execute(minSQL.format(myid))
            rows = cur.fetchall()
    return rows[0]

def findPastTS(mycon, timerange):
    # Return TS cleanup timeframe 
    #
    oldestTS = findOldestDasherTimestamp(mycon)

    pastSQL = """
    select '{}'::TIMESTAMPTZ + INTERVAL '{}';
    """

    # Retrieve Valid Session IDs
    #
    with mycon:
        with mycon.cursor() as cur:
            cur.execute(pastSQL.format(oldestTS[0], timerange))
            rows = cur.fetchall()
    return rows[0]

def findOldestDasherTimestamp(mycon):
    # Return TS cleanup timeframe 
    #
    oldestSQL = """
    SELECT MIN(oldest_server_timestamp) from dasher;
    """

    # Retrieve Valid Session IDs
    #
    with mycon:
        with mycon.cursor() as cur:
            cur.execute(oldestSQL)
            rows = cur.fetchall()
    return rows[0]

def insertTestRecord(mycon, limit):
    # Returning test_id
    #
    startTestSQL = """
    INSERT INTO trimmer_tests (test_id, limit_size) 
    SELECT MAX(test_id)+1, {} 
    FROM trimmer_tests
    RETURNING test_id
    """

    # Insert test record and return test_id
    #
    with mycon:
        with mycon.cursor() as cur:
            cur.execute(startTestSQL.format(limit))
            rows = cur.fetchall()
    return rows[0][0]
    

def deleteByDasherID(dasherID, dTS):
    # Delete all rows from Dasher < dTS
    #
    deleteLimitedSQL = """
    DELETE FROM dasher_location
    WHERE dasher_id = {} AND server_timestamp < '{}'
    ORDER BY dasher_id ASC, server_timestamp ASC
    LIMIT {};
    """

    updateOldestSQL = """
    UPDATE dasher 
    SET oldest_server_timestamp = (SELECT min(server_timestamp) FROM dasher_location WHERE dasher_id = {})
    WHERE dasher_id = {};
    """

    # deleteBatch = 20.... G.deleteBatch

    mycon = getcon(dbstr("defaultdb", "root", "localhost", 26257))
    mycon.set_session(autocommit=True)
    
    totalDeleted = 0

    with mycon:
        with mycon.cursor() as cur:
            while True:
                cur.execute(deleteLimitedSQL.format(dasherID, dTS, G.deleteBatch))
                rowsDeleted = cur.rowcount
                totalDeleted += rowsDeleted

                # print("Rows Deleted: {}".format(rowsDeleted))
                if rowsDeleted == 0:
                    print("Total Rows Deleted for dasher_id {} :: {}".format(dasherID, totalDeleted))
                    onestmt(mycon, updateOldestSQL.format(dasherID, dasherID))
                    return
    return 

def split(a, n):
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))

def worker_steady(num, dbstr, bkey, ekey):
    """Delete Worker Thread"""
    # print("Delete Thread : {}".format(num))

    mycon = getcon(dbstr)
    mycon.set_session(autocommit=True)

    deleteSQL = """
    WITH delthread as (
        DELETE
        FROM u
        WHERE 
            id BETWEEN {} AND {}
            AND delboolean is true
        LIMIT 10
        RETURNING (id)
    )
    INSERT INTO delruntime (id, lastval, rowsdeleted)
    SELECT {}, max(id), count(*) from delthread
    RETURNING lastval, rowsdeleted;
    """

    delnum = -1

    while delnum != 0:
        with mycon:
            with mycon.cursor() as cur:
                # print(deleteSQL.format(bkey, ekey, num))
                cur.execute(deleteSQL.format(bkey, ekey, num))
                rows = cur.fetchall()
                for row in rows:
                    # print("{} : {}".format(row[0], row[1]))
                    delnum = row[1]
                    if delnum == 0:
                        return
                    bkey = "'"+ row[0] + "'"

                    # print("bkey : {}, delnum : {}".format(bkey, delnum))

    # print("Worker: {} Finished!!!".format(num))


####
#### MAIN
####
# def getArgs():
#     """ Get command line args """
#     desc = 'Trim table by PK for CockroachDB'
#     parser = argparse.ArgumentParser(description=desc)
#     parser.add_argument("-v", "--verbose", action="store_true", default=False, help='Verbose logging')
#     parser.add_argument("-o", "--console-log", dest='console_log', default=True, help='send log to console and files')
#     parser.add_argument("-z", "--logdir", dest='log_dir', default=True, help='send log to console and files')
#     parser.add_argument("-l", "--host", dest='host', default='glenn-bpf-0001.roachprod.crdb.io', help='Host AdminUI')
#     parser.add_argument("-d", "--db", dest='database', default='defaultdb', help='Database Name')
#     parser.add_argument("-r", "--adminport", dest='adminport', default='26258', help='AdminUI Port')
#     parser.add_argument("-p", "--dbport", dest='dbport', default='26257', help='Database Port')
#     parser.add_argument("-u", "--user", dest='user', default='root', help='Datbase User')
#     parser.add_argument("-n", "--numtop", dest='numtop', default=10, help='Number of Top Ranges to Display')
#     options = parser.parse_args()
#     return options

def main():

    # # get command line args
    # options = getArgs()
    # config = {}
    # config['log_dir'] = os.getcwd()
 
    # # logging config
    # base_logging_path = osPath.join(config['log_dir'], G.LOG_DIRECTORY_NAME, __appname__)
    # makeDirIfNeeded(base_logging_path)
    # general_logging_path = osPath.join(base_logging_path, 'GENERAL')
    # makeDirIfNeeded(general_logging_path)
    # formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    # level = logging.DEBUG if options.verbose else logging.INFO
    # handler = logging.FileHandler(osPath.join(general_logging_path, 'general_{}.log'.format(datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3])))
    # handler.setFormatter(formatter)

    # if options.console_log:
    #     console_handler = logging.StreamHandler()
    #     console_handler.setFormatter(formatter)

    # DB connect string
    dbconnstr = dbstr("defaultdb", "root", "localhost", 26257)

    # Connect to the cluster.
    conn = getcon(dbconnstr)
    conn.set_session(autocommit=True)

    # Delete Report Before
    delRptSQL = """
    select delboolean, count(*)
    from u
    group by 1;
    """
    print("--------------------------------------------")
    print("--- Table to TRIM Before")
    print("--------------------------------------------")
    rptRowsToDelete(conn, delRptSQL)

    # Prepare Delete Tables for tracking
    prepDelRuntime(conn)
    # Table to Trim
    trimTable = "u"
    # PK for trimTable... UUID only for first version... will do INT as well
    trimPK = "{id}"

    # Find Histograms for Table to Trim for UUID PK
    histo = getLatestHistoUUID(conn, trimTable, trimPK)
    
    # Extract threads structure and populate table
    threadsDataStruct = []
    threadsDataStruct = populateThreadsTable(conn, histo)


    # Start delete threads
    conn.close()
    threads = []
    print("\nThreads to Spawn: {}".format(len(threadsDataStruct)))
    for i in range(len(threadsDataStruct)):
        bkey = threadsDataStruct[i][1]
        ekey = threadsDataStruct[i][2]
        # print("i: {}, b: {}, e: {}".format(i, bkey, ekey))
        t = threading.Thread(target=worker_steady, args=(i, dbconnstr, bkey, ekey))
        threads.append(t)
        t.start()

    # Wait for all of them to finish
    for x in threads:
        x.join()

    time.sleep(2)

    # Connect to the cluster
    conn = getcon(dbconnstr)
    conn.set_session(autocommit=True)

    # Delete Report After
    delRptSQL = """
    select delboolean, count(*)
    from u
    group by 1;
    """
    print("\n--------------------------------------------")
    print("--- Table to TRIM after")
    print("--------------------------------------------")
    rptRowsToDelete(conn, delRptSQL)

    rptFinal(conn)


#  Run Main
####################################
if __name__ == '__main__':
    main()