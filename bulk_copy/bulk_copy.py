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


__appname__ = 'bulk_copy'
__version__ = '0.1.0'
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


# CREATE TABLE public.delivery_min_earning_info_tmp_1 (
#   delivery_id INT8 NOT NULL,
#   shift_id INT8 NOT NULL,
#   created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp():::TIMESTAMPTZ,
#   distinct_active_time INT8 NOT NULL,
#   minimum_wage_amount INT8 NOT NULL,
#   mileage_reimbursement_amount INT8 NOT NULL,
#   pickup_zip_code STRING NOT NULL,
#   pickup_zip_code_pay_target_uuid UUID NOT NULL,
#   CONSTRAINT pk_delivery_min_earning_info PRIMARY KEY (shift_id DESC, delivery_id DESC, created_at DESC)
# );

def split(a, n):
    # Split a data structure
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))

def findFirst(connStr, sourceTable):
    mycon = psycopg2.connect(connStr)
    mycon.set_session(autocommit=True)

    getMaxSQL = """
    SELECT shift_id, delivery_id, created_at
    FROM {}
    ORDER BY shift_id DESC
    LIMIT 1; 
    """

    with mycon.cursor() as cur:
        cur.execute(getMaxSQL.format(sourceTable))
        rows = cur.fetchall() 

    return rows[0][0], rows[0][1], rows[0][2]

def worker_steady(num, connStr, batchSize, b_shift_id, b_delivery_id, b_created_at, sourceTable, destTable, cntr):
    """Copy Worker Thread"""
    print("Copy Thread : {}".format(num))
    print("Thread Progress Interval : {}".format(cntr))


    mycon = psycopg2.connect(connStr)
    mycon.set_session(autocommit=True)

    getKeySQL = """
    SELECT shift_id, delivery_id, created_at
    FROM {} 
    WHERE shift_id <= {}
    ORDER BY shift_id DESC, delivery_id DESC, created_at DESC
    LIMIT {};
    """
#   distinct_active_time INT8 NOT NULL,
#   minimum_wage_amount INT8 NOT NULL,
#   mileage_reimbursement_amount INT8 NOT NULL,
#   pickup_zip_code STRING NOT NULL,
#   pickup_zip_code_pay_target_uuid UUID NOT NULL,
    copySQL = """
    UPSERT INTO {} (shift_id, delivery_id, created_at, distinct_active_time, minimum_wage_amount, mileage_reimbursement_amount, pickup_zip_code, pickup_zip_code_pay_target_uuid)
    SELECT shift_id, delivery_id, created_at, distinct_active_time, minimum_wage_amount, mileage_reimbursement_amount, pickup_zip_code, pickup_zip_code_pay_target_uuid
    FROM {}
    WHERE shift_id <= {}
    ORDER BY shift_id DESC
    LIMIT {}
    --ON CONFLICT DO NOTHING
    RETURNING shift_id, delivery_id, created_at;
    """

    notFinished = True
    ctr = 0

    while notFinished:
        with mycon:
            with mycon.cursor() as cur:
                # print("Before getKeySQL")

                # first time through
                if ctr == 0:
                    # cur.execute(getKeySQL.format(sourceTable, b_shift_id, batchSize))
                    cur.execute(copySQL.format(destTable, sourceTable, b_shift_id, batchSize))
                    rows = cur.fetchall()
                    ## Process first columns
                    roff = len(rows) - 1
                    b_shift_id = rows[roff][0]
                    b_delivery_id = rows[roff][1]
                    b_created_at = rows[roff][2]
                    last_b_shift_id = rows[0][0]
                    last_b_delivery_id = rows[0][1]
                    last_b_created_at = rows[0][2]
                    print("{}::  Starting row:: {}, {}, {}".format(datetime.datetime.now(), b_shift_id, b_delivery_id, b_created_at))


                if (last_b_shift_id == b_shift_id) & (last_b_delivery_id == b_delivery_id) & (last_b_created_at == b_created_at):
                    notFinished = False
                    break
                else:
                    cur.execute(copySQL.format(destTable, sourceTable, b_shift_id, batchSize))
                    rows = cur.fetchall()
                    roff = len(rows) - 1

                    last_b_shift_id = rows[0][0]
                    last_b_delivery_id = rows[0][1]
                    last_b_created_at = rows[0][2]

                    b_shift_id = rows[roff][0]
                    b_delivery_id = rows[roff][1]
                    b_created_at = rows[roff][2]
    
                    ## Print Progress every cntr inserts
                    if ctr == cntr:
                        print("{}::      Last row:: {}, {}, {}".format(datetime.datetime.now(), rows[roff][0], rows[roff][1], rows[roff][2]))
                        ctr = 1
                    else:
                        ctr += 1



####
#### MAIN
####
def getArgs():
    """ Get command line args """
    desc = 'Trim table by PK for CockroachDB based on MVCC timestamp'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("-v", "--verbose", action="store_true", default=False, help='Verbose logging')
    parser.add_argument("-u", "--url", dest='url', default='postgresql://root@127.0.0.1:26257/defaultdb?sslmode=disable', help='example... postgres://username@clustername.gcp-us-central1.com:26257/defaultdb?sslmode=verify-full&sslrootcert=~/certs/ca.crt')
    parser.add_argument("-b", "--batch", dest='batch', default=1000, help='Number Rows to delete per Batch')
    parser.add_argument("-c", "--cntr", dest='cntr', default=1000, type=int, help='Progress Counter')
    parser.add_argument("-s", "--source", dest='source', default='delivery_min_earning_info_tmp_1', type=str, required=True, help='Name of Table temp source table')
    parser.add_argument("-t", "--dest", dest='dest', default='delivery_min_earning_info', type=str, required=True, help='Name of target destination table')

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
    # dbconnstr = dbstr(options.database, options.user, "disable", options.host, options.dbport)
    # dbconnstr = 'postgresql://root@127.0.0.1:26257/defaultdb?sslmode=require&sslrootcert=/home/glenn/crdb/ca.crt&sslcert=/home/glenn/crdb/client.root.crt&sslkey=/glenn/crdb/delivery/client.root.key'

    connStr = options.url

    # Connect to the cluster.
    conn = psycopg2.connect(connStr)
    conn.set_session(autocommit=True)

    # Options
    sourceTable = options.source 
    destTable = options.dest
    cntr = options.cntr
    BatchSize = int(options.batch)

    b_shift_id, b_delivery_id, b_created_at = findFirst(connStr, sourceTable)
    print("===Max Values============================================")
    print("\tshift_id: {}\n\tdeliver_id: {}\n\tcreated_at: {}".format(b_shift_id, b_delivery_id, b_created_at))
    print("=========================================================\n")

    print("-------------------------------------------------------------------------------------------")
    print("-----      Source Table: '{}' ".format(sourceTable))
    print("----- Destination Table: {}".format(destTable))
    print("-------------------------------------------------------------------------------------------")
    print("-----    Progress Interval: {}".format(cntr))
    print("-----            BatchSize: {} rows per thread".format(BatchSize))
    print("-------------------------------------------------------------------------------------------")

    threads = []
    for i in range(1):
        t = threading.Thread(target=worker_steady, args=(i, connStr, BatchSize, b_shift_id, b_delivery_id, b_created_at, sourceTable, destTable, cntr))
        threads.append(t)
        t.start()

    print("\n{} Threads Spawned".format(i+1))

    time.sleep(2)

    # Wait for all of them to finish
    for x in threads:
        x.join()

    # Done
    print("Finished Copy")


#  Run Main
####################################
if __name__ == '__main__':
    main()