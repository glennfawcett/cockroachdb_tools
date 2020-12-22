# Hottest Ranges Tool
This is a set of scritps and tools to help diagnose various situations that occur with CockroachDB.  This requres Python3 to be able to run.

## Python3 setup linux
```bash
## Install Python3
sudo apt-get update
sudo apt install python3-pip
sudo pip3 install --upgrade pip
sudo apt-get install libpq-dev python-dev
sudo pip3 install psycopg2
sudo pip3 install numpy
```

## Python3 setup OSX
```bash
brew install python3
brew install pip3
pip3 install psycopg2
pip3 install numpy
```

## Hottest Ranges

The [hottest_ranges3.py](hottest_ranges3.py) script retrieves raft data from the restful interface, sorts the ranges by QPS and looks up information about the Object from the ranges table:

```bash
$ python3 hottest_ranges3.py --numtop 10 --host glenn-testdb-0001.roachprod.crdb.io --adminport 26258 --dbport 26257
rank  rangeId	       QPS	     Nodes	 leaseHolder	DBname, TableName, IndexName
  1:      133	 68.540433	 [5, 1, 4]	           4	['testdb', 'basetable', 'idx1_basetable']
  2:      294	 66.942967	 [3, 2, 1]	           1	['testdb', 'basetable', 'idx1_basetable']
  3:      188	 59.163897	 [5, 3, 2]	           2	['testdb', 'basetable', 'idx1_basetable']
  4:      149	 59.133435	 [5, 3, 2]	           2	['testdb', 'basetable', 'idx1_basetable']
  5:      187	 57.002015	 [5, 3, 4]	           3	['testdb', 'basetable', 'idx1_basetable']
  6:      252	 56.774282	 [5, 3, 4]	           3	['testdb', 'basetable', 'idx1_basetable']
  7:      250	 56.735735	 [3, 1, 4]	           3	['testdb', 'basetable', 'idx1_basetable']
  8:      171	 56.317065	 [5, 3, 2]	           2	['testdb', 'basetable', 'idx1_basetable']
  9:      129	 55.468659	 [5, 1, 4]	           4	['testdb', 'basetable', 'idx1_basetable']
 10:      281	 54.008324	 [5, 3, 2]	           2	['testdb', 'basetable', 'idx1_basetable']
 ```
