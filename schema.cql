DROP KEYSPACE ycsb;
CREATE KEYSPACE ycsb WITH REPLICATION =
	{'class' : 'SimpleStrategy', 'replication_factor': 3};
CREATE TABLE ycsb.usertable
	( y_id   VARCHAR PRIMARY KEY,
	  field0 VARCHAR,
      tag varchar );