-- Create Airflow metadata database
CREATE DATABASE airflow;

-- Grant replication privilege so Debezium can read WAL
ALTER USER dataeng WITH REPLICATION;
