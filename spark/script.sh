#!/bin/bash

exec /bin/bash /master.sh &
exec spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 spark-stream.py 