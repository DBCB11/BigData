#!/bin/bash
declare -A HOSTS_AND_PORTS=(
  ["kafka-1"]=9092
  ["kafka-3"]=9092
  ["kafka-2"]=9092
)

echo "Waiting for all services to be ready..."

for HOST in "${!HOSTS_AND_PORTS[@]}"; do
  PORT=${HOSTS_AND_PORTS[$HOST]}
  echo "Checking $HOST:$PORT..."
  while ! nc -z $HOST $PORT; do
    echo "Still waiting for $HOST:$PORT..."
    sleep 2
  done
  echo "$HOST:$PORT is ready!"
done

exec python /kafka/producer/app.py &
exec python /kafka/consumer/app.py &
exec python /kafka/realtime_producer/app.py &
exec python /kafka/realtime_consumer/app.py