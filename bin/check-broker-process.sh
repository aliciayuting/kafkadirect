SIGNAL=${SIGNAL:-TERM}
PIDS=$(ps ax | grep -i 'kafka\.Kafka' | grep java | grep -v grep | awk '{print $1}')

if [ -z "$PIDS" ]; then
  echo "No kafka broker started"
  exit 1
else
  echo "kafka broker started"
  exit 0
fi