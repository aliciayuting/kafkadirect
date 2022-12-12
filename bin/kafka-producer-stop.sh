SIGNAL=${SIGNAL:-TERM}
PIDS=$(ps ax | grep java | grep -i ProducerLatency | grep -v grep | awk '{print $1}')

if [ -z "$PIDS" ]; then
  echo "No producer process to stop"
  exit 1
else
  kill -s $SIGNAL $PIDS
fi