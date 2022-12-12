SIGNAL=${SIGNAL:-TERM}
PIDS=$(ps ax | grep java | grep -i ConsumerLatency | grep -v grep | awk '{print $1}')

if [ -z "$PIDS" ]; then
  echo "No consumer process to stop"
  exit 1
else
  kill -s $SIGNAL $PIDS
fi