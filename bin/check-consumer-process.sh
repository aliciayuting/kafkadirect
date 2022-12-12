SIGNAL=${SIGNAL:-TERM}
PIDS=$(ps ax | grep java | grep -i ConsumerLatency | grep -v grep | awk '{print $1}')

if [ -z "$PIDS" ]; then
  echo "No consumer process started"
  exit 1
else
  echo "consumer process started"
  exit 0  
fi