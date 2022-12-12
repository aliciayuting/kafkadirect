SIGNAL=${SIGNAL:-TERM}
PIDS=$(ps ax | grep java | grep -i QuorumPeerMain | grep -v grep | awk '{print $1}')

if [ -z "$PIDS" ]; then
  echo "No kafka zookeeper started"
  exit 1
else
  echo "zookeeper started"
  exit 0
fi