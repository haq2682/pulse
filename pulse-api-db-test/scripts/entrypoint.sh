#!/bin/bash
set -e

echo "Fixing keyfile permissions..."
chown mongodb:mongodb /etc/mongo-keyfile
chmod 400 /etc/mongo-keyfile

echo "Starting MongoDB..."
/usr/local/bin/docker-entrypoint.sh mongod \
  --replSet rs0 \
  --keyFile /etc/mongo-keyfile \
  --bind_ip_all &

MONGO_PID=$!

echo "Waiting for MongoDB to accept connections..."

until mongosh --quiet --eval "db.adminCommand('ping')" > /dev/null 2>&1; do
  sleep 2
done

echo "Initiating replica set (if needed)..."

mongosh --quiet <<EOF
try {
  rs.status()
} catch (e) {
  rs.initiate({
    _id: "rs0",
    members: [{ _id: 0, host: "mongodb:27017" }]
  })
}
EOF

echo "Waiting for PRIMARY state..."

until mongosh --quiet --eval "rs.isMaster().ismaster" | grep true > /dev/null; do
  sleep 2
done

echo "Replica set ready. Running ingestion..."

python3 /docker-entrypoint-initdb.d/ingest.py

echo "Startup complete."
wait $MONGO_PID
