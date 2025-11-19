#!/bin/bash
/app/bash/setup_kafka.sh
/app/bash/start_worker_linux.sh
exec "$@"
