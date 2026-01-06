#!/bin/bash
set -e

cd /home/ubuntu/supreme-stockequity-trading-bot || exit 1

RUN_ID=$(date +"%Y-%m-%d_%H-%M-%S")

/usr/bin/git fetch origin
/usr/bin/git reset --hard origin/main

for SHARD in {0..7}; do
  /home/ubuntu/optionsenv/bin/python stock_shortdb_masterfile.py \
    --shard $SHARD --shards 8 --run_id "$RUN_ID" \
    >> logs/shortdb_${SHARD}.log 2>&1 &
done

wait

/home/ubuntu/optionsenv/bin/python stock_master_ingest_shortdb.py \
  --run_id "$RUN_ID" >> logs/shortdb_master.log 2>&1
