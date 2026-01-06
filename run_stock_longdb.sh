#!/bin/bash
set -e

cd /home/ubuntu/supreme-stockequity-trading-bot || exit 1

LOGDIR="/home/ubuntu/supreme-stockequity-trading-bot/logs"
mkdir -p "$LOGDIR"

# 🔴 WRAPPER LOG: capture EVERYTHING (cron-safe)
exec >> "$LOGDIR/cron_5w_wrapper.log" 2>&1
set -x
echo "===== CRON START $(date) ====="
whoami
pwd

RUN_ID=$(date +"%Y-%m-%d_%H-%M-%S")
echo "RUN_ID=$RUN_ID"

# Record code version (read-only, safe)
git rev-parse --short HEAD || true

for SHARD in {0..3}; do
  /home/ubuntu/optionsenv/bin/python -u stock_long_db_masterfile.py \
    --shard $SHARD \
    --shards 4 \
    --run_id "$RUN_ID" \
    >> "$LOGDIR/5week_${SHARD}.log" 2>&1 &
done

wait

/home/ubuntu/optionsenv/bin/python -u stock_longdb_master_ingest.py \
  --run_id "$RUN_ID" >> "$LOGDIR/5week_master.log" 2>&1

echo "===== CRON END $(date) ====="
