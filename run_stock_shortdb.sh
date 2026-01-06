#!/bin/bash
set -e

cd /home/ubuntu/supreme-stockequity-trading-bot || exit 1

LOGDIR="/home/ubuntu/supreme-stockequity-trading-bot/logs"
mkdir -p "$LOGDIR"

# 🔴 WRAPPER LOG: capture ALL stdout/stderr from this script
exec >> "$LOGDIR/cron_10min_wrapper.log" 2>&1
set -x
echo "===== CRON START $(date) ====="
whoami
pwd

RUN_ID=$(date +"%Y-%m-%d_%H-%M-%S")
echo "RUN_ID=$RUN_ID"

# Record which commit ran (safe, no mutation)
git rev-parse --short HEAD || true

for SHARD in {0..3}; do
  /home/ubuntu/optionsenv/bin/python -u stock_shortdb_masterfile.py \
    --shard $SHARD \
    --shards 4 \
    --run_id "$RUN_ID" \
    >> "$LOGDIR/10min_${SHARD}.log" 2>&1 &
done

wait

/home/ubuntu/optionsenv/bin/python -u stock_master_ingest_shortdb.py \
  --run_id "$RUN_ID" >> "$LOGDIR/10min_master.log" 2>&1

echo "===== CRON END $(date) ====="
