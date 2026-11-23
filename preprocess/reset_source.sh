#!/bin/bash
# reset_source.sh - forget the preprocessing of one source so its files count as new again.
#
#   bash preprocess/reset_source.sh messages           local state only
#   bash preprocess/reset_source.sh messages --hdfs    also its HDFS landing and raw data
#
# Sources: structured, events, messages, documents.
# Removes that source's rows from the preprocess manifest, its accepted and
# rejected copies, reports and duplicate-key list. The staging input files are never touched.
set -e
cd "$(dirname "$0")/.."
S="${GSS_STAGING:-staging}"      # data folder (see GSS_STAGING in generators/gen_common.py)

src="$1"
case "$src" in
  structured) local_dirs="exports"; keys="";                landing="/lake/landing/exports/netsuite /lake/landing/exports/lion /lake/landing/exports/maint"
              raw="/lake/raw/netsuite /lake/raw/lion /lake/raw/maint" ;;
  events)     local_dirs="events";  keys="seen_event_id";   landing="/lake/landing/exports/events"; raw="/lake/raw/events" ;;
  messages)   local_dirs="messages"; keys="seen_msg_id";    landing="/lake/landing/messages/email /lake/landing/messages/whatsapp"
              raw="/lake/raw/messages /lake/checkpoints/stream_messages /lake/processed/message /lake/curated/request" ;;
  documents)  local_dirs="documents"; keys="seen_doc_hash"; landing="/lake/landing/documents"; raw="/lake/raw/documents" ;;
  *) echo "usage: bash preprocess/reset_source.sh structured|events|messages|documents [--hdfs]"; exit 1 ;;
esac

if [ -f "$S/state/$src.lock" ]; then
  echo "A $src run seems to be active ($S/state/$src.lock). Stop it first, or delete the lock if it crashed."
  exit 1
fi

m="$S/preprocess_manifest.csv"
if [ -s "$m" ]; then
  before=$(($(wc -l < "$m") - 1))
  awk -F, -v s="$src" 'NR == 1 || $3 != s' "$m" > "$m.tmp" && mv "$m.tmp" "$m"
  after=$(($(wc -l < "$m") - 1))
  echo "manifest: removed $((before - after)) $src rows ($after rows of other sources kept)"
fi
for d in $local_dirs; do
  rm -rf "$S/accepted/$d" "$S/rejected/$d"
done
rm -rf "$S"/accepted/_pending/*_"$src"
rm -f "$S"/reports/*_"$src".csv
for k in $keys; do rm -f "$S/state/$k.txt"; done
echo "local: accepted, rejected, reports and duplicate keys for $src removed"

if [ "$2" == "--hdfs" ]; then
  # raw goes too: it records which landing files it holds, so after a reset and a new
  # release the old rows would otherwise stay in raw next to the new ones
  hdfs dfs -rm -r -f -skipTrash $landing $raw "/lake/quarantine/preprocess_rejects/$src"
  hdfs dfs -mkdir -p $landing $raw
  echo "hdfs: emptied $landing $raw"
fi