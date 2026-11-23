#!/bin/bash
# Creates the GSS lake zones in HDFS. Safe to rerun: -p skips folders that exist.
set -e

hdfs dfs -mkdir -p \
  /lake/landing/exports/{netsuite,lion,maint,events} \
  /lake/landing/messages/{email,whatsapp} \
  /lake/landing/documents \
  /lake/landing/_incoming \
  /lake/raw/{netsuite,lion,maint,events,messages,documents} \
  /lake/processed \
  /lake/curated \
  /lake/quarantine/{preprocess_rejects,bad_rows,review_queue,unlinked_documents} \
  /lake/checkpoints/stream_messages

hdfs dfs -ls -R /lake | grep '^d' | awk '{print $8}'