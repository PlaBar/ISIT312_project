# Process catalogue

Every process in the GSS data lake: what it reads, what it writes, when it runs, how to tell it
worked and what to do when it does not. Spark jobs are run with
`bash lake/run_spark.sh <job.scala>`. Each ends with `GSS_JOB_OK`, and its log is
`$GSS_STAGING/reports/spark_<job>.log`. Schedules refer to `lake/ops/crontab.txt`.

## 1. Sources (generators, run once)

| Process | Reads | Writes | Output on this data |
|---|---|---|---|
| `structured_gen.py` | — | `staging/exports/{netsuite,lion,maint}/*.csv`, labels `shipments_master`, `flight_ref_truth`, `defect_truth`, `planted_errors` | 14 tables, 1,440 flights, 700 planted errors |
| `events_gen.py` | flights | `staging/events/events_YYYYMM.jsonl` | 463 events, 12 planted errors |
| `messages_gen.py` | shipments | `staging/messages/<channel>/*.json`, labels `requests_truth`, `message_attachments`, `planted_errors_messages` | 39,673 messages, 10,800 requests, 1,709 planted errors |
| `documents_gen.py` | shipments, attachments | `staging/documents/YYYY/MM/TYPE/*.txt`, labels `documents_truth`, `planted_errors_documents` | 6,111 documents (+70 duplicates, 20 junk), 1,213 planted errors |

## 2. Preprocessing (before the lake)

| Process | Reads | Writes | Schedule | Checks / success |
|---|---|---|---|---|
| `pp_structured.py` | staging exports | `/lake/landing/exports/<system>/`, rejects to `/lake/quarantine/preprocess_rejects/` | nightly (`run_preprocess.sh`) | PP-01..PP-09, PP-14; manifest status RELEASED |
| `pp_events.py` | event JSONL | `/lake/landing/exports/events/` | nightly | PP-03, PP-05..PP-09 |
| `pp_documents.py` | document text files | `/lake/landing/documents/<TYPE>/<type>_<yyyymm>_<run>.jsonl` | nightly | PP-02, PP-09 (text hash), PP-12, PP-13 |
| `pp_messages.py` | message JSON | `/lake/landing/messages/<channel>/messages_<run>.jsonl` | every 15 min (`run_pipeline.sh stream`) | PP-07 (UTC), PP-09, PP-10, PP-11, PP-12 |
| `check_preprocessing.py <source>` | outcomes + planted errors | report | after a change | every planted error ends as intended (all 4 sources: 100%) |

Common to all: only new files (manifest `$GSS_STAGING/preprocess_manifest.csv`), atomic release
through `/lake/landing/_incoming`, a lock per source, and a reject log plus quality report per run
in `$GSS_STAGING/reports/`. To undo a source, run `bash preprocess/reset_source.sh <source> [--hdfs]`.

## 3. Batch layers (Spark, nightly from 01:30, in this order)

| # | Job | Reads | Writes | Check (printed) | Expected |
|---|---|---|---|---|---|
| 1 | `LandingToRaw.scala` | landing CSV / JSONL | `/lake/raw/<system>/<table>/dt=<load date>` | Layer 2: raw rows = manifest `rows_accepted`, per file | 26,857 rows; documents 6,111, events 454, structured 20,292 |
| 2 | `Cleanse.scala` | raw | `/lake/processed/<table>`, `/lake/quarantine/bad_rows/<table>`, `dq_report` | raw = duplicates + quarantined + processed, per table | e.g. invoice 1,448 → 11 duplicates → 1,437; invoice_line 63 quarantined |
| 3 | `BuildCurated.scala` | processed | `flight_key_map`, `dim_*`, `fact_invoice`, `fact_cost`, `fact_flight`, `fact_maintenance`, `fact_event` | match counts by confidence; one flight's revenue, cost and margin | 92.9% of revenue and 87.1% of cost tied to flights |
| 4 | `DefectClassify.scala` | processed/defect, `defect_terms.txt` | `curated/fact_defect` | agreement with recorded severity; UNKNOWN count | 85/85 agree, 0 UNKNOWN |
| 5 | `LinkDocuments.scala` | document_text, flight, manifest, airport, message | `document_link`, `flight_documents`, `quarantine/unlinked_documents` | links by method; unlinked; failed check digits; permit check | 46 unlinked (29 belong to the 14 flights rejected in preprocessing), 27 bad check digits, 0 permits missing |
| 6 | `load_hbase.sh` (`HBaseSink.scala` + ImportTsv) | curated/request | HBase `gss_requests` | ImportTsv counters; demo lookups | 10,707 rows, 0 bad lines |

Run all six with `bash lake/ops/run_pipeline.sh nightly`. If a step fails, resume from it with
`--from <step>`.

## 4. Messages (streaming)

| Process | Reads | Writes | Schedule | Check |
|---|---|---|---|---|
| `StreamMessages.scala` (+ `Rules.scala`) | `/lake/landing/messages/*/*.jsonl` (file stream, checkpoint `/lake/checkpoints/stream_messages`) | `raw/messages/message/dt=`, `processed/message`, `curated/request` | every 15 min as Trigger.Once (`run_pipeline.sh stream`), or continuously for the demo | streamed = released messages, duplicates counted: 39,381 = 39,381, 0 duplicates; 10,707 requests (7,306 complete, 3,401 needing information) |
| `release_loop.sh` | staging messages | landing | demo only | one batch per interval |
| `RulesCheck.scala` | landing messages, twin predictions | report | after a rule change | Scala = Python twin on every message |
| `CompactStream.scala` | processed/message, curated/request | the same folders, compacted | weekly, Sunday 03:00 | files N → few; duplicates dropped |

## 5. Serving and reporting

| Process | Reads | Writes | Schedule | Check |
|---|---|---|---|---|
| `HiveDdl.scala` | Parquet schemas of curated (+ message, dq_report, unlinked_documents) | `create_tables.hql`, `check_tables.hql` | after a job changes columns | — |
| `hive -f create_tables.hql` | — | database `gss`, 19 external tables | same | `OK` per statement |
| `hive -f check_tables.hql` | all tables | report | after rebuilding | Hive count = Spark count (19/19); type test row |
| `run_monthly_report.sh [yyyyMM]` | curated via Hive | `$GSS_STAGING/reports/monthly_report_<yyyyMM>.txt` | 06:00 on the 1st | 8 sections; about 1-5 min (Hive on MapReduce, local mode) |
| `metastore.sh start/stop/status` | — | metastore service on port 9083 | at boot | `status` |

## 6. Operations

| Process | Does | Schedule |
|---|---|---|
| `start_services.sh` | waits for the shared folder, starts HDFS, waits for safe mode off, starts the metastore | at boot |
| `run_pipeline.sh stream / nightly / weekly` | the scheduled runs above; records `OK` or `FAILED` (with the failed step) in `$GSS_STAGING/state/last_<stage>.txt` | 15 min / 01:30 / Sunday 03:00 |
| `health_check.sh` | NameNode, DataNode, safe mode, HDFS use, metastore, age and result of the last runs; one line in `health.log` | hourly |
| `run_spark.sh` | runs a Scala job (or library + job), one at a time (lock), success only with `GSS_JOB_OK` | — |

Services and memory: HDFS and the metastore run all the time. HBase runs only during its load,
because it needs 1 to 1.5 GB. YARN runs only for the benchmark.

## 7. Evaluation

| Process | Measures | Result |
|---|---|---|
| `check_dictionaries.py` | vocabulary coverage of the request fields | origin and destination 100%, cargo 99.96%, handling and relative dates 100% |
| `rules_twin.py` | request rules vs truth (Python) | detection 100% / 100%; fields 100% except cargo 99.3% |
| `link_twin.py` | document links vs truth (Python) | flight 99.8% precision / 99.6% recall; AWB 100% / 99.5%; message 100% / 99.4% |
| `repair_twin_reference.py` | flight-reference repair vs truth (Python) | invoices 99.8% / 92.8%; PO lines 99.7% / 91.0% |
| `Accuracy.scala` | all of the above on the lake's actual outputs, with reachable recall | `$GSS_STAGING/reports/accuracy_<time>.csv` |
| `run_benchmarks.sh` | the same query in MapReduce, Hive and Spark at 10 thousand, 100 thousand and 1 million rows; HBase vs Hive lookup; results compared line by line | `$GSS_STAGING/reports/bench/results_<time>.csv` |

## 8. Troubleshooting (seen during the build)

| Symptom | Cause | Fix |
|---|---|---|
| `Connection refused` to port 8020 | HDFS not started after a reboot | `start-dfs.sh` |
| `WARN hdfs.DFSClient: Caught exception java.lang.InterruptedException` | a harmless Hadoop 2.7 bug (HDFS-10429) | none |
| Hive `Could not connect to meta store` | the metastore service is not running | `bash lake/hive/metastore.sh start` |
| Hive `Table not found` | `create_tables.hql` not run | `hive -f lake/hive/create_tables.hql` |
| `$'\r': command not found` | CRLF line endings from Windows | `sed -i 's/\r$//' <file>`; set VS Code *Files: Eol* to `\n` |
| Spark `Task not serializable` (REPL object in a UDF) | spark-shell wraps objects in classes that cannot be serialised | run the logic on the driver (as in StreamMessages) |
| Spark `task of very large size` warning | data built on the driver is shipped as one block | harmless at this size |
| ImportTsv `Unknown program` | `hbase-server-*.jar` matched the `-tests` jar too | name the plain jar (`load_hbase.sh` does) |
| MapReduce totals split per route | Streaming key fields not set for the combiner and reducer | `stream.num.reduce.output.key.fields=2` |
| Hive shows times 11 h later | Hive displays Parquet UTC times in the JVM's zone | `-Duser.timezone=UTC` (in `env.sh` and `.bashrc`) |
