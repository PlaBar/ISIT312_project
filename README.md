# GSS Data Lake

A Hadoop data lake for **Global Service Solutions (GSS)**, a UAE air-charter and cargo operator
based at Dubai World Central (DWC). Built for ISIT312 Big Data Management.

GSS's data sits in separate systems that do not talk to each other:
- NetSuite holds finance (invoices, payments, purchase orders).
- LION holds operations (flights, cargo manifests).
- A maintenance system holds aircraft checks and defects.
- An event log records delays, diversions and AOG.
- Customers send requests by email and WhatsApp.
- Paperwork (quotes, air waybills, packing lists, permits) arrives as text files.

The lake brings all of it into one place on HDFS, cleans it, links records across systems and
answers questions none of the systems can answer alone. Examples: what each flight earned and
cost, which customer requests became flights, and which flights lack a required permit.

All data is **synthetic** and generated with a fixed seed. Errors are planted in it on purpose,
and the true answers are kept aside in `generators/labels/`, so every cleaning and matching step
can be measured.

---

## 1. Architecture

```
 generators/ ──► staging (local) ──► preprocess/ ──► /lake/landing ──► /lake/raw ──► /lake/processed ──► /lake/curated
 (synthetic        CSV, JSON,          PP-01..PP-14      atomic release   immutable     typed, deduplicated  star schema, links,
  source data)     text files          checks, bundles   (_incoming + mv) Parquet       referenced           requests, documents
                                            │                                   │                 │                  │
                                            └──► /lake/quarantine ◄─────────────┴─────────────────┘                  │
                                                 preprocess_rejects, bad_rows, unlinked_documents                     │
                                                                                                                       ▼
                                      messages: release_loop / cron ──► StreamMessages (Structured Streaming)    Hive (SQL, reports)
                                                                         + Rules.scala                            HBase (key lookups)
```

| Zone | Contents | Written by |
|---|---|---|
| `/lake/landing` | files as released by preprocessing (CSV, JSONL bundles) | `preprocess/*.py` |
| `/lake/raw` | the same rows as Parquet, never changed, partitioned by load date | `LandingToRaw.scala`, `StreamMessages.scala` |
| `/lake/processed` | typed, deduplicated, references checked, UTC timestamps | `Cleanse.scala`, `StreamMessages.scala` |
| `/lake/curated` | star schema and business tables | `BuildCurated`, `DefectClassify`, `LinkDocuments`, `StreamMessages` |
| `/lake/quarantine` | rejected files, bad rows, unlinked documents, each with a reason | every layer |
| `/lake/checkpoints` | streaming checkpoint | `StreamMessages.scala` |

The curated star schema:
- **Dimensions:** `dim_date`, `dim_customer`, `dim_supplier`, `dim_airport`, `dim_aircraft`.
- **Facts:** `fact_invoice`, `fact_cost`, `fact_flight`, `fact_maintenance`, `fact_event`, `fact_defect`.
- **Business tables:** `flight_key_map`, `request`, `document_link`, `flight_documents`.

Every row keeps its lineage (`_src_file`, `_run_id`), back to the file and the preprocessing run
it came from.

## 2. Tools (as installed on the VM)

| Tool | Version | Used for |
|---|---|---|
| Hadoop (HDFS, YARN, MapReduce) | 2.7.3 | storage; MapReduce only for the benchmark and Hive |
| Spark | 3.0.3 (Scala 2.12), `local[2]` | all batch and streaming jobs, run with `spark-shell -i` (no sbt) |
| Hive | 2.1.1, Derby metastore as a service on port 9083 | SQL over the curated zone, monthly report |
| HBase | 1.2.6 | serving table `gss_requests` for instant lookups |
| Python | 3.4, standard library only | generators, preprocessing, evaluation twins |

## 3. Project layout

```
generators/            synthetic sources + hidden truth (labels/)
  gen_common.py          seed, period, fleet, airports, cargo types, ID formats
  structured_gen.py      NetSuite, LION and maintenance CSVs (700 planted errors)
  events_gen.py          event log JSONL
  messages_gen.py        39,673 emails / WhatsApp messages, 10,800 requests
  documents_gen.py       6,111 documents (+ duplicates and junk)
preprocess/            before the lake: checks PP-01..PP-14, atomic release to HDFS
  config/schemas.json    the contract every source is checked against
  pp_common.py           manifest, reject log, lineage, release (_incoming + mv)
  pp_structured.py  pp_events.py  pp_messages.py  pp_documents.py
  run_preprocess.sh      batch run;  release_loop.sh  feeds messages a batch at a time
  reset_source.sh        undo one source (local state, and HDFS with --hdfs)
  env.sh                 environment for scripts and cron (PATH, JAVA_HOME, GSS_STAGING, UTC)
lake/
  run_spark.sh           runs one Scala job (or library + job) with spark-shell, one at a time
  ingest/LandingToRaw.scala       landing -> raw, Layer 2 check against the manifest
  process/Cleanse.scala           raw -> processed, quarantine, DQ report
  process/BuildCurated.scala      flight-reference repair + star schema (Spark SQL)
  process/DefectClassify.scala    defect system / component / severity by keyword rules
  extract/dictionaries/*.txt      vocabulary: airports, cargo, dates, defect terms
  extract/Rules.scala             reads a customer message: request? route, cargo, weight, date
  extract/RulesCheck.scala        Scala rules vs the Python twin, message by message
  stream/StreamMessages.scala     Structured Streaming: landing -> raw / processed / request
  link/LinkDocuments.scala        documents -> flight, shipment, quote, message; permit check
  hive/HiveDdl.scala  create_tables.hql  check_tables.hql  metastore.sh
  hive/monthly_report.hql  run_monthly_report.sh
  hbase/HBaseSink.scala  create_table.hbase  load_hbase.sh
  ops/run_pipeline.sh  start_services.sh  health_check.sh  CompactStream.scala  crontab.txt
  bench/run_benchmarks.sh  BenchPrepare.scala  BenchSpark.scala  mapper.py  reducer.py
evaluation/
  check_preprocessing.py  check_dictionaries.py  rules_twin.py  link_twin.py
  repair_twin_reference.py  Accuracy.scala
README.md  data_dictionary.md  assumptions_register.md  process_catalogue.md   (project root)
```

The code lives in a VirtualBox shared folder (`~/gss-datalake`, edited in VS Code on the host).
The data lives on the VM's own disk (`GSS_STAGING=$HOME/gss-data/staging`), because the
shared folder is too slow for tens of thousands of small files. Files must use LF line endings.

## 4. Setup (once)

```bash
# ~/.bashrc
export GSS_STAGING=$HOME/gss-data/staging
export PATH=$PATH:/usr/share/spark/bin:/usr/share/hive/bin:/usr/share/hbase/bin
export HIVE_HOME=/usr/share/hive HBASE_HOME=/usr/share/hbase
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Duser.timezone=UTC"

start-dfs.sh
bash lake/hive/metastore.sh start
bash lake/ingest/init_hdfs.sh            # creates the /lake zones
```

## 5. Building the lake from nothing

```bash
cd ~/gss-datalake
# 1. sources and truth
python3 generators/structured_gen.py && python3 generators/events_gen.py
python3 generators/messages_gen.py && python3 generators/documents_gen.py
# 2. preprocessing and release
bash preprocess/run_preprocess.sh --check          # structured, events, documents
# 3. batch layers
bash lake/run_spark.sh lake/ingest/LandingToRaw.scala
bash lake/run_spark.sh lake/process/Cleanse.scala
bash lake/run_spark.sh lake/process/BuildCurated.scala
bash lake/run_spark.sh lake/process/DefectClassify.scala
# 4. messages: streaming (demo: release_loop.sh in a second terminal), then the rest at once
bash lake/run_spark.sh lake/extract/Rules.scala lake/stream/StreamMessages.scala
bash preprocess/run_preprocess.sh messages
GSS_STREAM_ONCE=1 bash lake/run_spark.sh lake/extract/Rules.scala lake/stream/StreamMessages.scala
# 5. documents, Hive, HBase, report
bash lake/run_spark.sh lake/link/LinkDocuments.scala
bash lake/run_spark.sh lake/hive/HiveDdl.scala
hive -f lake/hive/create_tables.hql && hive -f lake/hive/check_tables.hql
start-hbase.sh && bash lake/hbase/load_hbase.sh && stop-hbase.sh
bash lake/hive/run_monthly_report.sh 202610
# 6. evaluation
bash lake/run_spark.sh evaluation/Accuracy.scala
start-yarn.sh && bash lake/bench/run_benchmarks.sh && stop-yarn.sh
```

After that, `crontab lake/ops/crontab.txt` keeps the lake running on its own. It runs
`run_pipeline.sh stream` every 15 minutes, `nightly` at 01:30, `weekly` on Sunday at 03:00
and the monthly report on the 1st. `bash lake/ops/health_check.sh` shows the state in one line.

## 6. How correctness is checked

| Layer | Check | Result on this data |
|---|---|---|
| Preprocessing | every planted error must end in the right outcome (`check_preprocessing.py`) | 700/700 structured, 12/12 events, 1,709/1,709 messages, 1,213/1,213 documents |
| Landing → raw | rows in raw = rows accepted in the manifest, per file | 26,857 rows, all files match |
| Raw → processed | raw = duplicates + quarantined + processed, per table | balanced for every table |
| Streaming | streamed messages = released messages, duplicates counted | 39,381 = 39,381, 0 duplicates |
| Hive | Hive row count = Spark row count, per table; type test row | 19/19 tables equal |
| HBase | rows loaded = requests, bad lines | 10,707 rows, 0 bad lines |
| Rules (Scala) | identical to the Python twin, message by message (`RulesCheck.scala`) | — |
| Benchmark | MapReduce, Hive and Spark results identical line by line | caught a key-definition bug in the Streaming job |
| Inference | precision / recall against the hidden truth (`Accuracy.scala`) | `$GSS_STAGING/reports/accuracy_*.csv` |

Python twins (`evaluation/*_twin.py`) hold the same rules as the Scala jobs and were scored
against the truth before the Scala was written. That was needed because Scala could not be
tested outside the VM. Their expected scores are in the process catalogue.

## 7. Known limitations

- **Synthetic data:** the extraction rules were written knowing the generator's vocabulary, so
  their near-perfect scores are an upper bound. Real messages and notes would score lower.
- **Speed:** jobs are slow to start, not slow to read. Every `hdfs`, `hive` and `spark-shell`
  call starts a JVM (2 to 40 s), while the whole lake is tens of MB. HBase answers a lookup in
  hundredths of a second.
- **spark-shell in place of compiled jars:** objects defined in the REPL cannot be shipped to
  executors in a UDF, so the message and document rules run on the driver, one micro-batch at
  a time. Built with sbt, they would be UDFs.
- **Derby metastore:** a single-user database behind a metastore service. A real deployment
  would use MySQL or PostgreSQL.
- **At-least-once streaming output:** a batch can be written twice after a crash. Readers and
  the weekly compaction keep the latest copy per `msg_id`.
- **Margin is contribution, not profit:** `fact_flight.margin_usd` is revenue minus the
  direct costs that can be tied to a flight (fuel, handling, navigation). About 93% of revenue
  and 87% of cost could be tied to a flight.

See `assumptions_register.md` for every assumption, `process_catalogue.md` for every job and
`data_dictionary.md` for every table and ID format, all in the project root.
