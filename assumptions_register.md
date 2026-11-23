# Assumptions register

Each entry says what was assumed, why, where it is used and what would change if it were
wrong. Code comments mark these assumptions with `[A]`; facts taken from GSS's real business are
marked `[R]`.

Status: **open** = still an assumption; **checked** = tested against the data or the truth
labels (with the result); **design** = a deliberate project choice, not a claim about GSS.

## A. Business and reference data

| ID | Assumption | Why | Used in | If wrong | Status |
|---|---|---|---|---|---|
| A01 | About 40 flights, 40 shipments and 300 customer requests a month `[R]`; 1,200 messages and 160 documents a month `[A]` | real GSS volumes where known; the rest sized to match | `gen_common.py` | volumes, not logic, change | open |
| A02 | Fleet of 4 freighters: A6-GSA and A6-GSB (B737-400F, 19 t), A6-GSC (A300B4-200F, 40 t), A6-GSD (IL-76TD, 42 t) | plausible for a UAE charter operator | generators, `dim_aircraft` | fleet reports change | open |
| A03 | Home base DWC; destinations NBO ADD EBB JUB MGQ FIH LOS KBL BGW EBL TAS KHI DAC; block times from DWC per airport | typical humanitarian and oil-and-gas routes from the UAE | generators, `fact_flight.block_hours` | route mix and hours change | open |
| A04 | Rate card USD 8,500 / 13,500 / 15,500 per block hour (B737 / A300 / IL-76); fuel burn 3,000 / 7,000 / 8,500 litres per block hour | order-of-magnitude market values | `structured_gen.py`, BuildCurated evidence rules | the invoice and fuel evidence for flight matching would need new values | open |
| A05 | AED is pegged at 3.6725 per USD | the official peg | invoices, `fact_invoice.total_usd` | none while the peg holds | checked (official peg) |
| A06 | Payment terms NET15 / NET30 / NET45 / PREPAID | common terms | `fact_invoice`, report section 7 | ageing buckets change | open |
| A07 | A clearance permit is needed for dangerous goods, pharma, valuables and vehicles, or on routes to and from KBL, MGQ, JUB and BGW | simplified view of overflight and landing permit practice | `documents_gen.py`, `flight_documents.permit_required` | the compliance check would flag different flights | open |
| A08 | Times are UAE local (UTC+4, no daylight saving) in the source systems; the lake stores UTC | UAE has had no DST since 1970 | preprocessing PP-07, Cleanse, `Rules.sentLocalDate` | dates near midnight would shift by a day | checked (Hive type test) |
| A09 | Reporting month = the UAE calendar date of departure (`date_key`) | management reports follow local days | `fact_flight.date_key`, monthly report | flights near midnight UTC move month | design |
| A10 | `margin_usd` = revenue minus direct costs tied to the flight; overheads (crew, leases, insurance) are not allocated | only direct costs carry flight evidence | `fact_flight`, report | margin reads as profit if this is not stated | design |

## B. Data and its errors

| ID | Assumption | Why | Used in | If wrong | Status |
|---|---|---|---|---|---|
| B01 | All data is synthetic, from seed 312, period 2023-11-01 to 2026-10-31 (36 months) | no access to real GSS data; reproducible | all generators | the project shows the method, not GSS facts | design |
| B02 | Planted errors represent the real problems of each system: encodings (UTF-8 BOM, Windows-1252), CRLF, padded and lower-case codes, null tokens, bad dates, duplicates, broken JSON, missing fields | typical of CSV exports and messaging APIs | generators, PP-01..PP-14 | other real errors would need new rules | checked: 700 + 12 + 1,709 + 1,213 planted errors all handled as intended |
| B03 | Null tokens are `N/A`, `NA`, `NULL`, `null` and `-` | common export conventions | `schemas.json`, LinkDocuments manifest AWB filter | other tokens would pass as values | open |
| B04 | Invoices and PO lines often lack a clean flight reference; they carry free text (memo, item description) with a date, route, registration or flight number | the problem the flight-reference repair addresses | `structured_gen.py`, BuildCurated | if references were clean, the repair is not needed | design |
| B05 | One shipment per flight; the manifest's AWB identifies the flight | charter operations: one customer per flight | LinkDocuments `awb_manifest` | shared flights would need weight splits | open |

## C. Interpreting text

| ID | Assumption | Why | Used in | If wrong | Status |
|---|---|---|---|---|---|
| C01 | In customer messages, "Dubai" alone means DWC, the GSS home base; DXB is written as DXB | GSS operates from DWC | `airports.txt`, `Rules.scala` | origin wrong for Dubai International requests | checked: 100% origin accuracy on the synthetic data (generator follows the same rule) |
| C02 | In finance and operations free text, "Dubai" may mean DWC or DXB, and the evidence decides | staff write the city, not the airport | `BuildCurated.cityOf` | wrong matches where both fit | checked: invoice precision 99.8% (twin) |
| C03 | "ADD" in lower case is Addis Ababa only next to a route marker (`dwc/add`, `into add`) | "add" is an English word | `airports.txt`, `Rules.route` | missed or false ADD routes | checked: 100% on the synthetic data |
| C04 | Ready-date rules: dd/mm = next such date on or after the sent date; "on the 12th" = this month or next; a weekday = the next one strictly after; ASAP / urgent = the sent date; "next week" = the following Monday | how dispatchers read requests | `date_phrases.txt`, `Rules.readyDate` | ready dates shift | checked: 100% on the synthetic data |
| C05 | A date right after "discussed", "phone", "call", "spoke", "our ref" or "invoice" is not the ready date | such dates refer to earlier events | `date_phrases.txt` | phone-call dates taken as ready dates | checked |
| C06 | One typo is tolerated in cargo words of 5+ letters (dropped, added, changed or swapped letter); short words must match exactly | messages are typed in a hurry; "kg cargo" must not read as "dg cargo" | `cargo_types.txt`, `Rules.phraseValue` | cargo missed or misread | checked: cargo accuracy 99.3%; the rest are requests with no cargo named, where a handling note implied one |
| C07 | A message is a request if it is inbound, not from a GSS address, uses a request word and names a destination, cargo or weight | separates requests from tracking, invoices and spam | `Rules.extract` | requests missed or noise counted | checked: precision and recall 100% (twin) |
| C08 | Defect notes start with the system; the most specific component term wins; the highest severity term wins; the maintenance system's own severity overrides the rules where present | maintenance writing conventions | `defect_terms.txt`, DefectClassify | wrong classes | checked: 85/85 agreement with recorded severities; 100% vs truth, optimistic (shared vocabulary) |
| C09 | Flight-reference evidence: a charter-fee quantity of 2 x block hours at the rate card price, fuel litres within 0.93 to 1.12 of burn x block hours, a supplier's station on the route | ties invoices and POs to flights without a flight number | BuildCurated | lower recall | checked: precision 99.7-99.8%, recall 91-93% (twin) |
| C10 | Document links, in order: valid AWB in the manifest; flight number in LION; valid AWB printed on a linked air waybill; the one flight named in the message thread; for packing lists, the one air waybill with the same pieces and weight within 4 days | strongest evidence first | LinkDocuments | wrong or missing links | checked: flight precision 99.8%, recall 99.6% (twin) |

## D. Technical choices

| ID | Assumption / choice | Why | If changed | Status |
|---|---|---|---|---|
| D01 | Spark runs `local[2]`, jobs through `spark-shell -i` | no sbt on the VM, small data | with sbt: jars, UDFs, YARN cluster mode | design |
| D02 | Data on the VM disk (`GSS_STAGING`), code in the shared folder | the shared folder was far too slow for tens of thousands of small files (preprocessing stalled) | — | checked (Step 12) |
| D03 | Messages and documents are packed into JSONL bundles (per channel and run; per type and month) | the small-files problem: 6,111 documents became 146 files | per-file processing would be slow on HDFS | checked |
| D04 | Release to HDFS through `/lake/landing/_incoming` and one `mv` | the stream must never see a half-written file | partial reads | design |
| D05 | Streaming with `foreachBatch`, at-least-once output, readers deduplicate by key | simple and restartable | exactly-once would need idempotent writes per batch | design |
| D06 | Hive external tables, DDL generated from the Parquet schema | types must match exactly; tables survive rebuilds | hand-typed DDL breaks at query time | checked: 19/19 tables |
| D07 | HBase row key `sender handle # reversed time # msg id` | a customer's latest requests by one prefix scan | other questions (by route, by date) would need other tables | design |
| D08 | Hadoop-side Java runs in UTC (`-Duser.timezone=UTC`) | Hive otherwise shows stored UTC times in the VM's zone (+11 h) | displayed times shift | checked (type test) |
| D09 | One Spark job at a time (lock in `run_spark.sh`) | 2 cores, 7.8 GB RAM shared with HDFS, Hive and HBase | memory pressure, swapping | design |
