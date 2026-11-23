# GSS Data Lake: Data Dictionary

All data is synthetic. Tags: [R] reported by GSS, [A] project assumption, [I] industry practice.
Period: 36 months, 2023-11-01 to 2026-10-31 [A]. Time zone of source systems: UAE, UTC+04:00 [A].

## 1. Identifier formats [A]

| Entity | Format | Example | Notes |
|---|---|---|---|
| Customer | CUS-NNNN | CUS-0042 | Customers and brokers |
| Supplier | SUP-NNN | SUP-007 | |
| Invoice | INV-NNNNNN | INV-000315 | |
| Invoice line | IL-NNNNNNN | IL-0001027 | |
| Payment | PAY-NNNNNN | PAY-000288 | |
| Purchase order | PO-NNNNNN | PO-000911 | |
| PO line | POL-NNNNNNN | POL-0002410 | |
| Aircraft (Lion) | ACNN | AC02 | |
| Registration, standard | A6-GSX | A6-GSB | Lion form, used in the lake |
| Registration, maintenance | A6GSX | A6GSB | No hyphen; fixed by PP-08 |
| Flight | GSNNNN | GS0412 | Unique per flight, GS0001 to GS1440 |
| Manifest line | MF-NNNNNN | MF-001233 | |
| Air waybill | 176-NNNNNNNN | 176-12345675 | 8th digit = first 7 digits mod 7 [I] |
| Maintenance event | ME-NNNNN | ME-00087 | |
| Defect | DEF-NNNNN | DEF-00213 | |
| Message | M-YYYY-NNNNNN | M-2024-000123 | |
| Thread | T-YYYY-NNNNNN | T-2024-000045 | One thread per request conversation |
| Request (truth) | R-YYYY-NNNNN | R-2024-00031 | Only in labels/ |
| Operational event | E-NNNNNN | E-000981 | |
| Quotation | Q-YYYY-NNNN | Q-2024-0118 | |
| Permit | PRM-YYYY-NNNN | PRM-2024-0052 | |
| Shipment reference | SHP-YYYY-NNNN | SHP-2024-0090 | Packing lists |

## 2. Reference data [A]

Fleet: 4 freighters. A6-GSA and A6-GSB B737-400F (max payload 19,000 kg), A6-GSC A300B4-200F (40,000 kg), A6-GSD IL-76TD (42,000 kg).
Home base: Dubai World Central (DWC/OMDW). Other UAE: DXB/OMDB, SHJ/OMSJ.
Destinations: NBO/HKJK, ADD/HAAB, EBB/HUEN, JUB/HJJJ, MGQ/HCMM, FIH/FZAA, LOS/DNMM, KBL/OAKB, BGW/ORBI, EBL/ORER, TAS/UTTT, KHI/OPKC, DAC/VGHS.
Cargo types: GENERAL, PERISHABLE, PHARMA, DANGEROUS_GOODS, HUMANITARIAN, AOG_SPARES, OIL_GAS_EQUIPMENT, VEHICLES, VALUABLES.
Special handling codes [I]: PER (perishable), PIL (pharma), DGR (dangerous goods), VAL (valuables), HEA (heavy), AOG (aircraft on ground).

## 3. Volumes per month (calibrated)

| Item | Per month | 36 months | Tag |
|---|---|---|---|
| Requests | 300 | 10,800 | [R] |
| Flights | 40 | 1,440 | [R] |
| Shipments | 40 | 1,440 | [R] |
| Messages | about 1,200 | about 43,000 | [A] |
| Shipment documents | about 160 | about 5,800 | [A] |
| Invoices | about 40 (one per flight, plus some ad hoc) | about 1,500 | [A] |
| Purchase orders | about 120 (fuel, handling, navigation per flight) | about 4,300 | [A] |

## 4. Structured sources (CSV, comma separated, header row, UTF-8 unless planted otherwise)

Staging path: staging/exports/<system>/<table>.csv. Type codes: str, int, dec(2) = decimal with 2 places, date_dmy = DD/MM/YYYY, date_iso = YYYY-MM-DD, date_mon = DD-Mon-YYYY, ts_lion = YYYY-MM-DD HH:MM.

### 4.1 NetSuite (staging/exports/netsuite/)

customer.csv (primary key customer_id)
| Column | Type | Example | Notes |
|---|---|---|---|
| customer_id | str | CUS-0042 | |
| name | str | Gulf Freight LLC | Some trailing spaces |
| country | str | United Arab Emirates | Full country name |
| payment_terms | str | NET30 | NET15, NET30, NET45, PREPAID |

supplier.csv (supplier_id)
| Column | Type | Example | Notes |
|---|---|---|---|
| supplier_id | str | SUP-007 | |
| name | str | Skyline Fuels FZE | |
| category | str | FUEL | FUEL, GROUND_HANDLING, NAVIGATION, AIRPORT_CHARGES, CATERING, MAINTENANCE, CREW_HOTEL, INSURANCE |
| country | str | Kenya | |

invoice.csv (invoice_id)
| Column | Type | Example | Notes |
|---|---|---|---|
| invoice_id | str | INV-000315 | |
| customer_id | str | CUS-0042 | Must exist in customer (Spark check) |
| invoice_date | date_dmy | 12/03/2024 | |
| due_date | date_dmy | 11/04/2024 | Must not precede invoice_date (PP-05) |
| currency | str | USD | USD or AED |
| total | dec(2) | 48500.00 | Sum of invoice_line.amount |
| status | str | PAID | OPEN, PARTIAL, PAID, VOID |
| memo | str | Charter DWC-NBO 12/03 A6-GSB | Free-text flight reference, no flight number |

invoice_line.csv (line_id)
| Column | Type | Example | Notes |
|---|---|---|---|
| line_id | str | IL-0001027 | |
| invoice_id | str | INV-000315 | |
| description | str | Charter fee | Charter fee, Fuel surcharge, Handling, Overflight, DG surcharge, Insurance |
| qty | dec(2) | 1.00 | Positive |
| unit_price | dec(2) | 42000.00 | Positive |
| amount | dec(2) | 42000.00 | qty x unit_price |

payment.csv (payment_id)
| Column | Type | Example | Notes |
|---|---|---|---|
| payment_id | str | PAY-000288 | |
| invoice_id | str | INV-000315 | Must exist in invoice (Spark check) |
| paid_date | date_dmy | 02/04/2024 | |
| amount | dec(2) | 48500.00 | Positive |
| method | str | BANK_TRANSFER | BANK_TRANSFER, CHEQUE, CARD |

purchase_order.csv (po_id)
| Column | Type | Example | Notes |
|---|---|---|---|
| po_id | str | PO-000911 | |
| supplier_id | str | SUP-007 | |
| po_date | date_dmy | 10/03/2024 | |
| status | str | RECEIVED | OPEN, RECEIVED, CLOSED, CANCELLED |
| total | dec(2) | 9120.00 | |

po_line.csv (po_line_id)
| Column | Type | Example | Notes |
|---|---|---|---|
| po_line_id | str | POL-0002410 | |
| po_id | str | PO-000911 | |
| item_desc | str | Jet A1 uplift NBO 12-Mar-24 | Free-text route and date |
| qty | dec(2) | 12000.00 | Positive |
| unit_cost | dec(2) | 0.76 | Positive |
| amount | dec(2) | 9120.00 | qty x unit_cost |

### 4.2 Lion (staging/exports/lion/)

airport.csv (icao_code)
| Column | Type | Example | Notes |
|---|---|---|---|
| icao_code | str | HKJK | Some lower case (PP-08) |
| iata_code | str | NBO | |
| name | str | Jomo Kenyatta International | |
| country | str | Kenya | |

aircraft.csv (aircraft_id)
| Column | Type | Example | Notes |
|---|---|---|---|
| aircraft_id | str | AC02 | |
| registration | str | A6-GSB | |
| type | str | B737-400F | |
| max_payload_kg | int | 19000 | |

flight.csv (flight_no)
| Column | Type | Example | Notes |
|---|---|---|---|
| flight_no | str | GS0412 | |
| aircraft_id | str | AC02 | |
| origin_icao | str | OMDW | |
| dest_icao | str | HKJK | |
| sched_dep | ts_lion | 2024-03-12 06:30 | Local UAE time |
| sched_arr | ts_lion | 2024-03-12 11:10 | |
| actual_dep | ts_lion | 2024-03-12 07:05 | Empty if cancelled |
| actual_arr | ts_lion | 2024-03-12 11:52 | Empty if cancelled |
| status | str | ARRIVED | SCHEDULED, DEPARTED, ARRIVED, CANCELLED, DIVERTED |

manifest.csv (manifest_id)
| Column | Type | Example | Notes |
|---|---|---|---|
| manifest_id | str | MF-001233 | |
| flight_no | str | GS0412 | Must exist in flight (Spark check) |
| cargo_desc | str | Pharmaceuticals 2-8C | |
| weight_kg | dec(2) | 2500.00 | Positive |
| volume_m3 | dec(2) | 14.20 | Positive |
| consignee | str | Nairobi Medical Supplies | |
| awb_no | str | 176-12345675 | Sometimes empty |

### 4.3 Maintenance programme (staging/exports/maint/)

aircraft_ref.csv (registration)
| Column | Type | Example | Notes |
|---|---|---|---|
| registration | str | A6GSB | Maintenance form, no hyphen |
| type | str | B737-400F | |

maintenance_event.csv (event_id)
| Column | Type | Example | Notes |
|---|---|---|---|
| event_id | str | ME-00087 | |
| registration | str | A6GSB | |
| event_type | str | A_CHECK | A_CHECK, C_CHECK, LINE_CHECK, ENGINE_INSPECTION, COMPONENT_CHANGE |
| due_date | date_mon | 15-Mar-2024 | |
| done_date | date_mon | 14-Mar-2024 | Empty if not done |
| flight_hours_at | dec(2) | 31250.50 | |
| cycles_at | int | 18402 | |
| status | str | DONE | PLANNED, DONE, OVERDUE |

defect.csv (defect_id)
| Column | Type | Example | Notes |
|---|---|---|---|
| defect_id | str | DEF-00213 | |
| registration | str | A6GSB | |
| reported_date | date_mon | 12-Mar-2024 | |
| description | str | hyd press low LH sys, topped up, monitor | Free text, see 6.3 |
| severity | str | MEDIUM | LOW, MEDIUM, HIGH, AOG; sometimes empty |
| resolved | str | Y | Y or N |

### 4.4 Planted structured errors [A]

| Error | Where | Approx. rate | Caught by |
|---|---|---|---|
| Null tokens N/A, NULL, - | All tables | 2% of cells | PP-06 fixes |
| Leading or trailing spaces | Names, ids | 3% | PP-06 fixes |
| Registration without hyphen | maint tables | 100% | PP-08 fixes |
| Lower-case airport code | airport, flight | 2% | PP-08 fixes |
| Unparseable date | invoice, flight | 0.5% | PP-07 rejects row |
| Negative or zero amount, weight | invoice_line, manifest | 0.5% | PP-05 rejects row |
| due_date before invoice_date | invoice | 0.5% | PP-05 rejects row |
| Exact duplicate row | All tables | 1% | PP-09 drops |
| Wrong column count | One file per run | rare | PP-03 rejects line |
| Windows-1252 file with BOM | One NetSuite export | 1 file | PP-02 converts |
| Orphan foreign key | payment, manifest | 0.5% | Spark Cleanse, quarantine |
| Same row in two monthly exports | invoice | 1% | Spark Cleanse, cross-load dedupe |
| Memo or item text in variant forms | invoice.memo, po_line.item_desc | 100% | Spark BuildCurated repair |
| Memo with no route | invoice.memo | 5% | Spark, quarantine unmatched |

## 5. Semi-structured sources

### 5.1 Message envelope (staging/messages/<channel>/<msg_id>.json, one file per message)

| Field | Type | Required | Example | Notes |
|---|---|---|---|---|
| msg_id | str | yes | M-2024-000123 | PP-10 |
| thread_id | str | no | T-2024-000045 | |
| channel | str | yes | email | email or whatsapp (PP-10) |
| direction | str | no | inbound | inbound or outbound |
| sent_at | str | yes | 2024-03-12T08:41:07+04:00 | PP-07 converts to UTC |
| sender.display | str | no | Ahmed (Gulf Freight) | |
| sender.handle | str | no | ahmed@example.com | PP-11: lower case email, +971 phone |
| to, cc | list | no | ["requests@gss.example"] | Email only |
| subject | str | no | Cargo request DWC-NBO | null on WhatsApp |
| message_id, in_reply_to | str | no | <abc123@mail.example> | Email only |
| body | str | yes | free text | See 6.1 |
| attachments | list | no | ["documents/2024/03/PACKING/packing_20240312_004.txt"] | |
| language | str | no | en | Always en |

Planted envelope errors: missing body (reject), channel "sms" (reject), sent_at without zone (assume +04:00), phone as 050 1234567 (PP-11 fixes), duplicate file with same msg_id (PP-09 drops), truncated JSON (PP-03 rejects).

### 5.2 Operational event (staging/events/events_<yyyymm>.jsonl, one JSON object per line) [A]

| Field | Type | Required | Example |
|---|---|---|---|
| event_id | str | yes | E-000981 |
| flight_no | str | yes | GS0412 |
| event_type | str | yes | DELAY, AIRCRAFT_SWAP, DIVERSION, CANCELLATION, AOG |
| event_time | str | yes | 2024-03-12T14:10:00 (UAE local) |
| detail | str | no | awaiting ground handler |
| source | str | no | lion or ops_chat |

## 6. Unstructured content

### 6.1 Message bodies (inside body)
Requests use informal English with these variants:
- Weight: 2500 kg, 2,500kg, 2.5T, 2.5 tons, 2500 kilos
- Route: DWC-NBO, DXB to Nairobi, Dubai > Nairobi, from Dubai to NBO
- Date: 12/03, 12 March, Thursday, next week, ASAP
- Cargo: words from the cargo dictionary, with typos (perishibles, pharma, DG)
- Noise: greetings, signatures, disclaimers, missing fields
Non-requests: greetings, invoice chasers, spam, internal notes (about 25% of messages [A]).

### 6.2 Shipment documents (staging/documents/<yyyy>/<mm>/<TYPE>/<type>_<yyyymmdd>_<seq>.txt)
First line names the type (PP-13): QUOTATION, AIR WAYBILL, PACKING LIST, CLEARANCE PERMIT. Layouts as in Summary 7.4. File names carry only type and date.

### 6.3 Defect notes (defect.description)
Pattern: <system abbreviation> <fault>, <action taken>, <status>. Example: hyd press low LH sys, topped up, monitor.
Systems [I]: hyd, elec, eng, apu, ldg gear, avionics, fuel, press, flt ctrl.

## 7. Lineage and preprocessing records

Added by PP-14 to every accepted CSV row and JSON message: _src_file (staging file name), _run_id (RUN-YYYYMMDD-HHMMSS).

| Record | File | Columns |
|---|---|---|
| preprocess_manifest | staging/preprocess_manifest.csv | run_id, file_name, source_type, sha256, size_bytes, rows_in, rows_accepted, rows_rejected, status, released_to, processed_at |
| reject_log | staging/reports/reject_log_<run_id>.csv | run_id, file_name, row_or_line_no, rule_id, reason, raw_value |
| quality_report | staging/reports/quality_<run_id>.csv | run_id, source_type, rule_id, checked, passed, failed, pct_failed |

## 8. Hidden truth files (generators/labels/, read only by evaluation/)

| File | Columns |
|---|---|
| requests_truth.csv | msg_id, thread_id, is_request, request_id, origin_iata, dest_iata, cargo_type, weight_kg, ready_date, special_handling |
| documents_truth.csv | doc_file, doc_type, awb_no, flight_no, po_id, shipment_ref |
| flight_ref_truth.csv | source_system, source_ref, flight_no |
| defect_truth.csv | defect_id, system_class, component, severity_class |

## 9. Lake tables

Processed and curated tables follow Summary 7.2 (dims and facts) and 7.5 (derived tables). Canonical keys:
- flight_key = flight_no after repair, aircraft_key = registration_std, customer_key = customer_id, shipment_key = awb_no.
- date_key = YYYYMMDD integer.
- Every curated row carries src_ref (file name or msg_id).
