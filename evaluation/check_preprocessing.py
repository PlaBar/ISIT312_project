"""
check_preprocessing.py - did preprocessing handle every planted error as designed?

Compares the generators' planted-error lists (generators/labels/planted_errors*.csv)
with what preprocessing produced (staging/accepted/, the reject logs in
staging/reports/). For every planted error it checks the expected outcome:

  fixed           row accepted with the problem repaired (spaces, null tokens,
                  lower-case codes, registration format, encoding)
  row rejected    row in the reject log under the expected rule
  dropped         duplicate removed under PP-09
  left for Spark  row accepted unchanged, because the problem needs other tables
                  (orphan keys, the same row in two monthly exports)

It also lists rejects that no planted error explains (false positives).
The printed table is ready for the report.

Python 3.4, standard library only. Run after preprocessing:
  python3 evaluation/check_preprocessing.py structured
  python3 evaluation/check_preprocessing.py events
  python3 evaluation/check_preprocessing.py messages
  python3 evaluation/check_preprocessing.py documents
"""

import csv
import glob
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STAGING = os.environ.get("GSS_STAGING") or os.path.join(ROOT, "staging")
LABELS = os.path.join(ROOT, "generators", "labels")

# planted error -> (expected outcome, rule id)
EXPECTED_STRUCTURED = {
    "duplicate_row":           ("dropped", "PP-09"),
    "padded_value":            ("fixed", "PP-06"),
    "null_token_optional":     ("fixed", "PP-06"),
    "null_token_required":     ("row rejected", "PP-05"),
    "lower_case_code":         ("fixed", "PP-08"),
    "unparseable_date":        ("row rejected", "PP-07"),
    "non_positive_amount":     ("row rejected", "PP-05"),
    "non_positive_weight":     ("row rejected", "PP-05"),
    "due_before_invoice":      ("row rejected", "PP-05"),
    "wrong_column_count":      ("row rejected", "PP-03"),
    "utf8_bom":                ("fixed", "PP-02"),
    "windows_1252_encoding":   ("fixed", "PP-02"),
    "orphan_flight_no":        ("left for Spark", "Cleanse"),
    "orphan_invoice_id":       ("left for Spark", "Cleanse"),
    "repeated_in_next_export": ("left for Spark", "Cleanse"),
}
SYSTEM_DIR = {"netsuite": "exports/netsuite", "lion": "exports/lion", "maint": "exports/maint"}
NULLS = {"N/A", "NA", "NULL", "null", "-"}


def read_csv(path):
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_rejects(source):
    out = []
    for p in sorted(glob.glob(os.path.join(STAGING, "reports", "reject_log_*_{}.csv".format(source)))):
        out.extend(read_csv(p))
    return out


def load_accepted(rel):
    path = os.path.join(STAGING, "accepted", rel)
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        rows = list(csv.reader(f))
    return rows


def check_structured():
    planted = read_csv(os.path.join(LABELS, "planted_errors.csv"))
    rejects = load_rejects("structured")
    by_file = {}
    for r in rejects:
        by_file.setdefault(r["file_name"], []).append(r)
    accepted_cache = {}

    def accepted(rel):
        if rel not in accepted_cache:
            accepted_cache[rel] = load_accepted(rel)
        return accepted_cache[rel]

    # a row that carries a rejecting error is rightly rejected, so its other errors
    # (padding, duplicates) are never "fixed" or "dropped": they count as handled if
    # every copy of the row is in the reject log
    rejecting = set()
    for p in planted:
        if EXPECTED_STRUCTURED[p["error"]][0] == "row rejected":
            rejecting.add((p["table"], p["file"], p["key"].strip()))

    results = {}
    explained = set()
    failures = []
    for p in planted:
        outcome, rule = EXPECTED_STRUCTURED[p["error"]]
        system = p["table"].split(".")[0]
        rel = "{}/{}".format(SYSTEM_DIR[system], p["file"])
        key = p["key"].strip()
        rows = accepted(rel)
        ok = False
        if outcome in ("fixed", "dropped") and (p["table"], p["file"], key) in rejecting:
            hits = [r for r in by_file.get(rel, []) if r["raw_value"].split(",")[0].strip() == key]
            ok = bool(hits) and (rows is None or not any(row and row[0] == key for row in rows[1:]))
            outcome = "row rejected (other error)"
        elif outcome == "row rejected" or outcome == "dropped":
            hits = [r for r in by_file.get(rel, []) if r["rule_id"] == rule
                    and r["raw_value"].split(",")[0].strip() == key]
            ok = bool(hits)
            for h in hits:
                explained.add((h["file_name"], h["row_or_line_no"]))
            if outcome == "dropped" and rows is not None:
                ok = ok and sum(1 for row in rows[1:] if row and row[0] == key) == 1
        elif outcome == "fixed":
            if rows is None:
                ok = False
            elif key == "":                         # file-level fix (encoding)
                ok = len(rows) > 1
            else:
                match = [row for row in rows[1:] if row and row[0] == key]
                ok = len(match) == 1 and all(c == c.strip() and c not in NULLS for c in match[0])
                if ok and p["error"] == "lower_case_code":
                    ok = not any(len(c) == 4 and c.isalpha() and c != c.upper() for c in match[0][:4])
        elif outcome == "left for Spark":
            ok = rows is not None and any(row and row[0] == key for row in rows[1:])
        res = results.setdefault(p["error"], [EXPECTED_STRUCTURED[p["error"]][0], rule, 0, 0])
        res[2] += 1
        res[3] += 1 if ok else 0
        if not ok:
            failures.append(p)

    # rejects of rows copied by a planted duplicate are explained by the original's error
    unexplained = [r for r in rejects if (r["file_name"], r["row_or_line_no"]) not in explained]
    planted_keys = set(((("{}/{}".format(SYSTEM_DIR[p["table"].split(".")[0]], p["file"])), p["key"].strip())
                        for p in planted))
    false_pos = [r for r in unexplained
                 if (r["file_name"], r["raw_value"].split(",")[0].strip()) not in planted_keys]

    print("Structured preprocessing vs planted errors")
    print("{:<26} {:<15} {:<8} {:>7} {:>9}".format("planted error", "expected", "rule", "planted", "as design"))
    total = good = 0
    for err in sorted(results):
        outcome, rule, n, k = results[err]
        total += n
        good += k
        print("{:<26} {:<15} {:<8} {:>7} {:>9}".format(err, outcome, rule, n, k))
    print("{:<26} {:<15} {:<8} {:>7} {:>9}".format("TOTAL", "", "", total, good))
    print("")
    print("Rejects in the logs:              {}".format(len(rejects)))
    print("Rejects not explained by a plant: {}".format(len(false_pos)))
    for r in false_pos[:10]:
        print("  {} line {} {} {}".format(r["file_name"], r["row_or_line_no"], r["rule_id"], r["reason"]))
    if failures:
        print("Planted errors not handled as designed:")
        for p in failures[:10]:
            print("  {} {} {} {}".format(p["table"], p["file"], p["key"], p["error"]))
    return 0 if good == total and not false_pos else 1


EXPECTED_EVENTS = {
    "truncated_json":     ("line rejected", "PP-03"),
    "missing_flight_no":  ("line rejected", "PP-05"),
    "unknown_event_type": ("line rejected", "PP-05"),
    "bad_timestamp":      ("line rejected", "PP-07"),
    "duplicate_line":     ("dropped", "PP-09"),
}


def check_events():
    import json
    planted = read_csv(os.path.join(LABELS, "planted_errors_events.csv"))
    rejects = load_rejects("events")
    accepted_ids = []
    for path in sorted(glob.glob(os.path.join(STAGING, "accepted", "events", "*.jsonl"))):
        with open(path, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    accepted_ids.append(json.loads(line)["event_id"])
    used = set()
    results, failures = {}, []
    for p in planted:
        outcome, rule = EXPECTED_EVENTS[p["error"]]
        rel = "events/" + p["file"]
        cands = [i for i, r in enumerate(rejects) if i not in used and r["file_name"] == rel
                 and r["rule_id"] == rule]
        # match on the event id where the line still shows it; a truncated line may not
        hit = [i for i in cands if '"{}"'.format(p["key"]) in rejects[i]["raw_value"]] or \
              ([i for i in cands if p["error"] == "truncated_json"][:1])
        ok = bool(hit)
        if hit:
            used.add(hit[0])
        if p["error"] == "duplicate_line":
            ok = ok and accepted_ids.count(p["key"]) == 1
        elif ok:
            ok = p["key"] not in accepted_ids
        res = results.setdefault(p["error"], [outcome, rule, 0, 0])
        res[2] += 1
        res[3] += 1 if ok else 0
        if not ok:
            failures.append(p)
    false_pos = [r for i, r in enumerate(rejects) if i not in used]
    dup_ids = len(accepted_ids) - len(set(accepted_ids))
    return report("Event preprocessing vs planted errors", results, rejects, false_pos, failures,
                  ["Accepted events:                  {}".format(len(accepted_ids)),
                   "Duplicate event ids accepted:     {}".format(dup_ids)], extra_fail=dup_ids > 0)


EXPECTED_MESSAGES = {
    "missing_body":         ("file rejected", "PP-10"),
    "bad_channel":          ("file rejected", "PP-10"),
    "truncated_json":       ("file rejected", "PP-03"),
    "sent_at_without_zone": ("fixed", "PP-07"),
    "local_phone_format":   ("fixed", "PP-11"),
    "upper_case_email":     ("fixed", "PP-11"),
    "duplicate_file":       ("dropped", "PP-01"),
}


def check_messages():
    import json
    import re
    from datetime import datetime, timedelta
    planted = read_csv(os.path.join(LABELS, "planted_errors_messages.csv"))
    rejects = load_rejects("messages")
    manifest = [r for r in read_csv(os.path.join(STAGING, "preprocess_manifest.csv"))
                if r["source_type"] == "messages"]
    status = dict((os.path.basename(r["file_name"]), r) for r in manifest)
    rejected_by = dict((os.path.basename(r["file_name"]), r["rule_id"]) for r in rejects)
    accepted = {}
    for ch in ("email", "whatsapp"):
        for path in glob.glob(os.path.join(STAGING, "accepted", "messages", ch, "*.jsonl")):
            with open(path, encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        m = json.loads(line)
                        accepted[m["msg_id"]] = m
    results, failures, explained = {}, [], set()
    for p in planted:
        outcome, rule = EXPECTED_MESSAGES[p["error"]]
        key, fname = p["key"], p["file"]
        ok = False
        if outcome == "file rejected":
            row = status.get(fname)
            ok = row is not None and row["status"] == "REJECTED" and rejected_by.get(fname) == rule \
                and key not in accepted
            explained.add(fname)
        elif outcome == "dropped":
            dup = fname.replace(".json", "_dup.json")
            row = status.get(dup)
            ok = row is not None and row["status"] == "DUPLICATE" and rejected_by.get(dup) == rule \
                and key in accepted
            explained.add(dup)
        elif outcome == "fixed" and key in accepted:
            m = accepted[key]
            h = m["sender"]["handle"]
            if p["error"] == "sent_at_without_zone":
                ok = "sent_at_zone_assumed" in m["_dq_flags"] and m["sent_at"].endswith("Z")
            elif p["error"] == "local_phone_format":
                ok = re.match(r"^\+9715\d{8}$", h) is not None
            else:
                ok = h == h.lower() and "@" in h
        res = results.setdefault(p["error"], [outcome, rule, 0, 0])
        res[2] += 1
        res[3] += 1 if ok else 0
        if not ok:
            failures.append(p)
    false_pos = [r for r in rejects if os.path.basename(r["file_name"]) not in explained]

    # every accepted message: sent_at in UTC = source time minus its offset
    bad_utc = 0
    for m in accepted.values():
        src = m["sent_at_source"]
        local = datetime.strptime(src[:19], "%Y-%m-%dT%H:%M:%S")
        off = src[19:] or "+04:00"
        delta = timedelta(hours=int(off[1:3]), minutes=int(off[4:6])) * (1 if off[0] == "+" else -1)
        if (local - delta).strftime("%Y-%m-%dT%H:%M:%SZ") != m["sent_at"]:
            bad_utc += 1
    n_files = len(manifest)
    return report("Message preprocessing vs planted errors", results, rejects, false_pos, failures,
                  ["Files processed:                  {}".format(n_files),
                   "Messages accepted:                {}".format(len(accepted)),
                   "Accepted with wrong UTC time:     {}".format(bad_utc)], extra_fail=bad_utc > 0)


EXPECTED_DOCUMENTS = {
    "empty_file":            ("file rejected", "PP-13"),
    "too_short":             ("file rejected", "PP-13"),
    "unknown_type":          ("file rejected", "PP-13"),
    "duplicate_content":     ("dropped", "PP-09"),
    "crlf_line_endings":     ("fixed", "PP-02"),
    "utf8_bom":              ("fixed", "PP-02"),
    "windows_1252":          ("fixed", "PP-02"),
    "messy_whitespace":      ("fixed", "PP-12"),
    "awb_check_digit_wrong": ("left for Spark", "Link"),
}


def check_documents():
    import json
    planted = read_csv(os.path.join(LABELS, "planted_errors_documents.csv"))
    truth = read_csv(os.path.join(LABELS, "documents_truth.csv"))
    rejects = load_rejects("documents")
    manifest = dict((r["file_name"], r) for r in read_csv(os.path.join(STAGING, "preprocess_manifest.csv"))
                    if r["source_type"] == "documents")
    rule_of = dict((r["file_name"], r["rule_id"]) for r in rejects)
    accepted = {}
    for path in glob.glob(os.path.join(STAGING, "accepted", "documents", "*", "*.jsonl")):
        with open(path, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    d = json.loads(line)
                    accepted[d["doc_file"]] = d
    results, failures, explained = {}, [], set()
    for p in planted:
        outcome, rule = EXPECTED_DOCUMENTS[p["error"]]
        rel = p["file"]
        row = manifest.get(rel)
        ok = False
        if outcome == "file rejected":
            # identical junk files (e.g. two empty files): the first is rejected under PP-13,
            # the rest are caught earlier as byte-identical duplicates under PP-01
            ok = row is not None and rel not in accepted and (
                (row["status"] == "REJECTED" and rule_of.get(rel) == rule) or
                (row["status"] == "DUPLICATE" and rule_of.get(rel) == "PP-01"))
            explained.add(rel)
        elif outcome == "dropped":
            ok = row is not None and row["status"] == "DUPLICATE" and rule_of.get(rel) in ("PP-09", "PP-01") \
                and rel not in accepted and p["key"] in accepted
            explained.add(rel)
        elif outcome == "fixed":
            d = accepted.get(rel)
            if d is not None:
                t = d["text"]
                ok = "\r" not in t and not t.startswith("\ufeff")
                if p["error"] == "messy_whitespace":
                    ok = ok and all(line == line.rstrip() for line in t.split("\n")) and "\n\n\n" not in t
                if p["error"] == "windows_1252":
                    ok = ok and "2-8\u00b0C" in t
        elif outcome == "left for Spark":
            ok = rel in accepted
        res = results.setdefault(p["error"], [outcome, rule, 0, 0])
        res[2] += 1
        res[3] += 1 if ok else 0
        if not ok:
            failures.append(p)
    false_pos = [r for r in rejects if r["file_name"] not in explained]
    missing = [t["doc_file"] for t in truth if t["doc_file"] not in accepted]
    wrong_type = [t["doc_file"] for t in truth if t["doc_file"] in accepted
                  and accepted[t["doc_file"]]["doc_type"] != t["doc_type"]]
    attach = read_csv(os.path.join(LABELS, "message_attachments.csv"))
    unresolved = [a["rel_path"] for a in attach if a["rel_path"] not in accepted]
    return report("Document preprocessing vs planted errors", results, rejects, false_pos, failures,
                  ["Real documents in the truth file: {}".format(len(truth)),
                   "Accepted documents:               {}".format(len(accepted)),
                   "Real documents not accepted:      {}".format(len(missing)),
                   "Accepted with the wrong type:     {}".format(len(wrong_type)),
                   "Message attachments not found:    {}".format(len(unresolved))],
                  extra_fail=bool(missing or wrong_type or unresolved))


def report(title, results, rejects, false_pos, failures, extra=(), extra_fail=False):
    print(title)
    print("{:<26} {:<15} {:<8} {:>7} {:>9}".format("planted error", "expected", "rule", "planted", "as design"))
    total = good = 0
    for err in sorted(results):
        outcome, rule, n, k = results[err]
        total += n
        good += k
        print("{:<26} {:<15} {:<8} {:>7} {:>9}".format(err, outcome, rule, n, k))
    print("{:<26} {:<15} {:<8} {:>7} {:>9}".format("TOTAL", "", "", total, good))
    print("")
    for line in extra:
        print(line)
    print("Rejects in the logs:              {}".format(len(rejects)))
    print("Rejects not explained by a plant: {}".format(len(false_pos)))
    for r in false_pos[:10]:
        print("  {} line {} {} {}".format(r["file_name"], r["row_or_line_no"], r["rule_id"], r["reason"]))
    if failures:
        print("Planted errors not handled as designed:")
        for p in failures[:10]:
            print("  {} {} {} {}".format(p["table"], p["file"], p["key"], p["error"]))
    return 0 if good == total and not false_pos and not extra_fail else 1


CHECKS = {"structured": check_structured, "events": check_events, "messages": check_messages,
          "documents": check_documents}


def main():
    what = sys.argv[1] if len(sys.argv) > 1 else "structured"
    if what not in CHECKS:
        sys.exit("unknown source: {} (available: {})".format(what, ", ".join(sorted(CHECKS))))
    sys.exit(CHECKS[what]())


if __name__ == "__main__":
    main()