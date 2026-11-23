"""
pp_structured.py - preprocessing of the NetSuite, Lion and maintenance CSV exports.

For every table in preprocess/config/schemas.json ("structured" section):
  PP-01..03  via pp_common: new files only, clean UTF-8, consistent column count
  PP-04      header must equal the expected column list, or the file is rejected
  PP-06      trims spaces; N/A, NA, NULL, null and - become empty
  PP-05      required values present; int and decimal parse; positive where required;
             values in the allowed list; row rules (e.g. due_date not before
             invoice_date). Failing rows are rejected with a reason
  PP-07      dates and timestamps become ISO 8601; unparseable values reject the row
  PP-08      identifiers standardised (A6GSA -> A6-GSA, codes upper case); values
             that still do not match their pattern are kept but flagged in _dq_flags
  PP-09      rows repeated on the primary key within a file are dropped
  PP-14      via pp_common: _dq_flags, _src_file and _run_id added, file released

In the quality report, PP-06 and PP-08 count a value as "failed" when it had to be
fixed; such rows are still accepted.

Free-text columns (memo, item_desc, description, cargo_desc) are only trimmed:
their wording is left for the lake to interpret.

Accepted files keep the original columns in the original order, then
_dq_flags, _src_file, _run_id.

Python 3.4, standard library only. Run from the project folder:
  python3 preprocess/pp_structured.py              release to HDFS (Hadoop must be running)
  python3 preprocess/pp_structured.py --no-hdfs    test locally, nothing sent to HDFS
"""

import os
import re
import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pp_common as pc

MONTHS = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
          "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}


def parse_date(value, formats):
    """strptime for each format; %b is matched against English month names here so
    the result does not depend on the VM's locale."""
    for fmt in formats:
        v, f = value, fmt
        if "%b" in fmt:
            m = re.search(r"[A-Za-z]{3}", value)
            if not m or m.group(0).lower() not in MONTHS:
                continue
            v = value[:m.start()] + "{:02d}".format(MONTHS[m.group(0).lower()]) + value[m.end():]
            f = fmt.replace("%b", "%m")
        try:
            return datetime.strptime(v, f)
        except ValueError:
            continue
    return None


def standardise(value, how):
    if how == "upper":
        return value.upper()
    if how == "registration":
        v = value.upper().replace("-", "").replace(" ", "")
        return v[:2] + "-" + v[2:] if len(v) > 2 else v
    return value


def clean_row(run, item, line_no, raw, spec, types, nulls):
    """Returns (clean_values, flags) or None if the row is rejected."""
    out, flags = [], []
    for col, value in zip(spec["columns"], raw):
        name = col["name"]
        v = value.strip()                                              # PP-06
        if v in nulls:
            v = ""
        run.check("PP-06", v == value)            # "failed" here means the cell needed fixing
        if v == "":
            if col.get("required"):
                run.check("PP-05", False)
                run.reject_row(item, line_no, "PP-05", "required value missing: {}".format(name), ",".join(raw))
                return None
            out.append("")
            continue
        kind = types[col["type"]]["kind"]
        if kind in ("date", "timestamp"):                               # PP-07
            d = parse_date(v, types[col["type"]]["input_formats"])
            if d is None:
                run.check("PP-07", False)
                run.reject_row(item, line_no, "PP-07", "unparseable date in {}: {}".format(name, v), ",".join(raw))
                return None
            run.check("PP-07", True)
            v = d.strftime(types[col["type"]]["output_format"])
        elif kind in ("integer", "decimal"):                           # PP-05 types
            try:
                n = int(v) if kind == "integer" else Decimal(v)
            except (ValueError, InvalidOperation):
                run.check("PP-05", False)
                run.reject_row(item, line_no, "PP-05", "not a number in {}: {}".format(name, v), ",".join(raw))
                return None
            if col.get("positive") and n <= 0:
                run.check("PP-05", False)
                run.reject_row(item, line_no, "PP-05", "{} must be positive: {}".format(name, v), ",".join(raw))
                return None
            v = str(n) if kind == "integer" else str(n.quantize(Decimal("0.01")))
        if "standardise" in col:                                       # PP-08
            v2 = standardise(v, col["standardise"])
            run.check("PP-08", v2 == v)           # "failed" = value had to be standardised
            v = v2
        if "allowed" in col:                                           # PP-05 allowed values
            if v not in col["allowed"]:
                run.check("PP-05", False)
                run.reject_row(item, line_no, "PP-05", "{} not allowed in {}".format(v, name), ",".join(raw))
                return None
        if "pattern" in col and not re.match(col["pattern"], v):       # PP-08 flag, keep row
            run.check("PP-08", False)
            flags.append(name)
        run.check("PP-05", True)
        out.append(v)
    by_name = dict((c["name"], v) for c, v in zip(spec["columns"], out))
    for rule in spec.get("row_rules", []):                            # PP-05 row rules
        if rule["check"] == "date_not_before":
            a, b = by_name.get(rule["column"]), by_name.get(rule["other"])
            if a and b and a < b:
                run.check(rule["rule_id"], False)
                run.reject_row(item, line_no, rule["rule_id"], rule["reason"], ",".join(raw))
                return None
            run.check(rule["rule_id"], True)
    return out, flags


def process_table(run, name, spec, schemas):
    types = schemas["types"]
    nulls = set(schemas["null_tokens"])
    expected = [c["name"] for c in spec["columns"]]
    pk_idx = [expected.index(k) for k in spec["primary_key"]]
    subdir = spec["staging_dir"].replace("staging/", "", 1)
    for item in run.new_files(subdir, spec["file_patterns"]):
        text = run.read_text(item)                                     # PP-02
        if text is None:
            continue
        header, rows = run.parse_csv(item, text, len(expected))        # PP-03
        if header is None:
            continue
        if header != expected:                                         # PP-04
            run.check("PP-04", False)
            run.reject_file(item, "PP-04", "header {} does not match {}".format(header, expected))
            continue
        run.check("PP-04", True)
        keep, seen = [], set()
        for line_no, raw in rows:
            res = clean_row(run, item, line_no, raw, spec, types, nulls)
            if res is None:
                continue
            values, flags = res
            key = tuple(values[i] for i in pk_idx)
            if key in seen:                                            # PP-09
                run.check("PP-09", False)
                run.reject_row(item, line_no, "PP-09", "duplicate primary key {}".format("|".join(key)),
                               ",".join(raw))
                continue
            run.check("PP-09", True)
            seen.add(key)
            src, rid = run.lineage(item)                               # PP-14
            keep.append(values + [";".join(flags), src, rid])
        item.rows_ok = len(keep)
        run.accept(item, pc.csv_text(expected + ["_dq_flags", "_src_file", "_run_id"], keep))


def main():
    hdfs = pc.wants_hdfs(sys.argv)
    schemas = pc.load_schemas()
    run = pc.Run("structured", hdfs=hdfs)
    only = sys.argv[sys.argv.index("--table") + 1] if "--table" in sys.argv else None
    for name in sorted(schemas["structured"]):
        if only and name != only:
            continue
        process_table(run, name, schemas["structured"][name], schemas)
    run.finish()


if __name__ == "__main__":
    main()