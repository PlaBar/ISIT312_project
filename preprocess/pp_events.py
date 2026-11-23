"""
pp_events.py - preprocessing of the operational event log (JSON lines).

For every staging/events/events_*.jsonl file:
  PP-01..03  via pp_common: new files only, clean UTF-8, every line valid JSON
  PP-05      required fields present (event_id, flight_no, event_type, event_time);
             event_type in the allowed list (schemas.json "events")
  PP-06      strings trimmed; N/A, NULL, - become empty
  PP-07      event_time must be ISO 8601 (YYYY-MM-DDTHH:MM:SS, UAE local time)
  PP-08      flight_no upper case without spaces; values not matching GS0000 flagged
  PP-09      an event_id seen before (in this run or an earlier one) is dropped
  PP-14      via pp_common: _dq_flags, _src_file, _run_id added; file released to
             /lake/landing/exports/events/

The detail field is free text and is only trimmed.
Accepted lines keep the field order event_id, flight_no, event_type, event_time,
detail, source, then _dq_flags, _src_file, _run_id.

Python 3.4, standard library only. Run from the project folder:
  python3 preprocess/pp_events.py              release to HDFS (Hadoop must be running)
  python3 preprocess/pp_events.py --no-hdfs    test locally, nothing sent to HDFS
"""

import json
import os
import re
import sys
from collections import OrderedDict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pp_common as pc

FIELDS = ["event_id", "flight_no", "event_type", "event_time", "detail", "source"]
FLIGHT_PATTERN = r"^GS\d{4}$"


def clean_event(run, item, line_no, obj, spec, nulls, seen):
    """Returns an OrderedDict ready to write, or None if the line is rejected."""
    raw = json.dumps(obj)[:300]
    ev = {}
    for f in FIELDS:                                                   # PP-06
        v = obj.get(f, "")
        v = "" if v is None else str(v)
        t = v.strip()
        if t in nulls:
            t = ""
        run.check("PP-06", t == v)
        ev[f] = t
    for f in spec["required_fields"]:                                  # PP-05 required
        if not ev.get(f):
            run.check("PP-05", False)
            run.reject_row(item, line_no, "PP-05", "required field missing: {}".format(f), raw)
            return None
    if ev["event_type"] not in spec["allowed_event_types"]:            # PP-05 allowed
        run.check("PP-05", False)
        run.reject_row(item, line_no, "PP-05", "unknown event_type: {}".format(ev["event_type"]), raw)
        return None
    run.check("PP-05", True)
    parsed = None                                                      # PP-07
    for fmt in spec["timestamp_input_formats"]:
        try:
            parsed = datetime.strptime(ev["event_time"], fmt)
            break
        except ValueError:
            continue
    if parsed is None:
        run.check("PP-07", False)
        run.reject_row(item, line_no, "PP-07", "unparseable event_time: {}".format(ev["event_time"]), raw)
        return None
    run.check("PP-07", True)
    ev["event_time"] = parsed.strftime(spec["timestamp_output_format"])
    flags = []
    fno = ev["flight_no"].upper().replace(" ", "")                     # PP-08
    run.check("PP-08", fno == ev["flight_no"])
    ev["flight_no"] = fno
    if not re.match(FLIGHT_PATTERN, fno):
        flags.append("flight_no")
    if ev["event_id"] in seen:                                         # PP-09
        run.check("PP-09", False)
        run.reject_row(item, line_no, "PP-09", "duplicate event_id {}".format(ev["event_id"]), raw)
        return None
    run.check("PP-09", True)
    seen.add(ev["event_id"])
    src, rid = run.lineage(item)                                       # PP-14
    out = OrderedDict((f, ev[f]) for f in FIELDS)
    out["_dq_flags"] = ";".join(flags)
    out["_src_file"] = src
    out["_run_id"] = rid
    return out


def main():
    hdfs = pc.wants_hdfs(sys.argv)
    schemas = pc.load_schemas()
    spec = schemas["events"]
    nulls = set(schemas["null_tokens"])
    run = pc.Run("events", hdfs=hdfs)
    seen = pc.SeenKeys(run.root, "event_id")
    subdir = spec["staging_dir"].replace("staging/", "", 1)
    for item in run.new_files(subdir, [spec["file_pattern"]]):
        text = run.read_text(item)                                     # PP-02
        if text is None:
            continue
        keep = []
        for line_no, obj in run.parse_jsonl(item, text):               # PP-03
            ev = clean_event(run, item, line_no, obj, spec, nulls, seen)
            if ev is not None:
                keep.append(json.dumps(ev))
        item.rows_ok = len(keep)
        run.accept(item, "".join(line + "\n" for line in keep))
    run.finish()
    seen.save()


if __name__ == "__main__":
    main()