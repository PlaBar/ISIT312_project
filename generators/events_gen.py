"""
events_gen.py - operational event log (delays, swaps, diversions, cancellations, AOG).

Writes JSON lines to staging/events/events_<yyyymm>.jsonl in the format of
docs/data_dictionary.md section 5.2, one event per line.

The events follow the real flights: the flights are rebuilt with
structured_gen.build_flights(), which uses the same seed, so a flight that
left 45 minutes late in flight.csv gets a DELAY event here.

Planted errors (written to generators/labels/planted_errors_events.csv):
  duplicate_line        same event twice                  PP-09 drops
  truncated_json        line cut off mid-way              PP-03 rejects line
  missing_flight_no     required field absent             PP-10 style check rejects line
  bad_timestamp         event_time not ISO                PP-07 rejects line
  unknown_event_type    e.g. "DELAYED"                    schema check rejects line

Python 3.4, standard library only. Run from the project folder:
  python3 generators/events_gen.py
"""

import glob
import json
import os
import sys
from collections import OrderedDict
from datetime import timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gen_common as g
import structured_gen

FIELDS = ["event_id", "flight_no", "event_type", "event_time", "detail", "source"]

DELAY_REASONS = [
    "awaiting ground handler", "late cargo acceptance", "crew duty time", "ATC slot",
    "fuel truck late", "weather at destination", "customs clearance pending",
    "loading delayed, pallets not ready", "late documents from shipper", "catering late",
]
TECH_REASONS = [
    "hyd leak found on walkaround", "engine start valve u/s", "awaiting spare part from DWC",
    "brake change required", "apu inop, waiting GPU",
]
CANCEL_REASONS = [
    "customer postponed shipment", "overflight permit not received", "cargo not ready",
    "aircraft unserviceable", "landing permit refused",
]
DIVERT_REASONS = ["weather", "runway closed", "fuel planning", "ATC restriction"]
SWAP_REASONS = ["tech", "payload", "maintenance check due", "schedule change"]

PLANTED = []


def event(time, flight_no, etype, detail, source):
    return OrderedDict([("event_id", ""), ("flight_no", flight_no), ("event_type", etype),
                        ("event_time", g.fmt_event_ts(time)), ("detail", detail), ("source", source)])


def build_events(flights):
    r = g.rng("events")
    out = []
    for f in flights:
        src = "lion" if g.chance(r, 0.6) else "ops_chat"
        if f["status"] == "CANCELLED":
            t = f["dep"] - timedelta(hours=r.randint(2, 30), minutes=r.choice([0, 15, 30, 45]))
            out.append(event(t, f["flight_no"], "CANCELLATION",
                             "flight cancelled - {}".format(r.choice(CANCEL_REASONS)), src))
            continue

        # aircraft swap the day before (3%)
        if g.chance(r, 0.03):
            others = [a["registration"] for a in g.FLEET if a["registration"] != f["registration"]]
            t = f["dep"] - timedelta(hours=r.randint(6, 24))
            detail = r.choice(["a/c swap {old} replaced by {new}, reason {why}",
                               "{old} -> {new} swap ({why})",
                               "Swap: {new} to operate instead of {old}, {why}"]).format(
                old=r.choice(others), new=f["registration"], why=r.choice(SWAP_REASONS))
            out.append(event(t, f["flight_no"], "AIRCRAFT_SWAP", detail, src))

        # delays of 15 minutes or more; very long ones are sometimes AOG
        if f["delay"] >= 15:
            t = f["dep"] + timedelta(minutes=r.randint(-30, 5))
            if f["delay"] > 120 and g.chance(r, 0.4):
                out.append(event(t, f["flight_no"], "AOG",
                                 "AOG at {} - {}".format(f["origin"], r.choice(TECH_REASONS)), src))
            else:
                reason = r.choice(DELAY_REASONS)
                detail = r.choice(["dep delayed {m} min, {why}", "{why}, new ETD +{m}min",
                                   "delay {m}m: {why}", "{why}"]).format(m=f["delay"], why=reason)
                out.append(event(t, f["flight_no"], "DELAY", detail, src))

        if f["status"] == "DIVERTED":
            alt = r.choice([d for d in g.DESTINATIONS if d != f["dest"]] + ["DXB"])
            t = f["arr"] + timedelta(minutes=f["delay"] - r.randint(10, 40))
            out.append(event(t, f["flight_no"], "DIVERSION",
                             "diverted to {} due {}".format(alt, r.choice(DIVERT_REASONS)), src))

    out.sort(key=lambda e: (e["event_time"], e["flight_no"], e["event_type"]))
    for i, e in enumerate(out, 1):
        e["event_id"] = g.event_id(i)
    return out


def to_lines(r, fname, events):
    """Serialise one month of events, planting line-level errors."""
    lines = []
    for e in events:
        e = OrderedDict(e)
        err = None
        x = r.random()
        if x < 0.005:
            del e["flight_no"]
            err = "missing_flight_no"
        elif x < 0.010:
            t = e["event_time"]
            e["event_time"] = "{}/{}/{} {}".format(t[8:10], t[5:7], t[0:4], t[11:16])
            err = "bad_timestamp"
        elif x < 0.015:
            e["event_type"] = {"DELAY": "DELAYED", "AOG": "A.O.G", "AIRCRAFT_SWAP": "SWAP",
                               "CANCELLATION": "CANCELLED", "DIVERSION": "DIVERTED"}[e["event_type"]]
            err = "unknown_event_type"
        line = json.dumps(e)
        if err is None and x < 0.020:
            line = line[:r.randint(20, len(line) - 10)]
            err = "truncated_json"
        if err:
            PLANTED.append(("events", fname, e["event_id"], err))
        lines.append(line)
        if err is None and g.chance(r, 0.01):
            lines.append(line)
            PLANTED.append(("events", fname, e["event_id"], "duplicate_line"))
    return lines


def main():
    flights = structured_gen.build_flights(g.make_customers())
    events = build_events(flights)

    g.ensure_dir(g.EVENTS)
    for p in glob.glob(os.path.join(g.EVENTS, "events_*.jsonl")):
        os.remove(p)

    r = g.rng("events-errors")
    by_month = {}
    for e in events:
        by_month.setdefault(e["event_time"][:7].replace("-", ""), []).append(e)
    n_lines = 0
    for ym in sorted(by_month):
        fname = "events_{}.jsonl".format(ym)
        lines = to_lines(r, fname, by_month[ym])
        with open(os.path.join(g.EVENTS, fname), "w", encoding="utf-8", newline="\n") as f:
            for line in lines:
                f.write(line + "\n")
        n_lines += len(lines)

    g.write_csv(os.path.join(g.LABELS, "planted_errors_events.csv"),
                ["table", "file", "key", "error"], PLANTED)

    types = {}
    for e in events:
        types[e["event_type"]] = types.get(e["event_type"], 0) + 1
    print("Events:         {} ({} lines written in {} files)".format(len(events), n_lines, len(by_month)))
    for t in sorted(types):
        print("  {:<16} {:>5}".format(t, types[t]))
    kinds = {}
    for p in PLANTED:
        kinds[p[3]] = kinds.get(p[3], 0) + 1
    print("Planted errors: {}".format(len(PLANTED)))
    for k in sorted(kinds):
        print("  {:<20} {:>5}".format(k, kinds[k]))
    print("")
    print("Files in staging/events/, planted errors in generators/labels/planted_errors_events.csv")


if __name__ == "__main__":
    main()