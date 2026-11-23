"""
link_twin.py - Python twin of lake/link/LinkDocuments.scala (Step 24).

Same parsing and linking rules as the Scala job, run on the local staging files, and
scored against the hidden truth (generators/labels/documents_truth.csv).

  python3 evaluation/link_twin.py            score
  python3 evaluation/link_twin.py --show 5   also print 5 wrong examples per field

Inputs (local, roughly what the lake holds after preprocessing and Cleanse):
  documents   $GSS_STAGING/documents/**.txt, cleaned like pp_documents.py; junk and
              duplicate texts are dropped as preprocessing does
  flights     $GSS_STAGING/exports/lion/flight_*.csv and manifest_*.csv
  messages    $GSS_STAGING/messages/*/*.json

Python 3.4, standard library only.
"""

import csv
import glob
import hashlib
import json
import os
import re
import sys
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STAGING = os.environ.get("GSS_STAGING") or os.path.join(ROOT, "staging")
LABELS = os.path.join(ROOT, "generators", "labels")
TYPES = [("CLEARANCE PERMIT", "PERMIT"), ("PACKING LIST", "PACKING"), ("AIR WAYBILL", "AWB"), ("QUOTATION", "QUOTE")]

# ---------------------------------------------------------------- parsing (same as Scala)
AWB_RE = re.compile(r"\b(\d{3})-(\d{8})\b")
FLIGHT_RE = re.compile(r"\bGS ?(\d{1,4})\b")
PO_RE = re.compile(r"\bPO-\d{6}\b")
SHP_RE = re.compile(r"\bSHP-\d{4}-\d{4}\b")
QUOTE_RE = re.compile(r"\bQ-\d{4}-\d{4}\b")
PERMIT_RE = re.compile(r"\bPRM-\d{4}-\d{4}\b")
PIECES_RE = re.compile(r"(?:Total pieces|Pieces):\s*(\d+)")
WEIGHT_RE = re.compile(r"(?:Total weight|Gross weight):\s*([\d,]+)")
DATE_IN_NAME = re.compile(r"_(\d{8})_\d+\.txt$")


def first(rx, text, group=0):
    m = rx.search(text)
    return m.group(group) if m else ""


def awb_valid(awb):
    """IATA rule: the 8th serial digit is the first seven taken mod 7."""
    m = re.match(r"^(\d{3})-(\d{8})$", awb)
    if not m:
        return False
    s = m.group(2)
    return int(s[:7]) % 7 == int(s[7])


def parse(doc_file, doc_type, text):
    m = FLIGHT_RE.search(text)
    return {
        "doc_file": doc_file, "doc_type": doc_type,
        "doc_date": first(DATE_IN_NAME, doc_file, 1),
        "awb_printed": first(AWB_RE, text),
        "flight_printed": "GS{:04d}".format(int(m.group(1))) if m else "",
        "po_id": first(PO_RE, text),
        "shipment_ref": first(SHP_RE, text),
        "quote_no": first(QUOTE_RE, text),
        "permit_no": first(PERMIT_RE, text),
        "pieces": first(PIECES_RE, text, 1),
        "weight": first(WEIGHT_RE, text, 1).replace(",", ""),
    }


def days_between(a, b):
    return abs((datetime.strptime(a, "%Y%m%d") - datetime.strptime(b, "%Y%m%d")).days)


# ---------------------------------------------------------------- linking (same as Scala)
def link(docs, flights, awb_flight, flight_awb, msgs):
    """docs: list of parsed dicts. flights: set of flight_no. awb_flight / flight_awb: from
    the manifest. msgs: [(msg_id, thread_id, body, attachments list)]. Adds the link
    fields to each doc and returns the docs."""
    by_attachment, by_quote_no, thread_flights = {}, {}, {}
    for msg_id, thread_id, body, atts in msgs:
        for a in atts:
            by_attachment[a] = (msg_id, thread_id)
        for q in QUOTE_RE.findall(body or ""):
            by_quote_no.setdefault(q, (msg_id, thread_id))
        for f in FLIGHT_RE.finditer(body or ""):
            thread_flights.setdefault(thread_id, set()).add("GS{:04d}".format(int(f.group(1))))

    for d in docs:
        d["awb_valid"] = d["awb_printed"] != "" and awb_valid(d["awb_printed"])
        d["msg_id"], d["thread_id"] = by_attachment.get(d["doc_file"]) or by_quote_no.get(d["quote_no"]) or ("", "")
        flight, method = "", ""
        if d["awb_valid"] and d["awb_printed"] in awb_flight:
            flight, method = awb_flight[d["awb_printed"]], "awb_manifest"
        elif d["flight_printed"] in flights:
            flight, method = d["flight_printed"], "flight_no"
        elif d["thread_id"] and len(thread_flights.get(d["thread_id"], ())) == 1:
            flight, method = list(thread_flights[d["thread_id"]])[0], "message_thread"
        d["flight_no"], d["method"] = flight, method

    # a valid AWB that the manifest lacks, but that is printed on a linked air waybill
    awb_doc_flight = {}
    for d in docs:
        if d["doc_type"] == "AWB" and d["flight_no"] and d["awb_valid"]:
            awb_doc_flight.setdefault(d["awb_printed"], d["flight_no"])
    for d in docs:
        if not d["flight_no"] and d["awb_valid"] and d["awb_printed"] in awb_doc_flight:
            d["flight_no"], d["method"] = awb_doc_flight[d["awb_printed"]], "awb_document"

    # packing lists with no number on them: the AWB with the same pieces and weight, dated
    # within 4 days; only when exactly one AWB fits
    awbs = [d for d in docs if d["doc_type"] == "AWB" and d["flight_no"]]
    for d in docs:
        if d["doc_type"] == "PACKING" and not d["flight_no"] and d["pieces"] and d["weight"]:
            fits = set(a["flight_no"] for a in awbs if a["pieces"] == d["pieces"] and a["weight"] == d["weight"]
                       and a["doc_date"] and d["doc_date"] and days_between(a["doc_date"], d["doc_date"]) <= 4)
            if len(fits) == 1:
                d["flight_no"], d["method"] = fits.pop(), "pieces_weight"

    # the shipment reference printed on the packing list belongs to the whole shipment
    ref_of_flight = {}
    for d in docs:
        if d["doc_type"] == "PACKING" and d["flight_no"] and d["shipment_ref"]:
            ref_of_flight.setdefault(d["flight_no"], d["shipment_ref"])
    # AWB number: a valid one printed on the document, else the manifest's for the flight,
    # else a valid one printed on the flight's air waybill
    awb_doc_of_flight = {}
    for d in docs:
        if d["doc_type"] == "AWB" and d["flight_no"] and d["awb_valid"]:
            awb_doc_of_flight.setdefault(d["flight_no"], d["awb_printed"])
    for d in docs:
        f = d["flight_no"]
        d["awb_no"] = d["awb_printed"] if d["awb_valid"] else (flight_awb.get(f) or awb_doc_of_flight.get(f, ""))
        if d["doc_type"] != "QUOTE" and not d["shipment_ref"]:
            d["shipment_ref"] = ref_of_flight.get(f, "")
        d["confidence"] = {"awb_manifest": "high", "flight_no": "high", "awb_document": "high", "message_thread": "medium",
                           "pieces_weight": "low"}.get(d["method"], "")
        d["linked"] = bool(d["flight_no"] or d["msg_id"])
    return docs


# ---------------------------------------------------------------- local inputs
def read_doc(path):
    raw = open(path, "rb").read()
    for enc in ("utf-8-sig", "cp1252"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    lines = [l.rstrip() for l in text.replace("\r\n", "\n").split("\n")]
    out, blank = [], 0
    for l in lines:
        blank = blank + 1 if not l.strip() else 0
        if blank <= 1:
            out.append(l)
    return "\n".join(out).strip("\n")


def load_docs():
    docs, seen = [], set()
    for path in sorted(glob.glob(os.path.join(STAGING, "documents", "*", "*", "*", "*.txt"))):
        text = read_doc(path)
        head = text.split("\n")[0].strip().upper() if text else ""
        t = next((v for k, v in TYPES if head.startswith(k)), None)
        h = hashlib.sha256(text.encode("utf-8")).hexdigest()
        if t is None or len(text) < 60 or h in seen:
            continue
        seen.add(h)
        rel = os.path.relpath(path, STAGING).replace(os.sep, "/")
        docs.append(parse(rel, t, text))
    return docs


def load_lion():
    flights, awb_flight, flight_awb = set(), {}, {}
    for p in glob.glob(os.path.join(STAGING, "exports", "lion", "flight_*.csv")):
        for r in csv.DictReader(open(p, encoding="utf-8")):
            flights.add(r["flight_no"].strip().upper())
    for p in glob.glob(os.path.join(STAGING, "exports", "lion", "manifest_*.csv")):
        for r in csv.DictReader(open(p, encoding="utf-8")):
            a, f = (r.get("awb_no") or "").strip(), (r.get("flight_no") or "").strip().upper()
            if re.match(r"^\d{3}-\d{8}$", a) and f:            # "-", "N/A": no AWB recorded
                awb_flight[a] = f
                flight_awb.setdefault(f, a)
    return flights, awb_flight, flight_awb


def load_messages():
    out, seen = [], set()
    for p in glob.glob(os.path.join(STAGING, "messages", "*", "*.json")):
        try:
            e = json.load(open(p, encoding="utf-8"))
        except ValueError:
            continue
        if e.get("msg_id") in seen or not e.get("body"):
            continue
        seen.add(e["msg_id"])
        out.append((e["msg_id"], e.get("thread_id"), e["body"], e.get("attachments") or []))
    return out


def main():
    show = int(sys.argv[sys.argv.index("--show") + 1]) if "--show" in sys.argv else 3
    docs = link(load_docs(), *load_lion(), msgs=load_messages())
    truth = dict((r["doc_file"], r) for r in csv.DictReader(open(os.path.join(LABELS, "documents_truth.csv"),
                                                                 encoding="utf-8")))
    by_method = {}
    for d in docs:
        by_method[(d["doc_type"], d["method"] or ("message only" if d["msg_id"] else "UNLINKED"))] = \
            by_method.get((d["doc_type"], d["method"] or ("message only" if d["msg_id"] else "UNLINKED")), 0) + 1
    print("Link twin: {} documents".format(len(docs)))
    for k in sorted(by_method):
        print("  {:<8} {:<15} {:>5}".format(k[0], k[1], by_method[k]))
    print("  field           precision   recall   (vs documents_truth.csv)")
    for f in ("flight_no", "awb_no", "shipment_ref", "quote_no", "msg_id"):
        pred = [d for d in docs if d[f] and d["doc_file"] in truth]
        ok = [d for d in pred if d[f] == truth[d["doc_file"]][f]]
        want = [d for d in docs if d["doc_file"] in truth and truth[d["doc_file"]][f]]
        got = [d for d in want if d[f] == truth[d["doc_file"]][f]]
        print("  {:<14} {:>9.1%} {:>9.1%}   ({} predicted, {} in truth)".format(
            f, len(ok) / float(len(pred) or 1), len(got) / float(len(want) or 1), len(pred), len(want)))
        bad = [d for d in docs if d["doc_file"] in truth and d[f] != truth[d["doc_file"]][f]]
        for d in bad[:show]:
            print("    {} {}: truth {!r} got {!r} ({})".format(d["doc_type"], d["doc_file"][-28:],
                                                             truth[d["doc_file"]][f], d[f], d["method"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())