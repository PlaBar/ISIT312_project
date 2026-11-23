"""
documents_gen.py - shipment documents as plain text files.

Writes staging/documents/<yyyy>/<mm>/<TYPE>/<type>_<yyyymmdd>_<seq>.txt using the
layouts of the Architecture Summary section 7.4. The first line always names the
document type (preprocessing step PP-13 checks it).

  QUOTE    one for every quote attached to a message (message_attachments.csv)
  AWB      one air waybill for every shipment that flew
  PACKING  one packing list for every shipment that flew; the ones attached to a
           confirmation message keep the file name that message points to
  PERMIT   clearance permits for regulated cargo or restricted destinations [A]

The text varies the way real documents do: different date formats, label wording,
spacing and flight-number styles (GS0412, GS 0412, GS412). Linking documents to
flights and shipments is the lake's job (LinkDocuments.scala), so file names carry
only the type and date.

Hidden truth (generators/labels/, read only by evaluation/)
  documents_truth.csv          the shipment, flight, quote and message each file belongs to
  planted_errors_documents.csv file-level errors for preprocessing

Planted errors
  empty_file, too_short, unknown_type   junk files that PP-13 must reject
  duplicate_content                     same text under a second name, PP-09 drops
  crlf_line_endings, utf8_bom           PP-02 normalises
  windows_1252                          a degree sign encoded in Windows-1252, PP-02 converts
  messy_whitespace                      trailing spaces and blank-line runs, PP-12 tidies
  awb_check_digit_wrong                 a mistyped AWB that linking should not trust

Python 3.4, standard library only. Run after structured_gen.py and messages_gen.py:
  python3 generators/documents_gen.py
"""

import csv
import glob
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gen_common as g
import structured_gen

PERMIT_CARGO = ["DANGEROUS_GOODS", "PHARMA", "VALUABLES", "VEHICLES"]
RESTRICTED_DEST = ["KBL", "MGQ", "JUB", "BGW"]
PERMIT_CLASS = {"DANGEROUS_GOODS": "DG Class 3", "PHARMA": "Pharmaceutical", "VALUABLES": "Valuable cargo",
                "VEHICLES": "Vehicles", "HUMANITARIAN": "Humanitarian", "GENERAL": "General (restricted route)",
                "PERISHABLE": "Perishable (restricted route)", "AOG_SPARES": "Aircraft parts",
                "OIL_GAS_EQUIPMENT": "Oilfield equipment"}
ITEM_NAMES = {"GENERAL": ["Cartons general goods", "Pallet mixed cargo", "Boxes spare parts"],
              "PERISHABLE": ["Pallet fresh fruit", "Cartons vegetables", "Boxes fresh fish"],
              "PHARMA": ["Cartons pharmaceuticals", "Insulated boxes vaccines", "Pallet medicines"],
              "DANGEROUS_GOODS": ["Drums paint UN1263", "Boxes lithium batteries UN3480", "Cans adhesive UN1133"],
              "HUMANITARIAN": ["Bales blankets", "Cartons food rations", "Bundles tents"],
              "AOG_SPARES": ["Crate engine parts", "Box avionics unit", "Crate landing gear parts"],
              "OIL_GAS_EQUIPMENT": ["Crate drilling parts", "Bundle drill pipes", "Skid pump unit"],
              "VEHICLES": ["Vehicle 4x4", "Vehicle pickup", "Crate vehicle spares"],
              "VALUABLES": ["Sealed box valuables", "Secure case documents", "Sealed crate"]}

PLANTED = []


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------
def read_csv(path, required_by):
    if not os.path.exists(path):
        sys.exit("{} not found: run {} first".format(os.path.basename(path), required_by))
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def handling_pos():
    """flight_no -> PO number of its ground-handling order, from the NetSuite exports
    and flight_ref_truth.csv. Used for the AWB 'Accounting information' line."""
    line_flight = {}
    for row in read_csv(os.path.join(g.LABELS, "flight_ref_truth.csv"), "structured_gen.py"):
        if row["source_system"] == "netsuite.po_line" and row["flight_no"]:
            line_flight[row["source_ref"]] = row["flight_no"]
    out = {}
    for path in sorted(glob.glob(os.path.join(g.EXPORTS, "netsuite", "po_line_*.csv"))):
        with open(path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                lid = row["po_line_id"].strip()
                if lid in line_flight and "andl" in row["item_desc"].lower():
                    out.setdefault(line_flight[lid], row["po_id"].strip())
    return out


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------
def fmt_date(r, d):
    return r.choice([g.fmt_dmy(d), g.fmt_mon(d), g.fmt_iso(d),
                     "{} {} {}".format(d.day, g.MONTH_FULL[d.month - 1], d.year)])


def fmt_flight(r, fno):
    x = r.random()
    if x < 0.05:
        return "GS " + fno[2:]
    if x < 0.10:
        return "GS" + str(int(fno[2:]))
    return fno


def place(r, iata):
    if g.chance(r, 0.3) and iata not in ("DXB",):
        return "{} ({})".format(g.airport(iata)["city"], iata)
    return iata


def pad(left, right, width):
    return left + " " * max(2, width - len(left)) + right


def cargo_label(code):
    return code.replace("_", " ").title().replace("Aog", "AOG").replace("Oil Gas", "Oil and gas")


def awb_typo(awb):
    """Change the check digit so the AWB is no longer valid."""
    return awb[:11] + str((int(awb[11]) + 3) % 10)


# ---------------------------------------------------------------------------
# Document bodies (layouts of Architecture Summary section 7.4)
# ---------------------------------------------------------------------------
def quote_text(r, a, cust_name, d):
    w = int(float(a["weight_kg"]))
    rate = float(a["rate_usd_kg"])
    valid = datetime.strptime(a["valid_until"], "%Y-%m-%d").date()
    lines = [
        pad("QUOTATION", pad("No: {}".format(a["quote_no"]), "Date: {}".format(fmt_date(r, d)), 26), 22),
        "To: {}".format(cust_name),
        "Route: {} to {}".format(place(r, a["origin_iata"]), place(r, a["dest_iata"])),
        pad("Cargo: {}".format(cargo_label(a["cargo_type"])), "Weight: {:,} kg".format(w), 44),
        pad("Rate: {:.2f} USD/kg".format(rate), "Total: {:,.2f} USD".format(rate * w), 44),
        "Valid until: {}".format(fmt_date(r, valid)),
    ]
    if g.chance(r, 0.5):
        lines += ["", "Rates subject to space and fuel price at time of booking."]
    return lines


def awb_text(r, s, shipper, consignee, nature, shc, po):
    awb = s["awb_no"]
    printed = awb
    if g.chance(r, 0.02):
        printed = awb_typo(awb)
    label = r.choice(["AWB No: {}", "AWB: {}", "Air Waybill No. {}"])
    flt = r.choice(["Flight: {}", "Flight No: {}", "Flt: {}"]).format(fmt_flight(r, s["flight_no"]))
    lines = [
        pad("AIR WAYBILL", label.format(printed), 25),
        pad("Shipper: {}".format(shipper), "Consignee: {}".format(consignee), 42),
        pad(flt, "Date: {}".format(fmt_date(r, s["dep"].date())), 38),
        pad("From: {}".format(s["origin_iata"]), "To: {}".format(s["dest_iata"]), 30),
        pad(pad("Pieces: {}".format(s["pieces"]), "Gross weight: {} kg".format(s["weight_kg"]), 27),
            "Nature of goods: {}".format(nature), 54),
        "Special handling: {}".format(shc or "NIL"),
    ]
    if po:
        lines.append("Accounting information: handling {}".format(po))
    return lines, printed


def packing_items(r, s):
    names = ITEM_NAMES[s["cargo_type"]]
    total_p, total_w = int(s["pieces"]), int(s["weight_kg"])
    n = min(total_p, r.randint(1, 3))
    pieces = [1] * n
    for _ in range(total_p - n):
        pieces[r.randrange(n)] += 1
    weights, left = [], total_w
    for i in range(n):
        w = left if i == n - 1 else max(1, int(total_w * pieces[i] / float(total_p)))
        weights.append(w)
        left -= w
    items = []
    for i in range(n):
        dims = "{}x{}x{}".format(r.choice([120, 100, 80]), r.choice([80, 100, 120]), r.randint(40, 180))
        items.append((names[i % len(names)], pieces[i], dims, weights[i]))
    return items


def packing_text(r, s, ship_ref, shipper, consignee):
    lines = ["PACKING LIST" + " " * r.randint(1, 6) + "Ref: {}".format(ship_ref)]
    if g.chance(r, 0.7):
        lines.append("Shipper: {}".format(shipper))
    if g.chance(r, 0.7):
        lines.append("Consignee: {}".format(consignee))
    if g.chance(r, 0.5):
        lines.append("AWB: {}".format(s["awb_no"]))
    if g.chance(r, 0.3):
        lines.append("Flight: {}".format(fmt_flight(r, s["flight_no"])))
    lines.append("Item | Pieces | Dimensions (cm) | Weight (kg)")
    for name, p, dims, w in packing_items(r, s):
        lines.append("{} | {} | {} | {}".format(name, p, dims, w))
    lines.append(pad("Total pieces: {}".format(s["pieces"]), "Total weight: {}".format(s["weight_kg"]), 39))
    return lines


def permit_text(r, s, permit_no, issued, valid):
    return [
        "CLEARANCE PERMIT No: {}".format(permit_no),
        "Cargo class: {}  Route: {} to {}".format(PERMIT_CLASS[s["cargo_type"]], s["origin_iata"], s["dest_iata"]),
        pad(pad("Issued: {}".format(fmt_date(r, issued)), "Valid until: {}".format(fmt_date(r, valid)), 30),
            "Flight: {}".format(fmt_flight(r, s["flight_no"])), 58),
        "Issued by: {} Civil Aviation Authority".format(g.airport(s["dest_iata"])["country"]
                                                        if s["dest_iata"] not in g.UAE_IATA
                                                        else g.airport(s["origin_iata"])["country"]),
    ]


# ---------------------------------------------------------------------------
# Writing with file-level messiness
# ---------------------------------------------------------------------------
def rel_path(doc_type, d, seq):
    return "documents/{:04d}/{:02d}/{}/{}_{}_{:03d}.txt".format(
        d.year, d.month, doc_type, doc_type.lower(), d.strftime("%Y%m%d"), seq)


def write_doc(r, rel, lines, cp1252_ok=False):
    text = "\n".join(lines) + "\n"
    enc, nl = "utf-8", "\n"
    if g.chance(r, 0.10):
        text = "\n".join(l + " " * r.randint(1, 4) for l in lines)
        text = text.replace("\n\n", "\n\n\n\n") + "\n\n\n"
        PLANTED.append(("documents", rel, rel, "messy_whitespace"))
    if g.chance(r, 0.05):
        nl = "\r\n"
        PLANTED.append(("documents", rel, rel, "crlf_line_endings"))
    if cp1252_ok and g.chance(r, 0.3):
        enc = "cp1252"
        PLANTED.append(("documents", rel, rel, "windows_1252"))
    elif g.chance(r, 0.02):
        enc = "utf-8-sig"
        PLANTED.append(("documents", rel, rel, "utf8_bom"))
    full = os.path.join(g.STAGING, rel)
    g.ensure_dir(os.path.dirname(full))
    with open(full, "w", encoding=enc, newline=nl) as f:
        f.write(text)
    return text


class SeqBook(object):
    """Next free sequence number per (type, date), keeping message attachment names."""
    def __init__(self, reserved):
        self.used = {}
        for doc_type, d, seq in reserved:
            self.used[(doc_type, d)] = max(self.used.get((doc_type, d), 0), seq)

    def next(self, doc_type, d):
        key = (doc_type, d)
        self.used[key] = self.used.get(key, 0) + 1
        return self.used[key]


def main():
    shipments = read_csv(os.path.join(g.LABELS, "shipments_master.csv"), "structured_gen.py")
    attachments = read_csv(os.path.join(g.LABELS, "message_attachments.csv"), "messages_gen.py")
    customers = dict((c["customer_id"], c) for c in g.make_customers())
    po_of = handling_pos()

    for s in shipments:
        s["dep"] = datetime.strptime(s["sched_dep"], "%Y-%m-%dT%H:%M:%S")
    flown = [s for s in shipments if s["status"] != "CANCELLED"]
    by_flight = dict((s["flight_no"], s) for s in flown)

    # clear previous run
    for root, _, files in os.walk(g.DOCUMENTS):
        for name in files:
            if name.endswith(".txt"):
                os.remove(os.path.join(root, name))

    reserved = []
    for a in attachments:
        base = os.path.basename(a["rel_path"])[:-4]
        parts = base.split("_")
        reserved.append((a["doc_type"], datetime.strptime(parts[1], "%Y%m%d").date(), int(parts[2])))
    book = SeqBook(reserved)

    r = g.rng("documents")
    rf = g.rng("document-files")
    truth = []
    written = []      # (rel, text) for duplicate-content planting
    counts = {}

    def record(rel, doc_type, s, quote_no="", msg_id="", ship_ref=""):
        truth.append([rel, doc_type, s["awb_no"] if s else "", s["flight_no"] if s else "",
                      "", ship_ref, quote_no, msg_id])
        counts[doc_type] = counts.get(doc_type, 0) + 1

    def consignee(s):
        return structured_gen.consignee_name({"dest": s["dest_iata"], "cargo": {"code": s["cargo_type"]}})

    # quotes attached to messages
    for a in attachments:
        if a["doc_type"] != "QUOTE":
            continue
        d = datetime.strptime(a["doc_date"], "%Y-%m-%d").date()
        text = write_doc(rf, a["rel_path"], quote_text(r, a, customers[a["customer_id"]]["name"], d))
        written.append((a["rel_path"], text))
        record(a["rel_path"], "QUOTE", by_flight.get(a["flight_no"]), a["quote_no"], a["msg_id"])

    # shipment documents
    attached_packing = dict((a["flight_no"], a) for a in attachments if a["doc_type"] == "PACKING")
    seq_ship, seq_permit = {}, {}
    for s in flown:
        y = s["dep"].year
        seq_ship[y] = seq_ship.get(y, 0) + 1
        ship_ref = g.shipment_ref(y, seq_ship[y])
        cust = customers[s["customer_id"]]
        cargo = [c for c in g.CARGO_TYPES if c["code"] == s["cargo_type"]][0]

        # air waybill, dated on the flight day or the day before
        d = s["dep"].date() - timedelta(days=r.randint(0, 1))
        rel = rel_path("AWB", d, book.next("AWB", d))
        nature = cargo["desc"]
        cp = False
        if s["cargo_type"] == "PHARMA":
            nature = "Pharmaceuticals 2-8\u00b0C"
            cp = True
        lines, printed = awb_text(r, s, cust["name"], consignee(s), nature, cargo["shc"], po_of.get(s["flight_no"]))
        if printed != s["awb_no"]:
            PLANTED.append(("documents", rel, rel, "awb_check_digit_wrong"))
        text = write_doc(rf, rel, lines, cp1252_ok=cp)
        written.append((rel, text))
        record(rel, "AWB", s, ship_ref=ship_ref)

        # packing list: the attached one keeps its name, the rest are filed a day or two before
        a = attached_packing.get(s["flight_no"])
        if a:
            rel, msg = a["rel_path"], a["msg_id"]
        else:
            d = s["dep"].date() - timedelta(days=r.randint(1, 2))
            rel, msg = rel_path("PACKING", d, book.next("PACKING", d)), ""
        text = write_doc(rf, rel, packing_text(r, s, ship_ref, cust["name"], consignee(s)))
        written.append((rel, text))
        record(rel, "PACKING", s, msg_id=msg, ship_ref=ship_ref)

        # clearance permit
        if s["cargo_type"] in PERMIT_CARGO or s["dest_iata"] in RESTRICTED_DEST or s["origin_iata"] in RESTRICTED_DEST:
            seq_permit[y] = seq_permit.get(y, 0) + 1
            issued = s["dep"].date() - timedelta(days=r.randint(3, 10))
            valid = s["dep"].date() + timedelta(days=5)
            rel = rel_path("PERMIT", issued, book.next("PERMIT", issued))
            text = write_doc(rf, rel, permit_text(r, s, g.permit_no(y, seq_permit[y]), issued, valid))
            written.append((rel, text))
            record(rel, "PERMIT", s, ship_ref=ship_ref)

    # duplicate content under a second name (PP-09 drops by content hash)
    rd = g.rng("document-dups")
    for rel, text in list(written):
        if g.chance(rd, 0.01):
            parts = os.path.basename(rel)[:-4].split("_")
            d = datetime.strptime(parts[1], "%Y%m%d").date()
            dup_rel = rel_path(parts[0].upper(), d, book.next(parts[0].upper(), d))
            full = os.path.join(g.STAGING, dup_rel)
            g.ensure_dir(os.path.dirname(full))
            with open(full, "w", encoding="utf-8", newline="\n") as f:
                f.write(text)
            PLANTED.append(("documents", dup_rel, rel, "duplicate_content"))

    # junk files that PP-13 must reject
    rj = g.rng("document-junk")
    for i in range(g.MONTHS):
        first, last = g.month_range(i)
        if not g.chance(rj, 0.6):
            continue
        d = g.random_date(rj, first, last)
        doc_type = rj.choice(["AWB", "PACKING", "QUOTE", "PERMIT"])
        dup_rel = rel_path(doc_type, d, book.next(doc_type, d))
        kind = rj.choice(["empty_file", "too_short", "unknown_type"])
        text = {"empty_file": "",
                "too_short": {"AWB": "AIR WAYBILL", "PACKING": "PACKING LIST", "QUOTE": "QUOTATION",
                              "PERMIT": "CLEARANCE PERMIT"}[doc_type] + "\n",
                "unknown_type": "DELIVERY NOTE  No: DN-{}\nReceived in good order.\nSigned: ________\n".format(
                    rj.randint(1000, 9999))}[kind]
        full = os.path.join(g.STAGING, dup_rel)
        g.ensure_dir(os.path.dirname(full))
        with open(full, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        PLANTED.append(("documents", dup_rel, "", kind))

    g.write_csv(os.path.join(g.LABELS, "documents_truth.csv"),
                ["doc_file", "doc_type", "awb_no", "flight_no", "po_id", "shipment_ref", "quote_no", "msg_id"],
                truth)
    g.write_csv(os.path.join(g.LABELS, "planted_errors_documents.csv"), ["table", "file", "key", "error"], PLANTED)

    n_files = sum(len([n for n in fs if n.endswith(".txt")]) for _, _, fs in os.walk(g.DOCUMENTS))
    print("Documents:      {} real documents, {} files on disk".format(len(truth), n_files))
    for t in ["QUOTE", "AWB", "PACKING", "PERMIT"]:
        print("  {:<8} {:>5}".format(t, counts.get(t, 0)))
    print("Per month:      about {:.0f}".format(len(truth) / float(g.MONTHS)))
    present = sum(1 for a in attachments if os.path.exists(os.path.join(g.STAGING, a["rel_path"])))
    print("Message attachments: {} of {} present".format(present, len(attachments)))
    kinds = {}
    for p in PLANTED:
        kinds[p[3]] = kinds.get(p[3], 0) + 1
    print("Planted errors: {}".format(len(PLANTED)))
    for k in sorted(kinds):
        print("  {:<24} {:>5}".format(k, kinds[k]))
    print("")
    print("Truth in generators/labels/: documents_truth, planted_errors_documents")


if __name__ == "__main__":
    main()