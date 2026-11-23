"""
structured_gen.py - simulated exports of NetSuite, Lion and the maintenance programme.

Writes CSV files to staging/exports/<system>/ in the formats of
docs/data_dictionary.md section 4, with the planted errors of section 4.4.

Also writes, for later steps:
  generators/labels/shipments_master.csv  clean flights and shipments, read by the
                                          other generators so everything lines up
  generators/labels/flight_ref_truth.csv  true flight for every invoice and PO line
  generators/labels/defect_truth.csv      true class of every defect note
  generators/labels/planted_errors.csv    every planted error, to check preprocessing

Python 3.4, standard library only. Run from the project folder:
  python3 generators/structured_gen.py
"""

import glob
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gen_common as g

NS = os.path.join(g.EXPORTS, "netsuite")
LION = os.path.join(g.EXPORTS, "lion")
MAINT = os.path.join(g.EXPORTS, "maint")
END = g.end_date()

# rates [A]
RATE_PER_BH = {"B737-400F": 8500, "A300B4-200F": 13500, "IL-76TD": 15500}     # USD per block hour
FUEL_L_PER_BH = {"B737-400F": 3000, "A300B4-200F": 7000, "IL-76TD": 8500}    # litres per block hour
AED_PER_USD = 3.6725
TERMS_DAYS = {"NET15": 15, "NET30": 30, "NET45": 45, "PREPAID": 0}

PLANTED = []      # (table, file, key, error)


def plant(table, fname, key, error):
    PLANTED.append((table, fname, key, error))


def money(x):
    return "{:.2f}".format(x)


# ---------------------------------------------------------------------------
# Text variants for free-text references (repaired later in Spark)
# ---------------------------------------------------------------------------
def date_variants(d):
    return {
        "dmy": g.fmt_dmy(d),                                                   # 12/03/2024
        "ddmm": "{:02d}/{:02d}".format(d.day, d.month),                         # 12/03
        "dot": "{:02d}.{:02d}.{:02d}".format(d.day, d.month, d.year % 100),     # 12.03.24
        "dash": "{:02d}-{:02d}-{:04d}".format(d.day, d.month, d.year),          # 12-03-2024
        "mon2": "{:02d}-{}-{:02d}".format(d.day, g.MONTH_ABBR[d.month - 1], d.year % 100),  # 12-Mar-24
        "long": "{} {} {}".format(d.day, g.MONTH_ABBR[d.month - 1], d.year),     # 12 Mar 2024
        "short": "{} {}".format(d.day, g.MONTH_ABBR[d.month - 1]),               # 12 Mar
    }


def invoice_memo(r, f):
    if g.chance(r, 0.05):
        return r.choice(["Charter services as agreed", "Charter per contract", "Air freight charges"])
    dv = date_variants(f["date"])
    o, d = f["origin"], f["dest"]
    oc, dc = g.airport(o)["city"], g.airport(d)["city"]
    t = r.choice([
        "Charter {o}-{d} {ddmm} {reg}",
        "{o}/{d} charter {dot}",
        "Charter {oc} - {dc} on {long}",
        "{d} charter {dash} ex {o}",
        "Flight {o} to {d} dated {dmy} reg {regm}",
        "Cargo charter {oc} to {dc}, {short}",
    ])
    return t.format(o=o, d=d, oc=oc, dc=dc, reg=f["registration"],
                    regm=g.reg_maint(f["registration"]), **dv)


def po_desc(r, kind, f):
    if g.chance(r, 0.05):
        return {"fuel": "Fuel uplift as per contract", "handling": "Handling charges",
                "nav": "Navigation charges"}[kind]
    dv = date_variants(f["date"])
    o, d = f["origin"], f["dest"]
    country = g.airport(d)["country"]
    options = {
        "fuel": ["Jet A1 uplift {o} {mon2}", "Fuel {o}-{d} {dmy}",
                 "JetA-1 {o} {ddmm} flt to {d}", "Uplift {o} for {d} sector {dot}"],
        "handling": ["Ground handling {d} {dmy} ex {o}", "Handling {o}-{d} {mon2}",
                     "Ramp and cargo handling at {d} on {ddmm}"],
        "nav": ["Nav charges {country} {o}-{d} {dmy}", "Overflight and landing nav {d} {mon2}",
                "ANS fees {o}/{d} {ddmm}"],
    }[kind]
    return r.choice(options).format(o=o, d=d, country=country, **dv)


# ---------------------------------------------------------------------------
# Lion: flights and manifests
# ---------------------------------------------------------------------------
def build_flights(customers):
    r = g.rng("flights")
    fleet_w = [3, 3, 2, 2]
    flights = []
    for first, last in g.all_months():
        for _ in range(g.FLIGHTS_PER_MONTH):
            day = g.random_date(r, first, last)
            hour = g.weighted_choice(r, list(range(24)), [1, 1, 2, 3, 4, 4, 4, 3, 3, 2, 2, 2,
                                                          2, 2, 2, 2, 2, 3, 3, 3, 2, 2, 1, 1])
            dep = datetime(day.year, day.month, day.day, hour, r.choice([0, 15, 30, 45]))
            dest_iata = r.choice(g.DESTINATIONS)
            if g.chance(r, 0.75):
                origin = g.weighted_choice(r, g.UAE_IATA, [85, 8, 7])
                dest = dest_iata
            else:
                origin, dest = dest_iata, g.HOME_BASE
            cargo = g.pick_cargo(r)
            weight = r.randint(cargo["kg"][0], cargo["kg"][1])
            ac = g.weighted_choice(r, g.FLEET, fleet_w)
            if weight > ac["max_payload_kg"] * 0.95:
                ac = r.choice([a for a in g.FLEET if a["max_payload_kg"] >= weight / 0.95] or [g.FLEET[3]])
                weight = min(weight, int(ac["max_payload_kg"] * 0.95))
            block_h = max(g.airport(origin)["block_h"], g.airport(dest)["block_h"])
            arr = dep + timedelta(minutes=int(round(block_h * 60 / 5.0)) * 5)
            status = "ARRIVED"
            if g.chance(r, 0.02):
                status = "CANCELLED"
            elif g.chance(r, 0.01):
                status = "DIVERTED"
            delay = g.weighted_choice(r, [r.randint(-5, 14), r.randint(15, 120), r.randint(121, 400)],
                                      [70, 25, 5])
            flights.append({
                "dep": dep, "arr": arr, "date": dep.date(), "origin": origin, "dest": dest,
                "aircraft": ac, "registration": ac["registration"], "block_h": block_h,
                "status": status, "delay": delay, "cargo": cargo, "weight": weight,
                "customer": r.choice(customers), "pieces": r.randint(1, 60),
            })
    flights.sort(key=lambda f: (f["dep"], f["origin"], f["dest"]))
    for i, f in enumerate(flights, 1):
        f["flight_no"] = g.flight_no(i)
        f["awb"] = g.awb_number(5000000 + i * 7 + 3)
    return flights


def consignee_name(f):
    words = {"GENERAL": "Trading House", "PERISHABLE": "Fresh Produce Co", "PHARMA": "Medical Supplies",
             "DANGEROUS_GOODS": "Industrial Chemicals", "HUMANITARIAN": "Relief Coordination Office",
             "AOG_SPARES": "Aviation Services", "OIL_GAS_EQUIPMENT": "Petroleum Services",
             "VEHICLES": "Motors", "VALUABLES": "Secure Logistics"}
    return "{} {}".format(g.airport(f["dest"])["city"], words[f["cargo"]["code"]])


# ---------------------------------------------------------------------------
# Writers with planted errors
# ---------------------------------------------------------------------------
def add_duplicates(r, table, fname, rows, rate=0.01):
    """Exact duplicate rows, caught by PP-09."""
    out = list(rows)
    for row in rows:
        if g.chance(r, rate):
            out.insert(r.randint(0, len(out)), list(row))
            plant(table, fname, row[0], "duplicate_row")
    return out


def pad_and_null(r, table, fname, rows, pad_cols, optional_cols, required_cols):
    """Spaces on ids and names (PP-06 fixes), null tokens in optional cells (PP-06 fixes)
    and a few in required cells (PP-06 empties them, then the row is rejected)."""
    for row in rows:
        for c in pad_cols:
            if g.chance(r, 0.03):
                row[c] = g.maybe_pad(r, row[c], 1.0)
                plant(table, fname, row[0].strip(), "padded_value")
        for c in optional_cols:
            if row[c] == "" and g.chance(r, 0.5):
                row[c] = r.choice(g.NULL_TOKENS)
                plant(table, fname, row[0].strip(), "null_token_optional")
        for c in required_cols:
            if g.chance(r, 0.003):
                row[c] = r.choice(g.NULL_TOKENS)
                plant(table, fname, row[0].strip(), "null_token_required")
    return rows


def write(system_dir, fname, header, rows, **kw):
    return g.write_csv(os.path.join(system_dir, fname), header, rows, **kw)


def by_month(items, date_of):
    out = {}
    for it in items:
        out.setdefault(g.fmt_yyyymm(date_of(it)), []).append(it)
    return sorted(out.items())


def gen_lion(flights):
    r = g.rng("lion-errors")
    counts = {}

    # airport.csv (one code lower case, PP-08)
    rows = [[a["icao"], a["iata"], a["name"], a["country"]] for a in g.AIRPORTS]
    rows[5][0] = rows[5][0].lower()
    plant("lion.airport", "airport.csv", rows[5][0].upper(), "lower_case_code")
    counts["airport"] = write(LION, "airport.csv", ["icao_code", "iata_code", "name", "country"], rows)

    rows = [[a["aircraft_id"], a["registration"], a["type"], str(a["max_payload_kg"])] for a in g.FLEET]
    counts["aircraft"] = write(LION, "aircraft.csv", ["aircraft_id", "registration", "type", "max_payload_kg"], rows)

    # flight_YYYYMM.csv and manifest_YYYYMM.csv
    counts["flight"] = counts["manifest"] = 0
    bad_line_month = g.fmt_yyyymm(g.month_range(17)[0])
    for ym, fl in by_month(flights, lambda f: f["date"]):
        fname = "flight_{}.csv".format(ym)
        rows = []
        for f in fl:
            o, d = g.airport(f["origin"])["icao"], g.airport(f["dest"])["icao"]
            if g.chance(r, 0.02):
                o = o.lower()
                plant("lion.flight", fname, f["flight_no"], "lower_case_code")
            adep = aarr = ""
            if f["status"] != "CANCELLED":
                adep = g.fmt_lion_ts(f["dep"] + timedelta(minutes=f["delay"]))
                extra = r.randint(30, 90) if f["status"] == "DIVERTED" else r.randint(-10, 10)
                aarr = g.fmt_lion_ts(f["arr"] + timedelta(minutes=f["delay"] + extra))
            sdep = g.fmt_lion_ts(f["dep"])
            if g.chance(r, 0.005):
                sdep = r.choice(["{:02d}/{:02d}/{} {:02d}:{:02d}", "{:02d}.{:02d}.{} {:02d}h{:02d}"]).format(
                    f["dep"].day, f["dep"].month, f["dep"].year, f["dep"].hour, f["dep"].minute)
                plant("lion.flight", fname, f["flight_no"], "unparseable_date")
            rows.append([f["flight_no"], f["aircraft"]["aircraft_id"], o, d, sdep,
                         g.fmt_lion_ts(f["arr"]), adep, aarr, f["status"]])
        rows = pad_and_null(r, "lion.flight", fname, rows, [0], [6, 7], [1])
        rows = add_duplicates(r, "lion.flight", fname, rows)
        counts["flight"] += write(LION, fname, ["flight_no", "aircraft_id", "origin_icao", "dest_icao",
                                                "sched_dep", "sched_arr", "actual_dep", "actual_arr",
                                                "status"], rows)

        fname = "manifest_{}.csv".format(ym)
        rows = []
        for f in fl:
            if f["status"] == "CANCELLED":
                continue
            f["manifest_id"] = g.manifest_id(len(MANIFEST_IDS) + 1)
            MANIFEST_IDS.append(f["manifest_id"])
            fno = f["flight_no"]
            if g.chance(r, 0.005):
                fno = g.flight_no(9000 + len(MANIFEST_IDS) % 999)
                plant("lion.manifest", fname, f["manifest_id"], "orphan_flight_no")
            weight = money(f["weight"])
            if g.chance(r, 0.005):
                weight = r.choice(["0.00", money(-f["weight"])])
                plant("lion.manifest", fname, f["manifest_id"], "non_positive_weight")
            volume = money(f["weight"] / r.choice([150.0, 167.0, 200.0, 250.0]))
            awb = "" if g.chance(r, 0.03) else f["awb"]
            rows.append([f["manifest_id"], fno, "{}, {} pcs".format(f["cargo"]["desc"], f["pieces"]),
                         weight, volume, consignee_name(f), awb])
        rows = pad_and_null(r, "lion.manifest", fname, rows, [0, 5], [6], [2])
        rows = add_duplicates(r, "lion.manifest", fname, rows)
        if ym == bad_line_month:
            bad = list(rows[3]) + ["EXTRA FIELD"]
            rows.insert(4, bad)
            plant("lion.manifest", fname, bad[0], "wrong_column_count")
        counts["manifest"] += write(LION, fname, ["manifest_id", "flight_no", "cargo_desc", "weight_kg",
                                                  "volume_m3", "consignee", "awb_no"], rows)
    return counts


MANIFEST_IDS = []


# ---------------------------------------------------------------------------
# NetSuite
# ---------------------------------------------------------------------------
def gen_netsuite(flights, customers, suppliers):
    r = g.rng("netsuite")
    re_ = g.rng("netsuite-errors")
    counts = {}
    truth = []

    # customer.csv with a UTF-8 byte-order mark (PP-02)
    rows = [[c["customer_id"], c["name"], c["country"], c["payment_terms"]] for c in customers]
    rows = pad_and_null(re_, "netsuite.customer", "customer.csv", rows, [1], [], [])
    rows = add_duplicates(re_, "netsuite.customer", "customer.csv", rows)
    plant("netsuite.customer", "customer.csv", "", "utf8_bom")
    counts["customer"] = write(NS, "customer.csv", ["customer_id", "name", "country", "payment_terms"],
                               rows, bom=True)

    # supplier.csv in Windows-1252 with an accented name (PP-02)
    rows = []
    for s in suppliers:
        name = s["name"]
        if s["country"] == "DR Congo" and s["category"] == "FUEL":
            name = "Soci\u00e9t\u00e9 Congolaise des Carburants d'Aviation"
        rows.append([s["supplier_id"], name, s["category"], s["country"]])
    rows = add_duplicates(re_, "netsuite.supplier", "supplier.csv", rows)
    plant("netsuite.supplier", "supplier.csv", "", "windows_1252_encoding")
    counts["supplier"] = write(NS, "supplier.csv", ["supplier_id", "name", "category", "country"],
                               rows, encoding="cp1252")

    # invoices, lines and payments
    invoices, lines, payments = [], [], []
    flown = [f for f in flights if f["status"] != "CANCELLED"]
    adhoc = []
    for first, last in g.all_months():
        for _ in range(r.randint(1, 2)):
            adhoc.append({"date": g.random_date(r, first, last), "customer": r.choice(customers)})
    events = [("flight", f["date"], f) for f in flown] + [("adhoc", a["date"], a) for a in adhoc]
    events.sort(key=lambda e: (e[1], e[0]))

    for kind, d, obj in events:
        inv_id = g.invoice_id(len(invoices) + 1)
        cust = obj["customer"]
        inv_date = d + timedelta(days=r.randint(0, 3)) if kind == "flight" else d
        terms = TERMS_DAYS[cust["payment_terms"]]
        due = inv_date + timedelta(days=terms)
        currency = "AED" if g.chance(r, 0.15) else "USD"
        fx = AED_PER_USD if currency == "AED" else 1.0
        if kind == "flight":
            f = obj
            ac_type = f["aircraft"]["type"]
            bh = round(f["block_h"] * 2, 2)
            items = [("Charter fee", bh, RATE_PER_BH[ac_type]),
                     ("Fuel surcharge", bh, 900.0),
                     ("Handling", 1.0, float(r.randint(1500, 4000)))]
            if f["cargo"]["code"] == "DANGEROUS_GOODS":
                items.append(("DG surcharge", 1.0, 2500.0))
            if f["cargo"]["code"] == "VALUABLES":
                items.append(("Insurance", 1.0, 1800.0))
            memo = invoice_memo(r, f)
            truth.append(("netsuite.invoice", inv_id, f["flight_no"]))
            f["invoice_id"] = inv_id
        else:
            items = [(r.choice(["Storage charges", "Documentation fee", "Demurrage"]), 1.0,
                      float(r.randint(300, 2500)))]
            memo = r.choice(["Storage charges DWC warehouse", "Documentation fee", "Demurrage at DWC"])
            truth.append(("netsuite.invoice", inv_id, ""))
        total = 0.0
        for desc, qty, price in items:
            price = round(price * fx, 2)
            amount = round(qty * price, 2)
            total += amount
            lines.append({"line_id": g.invoice_line_id(len(lines) + 1), "invoice_id": inv_id,
                          "desc": desc, "qty": qty, "price": price, "amount": amount, "date": inv_date})
        # payment behaviour
        status = "OPEN"
        pay_date = due + timedelta(days=g.weighted_choice(r, [r.randint(-10, 0), r.randint(1, 20),
                                                               r.randint(21, 60)], [55, 35, 10]))
        if g.chance(r, 0.01):
            status = "VOID"
        elif pay_date <= END:
            if g.chance(r, 0.05):
                status = "PARTIAL"
                payments.append({"invoice_id": inv_id, "date": pay_date,
                                 "amount": round(total * r.uniform(0.4, 0.7), 2)})
            else:
                status = "PAID"
                if g.chance(r, 0.1):
                    part = round(total * 0.5, 2)
                    payments.append({"invoice_id": inv_id, "date": pay_date - timedelta(days=7), "amount": part})
                    payments.append({"invoice_id": inv_id, "date": pay_date, "amount": round(total - part, 2)})
                else:
                    payments.append({"invoice_id": inv_id, "date": pay_date, "amount": round(total, 2)})
        elif pay_date - timedelta(days=30) <= END and g.chance(r, 0.3):
            status = "PARTIAL"
            payments.append({"invoice_id": inv_id, "date": END - timedelta(days=r.randint(0, 20)),
                             "amount": round(total * 0.5, 2)})
        invoices.append({"invoice_id": inv_id, "customer_id": cust["customer_id"], "date": inv_date,
                         "due": due, "currency": currency, "total": total, "status": status, "memo": memo})

    counts["invoice"] = counts["invoice_line"] = counts["payment"] = 0
    inv_months = by_month(invoices, lambda x: x["date"])
    carry = []          # rows repeated in the next month's export
    for idx, (ym, invs) in enumerate(inv_months):
        fname = "invoice_{}.csv".format(ym)
        rows = list(carry)
        carry = []
        for inv in invs:
            inv_date = g.fmt_dmy(inv["date"])
            due = g.fmt_dmy(inv["due"])
            if g.chance(re_, 0.005):
                dd = inv["date"]
                inv_date = re_.choice(["{:04d}/{:02d}/{:02d}".format(dd.year, dd.month, dd.day),
                                       "{:02d}.{:02d}.{:04d}".format(dd.day, dd.month, dd.year),
                                       "TBC"])
                plant("netsuite.invoice", fname, inv["invoice_id"], "unparseable_date")
            elif g.chance(re_, 0.005):
                due = g.fmt_dmy(inv["date"] - timedelta(days=5))
                plant("netsuite.invoice", fname, inv["invoice_id"], "due_before_invoice")
            row = [inv["invoice_id"], inv["customer_id"], inv_date, due, inv["currency"],
                   money(inv["total"]), inv["status"], inv["memo"]]
            rows.append(row)
            if g.chance(re_, 0.01) and idx + 1 < len(inv_months):
                carry.append(list(row))
                plant("netsuite.invoice", "invoice_{}.csv".format(inv_months[idx + 1][0]),
                      inv["invoice_id"], "repeated_in_next_export")
        rows = pad_and_null(re_, "netsuite.invoice", fname, rows, [0], [], [4])
        rows = add_duplicates(re_, "netsuite.invoice", fname, rows)
        counts["invoice"] += write(NS, fname, ["invoice_id", "customer_id", "invoice_date", "due_date",
                                               "currency", "total", "status", "memo"], rows)

    for ym, ls in by_month(lines, lambda x: x["date"]):
        fname = "invoice_line_{}.csv".format(ym)
        rows = []
        for l in ls:
            amount = money(l["amount"])
            if g.chance(re_, 0.005):
                amount = money(-l["amount"])
                plant("netsuite.invoice_line", fname, l["line_id"], "non_positive_amount")
            rows.append([l["line_id"], l["invoice_id"], l["desc"], money(l["qty"]), money(l["price"]), amount])
        rows = add_duplicates(re_, "netsuite.invoice_line", fname, rows)
        counts["invoice_line"] += write(NS, fname, ["line_id", "invoice_id", "description", "qty",
                                                    "unit_price", "amount"], rows)

    payments.sort(key=lambda p: (p["date"], p["invoice_id"]))
    for i, p in enumerate(payments, 1):
        p["payment_id"] = g.payment_id(i)
        p["method"] = g.weighted_choice(r, ["BANK_TRANSFER", "CHEQUE", "CARD"], [85, 10, 5])
    for ym, ps in by_month(payments, lambda x: x["date"]):
        fname = "payment_{}.csv".format(ym)
        rows = []
        for p in ps:
            inv = p["invoice_id"]
            if g.chance(re_, 0.005):
                inv = g.invoice_id(900000 + int(p["payment_id"][4:]))
                plant("netsuite.payment", fname, p["payment_id"], "orphan_invoice_id")
            rows.append([p["payment_id"], inv, g.fmt_dmy(p["date"]), money(p["amount"]), p["method"]])
        rows = add_duplicates(re_, "netsuite.payment", fname, rows)
        counts["payment"] += write(NS, fname, ["payment_id", "invoice_id", "paid_date", "amount", "method"], rows)

    # purchase orders
    def supplier_at(category, iata=None, country=None):
        if iata is not None:
            city = g.airport(iata)["city"]
            for s in suppliers:
                if s["category"] == category and s["name"].startswith(city):
                    return s
            country = g.airport(iata)["country"]
        for s in suppliers:
            if s["category"] == category and (country is None or s["country"] == country):
                return s
        raise KeyError(category)

    pos, po_lines = [], []
    def new_po(sup, d, items, flight):
        pid = g.po_id(len(pos) + 1)
        total = 0.0
        for desc, qty, cost in items:
            amount = round(qty * cost, 2)
            total += amount
            lid = g.po_line_id(len(po_lines) + 1)
            po_lines.append({"po_line_id": lid, "po_id": pid, "desc": desc, "qty": qty,
                             "cost": cost, "amount": amount, "date": d})
            truth.append(("netsuite.po_line", lid, flight["flight_no"] if flight else ""))
        if d > END - timedelta(days=20):
            status = "OPEN"
        else:
            status = g.weighted_choice(r, ["RECEIVED", "CLOSED", "CANCELLED"], [60, 38, 2])
        pos.append({"po_id": pid, "supplier_id": sup["supplier_id"], "date": d,
                    "status": status, "total": total})

    for f in flown:
        d = f["date"] - timedelta(days=r.randint(1, 5))
        ac_type = f["aircraft"]["type"]
        litres = float(int(f["block_h"] * FUEL_L_PER_BH[ac_type] * r.uniform(0.95, 1.1)))
        new_po(supplier_at("FUEL", f["origin"]), d,
               [(po_desc(r, "fuel", f), litres, round(r.uniform(0.70, 0.95), 2))], f)
        items = [(po_desc(r, "handling", f), 1.0, float(r.randint(1200, 3500)))]
        if g.chance(r, 0.5):
            items.append(("Cargo handling {} {}".format(f["dest"], date_variants(f["date"])["ddmm"]),
                          round(f["weight"] / 1000.0, 2), 45.0))
        new_po(supplier_at("GROUND_HANDLING", f["dest"]), d, items, f)
        nav_iata = f["dest"] if f["dest"] not in g.UAE_IATA else f["origin"]
        new_po(supplier_at("NAVIGATION", country=g.airport(nav_iata)["country"]), d,
               [(po_desc(r, "nav", f), 1.0, float(r.randint(400, 2500)))], f)
    for first, last in g.all_months():
        mlabel = "{} {}".format(g.MONTH_FULL[first.month - 1], first.year)
        for cat, desc, lo, hi in [("MAINTENANCE", "Line maintenance {}", 15000, 40000),
                                  ("INSURANCE", "Hull insurance {}", 22000, 22000),
                                  ("CREW_HOTEL", "Crew accommodation {}", 6000, 14000),
                                  ("AIRPORT_CHARGES", "DWC airport charges {}", 8000, 20000),
                                  ("CATERING", "Catering uplift {}", 1500, 4000)]:
            iata = "DWC" if cat == "AIRPORT_CHARGES" else None
            sup = supplier_at(cat, iata) if iata else supplier_at(cat)
            new_po(sup, first + timedelta(days=r.randint(0, 5)),
                   [(desc.format(mlabel), 1.0, float(r.randint(lo, hi)))], None)

    counts["purchase_order"] = counts["po_line"] = 0
    pos.sort(key=lambda p: p["po_id"])
    for ym, ps in by_month(pos, lambda x: x["date"]):
        fname = "purchase_order_{}.csv".format(ym)
        rows = [[p["po_id"], p["supplier_id"], g.fmt_dmy(p["date"]), p["status"], money(p["total"])] for p in ps]
        rows = pad_and_null(re_, "netsuite.purchase_order", fname, rows, [0], [], [3])
        rows = add_duplicates(re_, "netsuite.purchase_order", fname, rows)
        counts["purchase_order"] += write(NS, fname, ["po_id", "supplier_id", "po_date", "status", "total"], rows)
    for ym, ls in by_month(po_lines, lambda x: x["date"]):
        fname = "po_line_{}.csv".format(ym)
        rows = [[l["po_line_id"], l["po_id"], l["desc"], money(l["qty"]), money(l["cost"]), money(l["amount"])]
                for l in ls]
        rows = add_duplicates(re_, "netsuite.po_line", fname, rows)
        counts["po_line"] += write(NS, fname, ["po_line_id", "po_id", "item_desc", "qty", "unit_cost", "amount"], rows)

    return counts, truth, len(invoices), len(pos)


# ---------------------------------------------------------------------------
# Maintenance programme
# ---------------------------------------------------------------------------
DEFECTS = [
    ("hyd press low LH sys",          "HYDRAULIC",       "hydraulic system",       "MEDIUM"),
    ("hyd fluid leak RH MLG actuator", "HYDRAULIC",      "landing gear actuator",  "HIGH"),
    ("elec gen 2 trip in flt",        "ELECTRICAL",      "generator",              "HIGH"),
    ("elec bus tie light illum",      "ELECTRICAL",      "bus tie",                "MEDIUM"),
    ("eng 1 EGT exceedance on TO",    "ENGINE",          "engine",                 "HIGH"),
    ("eng 2 oil qty low",             "ENGINE",          "engine oil system",      "MEDIUM"),
    ("apu fails to start",            "APU",             "apu starter",            "MEDIUM"),
    ("apu bleed valve faulty",        "APU",             "apu bleed valve",        "LOW"),
    ("ldg gear unsafe light nose gear", "LANDING_GEAR",  "nose gear",              "HIGH"),
    ("brake wear pin at limit pos 3", "LANDING_GEAR",    "brake",                  "MEDIUM"),
    ("avionics FMC map shift",        "AVIONICS",        "fmc",                    "LOW"),
    ("avionics VHF 1 intermittent",   "AVIONICS",        "vhf radio",              "LOW"),
    ("fuel qty ind inop L wing tank", "FUEL",            "fuel quantity indicator", "MEDIUM"),
    ("fuel leak at R wing panel",     "FUEL",            "wing tank",              "AOG"),
    ("press cabin alt warning",       "PRESSURISATION",  "outflow valve",          "HIGH"),
    ("press pack 1 trip",             "PRESSURISATION",  "air conditioning pack",  "MEDIUM"),
    ("flt ctrl aileron trim jam",     "FLIGHT_CONTROLS", "aileron trim",           "AOG"),
    ("flt ctrl spoiler 4 fault msg",  "FLIGHT_CONTROLS", "spoiler",                "MEDIUM"),
]
ACTIONS = [("topped up", "monitor", "Y"), ("replaced", "closed", "Y"), ("reset iaw AMM", "ops normal", "Y"),
           ("deferred MEL", "c/f", "N"), ("inspected NFF", "closed", "Y"), ("cleaned and tested", "ops normal", "Y"),
           ("awaiting parts", "open", "N")]
BASE_HOURS = {"A6-GSA": 41200.0, "A6-GSB": 38750.0, "A6-GSC": 52300.0, "A6-GSD": 29800.0}
BASE_CYCLES = {"A6-GSA": 24100, "A6-GSB": 22600, "A6-GSC": 21900, "A6-GSD": 11300}


def gen_maint(flights):
    r = g.rng("maint")
    counts = {}
    rows = [[g.reg_maint(a["registration"]), a["type"]] for a in g.FLEET]
    counts["aircraft_ref"] = write(MAINT, "aircraft_ref.csv", ["registration", "type"], rows)

    # cumulative hours and cycles per aircraft per day (flown legs x2 for the return ferry)
    usage = {}
    for f in flights:
        if f["status"] == "CANCELLED":
            continue
        usage.setdefault(f["registration"], []).append((f["date"], f["block_h"] * 2, 2))

    def hours_cycles(reg, d):
        h, c = BASE_HOURS[reg], BASE_CYCLES[reg]
        for fd, bh, cy in usage.get(reg, []):
            if fd <= d:
                h += bh
                c += cy
        return h, c

    events = []
    for a in g.FLEET:
        reg = a["registration"]
        plan = []
        for i in range(g.MONTHS):
            first, last = g.month_range(i)
            plan.append(("LINE_CHECK", g.random_date(r, first, last)))
            if i % 3 == 1:
                plan.append(("A_CHECK", g.random_date(r, first, last)))
            if i % 6 == 4:
                plan.append(("ENGINE_INSPECTION", g.random_date(r, first, last)))
            if g.chance(r, 0.5):
                plan.append(("COMPONENT_CHANGE", g.random_date(r, first, last)))
        plan.append(("C_CHECK", g.random_date(r, *g.month_range(r.randint(6, 30)))))
        for etype, due in plan:
            done = None
            if due <= END:
                slip = g.weighted_choice(r, [r.randint(-7, 0), r.randint(1, 10), None], [85, 12, 3])
                if slip is not None and due + timedelta(days=slip) <= END:
                    done = due + timedelta(days=slip)
            if done is not None:
                status = "DONE"
            elif due < END:
                status = "OVERDUE"
            else:
                status = "PLANNED"
            h, c = hours_cycles(reg, done or due)
            events.append((due, reg, etype, done, h, c, status))
    events.sort(key=lambda e: (e[0], e[1], e[2]))
    rows = []
    for i, (due, reg, etype, done, h, c, status) in enumerate(events, 1):
        rows.append([g.maint_event_id(i), g.reg_maint(reg), etype, g.fmt_mon(due),
                     g.fmt_mon(done) if done else "", money(h), str(c), status])
    rows = pad_and_null(r, "maint.maintenance_event", "maintenance_event.csv", rows, [], [4], [])
    rows = add_duplicates(r, "maint.maintenance_event", "maintenance_event.csv", rows)
    counts["maintenance_event"] = write(MAINT, "maintenance_event.csv",
                                        ["event_id", "registration", "event_type", "due_date", "done_date",
                                         "flight_hours_at", "cycles_at", "status"], rows)

    # defects, about 3 per month across the fleet
    defects, truth = [], []
    for first, last in g.all_months():
        for _ in range(r.randint(2, 4)):
            defects.append((g.random_date(r, first, last), r.choice(g.FLEET)["registration"],
                            r.choice(DEFECTS), r.choice(ACTIONS)))
    defects.sort(key=lambda x: (x[0], x[1]))
    rows = []
    for i, (d, reg, (fault, sysc, comp, sev), (action, state, resolved)) in enumerate(defects, 1):
        did = g.defect_id(i)
        text = "{}, {}, {}".format(fault, action, state)
        if g.chance(r, 0.1):
            text = text.upper()
        severity = sev if g.chance(r, 0.8) else ""
        rows.append([did, g.reg_maint(reg), g.fmt_mon(d), text, severity, resolved])
        truth.append((did, sysc, comp, sev))
    rows = pad_and_null(r, "maint.defect", "defect.csv", rows, [], [4], [])
    rows = add_duplicates(r, "maint.defect", "defect.csv", rows)
    counts["defect"] = write(MAINT, "defect.csv", ["defect_id", "registration", "reported_date",
                                                   "description", "severity", "resolved"], rows)
    return counts, truth


# ---------------------------------------------------------------------------
# Labels and main
# ---------------------------------------------------------------------------
def write_labels(flights, ref_truth, defect_truth):
    g.write_csv(os.path.join(g.LABELS, "shipments_master.csv"),
                ["flight_no", "date", "sched_dep", "origin_iata", "dest_iata", "registration", "status",
                 "customer_id", "cargo_type", "weight_kg", "pieces", "awb_no", "manifest_id", "invoice_id"],
                [[f["flight_no"], g.fmt_iso(f["date"]), g.fmt_event_ts(f["dep"]), f["origin"], f["dest"],
                  f["registration"], f["status"], f["customer"]["customer_id"], f["cargo"]["code"],
                  str(f["weight"]), str(f["pieces"]), f["awb"] if f["status"] != "CANCELLED" else "",
                  f.get("manifest_id", ""), f.get("invoice_id", "")] for f in flights])
    g.write_csv(os.path.join(g.LABELS, "flight_ref_truth.csv"),
                ["source_system", "source_ref", "flight_no"], ref_truth)
    g.write_csv(os.path.join(g.LABELS, "defect_truth.csv"),
                ["defect_id", "system_class", "component", "severity_class"], defect_truth)
    g.write_csv(os.path.join(g.LABELS, "planted_errors.csv"),
                ["table", "file", "key", "error"], PLANTED)


def clear_old_exports():
    for d in (NS, LION, MAINT):
        g.ensure_dir(d)
        for p in glob.glob(os.path.join(d, "*.csv")):
            os.remove(p)


def main():
    clear_old_exports()
    customers = g.make_customers()
    suppliers = g.make_suppliers()
    flights = build_flights(customers)

    counts = {}
    counts.update(gen_lion(flights))
    ns_counts, ref_truth, n_inv, n_po = gen_netsuite(flights, customers, suppliers)
    counts.update(ns_counts)
    m_counts, defect_truth = gen_maint(flights)
    counts.update(m_counts)
    write_labels(flights, ref_truth, defect_truth)

    print("Rows written (including planted duplicates):")
    for system, tables in [("netsuite", ["customer", "supplier", "invoice", "invoice_line", "payment",
                                         "purchase_order", "po_line"]),
                           ("lion", ["airport", "aircraft", "flight", "manifest"]),
                           ("maint", ["aircraft_ref", "maintenance_event", "defect"])]:
        for t in tables:
            print("  {:<9} {:<18} {:>6}".format(system, t, counts[t]))
    files = sum(len(glob.glob(os.path.join(d, "*.csv"))) for d in (NS, LION, MAINT))
    print("")
    print("Files:          {} CSV files in staging/exports/".format(files))
    print("Flights:        {} ({} cancelled)".format(len(flights), sum(1 for f in flights if f["status"] == "CANCELLED")))
    print("Invoices:       {}   Purchase orders: {}".format(n_inv, n_po))
    kinds = {}
    for p in PLANTED:
        kinds[p[3]] = kinds.get(p[3], 0) + 1
    print("Planted errors: {}".format(len(PLANTED)))
    for k in sorted(kinds):
        print("  {:<26} {:>5}".format(k, kinds[k]))
    print("")
    print("Labels written to generators/labels/: shipments_master, flight_ref_truth, defect_truth, planted_errors")


if __name__ == "__main__":
    main()