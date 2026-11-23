"""
gen_common.py - shared settings and helpers for every GSS generator.

Python 3.4, standard library only. Every generator imports this module so
that IDs, reference data, dates and the random seed are the same everywhere.

Rule for determinism: Python 3.4 dictionaries and sets have no fixed order,
so reference data is kept in lists, and anything random is drawn from rng().
Run this file directly to self-test:  python3 generators/gen_common.py
"""

import csv
import os
import random
from datetime import date, datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# The data folder can live outside the project (e.g. on the VM's own disk, which is much
# faster than a VirtualBox shared folder): set GSS_STAGING to its path. Default: <project>/staging
STAGING = os.environ.get("GSS_STAGING") or os.path.join(ROOT, "staging")
EXPORTS = os.path.join(STAGING, "exports")
MESSAGES = os.path.join(STAGING, "messages")
EVENTS = os.path.join(STAGING, "events")
DOCUMENTS = os.path.join(STAGING, "documents")
LABELS = os.path.join(ROOT, "generators", "labels")
SCHEMAS = os.path.join(ROOT, "preprocess", "config", "schemas.json")

# ---------------------------------------------------------------------------
# Seed and period [A]
# ---------------------------------------------------------------------------
SEED = 312
START_DATE = date(2023, 11, 1)
MONTHS = 36
UAE_TZ = timezone(timedelta(hours=4))

# Volumes per month: requests, flights, shipments [R]; the rest [A]
REQUESTS_PER_MONTH = 300
FLIGHTS_PER_MONTH = 40
SHIPMENTS_PER_MONTH = 40
MESSAGES_PER_MONTH = 1200
DOCUMENTS_PER_MONTH = 160


def rng(name):
    """Independent, repeatable random stream for one generator or table.
    A string seed is hashed with SHA-512, so it is the same on every run."""
    return random.Random("{}:{}".format(SEED, name))


# ---------------------------------------------------------------------------
# Reference data [A]
# ---------------------------------------------------------------------------
FLEET = [
    {"aircraft_id": "AC01", "registration": "A6-GSA", "type": "B737-400F",   "max_payload_kg": 19000},
    {"aircraft_id": "AC02", "registration": "A6-GSB", "type": "B737-400F",   "max_payload_kg": 19000},
    {"aircraft_id": "AC03", "registration": "A6-GSC", "type": "A300B4-200F", "max_payload_kg": 40000},
    {"aircraft_id": "AC04", "registration": "A6-GSD", "type": "IL-76TD",     "max_payload_kg": 42000},
]

HOME_BASE = "DWC"

# block_h: approximate block time from DWC in hours [A]; 0 for UAE airports
AIRPORTS = [
    {"icao": "OMDW", "iata": "DWC", "name": "Al Maktoum International",        "city": "Dubai",       "country": "United Arab Emirates", "block_h": 0.0},
    {"icao": "OMDB", "iata": "DXB", "name": "Dubai International",             "city": "Dubai",       "country": "United Arab Emirates", "block_h": 0.0},
    {"icao": "OMSJ", "iata": "SHJ", "name": "Sharjah International",           "city": "Sharjah",     "country": "United Arab Emirates", "block_h": 0.0},
    {"icao": "HKJK", "iata": "NBO", "name": "Jomo Kenyatta International",     "city": "Nairobi",     "country": "Kenya",                "block_h": 5.0},
    {"icao": "HAAB", "iata": "ADD", "name": "Addis Ababa Bole International",  "city": "Addis Ababa", "country": "Ethiopia",             "block_h": 4.0},
    {"icao": "HUEN", "iata": "EBB", "name": "Entebbe International",           "city": "Entebbe",     "country": "Uganda",               "block_h": 5.3},
    {"icao": "HJJJ", "iata": "JUB", "name": "Juba International",              "city": "Juba",        "country": "South Sudan",          "block_h": 4.8},
    {"icao": "HCMM", "iata": "MGQ", "name": "Aden Adde International",         "city": "Mogadishu",   "country": "Somalia",              "block_h": 4.3},
    {"icao": "FZAA", "iata": "FIH", "name": "N'djili International",           "city": "Kinshasa",    "country": "DR Congo",             "block_h": 7.5},
    {"icao": "DNMM", "iata": "LOS", "name": "Murtala Muhammed International",  "city": "Lagos",       "country": "Nigeria",              "block_h": 8.5},
    {"icao": "OAKB", "iata": "KBL", "name": "Kabul International",             "city": "Kabul",       "country": "Afghanistan",          "block_h": 3.0},
    {"icao": "ORBI", "iata": "BGW", "name": "Baghdad International",           "city": "Baghdad",     "country": "Iraq",                 "block_h": 2.5},
    {"icao": "ORER", "iata": "EBL", "name": "Erbil International",             "city": "Erbil",       "country": "Iraq",                 "block_h": 2.7},
    {"icao": "UTTT", "iata": "TAS", "name": "Tashkent International",          "city": "Tashkent",    "country": "Uzbekistan",           "block_h": 3.4},
    {"icao": "OPKC", "iata": "KHI", "name": "Jinnah International",            "city": "Karachi",     "country": "Pakistan",             "block_h": 2.2},
    {"icao": "VGHS", "iata": "DAC", "name": "Hazrat Shahjalal International",  "city": "Dhaka",       "country": "Bangladesh",           "block_h": 4.8},
]
UAE_IATA = ["DWC", "DXB", "SHJ"]
DESTINATIONS = [a["iata"] for a in AIRPORTS if a["iata"] not in UAE_IATA]

# code, IATA special handling code ("" if none), relative weight, typical kg range
CARGO_TYPES = [
    {"code": "GENERAL",           "shc": "",    "weight": 30, "kg": (500, 15000),  "desc": "General cargo"},
    {"code": "PERISHABLE",        "shc": "PER", "weight": 12, "kg": (1000, 12000), "desc": "Fresh produce"},
    {"code": "PHARMA",            "shc": "PIL", "weight": 10, "kg": (300, 5000),   "desc": "Pharmaceuticals 2-8C"},
    {"code": "DANGEROUS_GOODS",   "shc": "DGR", "weight": 6,  "kg": (200, 4000),   "desc": "Dangerous goods class 3"},
    {"code": "HUMANITARIAN",      "shc": "",    "weight": 14, "kg": (2000, 30000), "desc": "Relief supplies"},
    {"code": "AOG_SPARES",        "shc": "AOG", "weight": 6,  "kg": (50, 2000),    "desc": "Aircraft spare parts"},
    {"code": "OIL_GAS_EQUIPMENT", "shc": "HEA", "weight": 10, "kg": (3000, 35000), "desc": "Drilling equipment"},
    {"code": "VEHICLES",          "shc": "HEA", "weight": 7,  "kg": (2000, 20000), "desc": "Vehicles"},
    {"code": "VALUABLES",         "shc": "VAL", "weight": 5,  "kg": (20, 500),     "desc": "Valuable cargo"},
]

SUPPLIER_CATEGORIES = ["FUEL", "GROUND_HANDLING", "NAVIGATION", "AIRPORT_CHARGES",
                       "CATERING", "MAINTENANCE", "CREW_HOTEL", "INSURANCE"]

_FIRST_NAMES = ["Ahmed", "Fatima", "Omar", "Priya", "Rahul", "Elena", "James", "Aisha",
                "Daniel", "Sara", "Yusuf", "Grace", "Hassan", "Irina", "Kofi", "Leila"]
_NAME_A = ["Gulf", "Desert", "Horizon", "Falcon", "Atlas", "Meridian", "Savanna", "Oasis",
           "Pioneer", "Crescent", "Summit", "Delta", "Coastal", "Northern", "Swift", "Global"]
_NAME_B = ["Freight", "Logistics", "Cargo", "Forwarding", "Aid Services", "Trading",
           "Supply Chain", "Shipping"]
_NAME_C = ["LLC", "FZE", "Ltd", "DMCC", "FZCO"]
_CUSTOMER_COUNTRIES = ["United Arab Emirates", "United Arab Emirates", "United Arab Emirates",
                       "Kenya", "Ethiopia", "Uganda", "Somalia", "Iraq", "Pakistan",
                       "Afghanistan", "Nigeria", "United Kingdom"]


def make_customers(n=60):
    """Customers and brokers with contact handles used by invoices and messages."""
    r = rng("customers")
    out, used = [], []
    i = 0
    while len(out) < n:
        name = "{} {} {}".format(r.choice(_NAME_A), r.choice(_NAME_B), r.choice(_NAME_C))
        if name in used:
            continue
        used.append(name)
        i += 1
        first = r.choice(_FIRST_NAMES)
        domain = name.split(" ")[0].lower() + name.split(" ")[1].lower() + ".example"
        is_broker = r.random() < 0.3
        out.append({
            "customer_id": customer_id(i),
            "name": name,
            "country": r.choice(_CUSTOMER_COUNTRIES),
            "payment_terms": "PREPAID" if is_broker and r.random() < 0.5
                             else r.choice(["NET15", "NET30", "NET30", "NET45"]),
            "kind": "broker" if is_broker else "direct",
            "contact": first,
            "email": "{}@{}".format(first.lower(), domain),
            "phone": "+9715{}{}".format(r.choice("02568"), r.randint(1000000, 9999999)),
        })
    return out


def make_suppliers():
    """One fuel, handling and airport-charges supplier per airport, one navigation
    supplier per country, and shared suppliers for the remaining categories."""
    out = []

    def add(name, category, country):
        out.append({"supplier_id": supplier_id(len(out) + 1), "name": name,
                    "category": category, "country": country})

    for a in AIRPORTS:
        add("{} Aviation Fuels".format(a["city"]), "FUEL", a["country"])
        add("{} Ground Services".format(a["city"]), "GROUND_HANDLING", a["country"])
        add("{} Airport Authority".format(a["city"]), "AIRPORT_CHARGES", a["country"])
    countries = []
    for a in AIRPORTS:
        if a["country"] not in countries:
            countries.append(a["country"])
    for c in countries:
        add("{} Air Navigation Services".format(c), "NAVIGATION", c)
    add("Skyline Inflight Catering FZE", "CATERING", "United Arab Emirates")
    add("Gulf Aero Technics FZE", "MAINTENANCE", "United Arab Emirates")
    add("Falcon Line Maintenance LLC", "MAINTENANCE", "United Arab Emirates")
    add("DWC Crew Hotel LLC", "CREW_HOTEL", "United Arab Emirates")
    add("Meridian Aviation Insurance Ltd", "INSURANCE", "United Kingdom")
    return out


# ---------------------------------------------------------------------------
# Identifiers (formats from docs/data_dictionary.md section 1)
# ---------------------------------------------------------------------------
def _fmt(prefix, n, width):
    return "{}{:0{}d}".format(prefix, n, width)

def customer_id(n):       return _fmt("CUS-", n, 4)
def supplier_id(n):       return _fmt("SUP-", n, 3)
def invoice_id(n):        return _fmt("INV-", n, 6)
def invoice_line_id(n):   return _fmt("IL-", n, 7)
def payment_id(n):        return _fmt("PAY-", n, 6)
def po_id(n):             return _fmt("PO-", n, 6)
def po_line_id(n):        return _fmt("POL-", n, 7)
def flight_no(n):         return _fmt("GS", n, 4)
def manifest_id(n):       return _fmt("MF-", n, 6)
def maint_event_id(n):    return _fmt("ME-", n, 5)
def defect_id(n):         return _fmt("DEF-", n, 5)
def event_id(n):          return _fmt("E-", n, 6)
def msg_id(year, n):      return "M-{}-{:06d}".format(year, n)
def thread_id(year, n):   return "T-{}-{:06d}".format(year, n)
def request_id(year, n):  return "R-{}-{:05d}".format(year, n)
def quote_no(year, n):    return "Q-{}-{:04d}".format(year, n)
def permit_no(year, n):   return "PRM-{}-{:04d}".format(year, n)
def shipment_ref(year, n): return "SHP-{}-{:04d}".format(year, n)


def awb_number(serial):
    """Air waybill 176-SSSSSSSC: 7-digit serial, check digit = serial mod 7 [I]."""
    serial = int(serial) % 10000000
    return "176-{:07d}{}".format(serial, serial % 7)


def awb_is_valid(awb):
    if len(awb) != 12 or awb[3] != "-" or not awb[4:].isdigit():
        return False
    return int(awb[4:11]) % 7 == int(awb[11])


def reg_std(reg):
    """A6GSA or a6-gsa -> A6-GSA (the form used in Lion and in the lake)."""
    r = reg.strip().upper().replace("-", "")
    return r[:2] + "-" + r[2:]


def reg_maint(reg):
    """A6-GSA -> A6GSA (the form the maintenance programme exports)."""
    return reg.strip().upper().replace("-", "")


def airport(iata):
    for a in AIRPORTS:
        if a["iata"] == iata:
            return a
    raise KeyError(iata)


# ---------------------------------------------------------------------------
# Dates and times. Month names are fixed so output does not depend on locale.
# ---------------------------------------------------------------------------
MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
MONTH_FULL = ["January", "February", "March", "April", "May", "June", "July",
              "August", "September", "October", "November", "December"]
WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def add_months(d, n):
    y, m = divmod(d.month - 1 + n, 12)
    return date(d.year + y, m + 1, 1)


def month_range(i):
    """(first_day, last_day) of month i, 0-based from START_DATE."""
    first = add_months(START_DATE, i)
    last = add_months(first, 1) - timedelta(days=1)
    return first, last


def all_months():
    return [month_range(i) for i in range(MONTHS)]


def end_date():
    return month_range(MONTHS - 1)[1]


def random_date(r, first, last):
    return first + timedelta(days=r.randint(0, (last - first).days))


def fmt_dmy(d):      return "{:02d}/{:02d}/{:04d}".format(d.day, d.month, d.year)        # NetSuite
def fmt_iso(d):      return "{:04d}-{:02d}-{:02d}".format(d.year, d.month, d.day)
def fmt_mon(d):      return "{:02d}-{}-{:04d}".format(d.day, MONTH_ABBR[d.month - 1], d.year)  # maintenance
def fmt_yyyymm(d):   return "{:04d}{:02d}".format(d.year, d.month)
def fmt_lion_ts(dt): return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}".format(dt.year, dt.month, dt.day, dt.hour, dt.minute)


def fmt_event_ts(dt):
    return "{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}".format(
        dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)


def fmt_msg_ts(dt):
    """Message envelope time with the UAE offset, e.g. 2024-03-12T08:41:07+04:00."""
    return fmt_event_ts(dt) + "+04:00"


# ---------------------------------------------------------------------------
# Random helpers (random.choices does not exist before Python 3.6)
# ---------------------------------------------------------------------------
def weighted_choice(r, items, weights):
    total = float(sum(weights))
    x = r.random() * total
    upto = 0.0
    for item, w in zip(items, weights):
        upto += w
        if x < upto:
            return item
    return items[-1]


def pick_cargo(r):
    return weighted_choice(r, CARGO_TYPES, [c["weight"] for c in CARGO_TYPES])


def chance(r, rate):
    return r.random() < rate


# ---------------------------------------------------------------------------
# Planted-error helpers (docs/data_dictionary.md section 4.4)
# ---------------------------------------------------------------------------
NULL_TOKENS = ["N/A", "NULL", "-"]


def maybe_null(r, value, rate=0.02):
    """Replace a value with a null token at the given rate."""
    return r.choice(NULL_TOKENS) if chance(r, rate) else value


def maybe_pad(r, value, rate=0.03):
    """Add leading or trailing spaces at the given rate."""
    if not chance(r, rate):
        return value
    return r.choice(["  {}", "{}  ", " {} "]).format(value)


# ---------------------------------------------------------------------------
# File output
# ---------------------------------------------------------------------------
def ensure_dir(path):
    os.makedirs(path, exist_ok=True)
    return path


def write_csv(path, header, rows, encoding="utf-8", bom=False):
    """Write a CSV with a header row. Returns the number of data rows."""
    ensure_dir(os.path.dirname(path))
    enc = "utf-8-sig" if (bom and encoding == "utf-8") else encoding
    n = 0
    with open(path, "w", newline="", encoding=enc) as f:
        w = csv.writer(f)
        w.writerow(header)
        for row in rows:
            w.writerow(row)
            n += 1
    return n


# ---------------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------------
def _self_test():
    import json
    import re

    problems = []

    def check(ok, msg):
        if not ok:
            problems.append(msg)

    # determinism
    check([rng("t").random() for _ in range(3)] == [rng("t").random() for _ in range(3)],
          "rng() is not repeatable")
    check(rng("a").random() != rng("b").random(), "rng() streams are not independent")

    # identifiers against the patterns in schemas.json
    with open(SCHEMAS, encoding="utf-8") as f:
        schemas = json.load(f)
    st = schemas["structured"]

    def pattern(table, col):
        for c in st[table]["columns"]:
            if c["name"] == col:
                return c["pattern"]
        raise KeyError(col)

    samples = [
        ("netsuite.customer", "customer_id", customer_id(42)),
        ("netsuite.supplier", "supplier_id", supplier_id(7)),
        ("netsuite.invoice", "invoice_id", invoice_id(315)),
        ("netsuite.invoice_line", "line_id", invoice_line_id(1027)),
        ("netsuite.payment", "payment_id", payment_id(288)),
        ("netsuite.purchase_order", "po_id", po_id(911)),
        ("netsuite.po_line", "po_line_id", po_line_id(2410)),
        ("lion.flight", "flight_no", flight_no(412)),
        ("lion.manifest", "manifest_id", manifest_id(1233)),
        ("lion.manifest", "awb_no", awb_number(1234567)),
        ("maint.maintenance_event", "event_id", maint_event_id(87)),
        ("maint.defect", "defect_id", defect_id(213)),
        ("lion.aircraft", "registration", FLEET[1]["registration"]),
    ]
    for table, col, value in samples:
        check(re.match(pattern(table, col), value) is not None,
              "{} {} does not match {}".format(table, value, pattern(table, col)))
    check(re.match(schemas["messages"]["msg_id_pattern"], msg_id(2024, 123)) is not None,
          "msg_id does not match schema")

    # AWB and registrations
    check(awb_number(1234567) == "176-12345675", "awb_number(1234567) wrong")
    check(awb_is_valid("176-12345675") and not awb_is_valid("176-12345670"), "awb_is_valid wrong")
    check(reg_maint("A6-GSB") == "A6GSB" and reg_std("a6gsb") == "A6-GSB", "registration helpers wrong")

    # dates round-trip through the schema input formats
    d = date(2024, 3, 12)
    for fmt_func, tname in ((fmt_dmy, "date_dmy"), (fmt_mon, "date_mon"), (fmt_iso, "date_iso")):
        fmts = schemas["types"][tname]["input_formats"]
        check(datetime.strptime(fmt_func(d), fmts[0]).date() == d, tname + " does not round-trip")
    dt = datetime(2024, 3, 12, 6, 30)
    check(datetime.strptime(fmt_lion_ts(dt), schemas["types"]["ts_lion"]["input_formats"][0]) == dt,
          "ts_lion does not round-trip")
    check(fmt_msg_ts(datetime(2024, 3, 12, 8, 41, 7)) == "2024-03-12T08:41:07+04:00", "fmt_msg_ts wrong")

    # period
    first, last = month_range(0)
    check((first, last) == (date(2023, 11, 1), date(2023, 11, 30)), "first month wrong")
    check(end_date() == date(2026, 10, 31), "end date wrong")

    # reference data
    customers = make_customers()
    suppliers = make_suppliers()
    check(len(set(c["customer_id"] for c in customers)) == 60, "customer ids not unique")
    check(len(set(c["name"] for c in customers)) == 60, "customer names not unique")
    check(make_customers() == customers, "customers not repeatable")
    check(len(set(s["supplier_id"] for s in suppliers)) == len(suppliers), "supplier ids not unique")
    for s in suppliers:
        check(s["category"] in SUPPLIER_CATEGORIES, "bad supplier category " + s["category"])
    check(len(set(a["iata"] for a in AIRPORTS)) == len(AIRPORTS), "duplicate airports")
    check(HOME_BASE in UAE_IATA, "home base not in UAE list")

    # summary
    print("Root:          {}".format(ROOT))
    print("Staging:       {}".format(STAGING))
    print("Period:        {} to {} ({} months)".format(START_DATE, end_date(), MONTHS))
    print("Fleet:         {}".format(", ".join(a["registration"] for a in FLEET)))
    print("Airports:      {} ({} destinations)".format(len(AIRPORTS), len(DESTINATIONS)))
    print("Cargo types:   {}".format(len(CARGO_TYPES)))
    print("Customers:     {} ({} brokers)".format(len(customers), sum(1 for c in customers if c["kind"] == "broker")))
    print("Suppliers:     {}".format(len(suppliers)))
    print("Sample ids:    {} {} {} {} {}".format(customer_id(1), invoice_id(1), flight_no(1),
                                                 awb_number(1234567), msg_id(2024, 1)))
    print("First customer: {} | {} | {} | {}".format(customers[0]["customer_id"], customers[0]["name"],
                                                    customers[0]["email"], customers[0]["phone"]))
    print("")
    if problems:
        print("FAILED with {} problem(s):".format(len(problems)))
        for p in problems:
            print("  - " + p)
        return 1
    print("OK: gen_common self-test passed")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_self_test())