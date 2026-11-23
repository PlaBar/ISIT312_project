import json, re, sys

path = "preprocess/config/schemas.json"
try:
    with open(path, encoding="utf-8") as f:
        s = json.load(f)
except ValueError as e:
    print("INVALID JSON: {}".format(e))
    sys.exit(1)

errors = []
known_types = set(s["types"])
expected = {
    "netsuite": ["customer", "supplier", "invoice", "invoice_line", "payment", "purchase_order", "po_line"],
    "lion": ["airport", "aircraft", "flight", "manifest"],
    "maint": ["aircraft_ref", "maintenance_event", "defect"],
}
for system, tables in sorted(expected.items()):
    for t in tables:
        if system + "." + t not in s["structured"]:
            errors.append("missing table {}.{}".format(system, t))

for name, t in sorted(s["structured"].items()):
    cols = [c["name"] for c in t["columns"]]
    for pk in t["primary_key"]:
        if pk not in cols:
            errors.append("{}: primary key {} not a column".format(name, pk))
    for c in t["columns"]:
        if c["type"] not in known_types:
            errors.append("{}.{}: unknown type {}".format(name, c["name"], c["type"]))
        if "pattern" in c:
            try:
                re.compile(c["pattern"])
            except re.error as e:
                errors.append("{}.{}: bad pattern {}".format(name, c["name"], e))
    for r in t["row_rules"]:
        for k in ("column", "other"):
            if r[k] not in cols:
                errors.append("{}: rule refers to unknown column {}".format(name, r[k]))
    print("{:<26} {:>2} columns  key={}".format(name, len(cols), ",".join(t["primary_key"])))

for section in ("messages", "events", "documents"):
    if section not in s:
        errors.append("missing section " + section)

samples = [("^CUS-\\d{4}$", "CUS-0042"), ("^A6-[A-Z]{3}$", "A6-GSB"),
           ("^176-\\d{8}$", "176-12345675"), ("^GS\\d{4}$", "GS0412"),
           ("^M-\\d{4}-\\d{6}$", "M-2024-000123")]
for pat, val in samples:
    if not re.match(pat, val):
        errors.append("pattern {} does not match example {}".format(pat, val))

print("")
if errors:
    print("FAILED with {} problem(s):".format(len(errors)))
    for e in errors:
        print("  - " + e)
    sys.exit(1)
print("OK: {} tables, messages, events and documents sections valid".format(len(s["structured"])))

