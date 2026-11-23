"""
rules_twin.py - Python twin of lake/extract/Rules.scala (Step 22).

The Scala rules cannot be unit-tested without Spark, so the same logic is written here line
for line and checked against the hidden truth (generators/labels/requests_truth.csv). Any
change to the rules is made in both files. Rules.scala's check job (RulesCheck.scala)
compares its own results with this twin's, message by message, to catch translation bugs.

  python3 evaluation/rules_twin.py            score all messages, write the predictions
  python3 evaluation/rules_twin.py --show 20  also print 20 wrong examples per field

Reads the original message files under $GSS_STAGING/messages (applying the same body and
timestamp cleaning as preprocess/pp_messages.py) and writes
$GSS_STAGING/reports/rules_twin_predictions.csv.

Python 3.4, standard library only.
"""

import csv
import json
import os
import re
import sys
from datetime import date, datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DICT = os.path.join(ROOT, "lake", "extract", "dictionaries")
STAGING = os.environ.get("GSS_STAGING") or os.path.join(ROOT, "staging")
LABELS = os.path.join(ROOT, "generators", "labels")
FIELDS = ["origin_iata", "dest_iata", "cargo_type", "weight_kg", "ready_date", "special_handling"]
OWN_DOMAIN = "@gss.example"


# ---------------------------------------------------------------------------
# Dictionaries
# ---------------------------------------------------------------------------
def load(name):
    out = []
    with open(os.path.join(DICT, name), encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                out.append(line.split("|"))
    return out


class Dicts(object):
    def __init__(self):
        a = load("airports.txt")
        self.airports = [(x[0], x[1], len(x) > 2 and x[2] == "exact_case") for x in a]
        c = load("cargo_types.txt")
        self.cargo = [(x[1], x[2]) for x in c if x[0] == "cargo"]
        self.shc = [(x[1], x[2]) for x in c if x[0] == "shc"]
        self.units = dict((x[1], int(x[2])) for x in c if x[0] == "unit")
        self.not_weight = [x[1] for x in c if x[0] == "not_weight"]
        self.request = [x[1] for x in c if x[0] == "request"]
        d = load("date_phrases.txt")
        self.months = dict((x[1], int(x[2])) for x in d if x[0] == "month")
        self.weekdays = dict((x[1], int(x[2])) for x in d if x[0] == "weekday")
        self.relative = [(x[1], x[2]) for x in d if x[0] == "relative"]
        self.context = [x[1] for x in d if x[0] == "context"]


def word_re(term):
    return r"(?<![A-Za-z0-9])" + re.escape(term) + r"(?![A-Za-z0-9])"


def find_all(term, text, ignore_case=True):
    """Start/end of every whole-word match."""
    return [(m.start(), m.end()) for m in re.finditer(word_re(term), text, re.I if ignore_case else 0)]


def longest_non_overlapping(hits):
    """hits = [(start, end, value, ...)]: keep the longest, drop those overlapping a kept one."""
    kept = []
    for h in sorted(hits, key=lambda h: (-(h[1] - h[0]), h[0])):
        if all(h[1] <= k[0] or h[0] >= k[1] for k in kept):
            kept.append(h)
    return sorted(kept, key=lambda h: h[0])


# ---------------------------------------------------------------------------
# Typo-tolerant phrase search (cargo types)
# ---------------------------------------------------------------------------
def near(a, b):
    """At most one letter dropped, added or changed, or two neighbouring letters swapped."""
    if a == b:
        return True
    if abs(len(a) - len(b)) > 1:
        return False
    if len(a) == len(b):
        diff = [i for i in range(len(a)) if a[i] != b[i]]
        if len(diff) == 1:
            return True
        return len(diff) == 2 and diff[1] == diff[0] + 1 and a[diff[0]] == b[diff[1]] and a[diff[1]] == b[diff[0]]
    s, l = (a, b) if len(a) < len(b) else (b, a)
    for i in range(len(l)):
        if l[:i] + l[i + 1:] == s:
            return True
    return False


WORD = re.compile(r"[a-z0-9\-]+")


def phrase_value(phrases, text, fuzzy):
    """Value of the longest phrase found (ties: the earliest in the text), or ''.
    Fuzzy: one word of 5+ letters may have one typo; the phrase may be written without its
    spaces, or with a typo across a space if it has 7+ letters and no word under 3."""
    low = text.lower()
    words = [(m.group(0), m.start()) for m in WORD.finditer(low)]
    best = None
    for phrase, value in phrases:
        pos = None
        exact = find_all(phrase, low)
        if exact:
            pos = exact[0][0]
        elif fuzzy:
            pw = phrase.split()
            joined = "".join(pw)
            n = len(pw)
            for i in range(len(words) - n + 1):
                window = [w for w, _ in words[i:i + n]]
                diff = [k for k in range(n) if window[k] != pw[k]]
                ok = len(diff) == 1 and len(pw[diff[0]]) >= 5 and near(window[diff[0]], pw[diff[0]])
                if not ok and n > 1:
                    jw = "".join(window)
                    ok = jw == joined or (len(joined) >= 7 and min(len(w) for w in pw) >= 3 and near(jw, joined))
                if ok:
                    pos = words[i][1]
                    break
            if pos is None and n > 1:
                for w, p in words:
                    if w == joined or (len(joined) >= 5 and near(w, joined)):
                        pos = p
                        break
        if pos is not None:
            key = (len(phrase), -pos)
            if best is None or key > best[0]:
                best = (key, value)
    return best[1] if best else ""


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------
PAIR_FWD = re.compile(r"^(\s*[-/>]\s*|\s+to\s+)$", re.I)       # A-B, A/B, A > B, A to B
PAIR_REV = re.compile(r"^\s+(ex|from)\s+$", re.I)             # B ex A, B from A
BEFORE_ORIGIN = re.compile(r"(?:^|[^a-z])(from|ex)\s+$", re.I)
BEFORE_DEST = re.compile(r"(?:^|[^a-z])(to|into|for)\s+$", re.I)


def airport_mentions(text, d):
    """[(start, end, code, weak)]; weak = a code that is also an English word ("add"), in
    lower case. route() only takes a place next to a route marker, so "please add
    insurance" never becomes Addis Ababa."""
    hits = []
    for term, code, exact in d.airports:
        for s, e in find_all(term, text):
            weak = exact and text[s:e] != term
            hits.append((s, e, code, weak))
    return longest_non_overlapping(hits)


def route(text, d):
    """(origin, dest). Pairs first (A-B, A to B, B ex A), then single places after
    from/ex (origin) or to/into/for (destination)."""
    ms = airport_mentions(text, d)
    origin, dest = "", ""
    for i in range(len(ms) - 1):
        a, b = ms[i], ms[i + 1]
        gap = text[a[1]:b[0]]
        if a[2] == b[2]:
            continue
        if PAIR_FWD.match(gap):
            return a[2], b[2]
        if PAIR_REV.match(gap):
            return b[2], a[2]
    for s, e, code, weak in ms:
        before = text[max(0, s - 8):s]
        if BEFORE_ORIGIN.search(before) and not origin:
            origin = code
        elif BEFORE_DEST.search(before) and not dest:
            dest = code
    if origin == dest:
        origin = ""
    return origin, dest


# ---------------------------------------------------------------------------
# Weight
# ---------------------------------------------------------------------------
NUMBER_UNIT = re.compile(r"(?<![A-Za-z0-9.,])(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)\s*([a-z]+)\b", re.I)


def weight(text, d):
    for m in NUMBER_UNIT.finditer(text):
        unit = m.group(2).lower()
        if unit not in d.units:
            continue
        before = text[max(0, m.start() - 12):m.start()].lower()
        if any(re.search(word_re(w) + r"\s*$", before) for w in d.not_weight):
            continue
        value = float(m.group(1).replace(",", ""))
        kg = int(round(value * d.units[unit]))
        if kg > 0:
            return str(kg)
    return ""


# ---------------------------------------------------------------------------
# Ready date
# ---------------------------------------------------------------------------
def next_on_or_after(sent, day, month):
    for year in (sent.year, sent.year + 1):
        try:
            x = date(year, month, day)
        except ValueError:
            continue
        if x >= sent:
            return x
    return None


def ready_date(text, sent, d):
    """The first date phrase that is not right after a context word ("as discussed on the
    phone 12/03"). Returns an ISO date or ''."""
    low = text.lower()
    month_alt = "|".join(sorted(d.months, key=len, reverse=True))
    cands = []                                                   # (start, end, date or None)

    for m in re.finditer(r"(?<![\d/.])(\d{1,2})[/.](\d{1,2})[/.](\d{4})(?![\d/]|\.\d)", low):
        try:
            cands.append((m.start(), m.end(), date(int(m.group(3)), int(m.group(2)), int(m.group(1)))))
        except ValueError:
            pass
    for m in re.finditer(r"(?<![\d/.])(\d{1,2})/(\d{1,2})(?![\d/])", low):
        dd, mm = int(m.group(1)), int(m.group(2))
        if 1 <= mm <= 12:
            cands.append((m.start(), m.end(), next_on_or_after(sent, dd, mm)))
    for m in re.finditer(r"(?<![a-z0-9])(\d{1,2})(?:st|nd|rd|th)?\s+(" + month_alt + r")(?![a-z])", low):
        cands.append((m.start(), m.end(), next_on_or_after(sent, int(m.group(1)), d.months[m.group(2)])))
    for m in re.finditer(r"(?<![a-z])(" + month_alt + r")\s+(\d{1,2})(?:st|nd|rd|th)?(?![\d])", low):
        cands.append((m.start(), m.end(), next_on_or_after(sent, int(m.group(2)), d.months[m.group(1)])))
    for m in re.finditer(r"\bon the (\d{1,2})(?:st|nd|rd|th)\b", low):
        day = int(m.group(1))
        x = None
        for k in (0, 1):
            y, mo = sent.year + (sent.month - 1 + k) // 12, (sent.month - 1 + k) % 12 + 1
            try:
                x = date(y, mo, day)
            except ValueError:
                continue
            if x >= sent:
                break
            x = None
        cands.append((m.start(), m.end(), x))
    for name, wd in d.weekdays.items():
        for s, e in find_all(name, low):
            days = (wd - 1 - sent.weekday()) % 7
            cands.append((s, e, sent + timedelta(days=days or 7)))
    rel = []
    for phrase, rule in d.relative:
        for s, e in find_all(phrase, low):
            rel.append((s, e, rule))
    for s, e, rule in longest_non_overlapping(rel):
        if rule == "IGNORE":
            continue
        if rule == "SENT":
            x = sent
        elif rule.startswith("SENT+"):
            x = sent + timedelta(days=int(rule[5:]))
        elif rule == "NEXT_MONDAY":
            x = sent + timedelta(days=7 - sent.weekday())
        else:
            continue
        cands.append((s, e, x))

    for s, e, x in longest_non_overlapping(cands):
        before = low[max(0, s - 30):s]
        if any(re.search(word_re(w), before) for w in d.context):
            continue
        if x is not None:
            return x.isoformat()
    return ""


# ---------------------------------------------------------------------------
# One message
# ---------------------------------------------------------------------------
def sent_local_date(sent_at_utc):
    """'2024-03-12T04:41:07Z' -> the UAE (UTC+4) calendar date."""
    t = datetime.strptime(sent_at_utc[:19], "%Y-%m-%dT%H:%M:%S")
    return (t + timedelta(hours=4)).date()


def extract(msg, d):
    """dict with is_request, the six fields, confidence and missing (the fields not found)."""
    body = msg.get("body") or ""
    handle = (msg.get("sender_handle") or "").lower()
    out = dict((f, "") for f in FIELDS)
    out["is_request"] = "0"
    out["confidence"] = ""
    out["missing"] = ""
    if msg.get("direction") != "inbound" or handle.endswith(OWN_DOMAIN):
        return out
    origin, dest = route(body, d)
    cargo = phrase_value(d.cargo, body, True)
    kg = weight(body, d)
    asks = any(find_all(w, body) for w in d.request)
    if not (asks and (dest or cargo or kg)):
        return out
    out.update({"is_request": "1", "origin_iata": origin, "dest_iata": dest, "cargo_type": cargo,
                "weight_kg": kg, "special_handling": phrase_value(d.shc, body, False),
                "ready_date": ready_date(body, sent_local_date(msg["sent_at"]), d)})
    missing = [f for f in ("origin_iata", "dest_iata", "cargo_type", "weight_kg", "ready_date") if not out[f]]
    out["missing"] = ";".join(missing)
    out["confidence"] = "high" if not missing else ("medium" if dest and (cargo or kg) else "low")
    return out


# ---------------------------------------------------------------------------
# Reading the messages (same cleaning as preprocess/pp_messages.py)
# ---------------------------------------------------------------------------
def clean_text(text):
    lines = [line.rstrip() for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n")]
    out, blank = [], 0
    for line in lines:
        blank = blank + 1 if not line.strip() else 0
        if blank <= 1:
            out.append(line)
    return "\n".join(out).strip()


def to_utc(value):
    v = value.strip()
    m = re.search(r"([+-])(\d{2}):?(\d{2})$", v)
    if v.endswith("Z"):
        return v[:19] + "Z"
    base, sign, hh, mm = (v[:m.start()], m.group(1), m.group(2), m.group(3)) if m else (v, "+", "04", "00")
    t = datetime.strptime(base[:19], "%Y-%m-%dT%H:%M:%S")
    off = timedelta(hours=int(hh), minutes=int(mm))
    t = t - off if sign == "+" else t + off
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def read_messages():
    out = {}
    for ch in ("email", "whatsapp"):
        folder = os.path.join(STAGING, "messages", ch)
        for name in sorted(os.listdir(folder)):
            try:
                with open(os.path.join(folder, name), encoding="utf-8") as f:
                    e = json.load(f)
            except ValueError:
                continue
            if not e.get("body") or not e.get("msg_id") or e["msg_id"] in out:
                continue
            out[e["msg_id"]] = {"msg_id": e["msg_id"], "direction": e.get("direction"),
                                "sender_handle": (e.get("sender") or {}).get("handle"),
                                "sent_at": to_utc(e["sent_at"]), "body": clean_text(e["body"])}
    return out


def main():
    show = int(sys.argv[sys.argv.index("--show") + 1]) if "--show" in sys.argv else 4
    d = Dicts()
    msgs = read_messages()
    with open(os.path.join(LABELS, "requests_truth.csv"), encoding="utf-8") as f:
        truth = dict((r["msg_id"], r) for r in csv.DictReader(f))
    preds = {}
    for mid in sorted(msgs):
        preds[mid] = extract(msgs[mid], d)

    rep = os.path.join(STAGING, "reports")
    if not os.path.isdir(rep):
        os.makedirs(rep)
    cols = ["msg_id", "is_request"] + FIELDS + ["confidence", "missing"]
    with open(os.path.join(rep, "rules_twin_predictions.csv"), "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for mid in sorted(preds):
            w.writerow([mid] + [preds[mid][c] for c in cols[1:]])

    ids = [m for m in preds if m in truth]
    tp = sum(1 for m in ids if preds[m]["is_request"] == "1" and truth[m]["is_request"] == "1")
    fp = sum(1 for m in ids if preds[m]["is_request"] == "1" and truth[m]["is_request"] != "1")
    fn = sum(1 for m in ids if preds[m]["is_request"] != "1" and truth[m]["is_request"] == "1")
    print("Rules twin on {} messages ({} requests in the truth file)".format(len(ids), tp + fn))
    print("  request detection  precision {:.1%}  recall {:.1%}  (tp {} fp {} fn {})".format(
        tp / float(tp + fp or 1), tp / float(tp + fn or 1), tp, fp, fn))
    both = [m for m in ids if preds[m]["is_request"] == "1" and truth[m]["is_request"] == "1"]
    print("  field accuracy on the {} requests found (blank = blank counts as correct):".format(len(both)))
    wrong = {}
    for f in FIELDS:
        ok = sum(1 for m in both if preds[m][f] == truth[m][f])
        print("    {:<17} {:6.1%}".format(f, ok / float(len(both) or 1)))
        wrong[f] = [m for m in both if preds[m][f] != truth[m][f]]
    conf = {}
    for m in both:
        c = preds[m]["confidence"]
        allok = all(preds[m][f] == truth[m][f] for f in FIELDS)
        conf.setdefault(c, [0, 0])
        conf[c][0] += 1
        conf[c][1] += allok
    print("  confidence  requests  all fields right")
    for c in ("high", "medium", "low"):
        n, ok = conf.get(c, [0, 0])
        print("    {:<8} {:>9}  {:6.1%}".format(c, n, ok / float(n or 1)))
    for f in FIELDS:
        for m in wrong[f][:show]:
            print("  [{}] truth {!r} got {!r}: {}".format(f, truth[m][f], preds[m][f],
                                                          msgs[m]["body"][:110].replace("\n", " ")))
    for m in [m for m in ids if preds[m]["is_request"] != truth[m]["is_request"]][:show]:
        print("  [is_request] truth {} got {}: {}".format(truth[m]["is_request"], preds[m]["is_request"],
                                                         msgs[m]["body"][:110].replace("\n", " ")))
    return 0


if __name__ == "__main__":
    sys.exit(main())