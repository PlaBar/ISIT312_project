"""
check_dictionaries.py - do the extraction dictionaries cover what customers actually write?

For every request in generators/labels/requests_truth.csv it looks up the message body and
checks, field by field, whether the dictionaries in lake/extract/dictionaries/ contain a
term that leads to the true value:

  origin / destination   an airport term for the true code appears in the body
  cargo type             the longest cargo phrase found (typo-tolerant) gives the true type
  special handling       the longest special-handling phrase found gives the true code,
                         and none is found where the truth has none
  relative dates         ASAP, tomorrow, next week, weekday names: a date_phrases term appears

This tests the vocabulary only, not the extraction logic of Rules.scala (Step 22), so the
scores are upper bounds for what Rules.scala can reach.

Python 3.4, standard library only:
  python3 evaluation/check_dictionaries.py
"""

import csv
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DICT = os.path.join(ROOT, "lake", "extract", "dictionaries")
STAGING = os.environ.get("GSS_STAGING") or os.path.join(ROOT, "staging")
LABELS = os.path.join(ROOT, "generators", "labels")


def load(name):
    out = []
    with open(os.path.join(DICT, name), encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if line.strip() and not line.startswith("#"):
                out.append(line.split("|"))
    return out


def whole(term, text, exact=False):
    flags = 0 if exact else re.I
    return re.search(r"(?<![A-Za-z0-9])" + re.escape(term) + r"(?![A-Za-z0-9])", text, flags) is not None


def in_route(term, text):
    """A code that is also an English word ("add") still counts, in any case, when it sits
    right next to a route marker: dwc/add, into add, add > dubai, ex add, from add."""
    t = re.escape(term.lower())
    low = text.lower()
    return re.search(r"([/>-]\s*|\b(to|from|ex|into|for)\s+)" + t + r"\b", low) is not None or \
        re.search(r"\b" + t + r"\s*([/>-]|\s+(to|ex)\b)", low) is not None


def near(a, b):
    """True if a and b differ by at most one edit or one swap of neighbouring letters."""
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


def phrase_hits(phrases, text, fuzzy):
    """[(len, value)] of phrases found in text. With fuzzy, one word of 5+ letters in the
    phrase may carry one typo; the phrase may be written without its spaces ("generalcargo")
    or with a typo across a space ("foo daid", "fresh ifsh") if it has 7+ letters and no
    word shorter than 3. Short words ("dg", "kg") must otherwise match exactly."""
    low = text.lower()
    words = re.findall(r"[a-z0-9\-]+", low)
    hits = []
    for phrase, value in phrases:
        if whole(phrase, low):
            hits.append((len(phrase), value))
            continue
        if not fuzzy:
            continue
        pw = phrase.split()
        joined = "".join(pw)
        found = False
        for i in range(len(words) - len(pw) + 1):
            window = words[i:i + len(pw)]
            diff = [k for k in range(len(pw)) if window[k] != pw[k]]
            if len(diff) == 1 and len(pw[diff[0]]) >= 5 and near(window[diff[0]], pw[diff[0]]):
                found = True
                break
            # a typo across the space: "foo daid", "fresh ifsh", "dangerou sgoods"
            jw = "".join(window)
            if len(pw) > 1 and (jw == joined or (len(joined) >= 7 and min(len(w) for w in pw) >= 3
                                                 and near(jw, joined))):
                found = True
                break
        if not found and len(pw) > 1:
            found = any(w == joined or (len(joined) >= 5 and near(w, joined)) for w in words)
        if found:
            hits.append((len(phrase), value))
    return hits


def main():
    airports = load("airports.txt")
    cargo_rows = load("cargo_types.txt")
    dates = load("date_phrases.txt")
    cargo = [(r[1], r[2]) for r in cargo_rows if r[0] == "cargo"]
    fallback = [(r[1], r[2]) for r in cargo_rows if r[0] == "cargo_fallback"]
    shc = [(r[1], r[2]) for r in cargo_rows if r[0] == "shc"]
    relative = [r[1] for r in dates if r[0] in ("relative", "weekday")]

    with open(os.path.join(LABELS, "requests_truth.csv"), encoding="utf-8") as f:
        truth = [r for r in csv.DictReader(f) if r["is_request"] == "1"]
    stats = {}

    def count(field, ok):
        s = stats.setdefault(field, [0, 0])
        s[0] += 1
        s[1] += 1 if ok else 0

    misses = {}
    for t in truth:
        body = None
        for ch in ("email", "whatsapp"):
            p = os.path.join(STAGING, "messages", ch, t["msg_id"] + ".json")
            if os.path.exists(p):
                try:
                    with open(p, encoding="utf-8") as f:
                        body = json.load(f).get("body")
                except ValueError:
                    body = None
        if not body:
            continue
        for field in ("origin_iata", "dest_iata"):
            if t[field]:
                ok = any(code == t[field] and (whole(term, body, flag == "exact_case") or
                                               (flag == "exact_case" and in_route(term, body)))
                         for term, code, flag in airports)
                count(field, ok)
                if not ok:
                    misses.setdefault(field, []).append(body[:90])
        if t["cargo_type"]:
            hits = phrase_hits(cargo, body, True) or phrase_hits(fallback, body, True)
            got = max(hits)[1] if hits else None
            count("cargo_type", got == t["cargo_type"])
            if got != t["cargo_type"]:
                misses.setdefault("cargo_type", []).append("{} -> {} ({})".format(t["cargo_type"], got, body[:70]))
        hits = phrase_hits(shc, body, False)
        got = max(hits)[1] if hits else ""
        count("special_handling", got == t["special_handling"])
        if got != t["special_handling"]:
            misses.setdefault("special_handling", []).append("{!r} -> {!r} ({})".format(
                t["special_handling"], got, body[:70]))
        if t["ready_date"] and any(whole(w, body) for w in ("asap", "tomorrow", "next week", "urgent",
                                                            "as soon as possible", "monday", "tuesday",
                                                            "wednesday", "thursday", "friday", "saturday", "sunday")):
            count("relative_date", any(whole(w, body) for w in relative))

    print("Dictionary coverage on {} requests (upper bound for Rules.scala)".format(len(truth)))
    worst = 1.0
    for field in ("origin_iata", "dest_iata", "cargo_type", "special_handling", "relative_date"):
        n, ok = stats.get(field, [0, 0])
        rate = ok / float(n) if n else 1.0
        worst = min(worst, rate)
        print("  {:<17} {:>6} of {:>6}  {:6.1%}".format(field, ok, n, rate))
    for field, xs in sorted(misses.items()):
        print("  examples not covered ({}):".format(field))
        for x in xs[:4]:
            print("    " + x.replace("\n", " "))
    return 0 if worst >= 0.97 else 1


if __name__ == "__main__":
    sys.exit(main())