"""
messages_gen.py - email and WhatsApp messages with the hidden truth file.

Writes one JSON file per message to staging/messages/<channel>/<msg_id>.json in the
envelope format of docs/data_dictionary.md section 5.1. The bodies are informal
English (section 6.1). About 1,100 messages a month, 36 months.

Conversation types
  request thread   customer asks for a charter -> GSS quotes -> customer replies
                   1,407 of the 10,800 requests become the flights that were flown (the shipments in
                   shipments_master.csv); their threads end with a booking message
                   that names the flight number and AWB
  non-request      thanks, invoice and payment notes, tracking questions, spam,
                   internal notes (about 28% of messages)

Hidden truth (generators/labels/, read only by evaluation/)
  requests_truth.csv       one row per message: is_request, and for requests the
                           fields AS WRITTEN in the body (a field the customer left out
                           is empty in the truth)
  message_attachments.csv  quotes and packing lists attached to messages; Step 8
                           (documents_gen.py) writes those files
  planted_errors_messages.csv

Truth rules for ready_date (relative words are resolved against sent_at, UAE time):
  12/03, 12 March, March 12  next date on or after the sent date with that day and month
  on the 12th                that day in the sent month, or the next month if already past
  Thursday                   next Thursday strictly after the sent date
  tomorrow                   sent date + 1
  ASAP, urgent               the sent date
  next week                  Monday of the following week
  12/03/2024, 12.03.2024     that date
Place names: "Dubai" means DWC (the GSS home base) [A]; DXB is only ever written as DXB.
Weights in tonnes are converted to kg (2.5T -> 2500).

Python 3.4, standard library only. Run from the project folder:
  python3 generators/messages_gen.py              all 36 months (about 39,700 messages)
  python3 generators/messages_gen.py --months 2   first 2 months only, for a quick test
"""

import csv
import glob
import json
import os
import sys
import time
from collections import OrderedDict
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gen_common as g

END = g.end_date()
GSS_EMAIL = "ops@gss.example"
GSS_REQUESTS = "requests@gss.example"
GSS_PHONE = "+97148850000"
STAFF = ["Maria", "Ravi", "Olga", "Sami"]
PLANTED = []

# ---------------------------------------------------------------------------
# Phrase banks (the dictionaries in lake/extract/dictionaries/ will mirror these)
# ---------------------------------------------------------------------------
CARGO_WORDS = {
    "GENERAL": ["general cargo", "gen cargo", "consolidated cargo", "boxes of general goods", "mixed cargo"],
    "PERISHABLE": ["perishables", "perishibles", "fresh fruit", "fresh flowers", "fresh fish", "vegetables"],
    "PHARMA": ["pharma", "pharmaceuticals", "vaccines", "medicines", "medical supplies"],
    "DANGEROUS_GOODS": ["DG cargo", "dangerous goods", "lithium batteries", "hazmat", "paint (DG)"],
    "HUMANITARIAN": ["relief goods", "humanitarian aid", "tents and blankets", "food aid", "relief cargo"],
    "AOG_SPARES": ["AOG spares", "aircraft parts", "aircraft spares", "a spare engine", "landing gear parts"],
    "OIL_GAS_EQUIPMENT": ["drilling equipment", "oilfield equipment", "drill pipes", "oil and gas equipment"],
    "VEHICLES": ["vehicles", "land cruisers", "armoured cars", "trucks", "pickup trucks"],
    "VALUABLES": ["valuables", "gold", "banknotes", "cash in transit", "jewellery"],
}
SH_WORDS = {
    "PER": ["perishable, keep cool", "needs cold chain", "keep chilled"],
    "PIL": ["temp controlled 2-8C", "pharma, 2-8 degrees", "cold chain 2-8"],
    "DGR": ["DG class 3, MSDS attached", "dangerous goods declaration ready", "DGR shipment"],
    "AOG": ["AOG urgent", "aircraft on ground", "AOG situation"],
    "HEA": ["heavy lift, one piece over 3T", "needs main deck, heavy piece", "heavy cargo"],
    "VAL": ["high value, escort required", "valuable cargo, secure handling", "VAL shipment"],
}
CITY_NAMES = {"DWC": ["Dubai", "Dubai World Central", "Al Maktoum"], "SHJ": ["Sharjah"], "DXB": []}


def city_names(iata):
    if iata in CITY_NAMES:
        return CITY_NAMES[iata]
    return [g.airport(iata)["city"]]


# ---------------------------------------------------------------------------
# Field phrases: each returns (text, value_as_truth)
# ---------------------------------------------------------------------------
def typo(r, word):
    if len(word) < 5 or " " in word[:4]:
        return word
    i = r.randint(1, len(word) - 3)
    if g.chance(r, 0.5):
        return word[:i] + word[i + 1] + word[i] + word[i + 2:]      # swap two letters
    return word[:i] + word[i + 1:]                                  # drop a letter


def cargo_phrase(r, code):
    if g.chance(r, 0.10):
        return "", ""
    w = r.choice(CARGO_WORDS[code])
    if g.chance(r, 0.08):
        w = typo(r, w)
    return w, code


def weight_phrase(r, kg):
    if g.chance(r, 0.10):
        return "", ""
    form = g.weighted_choice(r, ["kg", "kg_comma", "T", "tons", "tonnes", "kilos", "abt", "gross", "KG"],
                             [20, 10, 15, 10, 5, 8, 8, 6, 5])
    if form in ("T", "tons", "tonnes"):
        kg = max(100, int(round(kg / 100.0)) * 100)
        t = "{:d}".format(kg // 1000) if kg % 1000 == 0 else "{:.1f}".format(kg / 1000.0)
        text = {"T": "{}T", "tons": "{} tons", "tonnes": "{} tonnes"}[form].format(t)
    else:
        text = {"kg": "{} kg", "kg_comma": "{:,}kg", "kilos": "{} kilos", "abt": "abt {}kgs",
                "gross": "gross wt {} kg", "KG": "{}KG"}[form].format(kg)
    return text, str(kg)


def place(r, iata):
    names = city_names(iata)
    if names and g.chance(r, 0.5):
        return r.choice(names)
    return iata


def route_phrase(r, o, d):
    """Returns (text, origin_truth, dest_truth)."""
    if g.chance(r, 0.08):
        return r.choice(["to {D}", "for {D}", "into {D}"]).format(D=place(r, d)), "", d
    O, D = place(r, o), place(r, d)
    form = r.choice(["{O}-{D}", "{O} to {D}", "{O} > {D}", "from {O} to {D}", "ex {O} to {D}",
                     "{O}/{D}", "to {D} from {O}", "{D} ex {O}"])
    return form.format(O=O, D=D), o, d


def next_weekday(d, wd):
    days = (wd - d.weekday()) % 7
    return d + timedelta(days=days or 7)


def date_phrase(r, sent, ready):
    """Pick a way to write the ready date. Returns (text, truth_iso). Some forms
    change the date they mean (ASAP = sent date, next week = next Monday)."""
    if g.chance(r, 0.10):
        return "", ""
    delta = (ready - sent).days
    forms = [("ddmm", 25), ("d_month", 15), ("month_d", 5), ("asap", 10), ("next_week", 5),
             ("dotted", 8), ("full", 7)]
    if 1 <= delta <= 6:
        forms.append(("weekday", 15))
    if delta == 1:
        forms.append(("tomorrow", 8))
    if ready.month == sent.month and ready.day > sent.day:
        forms.append(("ordinal", 5))
    form = g.weighted_choice(r, [f for f, _ in forms], [w for _, w in forms])
    m_full = g.MONTH_FULL[ready.month - 1]
    if form == "asap":
        return r.choice(["ASAP", "asap", "urgent", "as soon as possible"]), g.fmt_iso(sent)
    if form == "next_week":
        monday = sent + timedelta(days=7 - sent.weekday())
        return "next week", g.fmt_iso(monday)
    if form == "weekday":
        return g.WEEKDAYS[ready.weekday()], g.fmt_iso(ready)
    if form == "tomorrow":
        return "tomorrow", g.fmt_iso(ready)
    if form == "ordinal":
        suffix = "th" if 11 <= ready.day <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(ready.day % 10, "th")
        return "on the {}{}".format(ready.day, suffix), g.fmt_iso(ready)
    text = {
        "ddmm": "{:02d}/{:02d}".format(ready.day, ready.month),
        "d_month": "{} {}".format(ready.day, m_full),
        "month_d": "{} {}".format(m_full, ready.day),
        "dotted": "{:02d}.{:02d}.{}".format(ready.day, ready.month, ready.year),
        "full": g.fmt_dmy(ready),
    }[form]
    return text, g.fmt_iso(ready)


# ---------------------------------------------------------------------------
# Bodies
# ---------------------------------------------------------------------------
def request_body(r, req, channel, cust):
    """Builds the free text of a request and fills req['truth']."""
    c_txt, c_val = cargo_phrase(r, req["cargo"])
    w_txt, w_val = weight_phrase(r, req["weight"])
    rt_txt, o_val, d_val = route_phrase(r, req["origin"], req["dest"])
    dt_txt, dt_val = date_phrase(r, req["sent"].date(), req["ready"])
    shc = ""
    sh_txt = ""
    cargo = [c for c in g.CARGO_TYPES if c["code"] == req["cargo"]][0]
    if cargo["shc"] and g.chance(r, 0.7):
        shc = cargo["shc"]
        sh_txt = r.choice(SH_WORDS[shc])
    req["truth"] = {"origin_iata": o_val, "dest_iata": d_val, "cargo_type": c_val,
                    "weight_kg": w_val, "ready_date": dt_val, "special_handling": shc}

    if c_txt and w_txt:
        cw = r.choice(["{w} of {c}", "{c} {w}", "{w} {c}", "{c}, {w}"]).format(w=w_txt, c=c_txt)
    else:
        cw = c_txt or w_txt
    dt = ""
    if dt_txt:
        if dt_txt.lower() in ("asap", "urgent", "as soon as possible") or dt_txt.startswith("on the"):
            dt = dt_txt
        else:
            dt = r.choice(["ready {}", "for {}", "loading {}", "cargo ready {}", "ETD {}", "{}"]).format(dt_txt)
    order = r.choice([[cw, rt_txt, dt], [rt_txt, cw, dt], [cw, dt, rt_txt]])
    core = ", ".join(p for p in order if p)
    leads = ["Please quote", "Kindly quote", "Can you quote", "We have", "Pls quote", "RFQ:"]
    if not core.split(" ")[0].lower() in ("to", "for", "into"):
        leads += ["Need a rate for", "Looking for a charter for", "Request for"]
    lead = r.choice(leads)
    text = "{} {}.".format(lead, core)
    if sh_txt:
        text += " " + sh_txt[0].upper() + sh_txt[1:] + "."
    extras = []
    if g.chance(r, 0.3):
        extras.append("{} pallets".format(r.randint(2, 30)))
    if g.chance(r, 0.15):
        extras.append("dims {}x{}x{} cm".format(r.choice([120, 100, 80]), r.choice([80, 100, 120]),
                                                r.randint(60, 180)))
    if g.chance(r, 0.10):
        earlier = req["sent"].date() - timedelta(days=r.randint(2, 20))
        extras.append("as discussed on the phone {:02d}/{:02d}".format(earlier.day, earlier.month))
    if g.chance(r, 0.15):
        extras.append("our ref {}{}".format(r.choice(["JOB", "REF", "QTN"]), r.randint(1000, 9999)))

    if channel == "whatsapp":
        parts = []
        if g.chance(r, 0.5):
            parts.append(r.choice(["Hi", "Hello", "Salam", "Hi bro", "Good morning"]))
        parts.append(text)
        parts.extend(extras)
        body = "\n".join(parts) if g.chance(r, 0.5) else " ".join(parts)
        return body.lower() if g.chance(r, 0.3) else body

    greeting = r.choice(["Dear GSS team,", "Hi team,", "Dear Sir/Madam,", "Hello,", "Dear {},".format(r.choice(STAFF))])
    body = greeting + "\n\n" + text
    if extras:
        body += "\n" + ". ".join(e[0].upper() + e[1:] for e in extras) + "."
    body += "\n\n" + r.choice(["Thanks", "Regards", "Best regards", "Thanks and regards", "Awaiting your offer"])
    body += ",\n{}\n{}\nTel {}".format(cust["contact"], cust["name"], cust["phone"])
    if g.chance(r, 0.3):
        body += ("\n\nThis email and any attachments are confidential and intended solely for the "
                 "addressee. If you received it in error please delete it.")
    return body


def staff_sign(r):
    return "\n\nBest regards,\n{}\nGSS Operations".format(r.choice(STAFF))


def render(r, m, cust):
    """Body text for every message kind except the request itself."""
    k, p = m["kind"], m["p"]
    name = cust["contact"] if cust else "team"
    if k == "quote":
        att = " Please see our quotation {} attached.".format(p["quote_no"]) if m["attachments"] else ""
        return ("Dear {},\n\nThank you for your enquiry. Our offer {o}-{d}, {w} kg: USD {rate:.2f}/kg "
                "all in, total USD {tot:,.0f}. Valid until {v}.{att}{s}").format(
            name, o=p["origin"], d=p["dest"], w=p["weight"], rate=p["rate"],
            tot=p["rate"] * p["weight"], v=g.fmt_dmy(p["valid"]), att=att, s=staff_sign(r))
    if k == "details":
        return "Dear {},\n\nThanks for the request. Please advise {}.{}".format(
            name, r.choice(["the gross weight and dims", "cargo ready date and weight",
                            "number of pieces and weight"]), staff_sign(r))
    if k == "no_capacity":
        return "Dear {},\n\nUnfortunately we have no capacity on these dates. We will revert if a slot opens.{}".format(
            name, staff_sign(r))
    if k == "negotiation":
        return r.choice(["Can you improve the rate? Client has a better offer.",
                         "Too expensive, can you do better?",
                         "Rate is high, pls check again",
                         "Can you check {} instead?".format(g.fmt_dmy(p["alt_date"]))])
    if k == "revised_quote":
        return "Dear {},\n\nWe can offer USD {:.2f}/kg as our best rate.{}".format(name, p["rate"], staff_sign(r))
    if k == "decline":
        return r.choice(["Thanks, will revert.", "Noted, client went with another carrier.",
                         "Ok noted, we will come back to you", "Thanks for the offer, on hold for now."])
    if k == "confirmation":
        att = " Packing list attached." if m["attachments"] else ""
        return r.choice(["Confirmed, please book.", "OK go ahead, please confirm booking.",
                         "Accepted. Please proceed."]) + att
    if k == "booking":
        return ("Dear {},\n\nBooking confirmed on {} departing {} {}, AWB {}. Please deliver cargo "
                "to DWC cargo terminal 6 hours before departure.{}").format(
            name, p["flight_no"], g.fmt_dmy(p["dep"].date()), p["dep"].strftime("%H:%M"), p["awb"], staff_sign(r))
    if k == "thanks":
        return r.choice(["Thanks for the smooth operation on {f}, consignee very happy.",
                         "Cargo received in good order. Thanks team!",
                         "Many thanks for your support last week.",
                         "Good morning, hope all well. Thanks again for the help."]).format(f=p.get("flight_no", ""))
    if k == "invoice":
        return r.choice(["Please send a copy of invoice {inv}.",
                         "Payment for {inv} sent today, please confirm receipt.",
                         "Kindly share statement of account, we see {inv} open.",
                         "Dear {n}, gentle reminder: invoice {inv} is overdue. Kindly arrange payment."]
                        ).format(inv=p["invoice_id"], n=name)
    if k == "tracking":
        return r.choice(["Any update on AWB {awb}?", "Pls share ETA for AWB {awb}",
                         "Has {f} landed? Consignee asking.", "Status of shipment {awb} please"]
                        ).format(awb=p["awb"], f=p["flight_no"])
    if k == "spam":
        return r.choice(["Congratulations! You have been selected for a free cruise. Click here to claim.",
                         "Cheap jet fuel at best prices, contact us for a quotation today!!!",
                         "Boost your website ranking with our SEO services.",
                         "Your mailbox is full. Login to verify your account.",
                         "Investment opportunity: 30% monthly returns guaranteed."])
    if k == "internal":
        return r.choice(["Crew for {f} confirmed.", "Reminder: {reg} A-check next week.",
                         "Overflight permit for {f} still pending, please chase.",
                         "Fuel price update received for {o}, please update the costing sheet."]
                        ).format(f=p["flight_no"], reg=p["registration"], o=p["origin"])
    raise ValueError(k)


# ---------------------------------------------------------------------------
# Conversation plan
# ---------------------------------------------------------------------------
def load_shipments():
    path = os.path.join(g.LABELS, "shipments_master.csv")
    if not os.path.exists(path):
        sys.exit("shipments_master.csv not found: run python3 generators/structured_gen.py first")
    with open(path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for s in rows:
        s["dep"] = datetime.strptime(s["sched_dep"], "%Y-%m-%dT%H:%M:%S")
        s["weight"] = int(s["weight_kg"])
    return rows


def business_time(r, d):
    return datetime(d.year, d.month, d.day, r.randint(7, 21), r.randint(0, 59), r.randint(0, 59))


def plan_messages(r, customers, shipments, months):
    cust_by_id = dict((c["customer_id"], c) for c in customers)
    first_day = g.month_range(0)[0]
    last_day = g.month_range(months - 1)[1]
    flown = [s for s in shipments if s["status"] != "CANCELLED"]
    msgs = []
    threads = []

    def channel_for(c):
        return "whatsapp" if g.chance(r, 0.6 if c["kind"] == "broker" else 0.25) else "email"

    def add(thread, kind, sent, inbound, p=None, attachments=None):
        m = {"thread": thread, "kind": kind, "sent": sent, "inbound": inbound, "p": p or {},
             "attachments": attachments or [], "channel": thread["channel"], "cust": thread["cust"]}
        msgs.append(m)
        return m

    def new_thread(cust, channel, subject):
        t = {"cust": cust, "channel": channel, "subject": subject, "n": len(threads)}
        threads.append(t)
        return t

    def request_thread(cust, req, ship):
        ch = channel_for(cust)
        t = new_thread(cust, ch, r.choice(["Cargo request {}-{}".format(req["origin"], req["dest"]),
                                           "RFQ {}".format(g.airport(req["dest"])["city"]),
                                           "Charter enquiry", "Quote needed", "Urgent request"]))
        m = add(t, "request", req["sent"], True, {"req": req})
        req["msg"] = m
        t_now = req["sent"]

        def later(lo_min, hi_min):
            return t_now + timedelta(minutes=r.randint(lo_min, hi_min))

        rate = round(r.uniform(2.2, 6.5), 2)
        if req["truth_hint_missing_weight"] and not ship:
            t_now = later(20, 240)
            add(t, "details", t_now, False)
            return
        if ship is None and g.chance(r, 0.15):
            t_now = later(20, 240)
            add(t, "no_capacity", t_now, False)
            return
        t_now = later(20, 360)
        attach = ship is not None or g.chance(r, 0.15)
        qp = {"origin": req["origin"], "dest": req["dest"], "weight": req["weight"], "rate": rate,
              "valid": t_now.date() + timedelta(days=3), "req": req, "ship": ship}
        add(t, "quote", t_now, False, qp, ["QUOTE"] if attach else [])
        if ship is None:
            if g.chance(r, 0.4):
                t_now = later(60, 2880)
                kind = "negotiation" if g.chance(r, 0.4) else "decline"
                add(t, kind, t_now, True, {"alt_date": req["ready"] + timedelta(days=r.randint(1, 5))})
            return
        if g.chance(r, 0.4):
            t_now = later(60, 1440)
            add(t, "negotiation", t_now, True, {"alt_date": req["ready"] + timedelta(days=2)})
            t_now = later(30, 600)
            add(t, "revised_quote", t_now, False, {"rate": round(rate * 0.93, 2)})
        t_now = later(60, 1440)
        add(t, "confirmation", t_now, True, {"ship": ship}, ["PACKING"] if g.chance(r, 0.6) else [])
        t_now = max(later(30, 300), t_now)
        if t_now < ship["dep"]:
            add(t, "booking", t_now, False, {"flight_no": ship["flight_no"], "dep": ship["dep"],
                                             "awb": ship["awb_no"], "ship": ship})
        if g.chance(r, 0.2):
            add(t, "thanks", ship["dep"] + timedelta(days=r.randint(1, 4), hours=r.randint(0, 8)), True,
                {"flight_no": ship["flight_no"]})

    def make_request(cust, origin, dest, cargo_code, weight, sent, ready, ship):
        return {"cust": cust, "origin": origin, "dest": dest, "cargo": cargo_code, "weight": weight,
                "sent": sent, "ready": ready, "ship": ship,
                "truth_hint_missing_weight": False}

    # requests that became the real flights
    for s in flown:
        if not (first_day <= s["dep"].date() <= last_day):
            continue
        sent_day = max(first_day, s["dep"].date() - timedelta(days=r.randint(3, 14)))
        sent = business_time(r, sent_day)
        ready = s["dep"].date() - timedelta(days=r.randint(0, 1))
        if ready < sent.date():
            ready = sent.date()
        req = make_request(cust_by_id[s["customer_id"]], s["origin_iata"], s["dest_iata"],
                           s["cargo_type"], s["weight"], sent, ready, s)
        request_thread(req["cust"], req, s)

    # requests that did not become flights
    n_total = g.REQUESTS_PER_MONTH * months
    n_conv = sum(1 for m in msgs if m["kind"] == "request")
    weights = [2 if c["kind"] == "broker" else 1 for c in customers]
    for _ in range(max(0, n_total - n_conv)):
        cust = g.weighted_choice(r, customers, weights)
        sent = business_time(r, g.random_date(r, first_day, last_day))
        dest = r.choice(g.DESTINATIONS)
        if g.chance(r, 0.8):
            origin = g.weighted_choice(r, g.UAE_IATA, [85, 8, 7])
        else:
            origin, dest = dest, g.HOME_BASE
        cargo = g.pick_cargo(r)
        weight = r.randint(cargo["kg"][0], cargo["kg"][1])
        ready = sent.date() + timedelta(days=r.randint(1, 21))
        req = make_request(cust, origin, dest, cargo["code"], weight, sent, ready, None)
        req["truth_hint_missing_weight"] = g.chance(r, 0.05)
        request_thread(cust, req, None)

    # non-request messages, about 300 a month
    by_month = {}
    for s in flown:
        by_month.setdefault(g.fmt_yyyymm(s["dep"].date()), []).append(s)
    for i in range(months):
        first, last = g.month_range(i)
        pool = by_month.get(g.fmt_yyyymm(first), flown[:40])
        for _ in range(300):
            kind = g.weighted_choice(r, ["thanks", "invoice", "tracking", "spam", "internal"],
                                     [20, 25, 20, 20, 15])
            s = r.choice(pool)
            sent = business_time(r, g.random_date(r, first, last))
            if kind == "spam":
                cust = None
                ch = "email" if g.chance(r, 0.8) else "whatsapp"
            elif kind == "internal":
                cust = None
                ch = "email" if g.chance(r, 0.6) else "whatsapp"
            else:
                cust = cust_by_id[s["customer_id"]]
                ch = channel_for(cust)
            t = new_thread(cust, ch, {"thanks": "Thank you", "invoice": "Invoice {}".format(s["invoice_id"]),
                                      "tracking": "Shipment status", "spam": "Special offer",
                                      "internal": "Ops note"}[kind])
            t["kind"] = kind
            inbound = kind != "invoice" or g.chance(r, 0.6)
            add(t, kind, sent, inbound, {"flight_no": s["flight_no"], "awb": s["awb_no"] or s["flight_no"],
                                         "invoice_id": s["invoice_id"] or g.invoice_id(1),
                                         "registration": s["registration"], "origin": s["origin_iata"]})

    end_dt = datetime(last_day.year, last_day.month, last_day.day, 23, 59, 59)
    msgs = [m for m in msgs if m["sent"] <= end_dt and m["sent"].date() >= first_day]
    msgs.sort(key=lambda m: (m["sent"], m["thread"]["n"]))
    return msgs


# ---------------------------------------------------------------------------
# Identifiers, envelopes, files
# ---------------------------------------------------------------------------
def assign_ids(msgs):
    seq_msg, seq_thread, seq_req, seq_quote, seq_doc = {}, {}, {}, {}, {}
    for m in msgs:
        y = m["sent"].year
        seq_msg[y] = seq_msg.get(y, 0) + 1
        m["msg_id"] = g.msg_id(y, seq_msg[y])
        t = m["thread"]
        if "thread_id" not in t:
            seq_thread[y] = seq_thread.get(y, 0) + 1
            t["thread_id"] = g.thread_id(y, seq_thread[y])
        if m["kind"] == "request":
            seq_req[y] = seq_req.get(y, 0) + 1
            m["p"]["req"]["request_id"] = g.request_id(y, seq_req[y])
        if m["kind"] == "quote":
            seq_quote[y] = seq_quote.get(y, 0) + 1
            m["p"]["quote_no"] = g.quote_no(y, seq_quote[y])
        paths = []
        for doc_type in m["attachments"]:
            d = m["sent"].date()
            key = (doc_type, d)
            seq_doc[key] = seq_doc.get(key, 0) + 1
            paths.append("documents/{:04d}/{:02d}/{}/{}_{}_{:03d}.txt".format(
                d.year, d.month, doc_type, doc_type.lower(), d.strftime("%Y%m%d"), seq_doc[key]))
        m["attachment_paths"] = paths


def envelope(r, m):
    cust = m["cust"]
    t = m["thread"]
    ch = m["channel"]
    if m["inbound"]:
        if cust is None and m["kind"] == "spam":
            display, handle = "Promotions", ("offers@deals-{}.example".format(r.randint(1, 99)) if ch == "email"
                                              else "+44770{}".format(r.randint(1000000, 9999999)))
        elif cust is None:
            staff = r.choice(STAFF)
            display, handle = "{} (GSS)".format(staff), ("{}@gss.example".format(staff.lower()) if ch == "email"
                                                         else "+97150{}".format(r.randint(1000000, 9999999)))
        else:
            display = "{} ({})".format(cust["contact"], " ".join(cust["name"].split(" ")[:2]))
            handle = cust["email"] if ch == "email" else cust["phone"]
    else:
        display, handle = "GSS Operations", (GSS_EMAIL if ch == "email" else GSS_PHONE)
    e = OrderedDict()
    e["msg_id"] = m["msg_id"]
    e["thread_id"] = t["thread_id"]
    e["channel"] = ch
    e["direction"] = "inbound" if m["inbound"] else "outbound"
    e["sent_at"] = g.fmt_msg_ts(m["sent"])
    e["sender"] = OrderedDict([("display", display), ("handle", handle)])
    if ch == "email":
        if m["inbound"]:
            e["to"] = [GSS_REQUESTS if cust is not None or m["kind"] == "spam" else GSS_EMAIL]
        else:
            e["to"] = [cust["email"] if cust else GSS_EMAIL]
        e["cc"] = ["accounts@gss.example"] if m["kind"] == "invoice" and g.chance(r, 0.5) else []
        first = t.get("first_msg") is None
        e["subject"] = t["subject"] if first else "RE: " + t["subject"]
        e["message_id"] = "<{:012x}@mail.example>".format(r.getrandbits(48))
        e["in_reply_to"] = t.get("last_email_id")
        t["last_email_id"] = e["message_id"]
    else:
        e["subject"] = None
    t["first_msg"] = t.get("first_msg") or m["msg_id"]
    e["body"] = m["body"]
    e["attachments"] = list(m["attachment_paths"])
    e["language"] = "en"
    return e


def plant_errors(r, e, fname):
    """Envelope-level errors for preprocessing (docs/data_dictionary.md section 5.1).
    Returns (json_text, duplicate_flag)."""
    x = r.random()
    mid = e["msg_id"]
    if x < 0.003:
        del e["body"]
        PLANTED.append(("messages", fname, mid, "missing_body"))
    elif x < 0.005:
        e["channel"] = "sms"
        PLANTED.append(("messages", fname, mid, "bad_channel"))
    elif x < 0.015:
        e["sent_at"] = e["sent_at"][:19]
        PLANTED.append(("messages", fname, mid, "sent_at_without_zone"))
    elif x < 0.045 and e["direction"] == "inbound":
        h = e["sender"]["handle"]
        if h.startswith("+9715"):
            e["sender"]["handle"] = "0{} {} {}".format(h[4:6], h[6:9], h[9:])
            PLANTED.append(("messages", fname, mid, "local_phone_format"))
        elif "@" in h:
            e["sender"]["handle"] = h.upper()
            PLANTED.append(("messages", fname, mid, "upper_case_email"))
    text = json.dumps(e, indent=2)
    if 0.045 <= x < 0.047:
        text = text[:r.randint(40, len(text) - 20)]
        PLANTED.append(("messages", fname, mid, "truncated_json"))
        return text, False
    dup = 0.047 <= x < 0.052
    if dup:
        PLANTED.append(("messages", fname, mid, "duplicate_file"))
    return text, dup


def main():
    months = g.MONTHS
    if "--months" in sys.argv:
        months = int(sys.argv[sys.argv.index("--months") + 1])
    t0 = time.time()
    r = g.rng("messages")
    customers = g.make_customers()
    shipments = load_shipments()
    msgs = plan_messages(r, customers, shipments, months)
    assign_ids(msgs)

    rb = g.rng("message-bodies")
    for m in msgs:
        if m["kind"] == "request":
            m["body"] = request_body(rb, m["p"]["req"], m["channel"], m["cust"])
        else:
            m["body"] = render(rb, m, m["cust"])

    out_dirs = {ch: g.ensure_dir(os.path.join(g.MESSAGES, ch)) for ch in ("email", "whatsapp")}
    for d in out_dirs.values():
        for p in glob.glob(os.path.join(d, "*.json")):
            os.remove(p)

    re_ = g.rng("message-errors")
    re_env = g.rng("message-envelopes")
    truth, attach_rows = [], []
    counts = {"email": 0, "whatsapp": 0}
    kinds = {}
    for m in msgs:
        e = envelope(re_env, m)
        fname = "{}.json".format(m["msg_id"])
        text, dup = plant_errors(re_, e, fname)
        folder = out_dirs[m["channel"]]
        with open(os.path.join(folder, fname), "w", encoding="utf-8", newline="\n") as f:
            f.write(text + "\n")
        if dup:
            with open(os.path.join(folder, "{}_dup.json".format(m["msg_id"])), "w", encoding="utf-8", newline="\n") as f:
                f.write(text + "\n")
        counts[m["channel"]] += 1 + (1 if dup else 0)
        kinds[m["kind"]] = kinds.get(m["kind"], 0) + 1

        p = m["p"]
        ship = p.get("ship") or (p["req"]["ship"] if "req" in p else None)
        ref_flight = ship["flight_no"] if ship else ("" if m["kind"] == "spam" else p.get("flight_no", ""))
        if m["kind"] == "request":
            req = p["req"]
            tr = req["truth"]
            truth.append([m["msg_id"], m["thread"]["thread_id"], "1", req["request_id"], tr["origin_iata"],
                          tr["dest_iata"], tr["cargo_type"], tr["weight_kg"], tr["ready_date"],
                          tr["special_handling"], ref_flight, m["kind"]])
        else:
            truth.append([m["msg_id"], m["thread"]["thread_id"], "0", "", "", "", "", "", "", "",
                          ref_flight, m["kind"]])
        for doc_type, path in zip(m["attachments"], m["attachment_paths"]):
            req = p.get("req")
            attach_rows.append([doc_type, path, m["msg_id"], m["thread"]["thread_id"],
                                req["request_id"] if req else "", ship["flight_no"] if ship else "",
                                p.get("quote_no", ""), m["cust"]["customer_id"],
                                p.get("origin", ship["origin_iata"] if ship else ""),
                                p.get("dest", ship["dest_iata"] if ship else ""),
                                req["cargo"] if req else (ship["cargo_type"] if ship else ""),
                                str(p.get("weight", ship["weight"] if ship else "")),
                                "{:.2f}".format(p["rate"]) if "rate" in p else "",
                                g.fmt_iso(p["valid"]) if "valid" in p else "",
                                g.fmt_iso(m["sent"].date())])

    g.write_csv(os.path.join(g.LABELS, "requests_truth.csv"),
                ["msg_id", "thread_id", "is_request", "request_id", "origin_iata", "dest_iata", "cargo_type",
                 "weight_kg", "ready_date", "special_handling", "flight_no", "kind"], truth)
    g.write_csv(os.path.join(g.LABELS, "message_attachments.csv"),
                ["doc_type", "rel_path", "msg_id", "thread_id", "request_id", "flight_no", "quote_no",
                 "customer_id", "origin_iata", "dest_iata", "cargo_type", "weight_kg", "rate_usd_kg",
                 "valid_until", "doc_date"], attach_rows)
    g.write_csv(os.path.join(g.LABELS, "planted_errors_messages.csv"),
                ["table", "file", "key", "error"], PLANTED)

    n_req = sum(1 for t in truth if t[2] == "1")
    print("Months:          {}".format(months))
    print("Messages:        {} ({} email files, {} WhatsApp files)".format(len(msgs), counts["email"], counts["whatsapp"]))
    print("Threads:         {}".format(len(set(m["thread"]["thread_id"] for m in msgs))))
    print("Requests:        {} ({} became flights)".format(n_req, sum(1 for t in truth if t[2] == "1" and t[10])))
    unrelated = sum(1 for m in msgs if m["kind"] in ("thanks", "invoice", "tracking", "spam", "internal"))
    print("Replies:         {} (quotes, negotiation, bookings in request threads)".format(len(msgs) - n_req - unrelated))
    print("Unrelated:       {} ({:.0f}% of messages: thanks, invoices, tracking, spam, internal)".format(
        unrelated, 100.0 * unrelated / len(msgs)))
    print("Attachments:     {} quotes, {} packing lists".format(
        sum(1 for a in attach_rows if a[0] == "QUOTE"), sum(1 for a in attach_rows if a[0] == "PACKING")))
    print("By kind:")
    for k in sorted(kinds):
        print("  {:<14} {:>6}".format(k, kinds[k]))
    pk = {}
    for p in PLANTED:
        pk[p[3]] = pk.get(p[3], 0) + 1
    print("Planted errors:  {}".format(len(PLANTED)))
    for k in sorted(pk):
        print("  {:<22} {:>5}".format(k, pk[k]))
    print("")
    print("Truth in generators/labels/: requests_truth, message_attachments, planted_errors_messages")
    print("Done in {:.0f} s".format(time.time() - t0))


if __name__ == "__main__":
    main()