"""
pp_messages.py - preprocessing of the email and WhatsApp message files.

For every staging/messages/<channel>/*.json file, oldest message first:
  PP-01..03  via pp_common: new files only (an identical file is logged as a duplicate),
             clean UTF-8, valid JSON object
  PP-10      required fields msg_id, channel, sent_at and body present and not empty;
             channel is email or whatsapp; msg_id looks like M-YYYY-NNNNNN.
             A failing message is rejected as a whole file
  PP-07      sent_at becomes UTC (2024-03-12T04:41:07Z). A time without a zone is
             taken as UAE time (+04:00). The original value is kept in sent_at_source
  PP-11      sender handle standardised: email addresses in lower case, phone numbers
             as +971... with no spaces (050 123 4567 -> +971501234567); to and cc
             addresses in lower case
  PP-12      text normalised without changing the wording: line endings, trailing
             spaces and runs of blank lines in body; subject trimmed
  PP-09      a msg_id seen before (in this run or an earlier one) is dropped
  PP-14      via pp_common: _dq_flags, _src_file, _run_id added; each message becomes one
             JSON line, and a run's messages are packed into one file per channel:
               /lake/landing/messages/<channel>/messages_<run_id>.jsonl
             (HDFS is built for few large files; the streaming job reads each new file)

--limit N takes only the N oldest new messages. release_loop.sh (Step 15) uses it to
feed the streaming job a few messages at a time.

Python 3.4, standard library only. Run from the project folder:
  python3 preprocess/pp_messages.py                 all new messages, released to HDFS
  python3 preprocess/pp_messages.py --limit 50      the 50 oldest new messages
  python3 preprocess/pp_messages.py --no-hdfs       test locally, nothing sent to HDFS
"""

import json
import os
import re
import sys
from collections import OrderedDict
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pp_common as pc

OUT_FIELDS = ["msg_id", "thread_id", "channel", "direction", "sent_at", "sent_at_source", "sender",
              "to", "cc", "subject", "message_id", "in_reply_to", "body", "attachments", "language"]
ZONE = re.compile(r"([+-])(\d{2}):?(\d{2})$")


def to_utc(value, default_offset):
    """'2024-03-12T08:41:07+04:00' -> datetime in UTC. A value without a zone gets the
    default offset. Python 3.4's %z only reads +0400, so the offset is parsed here."""
    v = value.strip()
    m = ZONE.search(v)
    zoned = m is not None
    if v.endswith("Z"):
        base, sign, hh, mm, zoned = v[:-1], "+", "00", "00", True
    elif m:
        base, sign, hh, mm = v[:m.start()], m.group(1), m.group(2), m.group(3)
    else:
        m2 = ZONE.search(default_offset)
        base, sign, hh, mm = v, m2.group(1), m2.group(2), m2.group(3)
    local = datetime.strptime(base, "%Y-%m-%dT%H:%M:%S")
    offset = timedelta(hours=int(hh), minutes=int(mm))
    if sign == "-":
        offset = -offset
    return (local - offset).replace(tzinfo=timezone.utc), zoned


def clean_handle(handle):
    """Returns (clean_handle, ok). Email -> lower case; phone -> +971XXXXXXXXX."""
    h = (handle or "").strip()
    if "@" in h:
        return h.lower(), True
    digits = re.sub(r"[^\d+]", "", h)
    if digits.startswith("00"):
        digits = "+" + digits[2:]
    if digits.startswith("0") and len(digits) == 10:          # 050 123 4567 (UAE mobile)
        digits = "+971" + digits[1:]
    if re.match(r"^\+\d{8,15}$", digits):
        return digits, True
    return h, False


def clean_text(text):
    lines = [line.rstrip() for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n")]
    out, blank = [], 0
    for line in lines:
        blank = blank + 1 if not line.strip() else 0
        if blank <= 1:
            out.append(line)
    return "\n".join(out).strip()


def clean_message(run, item, obj, spec, seen):
    """Returns the cleaned message (OrderedDict) or None if the file is rejected or dropped."""
    for f in spec["required_fields"]:                                  # PP-10
        v = obj.get(f)
        if v is None or (isinstance(v, str) and not v.strip()):
            run.check("PP-10", False)
            run.reject_file(item, "PP-10", "required field missing: {}".format(f))
            return None
    if obj["channel"] not in spec["allowed_channels"]:
        run.check("PP-10", False)
        run.reject_file(item, "PP-10", "channel not allowed: {}".format(obj["channel"]))
        return None
    if not re.match(spec["msg_id_pattern"], str(obj["msg_id"]).strip()):
        run.check("PP-10", False)
        run.reject_file(item, "PP-10", "msg_id not recognised: {}".format(obj["msg_id"]))
        return None
    run.check("PP-10", True)
    msg_id = obj["msg_id"].strip()
    flags = []

    try:                                                               # PP-07
        utc, zoned = to_utc(obj["sent_at"], spec["default_utc_offset"])
    except ValueError:
        run.check("PP-07", False)
        run.reject_file(item, "PP-07", "unparseable sent_at: {}".format(obj["sent_at"]))
        return None
    run.check("PP-07", zoned)                    # "failed" = no zone, UAE time assumed
    if not zoned:
        flags.append("sent_at_zone_assumed")

    sender = obj.get("sender") or {}                                   # PP-11
    handle, ok = clean_handle(sender.get("handle"))
    run.check("PP-11", handle == (sender.get("handle") or ""))
    if not ok:
        flags.append("sender_handle")
    addr = lambda xs: [a.strip().lower() for a in (xs or []) if isinstance(a, str) and a.strip()]

    if msg_id in seen:                                                 # PP-09
        run.check("PP-09", False)
        item.status = "DUPLICATE"
        run.rejects.append([run.run_id, item.rel, 0, "PP-09", "duplicate msg_id {}".format(msg_id), ""])
        return None
    run.check("PP-09", True)
    seen.add(msg_id)

    body = clean_text(obj["body"])                                     # PP-12
    run.check("PP-12", body == obj["body"])
    subject = obj.get("subject")
    if isinstance(subject, str):
        subject = subject.strip() or None

    src, rid = run.lineage(item)                                       # PP-14
    out = OrderedDict()
    out["msg_id"] = msg_id
    out["thread_id"] = (obj.get("thread_id") or "").strip() or None
    out["channel"] = obj["channel"]
    out["direction"] = obj.get("direction")
    out["sent_at"] = utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    out["sent_at_source"] = obj["sent_at"]
    out["sender"] = OrderedDict([("display", (sender.get("display") or "").strip() or None), ("handle", handle)])
    out["to"] = addr(obj.get("to"))
    out["cc"] = addr(obj.get("cc"))
    out["subject"] = subject
    out["message_id"] = obj.get("message_id")
    out["in_reply_to"] = obj.get("in_reply_to")
    out["body"] = body
    out["attachments"] = [a for a in (obj.get("attachments") or []) if isinstance(a, str)]
    out["language"] = obj.get("language") or "en"
    out["_dq_flags"] = ";".join(flags)
    out["_src_file"] = src
    out["_run_id"] = rid
    return out


def main():
    hdfs = pc.wants_hdfs(sys.argv)
    limit = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else None
    schemas = pc.load_schemas()
    spec = schemas["messages"]
    run = pc.Run("messages", hdfs=hdfs, quiet="--quiet" in sys.argv)
    seen = pc.SeenKeys(run.root, "msg_id")
    # oldest first across both channels: M-YYYY-NNNNNN sorts by time within a year
    order = lambda rel: (os.path.basename(rel)[2:6], os.path.basename(rel))
    items = run.new_files("messages", [spec["file_pattern"]], limit=limit, sort_key=order)
    by_channel = {}                 # channel folder -> [(item, json line)]
    for n, item in enumerate(items, 1):
        run.progress("cleaning messages", n, len(items))
        text = run.read_text(item)                                     # PP-02
        if text is None:
            continue
        obj = run.parse_json(item, text)                               # PP-03
        if obj is None:
            continue
        item.rows_in = 1
        msg = clean_message(run, item, obj, spec, seen)
        if msg is None:
            item.rows_bad = 1
            continue
        item.rows_ok = 1
        by_channel.setdefault(item.rel.split("/")[1], []).append((item, json.dumps(msg)))
    for channel in sorted(by_channel):
        pairs = by_channel[channel]
        run.accept_many([it for it, _ in pairs], "".join(line + "\n" for _, line in pairs),
                        "messages/{}/messages_{}.jsonl".format(channel, run.run_id))
    run.finish()
    seen.save()


if __name__ == "__main__":
    main()