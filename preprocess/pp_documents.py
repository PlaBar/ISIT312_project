"""
pp_documents.py - preprocessing of the shipment documents (quotes, AWBs, packing lists, permits).

For every staging/documents/<yyyy>/<mm>/<TYPE>/*.txt file:
  PP-01..02  via pp_common: new files only, clean UTF-8 (Windows-1252 and BOM converted,
             CRLF to LF, control characters removed)
  PP-12      text normalised without changing the wording: trailing spaces removed,
             runs of blank lines reduced to one, blank lines at the start and end removed
  PP-13      the first line must name a known document type (schemas.json "documents":
             QUOTATION, AIR WAYBILL, PACKING LIST, CLEARANCE PERMIT) and the text must
             have at least min_chars characters; otherwise the file is rejected. A type
             that differs from the folder the file sits in is flagged in _dq_flags
  PP-09      a document whose normalised text was seen before (SHA-256 of the text, in
             this run or an earlier one) is dropped as a duplicate
  PP-14      each document becomes one JSON line
               {"doc_file", "doc_type", "text", "chars", "_dq_flags", "_src_file", "_run_id"}
             and the lines are packed into one file per type and month:
               /lake/landing/documents/<TYPE>/<type>_<yyyymm>_<run_id>.jsonl
             (about 144 files instead of 6,000: HDFS is built for few large files).
             doc_file keeps the original path (documents/2024/03/AWB/awb_20240301_001.txt),
             which is the path message attachments point to

Fields inside the documents (AWB numbers, flights, dates) are not touched here:
finding them is LinkDocuments.scala's job.

Python 3.4, standard library only. Run from the project folder:
  python3 preprocess/pp_documents.py              release to HDFS (Hadoop must be running)
  python3 preprocess/pp_documents.py --no-hdfs    test locally, nothing sent to HDFS
"""

import hashlib
import json
import os
import sys
from collections import OrderedDict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pp_common as pc


def normalise(text):
    """PP-12: same words, tidy layout."""
    lines = [line.rstrip() for line in text.split("\n")]
    out, blank = [], 0
    for line in lines:
        blank = blank + 1 if not line.strip() else 0
        if blank <= 1:
            out.append(line)
    return "\n".join(out).strip("\n")


def detect_type(text, doc_types):
    """PP-13: the label whose name starts the first non-empty line, else None."""
    first = ""
    for line in text.split("\n"):
        if line.strip():
            first = line.strip().upper()
            break
    for name in sorted(doc_types, key=len, reverse=True):
        if first.startswith(name):
            return doc_types[name]
    return None


def main():
    hdfs = pc.wants_hdfs(sys.argv)
    schemas = pc.load_schemas()
    spec = schemas["documents"]
    run = pc.Run("documents", hdfs=hdfs, quiet="--quiet" in sys.argv)
    seen = pc.SeenKeys(run.root, "doc_hash")
    subdir = spec["staging_dir"].replace("staging/", "", 1)
    items = run.new_files(subdir, [spec["file_pattern"]])
    bundles = {}                    # (doc_type, yyyymm) -> [(item, json line)]
    for n, item in enumerate(items, 1):
        run.progress("cleaning documents", n, len(items))
        item.rows_in = 1
        text = run.read_text(item)                                     # PP-02
        if text is None:
            item.rows_bad = 1
            continue
        clean = normalise(text)                                        # PP-12
        run.check("PP-12", clean == text.strip("\n"))   # "failed" = layout had to be tidied
        doc_type = detect_type(clean, spec["doc_types"])               # PP-13
        if doc_type is None:
            first = clean.split("\n")[0][:40] if clean else "(empty)"
            run.check("PP-13", False)
            run.reject_file(item, "PP-13", "unknown document type, first line: {}".format(first))
            item.rows_bad = 1
            continue
        if len(clean) < spec["min_chars"]:
            run.check("PP-13", False)
            run.reject_file(item, "PP-13", "too short: {} characters".format(len(clean)))
            item.rows_bad = 1
            continue
        run.check("PP-13", True)
        flags = []
        folder_type = item.rel.split("/")[-2] if item.rel.count("/") >= 2 else ""
        if folder_type and folder_type != doc_type:
            flags.append("type_folder_mismatch")
        digest = hashlib.sha256(clean.encode("utf-8")).hexdigest()     # PP-09
        if digest in seen:
            run.check("PP-09", False)
            item.status = "DUPLICATE"
            run.rejects.append([run.run_id, item.rel, 0, "PP-09", "same text as an earlier document", ""])
            item.rows_bad = 1
            continue
        run.check("PP-09", True)
        seen.add(digest)
        src, rid = run.lineage(item)                                   # PP-14
        doc = OrderedDict([("doc_file", item.rel), ("doc_type", doc_type), ("text", clean),
                           ("chars", len(clean)), ("_dq_flags", ";".join(flags)),
                           ("_src_file", src), ("_run_id", rid)])
        item.rows_ok = 1
        parts = item.rel.split("/")                 # documents/<yyyy>/<mm>/<TYPE>/<name>.txt
        yyyymm = parts[1] + parts[2] if len(parts) >= 5 else "000000"
        bundles.setdefault((doc_type, yyyymm), []).append((item, json.dumps(doc)))
    for (doc_type, yyyymm) in sorted(bundles):
        pairs = bundles[(doc_type, yyyymm)]
        name = "{}_{}_{}.jsonl".format(doc_type.lower(), yyyymm, run.run_id)
        run.accept_many([it for it, _ in pairs], "".join(line + "\n" for _, line in pairs),
                        "documents/{}/{}".format(doc_type, name))
    run.finish()
    seen.save()


if __name__ == "__main__":
    main()