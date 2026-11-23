"""
pp_common.py - shared preprocessing engine for every source type.

Implements the steps that are the same for all data (Architecture Summary 3.2):
  PP-01  inventory and checksum: SHA-256 of each new file; files already in
         staging/preprocess_manifest.csv are skipped, so a rerun loads nothing twice
  PP-02  encoding and line endings: UTF-8 (BOM removed) or Windows-1252 converted
         to UTF-8, CRLF to LF, control characters removed; unreadable file rejected
  PP-03  parse check helpers for CSV (consistent column count), JSON and JSON lines
  PP-09  duplicate helpers: keys seen in this run, plus a persistent index for
         keys that must be unique across runs (message ids, document hashes)
  PP-14  lineage and release: _src_file and _run_id, accepted copies written to
         staging/accepted/, then released to HDFS landing; rejects to quarantine.
         Nothing is released and no file is marked done unless the run completes.

The source-specific steps (PP-04 to PP-08, PP-10 to PP-13) live in
pp_structured.py, pp_events.py, pp_messages.py and pp_documents.py, which use
the Run class below.

Release to HDFS is atomic for streaming: files are put into /lake/landing/_incoming/
first and then moved into place with one rename, so Spark never sees a half-copied
file. hdfs commands are batched (one JVM start per 200 files).

Python 3.4, standard library only. Self-test without Hadoop:
  python3 preprocess/pp_common.py
"""

import csv
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HDFS_LANDING = "/lake/landing"
HDFS_INCOMING = "/lake/landing/_incoming"
HDFS_REJECTS = "/lake/quarantine/preprocess_rejects"
HDFS_BATCH = 500
CONTROL_CHARS = re.compile("[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")

MANIFEST_COLS = ["run_id", "file_name", "source_type", "sha256", "size_bytes", "rows_in",
                 "rows_accepted", "rows_rejected", "status", "released_to", "processed_at"]
REJECT_COLS = ["run_id", "file_name", "row_or_line_no", "rule_id", "reason", "raw_value"]
QUALITY_COLS = ["run_id", "source_type", "rule_id", "checked", "passed", "failed", "pct_failed"]


def staging_dir(root=ROOT):
    """The data folder: $GSS_STAGING if set (for the real project), else <root>/staging.
    The self-test passes its own temporary root and is never redirected."""
    if root == ROOT and os.environ.get("GSS_STAGING"):
        return os.environ["GSS_STAGING"]
    return os.path.join(root, "staging")


def load_schemas(root=ROOT):
    with open(os.path.join(root, "preprocess", "config", "schemas.json"), encoding="utf-8") as f:
        return json.load(f)


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            h.update(block)
    return h.hexdigest()


def landing_dir(rel):
    """HDFS landing folder for a staging-relative path.
    exports/<system>/x.csv      -> /lake/landing/exports/<system>
    events/x.jsonl              -> /lake/landing/exports/events
    messages/<channel>/x.json   -> /lake/landing/messages/<channel>
    documents/<TYPE>/x.json     -> /lake/landing/documents/<TYPE>   (as pp_documents names them)"""
    parts = rel.replace("\\", "/").split("/")
    if parts[0] == "events":
        return HDFS_LANDING + "/exports/events"
    return HDFS_LANDING + "/" + "/".join(parts[:-1])


class Item(object):
    """One staging file being preprocessed. The file is read once: the bytes are kept
    after hashing until read_text() uses them."""
    def __init__(self, root, rel):
        self.rel = rel.replace("\\", "/")
        self.path = os.path.join(staging_dir(root), self.rel)
        with open(self.path, "rb") as f:
            self.raw = f.read()
        self.size = len(self.raw)
        self.sha = hashlib.sha256(self.raw).hexdigest()
        self.rows_in = 0
        self.rows_ok = 0
        self.rows_bad = 0
        self.status = None
        self.released_to = ""


class SeenKeys(object):
    """Persistent set of keys (one per line) under staging/state/, for PP-09 across runs."""
    def __init__(self, root, name):
        self.path = os.path.join(staging_dir(root), "state", "seen_{}.txt".format(name))
        self.keys = set()
        self.new = []
        if os.path.exists(self.path):
            with open(self.path, encoding="utf-8") as f:
                self.keys = set(line.strip() for line in f if line.strip())

    def __contains__(self, key):
        return key in self.keys

    def add(self, key):
        if key not in self.keys:
            self.keys.add(key)
            self.new.append(key)

    def save(self):
        if not self.new:
            return
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, "a", encoding="utf-8") as f:
            for k in self.new:
                f.write(k + "\n")
        self.new = []


class Run(object):
    """One preprocessing run for one source type (structured, events, messages, documents)."""

    def __init__(self, source_type, root=ROOT, hdfs=True, quiet=False):
        self.source_type = source_type
        self.root = root
        self.hdfs = hdfs
        self.quiet = quiet
        self.staging = staging_dir(root)
        self.started = datetime.now()
        self.run_id = self.started.strftime("RUN-%Y%m%d-%H%M%S")
        # run ids are per second; wait a moment rather than reuse one for the same source
        while os.path.exists(os.path.join(self.staging, "reports", "quality_{}_{}.csv".format(
                self.run_id, source_type))):
            time.sleep(0.2)
            self.started = datetime.now()
            self.run_id = self.started.strftime("RUN-%Y%m%d-%H%M%S")
        self.items = []
        self.rejects = []                 # reject_log rows
        self.quality = {}                 # rule_id -> [checked, passed]
        self.skipped = 0
        self._lock = os.path.join(self.staging, "state", "{}.lock".format(source_type))
        os.makedirs(os.path.dirname(self._lock), exist_ok=True)
        try:
            self._lock_fd = os.open(self._lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(self._lock_fd, self.run_id.encode())
        except OSError:
            sys.exit("Another {} preprocessing run is active (remove {} if it crashed)".format(
                source_type, self._lock))
        self._done = self._load_manifest()
        # accepted files of an earlier run of this source that failed before release
        pending_root = os.path.join(self.staging, "accepted", "_pending")
        if os.path.isdir(pending_root):
            for name in os.listdir(pending_root):
                if name.endswith("_" + source_type):
                    shutil.rmtree(os.path.join(pending_root, name), ignore_errors=True)

    # -- PP-01 ---------------------------------------------------------------
    def _load_manifest(self):
        done = {}
        self._done_names = set()
        path = os.path.join(self.staging, "preprocess_manifest.csv")
        if os.path.exists(path) and os.path.getsize(path) > 0:
            with open(path, encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    if row["status"] in ("RELEASED", "REJECTED", "ACCEPTED_LOCAL", "DUPLICATE"):
                        self._done_names.add(row["file_name"])
                    if row["status"] in ("RELEASED", "REJECTED", "ACCEPTED_LOCAL"):
                        done[row["sha256"]] = row["file_name"]
        return done

    def new_files(self, subdir, patterns, limit=None, sort_key=None):
        """PP-01: new files under staging/<subdir> matching any pattern.
        Staging files are written once and never edited, so a file name already in the
        manifest is skipped without reading it (this keeps reruns over 40,000 messages fast).
        Every other file is hashed; one whose SHA-256 matches a file already processed, or
        another file in this run, is a duplicate: logged under PP-01 and recorded in the
        manifest as DUPLICATE, but not released. limit caps the number of files taken;
        sort_key orders them (default: path name)."""
        import fnmatch
        base = os.path.join(self.staging, subdir)
        found = []
        for dirpath, dirnames, files in os.walk(base):
            dirnames.sort()
            for name in files:
                if any(fnmatch.fnmatch(name, p) for p in patterns):
                    rel = os.path.relpath(os.path.join(dirpath, name), self.staging).replace("\\", "/")
                    if rel not in self._done_names:
                        found.append(rel)
        found.sort(key=sort_key)
        out, seen_now = [], {}
        for n, rel in enumerate(found, 1):
            self.progress("PP-01 reading and checksums", n, len(found) if not limit else min(len(found), limit))
            item = Item(self.root, rel)
            original = self._done.get(item.sha) or seen_now.get(item.sha)
            self.check("PP-01", original is None)
            if original is not None:
                self.skipped += 1
                item.status = "DUPLICATE"
                item.raw = None
                self.rejects.append([self.run_id, rel, 0, "PP-01", "identical to {}".format(original), ""])
                self.items.append(item)
                continue
            seen_now[item.sha] = rel
            out.append(item)
            if limit and len(out) >= limit:
                break
        self.items.extend(out)
        return out

    # -- PP-02 ---------------------------------------------------------------
    def read_text(self, item):
        """Returns cleaned UTF-8 text, or None after rejecting the file."""
        raw = item.raw
        item.raw = None
        text = None
        for enc in ("utf-8-sig", "cp1252"):
            try:
                text = raw.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        if text is None:
            self.check("PP-02", False)
            self.reject_file(item, "PP-02", "unreadable encoding")
            return None
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        text = CONTROL_CHARS.sub("", text)
        self.check("PP-02", True)
        return text

    # -- PP-03 helpers ---------------------------------------------------------
    def parse_csv(self, item, text, expected_cols):
        """Returns (header, [(line_no, row)]) with rows of the wrong length rejected, or
        (None, []) if the file cannot be read as CSV."""
        try:
            rows = list(csv.reader(io.StringIO(text)))
        except csv.Error as e:
            self.check("PP-03", False)
            self.reject_file(item, "PP-03", "CSV parse error: {}".format(e))
            return None, []
        rows = [(i + 1, r) for i, r in enumerate(rows) if any(c.strip() for c in r)]
        if not rows:
            self.check("PP-03", False)
            self.reject_file(item, "PP-03", "empty file")
            return None, []
        header = [h.strip() for h in rows[0][1]]
        good = []
        for line_no, r in rows[1:]:
            item.rows_in += 1
            if len(r) != expected_cols:
                self.check("PP-03", False)
                self.reject_row(item, line_no, "PP-03", "expected {} columns, found {}".format(
                    expected_cols, len(r)), ",".join(r))
            else:
                self.check("PP-03", True)
                good.append((line_no, r))
        return header, good

    def parse_json(self, item, text):
        try:
            obj = json.loads(text)
        except ValueError as e:
            self.check("PP-03", False)
            self.reject_file(item, "PP-03", "invalid JSON: {}".format(str(e)[:80]))
            return None
        if not isinstance(obj, dict):
            self.check("PP-03", False)
            self.reject_file(item, "PP-03", "JSON is not an object")
            return None
        self.check("PP-03", True)
        return obj

    def parse_jsonl(self, item, text):
        good = []
        for i, line in enumerate(text.split("\n"), 1):
            if not line.strip():
                continue
            item.rows_in += 1
            try:
                obj = json.loads(line)
                if not isinstance(obj, dict):
                    raise ValueError("not an object")
                self.check("PP-03", True)
                good.append((i, obj))
            except ValueError as e:
                self.check("PP-03", False)
                self.reject_row(item, i, "PP-03", "invalid JSON line: {}".format(str(e)[:60]), line[:200])
        return good

    def progress(self, what, n, total, every=2000):
        """Prints a progress line every 2,000 files on long runs, so they never look frozen."""
        if not self.quiet and total >= every and (n % every == 0 or n == total):
            print("  {} {:>6} / {}".format(what, n, total))
            sys.stdout.flush()

    # -- logging ---------------------------------------------------------------
    def check(self, rule_id, ok):
        c = self.quality.setdefault(rule_id, [0, 0])
        c[0] += 1
        if ok:
            c[1] += 1

    def reject_row(self, item, line_no, rule_id, reason, raw):
        item.rows_bad += 1
        self.rejects.append([self.run_id, item.rel, line_no, rule_id, reason, (raw or "")[:300]])

    def reject_file(self, item, rule_id, reason):
        item.status = "REJECTED"
        self.rejects.append([self.run_id, item.rel, 0, rule_id, reason, ""])

    # -- PP-14 ---------------------------------------------------------------
    def lineage(self, item):
        return item.rel, self.run_id

    def accept(self, item, text, out_rel=None):
        """Write the cleaned file to staging/accepted/_pending/<run_id>/<out_rel>. When the run
        finishes, that folder is released to HDFS in one call and the files move to
        staging/accepted/<out_rel>. out_rel defaults to the staging path; a script can give
        another name, e.g. documents land as documents/<TYPE>/<name>.json."""
        item.out_rel = (out_rel or item.rel).replace("\\", "/")
        out = os.path.join(self._pending_dir(), item.out_rel)
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        item.status = "ACCEPTED"
        item.accepted_path = out

    def accept_many(self, items, text, out_rel):
        """Accept several staging files into ONE output file (e.g. all AWBs of a month as
        JSON lines). HDFS works best with few large files, and one put of 144 bundles is far
        quicker than one of 6,000 small files. Each source file is still recorded in the
        manifest. Give out_rel a name unique to this run, so a later run cannot overwrite it."""
        if not items:
            return
        out_rel = out_rel.replace("\\", "/")
        out = os.path.join(self._pending_dir(), out_rel)
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        for item in items:
            item.out_rel = out_rel
            item.status = "ACCEPTED"
            item.accepted_path = out

    def _hdfs(self, args):
        subprocess.check_call([hdfs_bin(), "dfs"] + args)

    def _pending_dir(self):
        return os.path.join(self.staging, "accepted", "_pending", self.run_id + "_" + self.source_type)

    def _release(self, accepted, reject_files):
        """Release with as few hdfs calls as possible (each starts a Java VM, ~3 s on the VM):
          1 mkdir   _incoming, every landing folder and the quarantine folder, in one call
          1 put     the run's pending folder into /lake/landing/_incoming/<run>
          1 mv      per landing folder, a wildcard rename into place, so a streaming reader
                    never sees a half-copied file
          1 rm      the empty _incoming/<run> folder
          1 put     rejected files and rows to the quarantine folder (only if there are any)
        If a crashed earlier run left the same names in a landing folder, the mv fails;
        those names are then removed and the move is repeated."""
        incoming = "{}/{}_{}".format(HDFS_INCOMING, self.run_id, self.source_type)
        qdir = "{}/{}/{}".format(HDFS_REJECTS, self.source_type, self.run_id)
        by_dir = {}                      # (staging folder, landing folder) -> file names
        for it in accepted:
            key = (os.path.dirname(it.out_rel), landing_dir(it.out_rel))
            names = by_dir.setdefault(key, [])
            if os.path.basename(it.out_rel) not in names[-1:]:
                names.append(os.path.basename(it.out_rel))
        dirs = sorted(set(h for _, h in by_dir))
        if accepted:
            dirs.append(HDFS_INCOMING)
        if reject_files:
            dirs.append(qdir)
        if not dirs:
            return
        self._hdfs(["-mkdir", "-p"] + dirs)
        if accepted:
            self._hdfs(["-put", self._pending_dir(), incoming])
            for rel_dir, hdir in sorted(by_dir):
                src = "{}/{}/*".format(incoming, rel_dir)
                try:
                    self._hdfs(["-mv", src, hdir + "/"])
                except subprocess.CalledProcessError:
                    names = by_dir[(rel_dir, hdir)]
                    for i in range(0, len(names), HDFS_BATCH):
                        self._hdfs(["-rm", "-f", "-skipTrash"] + [hdir + "/" + n for n in names[i:i + HDFS_BATCH]])
                    self._hdfs(["-mv", src, hdir + "/"])
            self._hdfs(["-rm", "-r", "-f", "-skipTrash", incoming])
        for i in range(0, len(reject_files), HDFS_BATCH):
            self._hdfs(["-put", "-f"] + reject_files[i:i + HDFS_BATCH] + [qdir + "/"])

    def _settle_pending(self, accepted):
        """Move this run's accepted files from the pending folder to staging/accepted/<rel>."""
        moved = {}
        for it in accepted:
            if it.accepted_path not in moved:              # a bundle is moved once
                dst = os.path.join(self.staging, "accepted", it.out_rel)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                os.replace(it.accepted_path, dst)
                moved[it.accepted_path] = dst
            it.accepted_path = moved[it.accepted_path]
        shutil.rmtree(self._pending_dir(), ignore_errors=True)

    def finish(self):
        """Release accepted files and rejects, then write manifest, reject log and quality report."""
        try:
            accepted = [it for it in self.items if it.status == "ACCEPTED"]
            rejected = [it for it in self.items if it.status == "REJECTED"]

            # rejected files and rejected rows are kept in staging/rejected/ with their reasons
            reject_files = []
            for it in rejected:
                dst = os.path.join(self.staging, "rejected", it.rel)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copyfile(it.path, dst)
                reject_files.append(dst)
            rows_by_file = {}
            for r in self.rejects:
                if r[2] != 0:
                    rows_by_file.setdefault(r[1], []).append(r[2:])
            for rel in sorted(rows_by_file):
                dst = os.path.join(self.staging, "rejected", rel + ".rejected_rows.csv")
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                with open(dst, "w", encoding="utf-8", newline="") as f:
                    w = csv.writer(f)
                    w.writerow(["row_or_line_no", "rule_id", "reason", "raw_value"])
                    w.writerows(rows_by_file[rel])
                reject_files.append(dst)

            if self.hdfs:
                self._release(accepted, reject_files)
                for it in accepted:
                    it.status, it.released_to = "RELEASED", landing_dir(it.out_rel)
            else:
                for it in accepted:
                    it.status, it.released_to = "ACCEPTED_LOCAL", os.path.join(self.staging, "accepted", it.out_rel)
            self._settle_pending(accepted)

            self._write_records()
        finally:
            os.close(self._lock_fd)
            os.remove(self._lock)
        return self.summary()

    def _write_records(self):
        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        mpath = os.path.join(self.staging, "preprocess_manifest.csv")
        new = not os.path.exists(mpath) or os.path.getsize(mpath) == 0
        with open(mpath, "a", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(MANIFEST_COLS)
            for it in self.items:
                w.writerow([self.run_id, it.rel, self.source_type, it.sha, it.size, it.rows_in,
                            it.rows_ok, it.rows_bad, it.status or "REJECTED", it.released_to, now])
        rdir = os.path.join(self.staging, "reports")
        os.makedirs(rdir, exist_ok=True)
        tag = "{}_{}".format(self.run_id, self.source_type)
        with open(os.path.join(rdir, "reject_log_{}.csv".format(tag)), "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(REJECT_COLS)
            w.writerows(self.rejects)
        with open(os.path.join(rdir, "quality_{}.csv".format(tag)), "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(QUALITY_COLS)
            for rule in sorted(self.quality):
                checked, passed = self.quality[rule]
                failed = checked - passed
                w.writerow([self.run_id, self.source_type, rule, checked, passed, failed,
                            "{:.2f}".format(100.0 * failed / checked) if checked else "0.00"])

    def summary(self):
        s = {"run_id": self.run_id, "files": len(self.items), "skipped": self.skipped,
             "released": sum(1 for i in self.items if i.status in ("RELEASED", "ACCEPTED_LOCAL")),
             "rejected_files": sum(1 for i in self.items if i.status == "REJECTED"),
             "duplicates": sum(1 for i in self.items if i.status == "DUPLICATE"),
             "rows_in": sum(i.rows_in for i in self.items),
             "rows_accepted": sum(i.rows_ok for i in self.items),
             "rows_rejected": sum(i.rows_bad for i in self.items),
             "seconds": (datetime.now() - self.started).total_seconds()}
        if not self.quiet:
            print("{run_id} {src}: {files} new files ({duplicates} duplicates), {released} released, "
                  "{rejected_files} rejected; rows {rows_in} in, {rows_accepted} accepted, "
                  "{rows_rejected} rejected; {seconds:.1f} s".format(src=self.source_type, **s))
            for rule in sorted(self.quality):
                checked, passed = self.quality[rule]
                if checked:
                    print("  {:<6} checked {:>7}  failed {:>5}".format(rule, checked, checked - passed))
        return s


def csv_text(header, rows):
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(header)
    w.writerows(rows)
    return buf.getvalue()


def hdfs_bin():
    """Path of the hdfs command: on PATH, or under $HADOOP_HOME/bin (cron and Python do not
    see shell aliases, and cron starts with a short PATH)."""
    found = shutil.which("hdfs")
    if found:
        return found
    for home in (os.environ.get("HADOOP_HOME"), os.environ.get("HADOOP_PREFIX"), "/usr/share/hadoop",
                 "/usr/local/hadoop"):
        if home and os.path.isfile(os.path.join(home, "bin", "hdfs")):
            return os.path.join(home, "bin", "hdfs")
    return None


def hdfs_available():
    return hdfs_bin() is not None


def wants_hdfs(argv):
    """--no-hdfs runs everything locally (accepted files stay in staging/accepted/)."""
    if "--no-hdfs" in argv:
        return False
    if not hdfs_available():
        sys.exit("hdfs command not found on PATH or in $HADOOP_HOME/bin; run with --no-hdfs to test locally")
    return True


# ---------------------------------------------------------------------------
# Self-test (no Hadoop needed)
# ---------------------------------------------------------------------------
def _self_test():
    import tempfile
    root = tempfile.mkdtemp(prefix="pp_common_test_")
    problems = []

    def check(ok, msg):
        if not ok:
            problems.append(msg)

    try:
        st = os.path.join(root, "staging", "exports", "lion")
        os.makedirs(st)
        with open(os.path.join(st, "a.csv"), "wb") as f:                  # BOM, CRLF, control char
            f.write("\ufeffid,name\r\n1,Alpha\x07\r\n2,Beta\r\n3,Gamma,EXTRA\r\n".encode("utf-8"))
        with open(os.path.join(st, "b.csv"), "wb") as f:                  # Windows-1252
            f.write("id,name\n1,Soci\u00e9t\u00e9\n".encode("cp1252"))
        with open(os.path.join(st, "c.csv"), "wb") as f:                  # same bytes as b.csv
            f.write("id,name\n1,Soci\u00e9t\u00e9\n".encode("cp1252"))
        with open(os.path.join(st, "d.csv"), "wb") as f:                  # not text in either encoding
            f.write(b"id,name\n1,\x81\x8d\x8f\x90\x9d\xff\xfe\x00bad\n")

        run = Run("structured", root=root, hdfs=False, quiet=True)
        items = run.new_files("exports/lion", ["*.csv"])
        check([i.rel for i in items] == ["exports/lion/a.csv", "exports/lion/b.csv", "exports/lion/d.csv"],
              "PP-01 did not skip the duplicate file c.csv: {}".format([i.rel for i in items]))
        texts = {}
        for it in items:
            t = run.read_text(it)
            if t is None:
                continue
            texts[it.rel] = t
            header, rows = run.parse_csv(it, t, 2)
            it.rows_ok = len(rows)
            run.accept(it, csv_text(header, [r for _, r in rows]))
        check("\r" not in texts["exports/lion/a.csv"] and "\x07" not in texts["exports/lion/a.csv"]
              and not texts["exports/lion/a.csv"].startswith("\ufeff"), "PP-02 did not clean a.csv")
        check("Soci\u00e9t\u00e9" in texts["exports/lion/b.csv"], "PP-02 did not convert Windows-1252")
        s = run.finish()
        check(s["rows_rejected"] == 1, "PP-03 should reject 1 row, got {}".format(s["rows_rejected"]))
        check(s["released"] == 2, "expected 2 accepted files, got {}".format(s["released"]))
        check(s["rejected_files"] == 1, "PP-02 should reject unreadable d.csv")
        check(os.path.exists(os.path.join(root, "staging", "rejected", "exports", "lion", "d.csv")),
              "rejected file not kept in staging/rejected/")
        check(os.path.exists(os.path.join(root, "staging", "accepted", "exports", "lion", "a.csv")),
              "accepted copy missing")
        check(os.path.exists(os.path.join(root, "staging", "rejected", "exports", "lion", "a.csv.rejected_rows.csv")),
              "rejected rows file missing")

        run2 = Run("structured", root=root, hdfs=False, quiet=True)
        check(run2.new_files("exports/lion", ["*.csv"]) == [], "rerun should find no new files (idempotence)")
        run2.finish()

        with open(os.path.join(root, "staging", "preprocess_manifest.csv"), encoding="utf-8") as f:
            m = list(csv.DictReader(f))
        status = dict((r["file_name"], r["status"]) for r in m)
        check(status == {"exports/lion/a.csv": "ACCEPTED_LOCAL", "exports/lion/b.csv": "ACCEPTED_LOCAL",
                         "exports/lion/c.csv": "DUPLICATE", "exports/lion/d.csv": "REJECTED"},
              "manifest statuses wrong: {}".format(status))
        a_row = [r for r in m if r["file_name"] == "exports/lion/a.csv"][0]
        check(a_row["rows_in"] == "3" and a_row["rows_rejected"] == "1", "manifest row counts wrong for a.csv")
        check(not os.path.exists(os.path.join(root, "staging", "accepted", "_pending", run.run_id + "_structured")),
              "pending folder not cleaned up")

        seen = SeenKeys(root, "test")
        seen.add("M-2024-000001")
        seen.save()
        check("M-2024-000001" in SeenKeys(root, "test"), "SeenKeys not persistent")

        check(landing_dir("exports/netsuite/invoice_202403.csv") == "/lake/landing/exports/netsuite", "landing exports")
        check(landing_dir("events/events_202403.jsonl") == "/lake/landing/exports/events", "landing events")
        check(landing_dir("messages/email/M-2024-000001.json") == "/lake/landing/messages/email", "landing messages")
        check(landing_dir("documents/2024/03/AWB/awb_20240301_001.txt") == "/lake/landing/documents/2024/03/AWB",
              "landing documents")
    finally:
        shutil.rmtree(root, ignore_errors=True)

    print("hdfs command: {}".format(hdfs_bin() or "NOT FOUND (set HADOOP_HOME or add Hadoop bin to PATH)"))
    if problems:
        print("FAILED with {} problem(s):".format(len(problems)))
        for p in problems:
            print("  - " + p)
        return 1
    print("OK: pp_common self-test passed (PP-01, PP-02, PP-03, PP-14, manifest, idempotent rerun)")
    return 0


if __name__ == "__main__":
    sys.exit(_self_test())