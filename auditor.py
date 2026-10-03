"""Replay-window resilience auditor core. Offline only, synthetic IDs only."""

import hashlib
import json
import random

TOOL_VERSION = "1.0.0"
ESTIMATE_OVERHEAD = 32


def generate_corpus(seed=1, count=100, base_time=1700000000, ttl_seconds=300, max_skew_seconds=60):
    """Build a deterministic synthetic corpus."""
    rng = random.Random(seed)
    corpus = []
    counter = 0
    seen_ids = []
    for i in range(count):
        r7 = (i % 7 == 0) and i > 2
        r11 = (i % 11 == 0)
        r13 = (i % 13 == 0) and i > 0
        r5 = (i % 5 == 0) and i > 0
        if r7 and seen_ids:
            pick = seen_ids[(i + rng.randint(0, 10)) % len(seen_ids)]
            entry = {
                "msg_id": pick,
                "timestamp": base_time + i,
                "kind": "duplicate",
                "seq": i,
            }
        elif r11:
            counter += 1
            msg_id = make_id(seed, counter)
            entry = {
                "msg_id": msg_id,
                "timestamp": base_time - ttl_seconds - 50 - (i % 10),
                "kind": "expired",
                "seq": i,
            }
        elif r13:
            counter += 1
            msg_id = make_id(seed, counter)
            entry = {
                "msg_id": msg_id,
                "timestamp": base_time + count + max_skew_seconds + 50,
                "kind": "skewed",
                "seq": i,
            }
        elif r5:
            counter += 1
            msg_id = make_id(seed, counter)
            seen_ids.append(msg_id)
            entry = {
                "msg_id": msg_id,
                "timestamp": base_time + i - 10,
                "kind": "out_of_order",
                "seq": i,
            }
        else:
            counter += 1
            msg_id = make_id(seed, counter)
            seen_ids.append(msg_id)
            entry = {
                "msg_id": msg_id,
                "timestamp": base_time + i,
                "kind": "fresh",
                "seq": i,
            }
        corpus.append(entry)
    return corpus


def make_id(seed, counter):
    """Make a synthetic public message id."""
    a = seed % 10000
    return "msg-" + str(a).zfill(4) + "-" + str(counter).zfill(6)


def estimate_entry_bytes(msg_id):
    """Portable estimate for one tracked id."""
    return len(msg_id.encode("utf-8")) + ESTIMATE_OVERHEAD


class ReplayWindow:
    """Small FIFO replay window with TTL and future-skew guard."""

    def __init__(self, window_size=1000, ttl_seconds=300, max_skew_seconds=60):
        self.window_size = int(window_size)
        self.ttl_seconds = int(ttl_seconds)
        self.max_skew_seconds = int(max_skew_seconds)
        self.seen = {}
        self.order = []
        self.evictions = 0

    def __len__(self):
        return len(self.seen)

    def decide(self, msg_id, timestamp, now):
        if msg_id in self.seen:
            return {"decision": "reject_duplicate", "reason": "id already tracked"}
        if timestamp < now - self.ttl_seconds:
            return {"decision": "reject_expired", "reason": "older than ttl"}
        if timestamp > now + self.max_skew_seconds:
            return {"decision": "reject_skew", "reason": "future beyond skew"}
        self.seen[msg_id] = int(timestamp)
        self.order.append(msg_id)
        if len(self.seen) > self.window_size:
            oldest = self.order.pop(0)
            if oldest in self.seen:
                del self.seen[oldest]
            self.evictions += 1
        return {"decision": "accept", "reason": "within window and ttl"}

    def tracked_ids(self):
        return list(self.order)


def default_now(corpus, base_time, count):
    """Pick a stable now that keeps fresh items inside TTL."""
    if not corpus:
        return int(base_time)
    stamps = []
    for e in corpus:
        if e.get("kind") != "skewed":
            stamps.append(int(e["timestamp"]))
    if not stamps:
        return int(base_time) + int(count)
    top = max(stamps)
    base_now = int(base_time) + int(count)
    if top > base_now:
        return int(top)
    return int(base_now)


def audit_corpus(corpus, window_size=1000, ttl_seconds=300, max_skew_seconds=60, now=None, base_time=1700000000):
    """Run every envelope through a fresh window in seq order."""
    if now is None:
        now = default_now(corpus, base_time, len(corpus))
    now = int(now)
    window = ReplayWindow(window_size, ttl_seconds, max_skew_seconds)
    decisions = []
    counts = {
        "total": 0,
        "accept": 0,
        "reject_duplicate": 0,
        "reject_expired": 0,
        "reject_skew": 0,
    }
    want_dup = 0
    hit_dup = 0
    want_exp = 0
    hit_exp = 0
    want_skew = 0
    hit_skew = 0
    ordered = sorted(corpus, key=lambda e: int(e.get("seq", 0)))
    for e in ordered:
        msg_id = str(e["msg_id"])
        ts = int(e["timestamp"])
        kind = str(e.get("kind", "fresh"))
        out = window.decide(msg_id, ts, now)
        dec = out["decision"]
        counts["total"] += 1
        if dec in counts:
            counts[dec] += 1
        if kind == "duplicate":
            want_dup += 1
            if dec == "reject_duplicate":
                hit_dup += 1
        if kind == "expired":
            want_exp += 1
            if dec == "reject_expired":
                hit_exp += 1
        if kind == "skewed":
            want_skew += 1
            if dec == "reject_skew":
                hit_skew += 1
        decisions.append({
            "seq": int(e.get("seq", 0)),
            "msg_id": msg_id,
            "kind": kind,
            "decision": dec,
        })
    recall = {}
    recall["duplicate_recall"] = (hit_dup / want_dup) if want_dup else 1.0
    recall["expired_recall"] = (hit_exp / want_exp) if want_exp else 1.0
    recall["skew_recall"] = (hit_skew / want_skew) if want_skew else 1.0
    result = {
        "now": now,
        "counts": counts,
        "recall": recall,
        "expected": {
            "duplicates": want_dup,
            "expired": want_exp,
            "skewed": want_skew,
        },
        "hits": {
            "duplicates": hit_dup,
            "expired": hit_exp,
            "skewed": hit_skew,
        },
        "evictions": window.evictions,
        "tracked": len(window),
        "decisions": decisions,
    }
    return result


def profile_cache(window_size, corpus, now=None, base_time=1700000000, ttl_seconds=300, max_skew_seconds=60):
    """Estimate cache growth by replaying accepts only."""
    audit = audit_corpus(corpus, window_size, ttl_seconds, max_skew_seconds, now, base_time)
    window = ReplayWindow(window_size, ttl_seconds, max_skew_seconds)
    use_now = int(audit["now"])
    total_bytes = 0
    per_entry = []
    ordered = sorted(corpus, key=lambda e: int(e.get("seq", 0)))
    for e in ordered:
        out = window.decide(str(e["msg_id"]), int(e["timestamp"]), use_now)
        if out["decision"] == "accept":
            n = estimate_entry_bytes(str(e["msg_id"]))
            per_entry.append(n)
            total_bytes += n
    tracked = len(window)
    if per_entry:
        avg = sum(per_entry) / len(per_entry)
    else:
        avg = float(ESTIMATE_OVERHEAD + 12)
    per_1k = int(round(avg * 1000))
    return {
        "tracked": tracked,
        "evictions": window.evictions,
        "estimated_bytes": int(total_bytes),
        "avg_bytes_per_entry": round(avg, 2),
        "bytes_per_1k": per_1k,
    }


def checksum_corpus(corpus):
    """SHA256 over canonical JSON of public vectors."""
    text = json.dumps(corpus, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def build_report(corpus, audit, profile, config):
    """Assemble a deterministic report dict."""
    checksum = checksum_corpus(corpus)
    report = {
        "tool": "replay-window-resilience-auditor",
        "version": TOOL_VERSION,
        "config": dict(sorted(config.items())),
        "checksum_sha256": checksum,
        "counts": audit["counts"],
        "recall": audit["recall"],
        "expected": audit["expected"],
        "hits": audit["hits"],
        "evictions": audit["evictions"],
        "tracked": audit["tracked"],
        "profile": profile,
    }
    return report


def write_json_report(report, path):
    text = json.dumps(report, sort_keys=True, indent=2)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text + "\n")
    return path


def write_markdown_report(report, path):
    lines = []
    lines.append("# Replay-Window Audit Report")
    lines.append("")
    lines.append("Tool: " + str(report.get("tool", "")) + " v" + str(report.get("version", "")))
    lines.append("Checksum: " + str(report.get("checksum_sha256", "")))
    lines.append("")
    lines.append("## Config")
    lines.append("")
    cfg = report.get("config", {})
    for k in sorted(cfg.keys()):
        lines.append("- " + str(k) + ": " + str(cfg[k]))
    lines.append("")
    lines.append("## Counts")
    lines.append("")
    counts = report.get("counts", {})
    for k in sorted(counts.keys()):
        lines.append("- " + str(k) + ": " + str(counts[k]))
    lines.append("")
    lines.append("## Recall")
    lines.append("")
    recall = report.get("recall", {})
    for k in sorted(recall.keys()):
        lines.append("- " + str(k) + ": " + str(recall[k]))
    lines.append("")
    lines.append("## Cache profile")
    lines.append("")
    prof = report.get("profile", {})
    for k in sorted(prof.keys()):
        lines.append("- " + str(k) + ": " + str(prof[k]))
    lines.append("")
    lines.append("Evictions: " + str(report.get("evictions", 0)))
    lines.append("Tracked: " + str(report.get("tracked", 0)))
    lines.append("")
    text = "\n".join(lines)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return path
