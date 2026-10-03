import hashlib
import json
import tempfile
import unittest

import auditor
import cli


class TestCorpus(unittest.TestCase):
    def test_generate_deterministic(self):
        a = auditor.generate_corpus(seed=7, count=60)
        b = auditor.generate_corpus(seed=7, count=60)
        self.assertEqual(a, b)
        c = auditor.generate_corpus(seed=8, count=60)
        self.assertNotEqual(a, c)

    def test_checksum_stable(self):
        corpus = auditor.generate_corpus(seed=3, count=40)
        h1 = auditor.checksum_corpus(corpus)
        h2 = auditor.checksum_corpus(corpus)
        self.assertEqual(h1, h2)
        self.assertEqual(len(h1), 64)

    def test_kinds_present(self):
        corpus = auditor.generate_corpus(seed=7, count=120)
        kinds = set(e["kind"] for e in corpus)
        self.assertIn("duplicate", kinds)
        self.assertIn("expired", kinds)
        self.assertIn("skewed", kinds)
        self.assertIn("fresh", kinds)


class TestWindow(unittest.TestCase):
    def test_duplicate_rejected(self):
        w = auditor.ReplayWindow(window_size=50, ttl_seconds=300, max_skew_seconds=60)
        now = 1700000100
        r1 = w.decide("msg-0007-000001", 1700000050, now)
        r2 = w.decide("msg-0007-000001", 1700000055, now)
        self.assertEqual(r1["decision"], "accept")
        self.assertEqual(r2["decision"], "reject_duplicate")

    def test_expired_rejected(self):
        w = auditor.ReplayWindow(window_size=50, ttl_seconds=300, max_skew_seconds=60)
        now = 1700001000
        r = w.decide("msg-0007-000002", now - 1000, now)
        self.assertEqual(r["decision"], "reject_expired")

    def test_skew_rejected(self):
        w = auditor.ReplayWindow(window_size=50, ttl_seconds=300, max_skew_seconds=60)
        now = 1700000000
        r = w.decide("msg-0007-000003", now + 500, now)
        self.assertEqual(r["decision"], "reject_skew")

    def test_out_of_order_accepted_when_inside_ttl(self):
        w = auditor.ReplayWindow(window_size=50, ttl_seconds=300, max_skew_seconds=60)
        now = 1700000100
        r1 = w.decide("msg-0007-000010", 1700000090, now)
        r2 = w.decide("msg-0007-000011", 1700000080, now)
        self.assertEqual(r1["decision"], "accept")
        self.assertEqual(r2["decision"], "accept")

    def test_eviction_fifo(self):
        w = auditor.ReplayWindow(window_size=2, ttl_seconds=300, max_skew_seconds=60)
        now = 1700000100
        w.decide("id-a", 1700000090, now)
        w.decide("id-b", 1700000091, now)
        w.decide("id-c", 1700000092, now)
        self.assertEqual(len(w), 2)
        self.assertEqual(w.evictions, 1)
        self.assertNotIn("id-a", w.seen)


class TestAuditAndProfile(unittest.TestCase):
    def test_audit_recall_full(self):
        corpus = auditor.generate_corpus(seed=7, count=120)
        audit = auditor.audit_corpus(corpus, window_size=500, ttl_seconds=300, max_skew_seconds=60)
        self.assertEqual(audit["recall"]["duplicate_recall"], 1.0)
        self.assertEqual(audit["recall"]["expired_recall"], 1.0)
        self.assertEqual(audit["recall"]["skew_recall"], 1.0)

    def test_memory_profiler_per_1k(self):
        corpus = auditor.generate_corpus(seed=7, count=80)
        audit = auditor.audit_corpus(corpus)
        prof = auditor.profile_cache(500, corpus, now=audit["now"])
        self.assertGreater(prof["bytes_per_1k"], 1000)
        self.assertGreaterEqual(prof["estimated_bytes"], prof["tracked"])
        expect = auditor.estimate_entry_bytes("msg-0007-000001")
        self.assertEqual(expect, len("msg-0007-000001".encode("utf-8")) + 32)

    def test_report_roundtrip_with_tempfile(self):
        corpus = auditor.generate_corpus(seed=5, count=50)
        audit = auditor.audit_corpus(corpus, window_size=100)
        prof = auditor.profile_cache(100, corpus, now=audit["now"])
        cfg = {"seed": 5, "count": 50, "window_size": 100, "ttl_seconds": 300, "max_skew_seconds": 60, "base_time": 1700000000}
        report = auditor.build_report(corpus, audit, prof, cfg)
        with tempfile.TemporaryDirectory() as tmp:
            jp = tmp + "/report.json"
            mp = tmp + "/report.md"
            auditor.write_json_report(report, jp)
            auditor.write_markdown_report(report, mp)
            with open(jp, "r", encoding="utf-8") as f:
                back = json.load(f)
            self.assertEqual(back["checksum_sha256"], report["checksum_sha256"])
            with open(mp, "r", encoding="utf-8") as f:
                text = f.read()
            self.assertIn("Replay-Window", text)
            self.assertIn(report["checksum_sha256"], text)

    def test_cli_yaml_and_run_with_tempfile(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = tmp + "/cfg.yaml"
            json_out = tmp + "/out.json"
            md_out = tmp + "/out.md"
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write("seed: 7\ncount: 60\nwindow_size: 200\nttl_seconds: 300\nmax_skew_seconds: 60\nbase_time: 1700000000\n")
            loaded = cli.load_simple_yaml(cfg_path)
            self.assertEqual(loaded["seed"], 7)
            cfg = {"seed": 7, "count": 60, "window_size": 200, "ttl_seconds": 300, "max_skew_seconds": 60, "base_time": 1700000000}
            code = cli.run(cfg, json_out, md_out, fail_on_miss=True)
            self.assertEqual(code, 0)
            with open(json_out, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.assertIn("checksum_sha256", data)
            raw = json.dumps(data, sort_keys=True, indent=2)
            check = hashlib.sha256(raw.encode("utf-8")).hexdigest()
            self.assertEqual(len(check), 64)


if __name__ == "__main__":
    unittest.main()
