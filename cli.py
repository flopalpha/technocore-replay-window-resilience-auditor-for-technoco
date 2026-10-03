"""Seeded offline CLI for the replay-window auditor."""

import argparse
import json

import auditor


def load_simple_yaml(path):
    """Load a flat key: value file without extra deps."""
    data = {}
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if ":" not in line:
                continue
            key, _, value = line.partition(":")
            key = key.strip()
            value = value.strip()
            if not key:
                continue
            data[key] = parse_scalar(value)
    return data


def parse_scalar(value):
    if value == "":
        return ""
    low = value.lower()
    if low in ("true", "yes"):
        return True
    if low in ("false", "no"):
        return False
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


def build_parser():
    p = argparse.ArgumentParser(description="Offline replay-window auditor")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--count", type=int, default=120)
    p.add_argument("--window-size", type=int, default=500)
    p.add_argument("--ttl", type=int, default=300)
    p.add_argument("--skew", type=int, default=60)
    p.add_argument("--base-time", type=int, default=1700000000)
    p.add_argument("--config", type=str, default="")
    p.add_argument("--json-out", type=str, default="report.json")
    p.add_argument("--md-out", type=str, default="report.md")
    p.add_argument("--fail-on-miss", action="store_true")
    return p


def resolve_config(args):
    cfg = {
        "seed": int(args.seed),
        "count": int(args.count),
        "window_size": int(args.window_size),
        "ttl_seconds": int(args.ttl),
        "max_skew_seconds": int(args.skew),
        "base_time": int(args.base_time),
    }
    if args.config:
        file_cfg = load_simple_yaml(args.config)
        for k in ("seed", "count", "window_size", "ttl_seconds", "max_skew_seconds", "base_time"):
            if k in file_cfg:
                cfg[k] = int(file_cfg[k])
        # allow short aliases used in example files
        if "ttl" in file_cfg and "ttl_seconds" not in file_cfg:
            cfg["ttl_seconds"] = int(file_cfg["ttl"])
        if "skew" in file_cfg and "max_skew_seconds" not in file_cfg:
            cfg["max_skew_seconds"] = int(file_cfg["skew"])
    return cfg


def run(cfg, json_out, md_out, fail_on_miss=False):
    corpus = auditor.generate_corpus(
        seed=cfg["seed"],
        count=cfg["count"],
        base_time=cfg["base_time"],
        ttl_seconds=cfg["ttl_seconds"],
        max_skew_seconds=cfg["max_skew_seconds"],
    )
    audit = auditor.audit_corpus(
        corpus,
        window_size=cfg["window_size"],
        ttl_seconds=cfg["ttl_seconds"],
        max_skew_seconds=cfg["max_skew_seconds"],
        now=None,
        base_time=cfg["base_time"],
    )
    profile = auditor.profile_cache(
        cfg["window_size"],
        corpus,
        now=audit["now"],
        base_time=cfg["base_time"],
        ttl_seconds=cfg["ttl_seconds"],
        max_skew_seconds=cfg["max_skew_seconds"],
    )
    report = auditor.build_report(corpus, audit, profile, cfg)
    auditor.write_json_report(report, json_out)
    auditor.write_markdown_report(report, md_out)
    print("total: " + str(audit["counts"]["total"]))
    print("accept: " + str(audit["counts"]["accept"]))
    print("checksum: " + str(report["checksum_sha256"]))
    print("per_1k: " + str(profile["bytes_per_1k"]))
    if fail_on_miss:
        rec = audit["recall"]
        if rec["duplicate_recall"] < 1.0 or rec["expired_recall"] < 1.0 or rec["skew_recall"] < 1.0:
            return 2
    return 0


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        cfg = resolve_config(args)
    except (ValueError, OSError) as e:
        print("config error: " + str(e))
        return 1
    try:
        return run(cfg, args.json_out, args.md_out, args.fail_on_miss)
    except (ValueError, OSError) as e:
        print("run error: " + str(e))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
