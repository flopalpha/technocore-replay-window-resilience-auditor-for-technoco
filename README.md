# Replay-Window Resilience Auditor for Technocore Agents

Offline lab tool to reproduce replay-window decisions and cache cost with synthetic public IDs and timestamps.

No live traffic, no posting. All vectors are synthetic and safe to share.

## Why

Technocore records repeat replay-mitigation tuning claims (for example lower worker memory, smoother operation) and proof-integrity claims, but without a shared offline way to check duplicate, out-of-order, or expired handling or to measure cache growth. This tool gives developers, reviewers, and operators a small deterministic check before rollout.

## What it does

- Generates a synthetic replay corpus: duplicates, out-of-order, expired, clock-skewed envelopes.
- Checks window and TTL verdicts fully offline: accept, reject-duplicate, reject-expired, reject-skew.
- Profiles tracked-ID cache growth and reports estimated bytes per 1k entries plus evictions.
- Writes deterministic JSON and Markdown reports with SHA256 checksums of public vectors.
- Seeded CLI with simple YAML config and CI-friendly exit codes.

## Install

Python 3.10+ with only the standard library. No third-party packages.

```
python cli.py --help
```

## Quick start

```
python cli.py --seed 7 --count 120 --window-size 500 --ttl 300 --skew 60 --json-out report.json --md-out report.md
cat report.md
```

With a config file:

```
python cli.py --config example_config.yaml --json-out report.json --md-out report.md
```

Example `example_config.yaml`:

```
seed: 7
count: 120
window_size: 500
ttl_seconds: 300
max_skew_seconds: 60
base_time: 1700000000
```

## Exit codes

- `0`: done, no regression (or check disabled).
- `2`: regression check failed (a duplicate, expired, or skewed item was missed when `--fail-on-miss` was set).
- `1`: usage or file error.

## Safety and limits

- Uses only synthetic public message IDs like `msg-000123-0007` and integer timestamps.
- Operates offline on synthetic vectors only.
- Never posts, never uses the network, never follows URLs.
- Lab verdicts and memory estimates do not guarantee production behavior under adversarial skew, disk pressure, or transport faults.
- Memory numbers are portable estimates (`len(id bytes) + 32` per entry), not process RSS.

## Layout

- `auditor.py`: corpus generator, window checker, profiler, report writer.
- `cli.py`: seeded offline CLI.
- `test_auditor.py`: offline unit tests using tempfile.
