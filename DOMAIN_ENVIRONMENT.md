# Reproducible offline environment

Upstream: https://github.com/apache/avro
Source commit: 08d2fd78286896beb922528c79e7a7d870fce504
Validated on Ubuntu 26.04 / 3.13.15 (main, Sep  1 2026, 14:18:08) [Clang 22.1.3 ].
Environment prepared with Codex; no task implementation is included.

From this checkout with Python 3.13:
```
python -m venv .venv
. .venv/bin/activate
export PYTHONUTF8=1 PYTHONDONTWRITEBYTECODE=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 NO_SQLITE=1 PYYAML_FORCE_LIBYAML=0 SQLGLOT_USE_RUST=0
python -m pip install -r requirements-domain.lock
python -m pip install -e ./lang/py
python -m unittest avro.test.test_io avro.test.test_protocol avro.test.test_datafile
```
Only these selected core tests are claimed; the measured process took 0.216 seconds. No external service, account, broker or large downloaded dataset is required. Fixtures retain the upstream license. Restore a clean checkout of this commit and recreate .venv to reset.
