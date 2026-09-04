# twscrape upstream baseline

_The upstream source snapshot used as a reference for the independent XGraph implementation_

---

## 📋 Baseline

| Field | Value |
| --- | --- |
| Upstream repository | `https://github.com/vladkens/twscrape` |
| Upstream branch | `main` |
| Baseline commit | `55ac729f39fbbe46e316746a55627a9ed920112c` |
| Nearest release | `v0.20.1` |
| Git description | `v0.20.1-1-g55ac729` |
| Independent repository using this snapshot | `https://github.com/Showiix/XGraph` |
| Recorded on | `2026-09-03` |

The original README and changelog are preserved in this directory. Update this file only when XGraph intentionally adopts a new upstream baseline.

## Reuse ledger

| Upstream asset | XGraph destination | Treatment | Scope |
| --- | --- | --- | --- |
| `twscrape/account.py` | `xgraph/accounts/models.py`, `xgraph/collector/client.py` | Adapt, do not import | Cookie requirements, credential fields, request-header conventions, and the per-account User-Agent seed (`credential_seed`, upstream seeds from the username) |
| `twscrape/api.py` | `xgraph/collector/operations.py`, `client.py` | Adapt | Required GraphQL operation paths, variables and features only |
| `twscrape/http.py` | `xgraph/collector/http.py` | Ported near-verbatim | Dual httpx / curl-cffi backends, browser-family resolution, unified `Response`, transport error taxonomy. Adaptations: `XGRAPH_HTTP_BACKEND` env var, loguru logger, and curl preferred by default (see the technical design for the measurement) |
| `twscrape/xclid.py` | `xgraph/collector/xclid.py` | Copied and adapted | Dynamic `x-client-transaction-id` algorithm; uses XGraph HTTP boundary and account User-Agent |
| `twscrape/queue_client.py` | `xgraph/collector/errors.py`, future account manager | Re-implemented from behavior | Rate-limit headers and error-code classification; no SQLite queue state |
| `twscrape/utils.py` | `xgraph/collector/parser.py` | Re-implemented from behavior | Nested User/Tweet traversal, normalization and cursor extraction |
| `twscrape/models.py` | `xgraph/domain/models.py`, `xgraph/collector/parser.py` | New XGraph models | Product fields only; no SNScrape model or upstream runtime dependency |
| `tests/mocked-data/raw_*.json` | `tests/xgraph/test_collector.py` | Reused as fixtures | Regression samples; no credential-bearing fixtures |

All adapted code remains under the upstream MIT license. `twscrape/` is kept unchanged and is not imported by XGraph runtime modules.

## Keeping the copies current

`make update` regenerates `twscrape/api.py` only — `scripts/update-gql-ops.py` hard-codes `API_FILE = "twscrape/api.py"`. The GraphQL operation ids and feature flags copied into `xgraph/collector/operations.py` are therefore **not** refreshed by it, and X rotates operation ids every few weeks.

`tests/xgraph/test_protocol_drift.py` compares the two copies so a rotation surfaces as a failing test instead of a production outage. The manual step after every `make update`:

```bash
make update                                  # refreshes the upstream snapshot
uv run pytest tests/xgraph/test_protocol_drift.py   # fails if XGraph is now stale
# copy the reported upstream values into xgraph/collector/operations.py
make test
```

Any further protocol constant copied out of `twscrape/` must be added to that guard in the same change; otherwise the copy has no mechanism keeping it correct.
