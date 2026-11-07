# P2 — Operability

Unify auth, keep the Desk worker visible/alive, align Desk UI with Terminal, and add light observability.

## Unified auth

| Path | Behavior |
|------|----------|
| Terminal `/desk` or `/tools/analyst-desk` | If logged in → signed SSO redirect to Desk `/auth/bridge` |
| Desk login | Terminal **username + password**, or shared `ANALYST_DESK_ADMIN_PASSWORD` |
| Desk open mode | If desk password unset → open (dev only) |

Env:

| Variable | Purpose |
|----------|---------|
| `ANALYST_DESK_PUBLIC_URL` | Desk base URL (default `http://127.0.0.1:5005`) |
| `TERMINAL_PUBLIC_URL` | Terminal base URL (default `http://127.0.0.1:5003`) |
| `ANALYST_DESK_BRIDGE_SECRET` | Optional SSO signing secret (defaults to `SECRET_KEY`) |
| `ANALYST_DESK_ADMIN_PASSWORD` | Shared desk password (still recommended in prod) |

SSO tokens expire in **5 minutes**.

## Always-on worker

```bash
python run_analyst_desk_worker.py
# or helper:
bash scripts/run_desk_worker.sh
```

Heartbeats write to `data/platform_ops_status.json` and show on:

- Terminal **Ops health** (`/ops`)
- Desk **Operations**
- Desk **`/health`** JSON

### launchd (macOS sketch)

`~/Library/LaunchAgents/com.geopolitiko.desk-worker.plist` — `ProgramArguments` → `python` + `run_analyst_desk_worker.py`, `WorkingDirectory` = repo, `KeepAlive` true, log to `logs/`.

### systemd (Linux sketch)

```ini
[Service]
WorkingDirectory=/path/to/GeopoliticalNews
ExecStart=/path/to/venv/bin/python run_analyst_desk_worker.py
Restart=always
RestartSec=10
```

## Design system

Desk `templates/analyst_desk/base.html` now uses the same soft-blue tokens as Terminal (`#6ea8ff`, Plus Jakarta Sans, calm surfaces).

## Observability

- `X-Request-ID` on Terminal + Desk responses
- Request latency logs: `service=terminal|analyst_desk …`
- Health: Terminal `/api/health`, Desk `/health`

## Local stack

```bash
# Terminal
python run.py
# Desk UI
python run_analyst_desk.py
# Desk worker
python run_analyst_desk_worker.py
```

Or: `bash scripts/run_local_stack.sh` (starts Desk UI + worker in background; run Terminal separately or via script).
