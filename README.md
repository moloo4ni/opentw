# OpenTW

A Telegram bot for monitoring model availability and response diagnostics
through OpenAI-compatible APIs, with Tonworld as its current reference
provider.

One process: a centralized monitor probes the models on a schedule, stores the
results in SQLite, keeps a pinned status panel in every chat, and delivers
event notifications to subscribers.

Русская версия: [README.ru.md](README.ru.md).

## Features

- Pinned Telegram status panel with all monitored models, refreshed in place
  after every cycle — no new messages.
- Per-model cards (`/status model-id`) with the last check, the last request,
  the verbatim model/API response, and 24-hour statistics (uptime, counters).
- Model availability monitoring on a schedule (15 minutes by default), one
  short probe request per model: `Reply with exactly "OK"`.
- A three-state status model: Operational, Down, Unknown.
- Strict exact-`OK` verification: only a reply that is exactly `OK` after
  trimming outer whitespace counts as success; reasoning output never counts.
- Response diagnostics: the verbatim provider text (content, gateway errors,
  finish reason) is stored in the check history and shown on the card.
- Subscription management with bulk select/deselect, admin-gated in groups
  and channels.
- Current-state notifications only: stale events are collapsed against the
  notified state before sending.
- Per-chat FIFO outbox with a durable SQLite queue.
- At-least-once delivery that survives Telegram outages and restarts.
- SQLite persistence with backup support (`-backup` flag, `VACUUM INTO`).
- A heartbeat file and a container `HEALTHCHECK` wired to monitoring
  freshness.

## Architecture

```
OpenAI-compatible API
        |
        v
     Monitor           (internal/monitor: cycles, concurrency, incidents)
        |
        v
  SQLite storage       (internal/storage: single schema.sql, outbox, chat_notified)
        |
        v
  Telegram bot         (internal/telegram: Bot API client, rendering, delivery worker)
        |
        v
Rich status panel and notifications
```

- `internal/api` — the OpenAI-compatible adapter: model catalog, one chat
  completion (streaming or not), SSE parsing, transport-failure
  classification.
- `internal/monitor` — cycle scheduling, bounded-concurrency probing, status
  classification, incident planning; commits states, events and incidents in
  one atomic transaction.
- `internal/storage` — SQLite: the single current schema (`schema.sql`,
  `PRAGMA user_version`), checks, subscriptions, panels, incidents, the
  notification outbox, and per-chat notified states.
- `internal/telegram` — a minimal Bot API client, panel/card/notification
  rendering, command and callback handlers, and the background delivery
  worker.
- `internal/config` — YAML + environment loading and validation.
- `internal/status` — the shared three-state status type.

## Requirements

- Go toolchain (see `go.mod`) for building from source.
- Podman for building the container image.
- A Telegram bot token from [@BotFather](https://t.me/BotFather).
- An API key and an OpenAI-compatible API base URL.
- Quadlet and systemd for the recommended container deployment on Linux.

## Quick Start

Run locally with Go:

```bash
cp .env.example .env                # fill BOT_TOKEN, API_KEY, API_BASE_URL
cp config.example.yaml config.yaml
# The example's sqlite_path is /data/opentw.db — a path inside the container.
# For a local run point it to a local file, e.g. ./opentw.db.
set -a && . ./.env && set +a
go run ./cmd/opentw -config config.yaml
```

Build the container image with Podman:

```bash
podman build --network=host --format docker -t localhost/opentw:latest .
```

- `--format docker` keeps the Docker-compatible `HEALTHCHECK` metadata in the
  image; Podman discards it when building the default OCI format, and the
  healthcheck metadata is what Podman consumes at run time.
- `--network=host` may be needed for the `go mod download` step in some build
  environments (a Podman/netavark quirk); it is not mandatory everywhere. If
  your build environment has working networking, a plain
  `podman build --format docker -t localhost/opentw:latest .` is enough.

There is no second deployment mechanism: the container is meant to run under
Podman with a Quadlet unit (see [Container deployment](#container-deployment)).

## Configuration

Copy `config.example.yaml` and adjust; secrets live only in the environment
(`.env.example` lists them):

| Variable | Purpose |
|---|---|
| `BOT_TOKEN` | Telegram bot token (required) |
| `API_KEY` | OpenAI-compatible API key (required) |
| `API_BASE_URL` | API base URL; overrides `api_base_url` from the YAML |

Configuration keys (all validated at startup; invalid configuration refuses
to start rather than degrading silently):

| Key | Default | Meaning |
|---|---|---|
| `api_base_url` | — | API base URL (`API_BASE_URL` env wins when set) |
| `interval` | `15m` | Monitoring interval; ≥ 5m |
| `http_timeout` | `120s` | Per-request timeout; ≥ 5s |
| `concurrency` | `4` | Maximum simultaneous model checks; ≥ 1 |
| `max_tokens` | `256` | Output token limit of the probe request; ≥ 1 |
| `streaming` | `true` | Use streaming: measures TTFT/TPS (stored in history); the OK check is unaffected |
| `sqlite_path` | `opentw.db` | SQLite database path |
| `models` | — | Model families with exact model IDs |

The success criterion is content-based, so `max_tokens` must be large enough
for reasoning models to finish and emit the final answer; 256 is the shipped
default.

## Telegram setup

1. Create a bot with [@BotFather](https://t.me/BotFather) → `/newbot`; note
   the token.
2. The bot registers its command menu itself at startup (`setMyCommands`).
3. In groups the bot works in the default Privacy Mode and sees only commands
   addressed to it (`/status@your_bot`); it does not read the conversation.
4. In channels add the bot as an administrator with the rights to post,
   pin, and delete messages (the pinned panel needs posting + pinning;
   menu deletion needs deletion).

## Commands

| Command | Behavior |
|---|---|
| `/start` | Same as `/status` |
| `/status` | The status panel of all models; created and pinned |
| `/status <model-id>` | The model card: status, last request, verbatim response, 24-hour stats (a regular message, not the panel); a unique ID prefix completes automatically, an ambiguous one lists candidates |
| `/subscribe` | Notification setup (admins only in groups and channels) |
| `/help` | Help |

A repeated `/status` creates a new panel, pins it, and unpins the previous
one; later updates edit the new message.

Clicking an error label in the panel or a notification (e.g. `HTTP 429`)
opens the model card as a separate message in the same chat; the panel itself
is not edited.

## Status semantics

| Emoji | Status | Condition |
|---|---|---|
| 🟢 | Operational | The reply to the probe is exactly `OK` after trimming outer whitespace; latency does not matter |
| 🔴 | Down | Anything else from the model's side: HTTP 200 with other content (an error, a rate limit, an empty generation, reasoning without a final answer), HTTP 429/404/5xx, timeout, connection refused for this model |
| ⚪ | Unknown | The monitor could not verify definitively: 401/403 (the monitor's own key), an unparseable response, other monitor-side situations |

The verdict follows the content, not the HTTP code: HTTP 200 with an error
message or an empty generation is Down, not Operational. The verbatim
original response is stored in the history and shown on the card.

TTFT, TPS, and token counts are still measured (with `streaming: true`) and
kept in the check history, but are not shown in the panel or cards: only the
final answer matters for the status.

## Notifications and delivery guarantees

- Only chats subscribed to a model receive its events, and only on a state
  change (Operational/Down). The first cycle after startup only establishes
  the baseline and sends nothing.
- All of one cycle's events for a chat arrive as **one message**, one line per
  model in the config order, without headers or footers:
  - 🔴 `model-id` - `HTTP 429` — a clickable error label (an HTTP code,
    `Timeout`, `Connection refused`, `Unexpected response`) opens the model
    card;
  - 🟢 `model-id` - Восстановилась (recovered).
- Per-chat FIFO is strict: a newer "recovered" never overtakes an undelivered
  "down", in any tick and across restarts; chats do not block each other.
- The cycle's outcome — new model states plus events — is committed to SQLite
  in **one atomic transaction**: a restart, OOM, or database failure mid-cycle
  loses nothing; the next cycle reopens the same transitions (the event is
  delayed by up to one interval, not lost).
- Delivery runs in a background worker with a growing backoff (30s → 1m →
  2m … capped at 15m) on temporary errors; there is no drop after N attempts.
- A global Telegram 429 pauses the whole batch, and a row that exhausted its
  in-request retries is not retried before `retry_after`.
- Delivery is at-least-once: a failure exactly between the send and the
  commit can produce a rare duplicate; exactly-once is impossible (the Bot
  API has no idempotency key for `sendRichMessage`).
- Only current changes are sent: on delivery, each model's current status is
  reconciled against the chat's last notified state (`chat_notified`). A state
  the chat already knows is not re-sent; events that became stale during a
  downtime (the model fell and recovered) are collapsed into one line for the
  model's **current** state. An Unknown current state sends nothing and keeps
  the evidence rows pending. Rows older than 24h (TTL) are dropped as a
  safety valve, and every drop is logged. The same reconciliation also arms
  after a lost/expired row or a restart, so a notification cannot depend on
  the survival of a single queue row.

## SQLite and backups

All history, incidents, subscriptions and panel bindings live in one SQLite
file (`sqlite_path`, e.g. `/data/opentw.db` on the named volume).

- The schema is a single `internal/storage/schema.sql`. A fresh database is
  created from it in full and marked with `PRAGMA user_version`; subsequent
  startups touch nothing.
- There are **no automatic migrations**. A legacy database (created by older
  builds) is rejected at startup with a clear error
  (`storage: database ... is not compatible ... start with a clean database`);
  the app never deletes or mutates it. Replacing a database (removing
  `opentw.db` together with its `-wal`/`-shm` sidecars after stopping the
  instance) is the operator's decision — make a backup first.
- Hot backup without stopping the bot (`opentw` is the container name from
  your Quadlet unit):

  ```bash
  podman exec opentw /app/opentw -config /app/config.yaml -backup /data/backup.db
  podman cp opentw:/data/backup.db ./opentw-$(date +%F).db   # copy to the host
  ```

  The snapshot is written with `VACUUM INTO`: consistent even against a live
  WAL database; writing into an existing file is refused.
- Cold path: stop the container (`systemctl stop opentw`), copy
  `/data/opentw.db` (WAL is checkpointed on graceful shutdown), start again.
- Restoring into the named volume (do not copy old `-wal`/`-shm` files):

  ```bash
  systemctl stop opentw
  podman run --rm -v opentw-data:/data -v "$PWD:/host" alpine \
      sh -c 'rm -f /data/opentw.db-* && cp /host/opentw-restore.db /data/opentw.db'
  systemctl start opentw
  ```

  Replace `opentw-restore.db` with your backup (made by the same opentw
  version) and `opentw-data` with your volume name. After the restart check
  `/status` and `podman logs`.

Never copy a live `opentw.db` without `-wal` (you would lose the last hours
of writes), and never `podman volume rm` / `podman volume prune` — they
destroy the volume with all data. One instance per database: two processes on
one file deliver every notification twice.

Retention: checks 7 days, incidents 30 days, delivered outbox rows 7 days,
undelivered rows 24 hours (dropped with a log line).

## Container deployment

1. Build the image (see [Quick Start](#quick-start)).
2. Prepare the configuration on the host: a `config.yaml` (from
   `config.example.yaml`) and an environment file with the secrets.
3. Write a Quadlet `.container` unit. The repository intentionally ships no
   unit file — the exact values depend on your volume names, paths, and
   network setup, so the operator authors the unit. The verified principles:

   - run the image with the config mounted read-only at `/app/config.yaml`
     and the secrets via `EnvironmentFile=`;
   - attach a **named volume** to `/data` (the SQLite database and heartbeat
     files live there; the volume inherits the image's `/data` owner on first
     initialization);
   - rely on systemd for restart policy and on journald for logs
     (`journalctl -u opentw.service`);
   - if your model count / concurrency / timeout is much larger than the
     shipped example, set your own `HealthStartPeriod=` covering
     `(N/concurrency + 1) × http_timeout` waves — the image's default covers
     only the shipped config.

   See `podman-systemd.unit(5)` and `podman-container.unit(5)` for the unit
   format.
4. `systemctl --user daemon-reload && systemctl --user start opentw.service`
   (or the system equivalents).

Updating: rebuild the image with the same tag, then restart the unit —
systemd starts the new image; the SQLite data survives in the named volume.
Updating in place must not touch the volume.

## Diagnostics

- Responses are capped at 16 384 runes with a visible `…`; truncation is
  always visible, never silent.
- `finish_reason` of the terminal chunk is recorded in the check's
  diagnostics when the model produced no final text (e.g.
  `finish_reason=length`).
- An empty final text with HTTP 200 is shown as "no final text" on the card —
  a fact, not an interpretation.
- Latency/TPS limitations: a buffered provider sends chunks almost at once,
  so TPS is only recorded when the generation time is meaningful; TTFT/TPS
  are stored but never displayed.
- Common symptoms:

| Symptom | Cause and action |
|---|---|
| `BOT_TOKEN is required` | `.env` missing or variables not exported |
| `telegram: fatal API error 401` | Wrong bot token — reissue via BotFather |
| ⚪ Unknown in the panel | Check the API key and `API_BASE_URL`; `curl $API_BASE_URL/v1/models` with the key must return JSON |
| All models Down with a provider error | Config IDs do not match the catalog — compare with `GET /v1/models`; clicking `HTTP 400` opens the card with the verbatim answer |
| Panel not pinned | The bot is not an admin or lacks the pin right |
| `HTTP 429` in the panel | The upstream provider's rate limit, not the monitor's key |
| `podman ps` shows `unhealthy` | The heartbeat (`/data/opentw.heartbeat`) is older than the threshold in `/data/opentw.heartbeat.maxage` (written by the app from the actual `interval`; the 2820s fallback matches the example config). Failed cycles do not refresh the heartbeat: see `podman logs` |
| `storage: database ... is not compatible` | A database from an older version — start clean: stop the instance and remove `opentw.db` with its `-wal`/`-shm` (only if the history is not needed) |
| `monitor: cycle failed` in logs, everything else works | A cycle-commit failure (usually disk/DB): cycles continue, states are not recorded until it recovers; events are not lost |
| All models ⚪ Unknown | A matching catalog+models failure (key/transport) — the event is "the monitor is blind", not "everything fell"; the panels recover on their own |

## Development

```bash
gofmt -l .
go build ./...
go vet ./...
go test ./... -count=1
go test -race ./... -count=1
```

The suite covers response classification (exact-`OK` and everything else,
streaming and non-streaming, 429/404/5xx, timeouts, connection failures, the
monitor's own key errors), panel/card/notification rendering, delivery of
only current states, fresh-schema creation and legacy-database rejection,
incident lifecycle and blind-window transitions, Telegram outages and 429
recovery from the outbox, subscription bulk operations, stale callbacks, and
persistence across restarts. API and Telegram are mocked with local HTTP
servers; the tests make no network calls.

Live tests against the real API are opt-in and require credentials; they are
never run in CI:

```bash
LIVE=1 API_KEY=... API_BASE_URL=https://... go test -tags live ./internal/monitor -run TestLiveCycle -v
```

## Limitations

- OpenAI-compatible support does not mean compatibility with every
  non-standard dialect; the reference provider's behavior (SSE framing,
  gateway errors inside HTTP 200 bodies, reasoning-only replies at small
  `max_tokens`) is what the implementation and tests are calibrated against.
- Provider differences in streaming and reasoning handling (buffered chunks,
  usage-only terminal chunks) are handled defensively, but exotic providers
  are untested.
- Telegram Rich Messages (`sendRichMessage`, `editMessageText` with
  `rich_message`, `tg-button` callbacks) were verified against local Bot API
  mocks only; behavior on clients without Rich Messages support is not
  documented by Telegram and needs a live check.
- No automatic database migrations; a schema change means a fresh database.
- The monitor probes an explicit model list from the config; it does not walk
  the whole catalog (thousands of entries would not fit the panel limit and
  would burn the quota). A model missing from the catalog is still probed for
  a real verdict.

## License

[MPL-2.0](LICENSE).
