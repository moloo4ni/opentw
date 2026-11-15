CREATE TABLE IF NOT EXISTS models (
    model_id      TEXT PRIMARY KEY,
    family        TEXT NOT NULL,
    last_status   TEXT,
    error_type    TEXT,
    error_message TEXT,
    last_check_at TEXT
);

CREATE TABLE IF NOT EXISTS checks (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    model_id      TEXT NOT NULL,
    ts            TEXT NOT NULL,
    http_status   INTEGER NOT NULL DEFAULT 0,
    status        TEXT NOT NULL,
    latency_us    INTEGER,
    ttft_us       INTEGER,
    gen_us        INTEGER,
    tps           REAL,
    input_tokens  INTEGER,
    output_tokens INTEGER,
    error_type    TEXT,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_checks_model_ts ON checks (model_id, ts DESC);

CREATE TABLE IF NOT EXISTS subscriptions (
    chat_id    INTEGER NOT NULL,
    model_id   TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (chat_id, model_id)
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_model ON subscriptions (model_id);

CREATE TABLE IF NOT EXISTS status_messages (
    chat_id    INTEGER PRIMARY KEY,
    message_id INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS incidents (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    model_id   TEXT NOT NULL,
    from_status TEXT NOT NULL,
    to_status  TEXT,
    started_at TEXT NOT NULL,
    ended_at   TEXT
);

CREATE INDEX IF NOT EXISTS idx_incidents_model ON incidents (model_id, id DESC);
