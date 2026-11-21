-- OpenTW — единственная актуальная начальная схема базы.
-- Применяется целиком при создании новой базы (storage.Open); маркер
-- PRAGMA user_version = 1 ставится в той же транзакции. Исторических
-- миграций нет: старые/чужие базы отвергаются при старте понятной ошибкой.

CREATE TABLE models (
    model_id      TEXT PRIMARY KEY,
    family        TEXT NOT NULL,
    last_status   TEXT,
    error_type    TEXT,
    error_message TEXT,
    last_check_at TEXT
);

CREATE TABLE checks (
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
    error_message TEXT,
    response      TEXT NOT NULL DEFAULT ''
);

CREATE INDEX idx_checks_model_ts ON checks (model_id, ts DESC);

CREATE TABLE incidents (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    model_id    TEXT NOT NULL,
    from_status TEXT NOT NULL,
    to_status   TEXT,
    started_at  TEXT NOT NULL,
    ended_at    TEXT
);

CREATE INDEX idx_incidents_model ON incidents (model_id, id DESC);

CREATE TABLE subscriptions (
    chat_id    INTEGER NOT NULL,
    model_id   TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (chat_id, model_id)
);

CREATE INDEX idx_subscriptions_model ON subscriptions (model_id);

CREATE TABLE status_messages (
    chat_id    INTEGER PRIMARY KEY,
    message_id INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

-- Outbox-доставка: события переживают рестарт и сбои Telegram.
CREATE TABLE notification_outbox (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id         INTEGER NOT NULL,
    kind            TEXT NOT NULL,
    payload         TEXT NOT NULL,
    dedupe_key      TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    sent_at         TEXT,
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT,
    UNIQUE (chat_id, dedupe_key)
);

CREATE INDEX idx_outbox_pending ON notification_outbox (id) WHERE sent_at IS NULL;

-- Оффсет long polling: не перечитывать уже обработанные апдейты после рестарта.
CREATE TABLE bot_state (
    id      INTEGER PRIMARY KEY CHECK (id = 1),
    offset  INTEGER NOT NULL
);

-- Последнее состояние, о котором чат был реально уведомлён (основа
-- доставки только актуальных изменений).
CREATE TABLE chat_notified (
  chat_id    INTEGER NOT NULL,
  model_id   TEXT    NOT NULL,
  status     TEXT    NOT NULL,
  updated_at TEXT    NOT NULL,
  PRIMARY KEY (chat_id, model_id)
);
