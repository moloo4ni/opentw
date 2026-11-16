-- Outbox-доставка: события переживают рестарт и сбои Telegram.
CREATE TABLE IF NOT EXISTS notification_outbox (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id     INTEGER NOT NULL,
    kind        TEXT NOT NULL,
    payload     TEXT NOT NULL,
    dedupe_key  TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    sent_at     TEXT,
    attempts    INTEGER NOT NULL DEFAULT 0,
    UNIQUE (chat_id, dedupe_key)
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending ON notification_outbox (id) WHERE sent_at IS NULL;

-- Оффсет long polling: не перечитывать уже обработанные апдейты после рестарта.
CREATE TABLE IF NOT EXISTS bot_state (
    id      INTEGER PRIMARY KEY CHECK (id = 1),
    offset  INTEGER NOT NULL
);
