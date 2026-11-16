-- Уважение retry_after на уровне очереди: строка с исчерпанными 429-ретраями
-- не перевзвешивается раньше next_attempt_at, иначе очередь молотит флуд-лимит
-- бота бессмысленными запросами (P1-4 outbox-аудита).
ALTER TABLE notification_outbox ADD COLUMN next_attempt_at TEXT;
