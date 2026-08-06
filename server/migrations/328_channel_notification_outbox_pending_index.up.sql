-- Single-statement migration: CREATE INDEX CONCURRENTLY cannot run inside a
-- transaction block or a multi-command string, and the migration runner sends
-- each file as one implicit transaction (repo convention; see migrations 143,
-- 228/229, 257). Split out of 319 so index builds never take a write-blocking
-- lock on populated binding tables during upgrades.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_channel_notification_outbox_pending
    ON channel_notification_outbox (next_attempt_at, created_at)
    WHERE status = 'pending';
