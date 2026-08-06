-- Single-statement migration: CREATE INDEX CONCURRENTLY cannot run inside a
-- transaction block or a multi-command string, and the migration runner sends
-- each file as one implicit transaction (repo convention; see migrations 143,
-- 228/229, 257). Split out of 259 so index builds never take a write-blocking
-- lock on populated binding tables during upgrades.
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_channel_issue_topic_route
    ON channel_issue_topic_binding (installation_id, channel_chat_id, topic_root_message_id)
    WHERE state = 'active';
