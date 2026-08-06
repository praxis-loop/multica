-- Lark project / issue-topic sync compatibility migration.
--
-- This migration reconciles the channel_project_binding /
-- channel_issue_topic_binding / channel_notification_outbox tables onto the
-- shape the upstream-based lark project/topic sync implementation expects. It
-- must be safe on three kinds of database:
--   * a fresh install (tables do not exist yet),
--   * this deployment's production DB (tables already carry the full target
--     shape from the discarded fork migrations), and
--   * other self-hosted DBs that ran an earlier/partial fork rollout and may
--     still hold legacy rows (e.g. channel_issue_topic_binding.installation_id
--     IS NULL for direct-topic bindings created before the column existed).
--
-- Everything here is transaction-safe: table/column/constraint DDL, idempotent
-- data backfill, and trigger functions. Index construction is deliberately kept
-- OUT of this file and split into per-index CREATE INDEX CONCURRENTLY migrations
-- (399+), because the migration runner sends each file as a single implicit
-- transaction and CONCURRENTLY cannot run inside a transaction block. Building
-- the indexes concurrently also avoids taking a write-blocking lock on
-- populated binding tables during upgrades of larger self-hosted instances.

CREATE TABLE IF NOT EXISTS channel_project_binding (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL,
    project_id UUID NOT NULL,
    installation_id UUID NOT NULL,
    channel_type TEXT NOT NULL DEFAULT 'feishu',
    channel_chat_id TEXT,
    channel_chat_name TEXT,
    state TEXT NOT NULL DEFAULT 'pending_group',
    bind_token_hash TEXT,
    bind_token_expires_at TIMESTAMPTZ,
    created_by_user_id UUID NOT NULL,
    bound_by_user_id UUID,
    unbound_by_user_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    bound_at TIMESTAMPTZ,
    unbound_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS channel_issue_topic_binding (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL,
    installation_id UUID,
    project_binding_id UUID,
    project_id UUID,
    issue_id UUID NOT NULL,
    channel_chat_id TEXT NOT NULL,
    topic_root_message_id TEXT NOT NULL,
    channel_thread_id TEXT,
    binding_source TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'active',
    created_by_user_id UUID,
    unbound_by_user_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    unbound_at TIMESTAMPTZ,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS channel_notification_outbox (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL,
    workspace_id UUID NOT NULL,
    project_id UUID,
    project_binding_id UUID,
    issue_topic_binding_id UUID,
    issue_id UUID NOT NULL,
    task_id UUID,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_at TIMESTAMPTZ,
    locked_by TEXT,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at TIMESTAMPTZ,
    event_order BIGINT GENERATED ALWAYS AS IDENTITY,
    PRIMARY KEY (id)
);

ALTER TABLE channel_project_binding
    ADD COLUMN IF NOT EXISTS id UUID NOT NULL DEFAULT gen_random_uuid(),
    ADD COLUMN IF NOT EXISTS workspace_id UUID,
    ADD COLUMN IF NOT EXISTS project_id UUID,
    ADD COLUMN IF NOT EXISTS installation_id UUID,
    ADD COLUMN IF NOT EXISTS channel_type TEXT NOT NULL DEFAULT 'feishu',
    ADD COLUMN IF NOT EXISTS channel_chat_id TEXT,
    ADD COLUMN IF NOT EXISTS channel_chat_name TEXT,
    ADD COLUMN IF NOT EXISTS state TEXT NOT NULL DEFAULT 'pending_group',
    ADD COLUMN IF NOT EXISTS bind_token_hash TEXT,
    ADD COLUMN IF NOT EXISTS bind_token_expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS created_by_user_id UUID,
    ADD COLUMN IF NOT EXISTS bound_by_user_id UUID,
    ADD COLUMN IF NOT EXISTS unbound_by_user_id UUID,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS bound_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS unbound_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE channel_issue_topic_binding
    ADD COLUMN IF NOT EXISTS installation_id UUID,
    ADD COLUMN IF NOT EXISTS project_binding_id UUID,
    ADD COLUMN IF NOT EXISTS project_id UUID;

ALTER TABLE channel_notification_outbox
    ADD COLUMN IF NOT EXISTS project_id UUID,
    ADD COLUMN IF NOT EXISTS project_binding_id UUID,
    ADD COLUMN IF NOT EXISTS issue_topic_binding_id UUID,
    ADD COLUMN IF NOT EXISTS event_order BIGINT GENERATED ALWAYS AS IDENTITY;

-- Legacy backfill, step 1: topic bindings that carry a project_binding_id can
-- inherit the installation from their parent project binding.
UPDATE channel_issue_topic_binding citb
SET installation_id = cpb.installation_id
FROM channel_project_binding cpb
WHERE citb.installation_id IS NULL
  AND cpb.id = citb.project_binding_id
  AND cpb.workspace_id = citb.workspace_id;

-- Legacy backfill, step 2: direct topic bindings (no project_binding_id) created
-- before installation_id existed. Recover the installation only when the topic's
-- chat maps to exactly one installation via a project binding on that same chat,
-- so we never guess an installation for an ambiguous chat. Rows whose chat is
-- unmapped or ambiguous are intentionally left NULL and orphaned below.
UPDATE channel_issue_topic_binding citb
SET installation_id = chat_map.installation_id
FROM (
    -- Cast to text for min(): Postgres has no min(uuid) aggregate. The
    -- HAVING clause guarantees a single distinct installation per chat, so
    -- min() just returns that one value.
    SELECT workspace_id, channel_chat_id, min(installation_id::text)::uuid AS installation_id
    FROM channel_project_binding
    WHERE channel_chat_id IS NOT NULL
      AND state IN ('pending_group', 'active', 'unbound', 'bot_revoked', 'bot_removed')
    GROUP BY workspace_id, channel_chat_id
    HAVING count(DISTINCT installation_id) = 1
) AS chat_map
WHERE citb.installation_id IS NULL
  AND citb.project_binding_id IS NULL
  AND citb.workspace_id = chat_map.workspace_id
  AND citb.channel_chat_id = chat_map.channel_chat_id;

-- Legacy compatibility: any still-unresolved installation cannot back an active
-- route, so retire just those rows to 'orphaned' instead of failing the whole
-- migration. Only 'active' rows are touched — manual_unbound / project_unbound /
-- bot_revoked / replaced / already-orphaned rows are left exactly as they are, so
-- this never resurrects or rewrites a deliberately-ended binding and is safe to
-- re-run. Orphaned rows are excluded from every active-route unique index, so
-- they neither block nor duplicate live sync.
UPDATE channel_issue_topic_binding
SET state = 'orphaned',
    unbound_at = COALESCE(unbound_at, now()),
    updated_at = now()
WHERE installation_id IS NULL
  AND state = 'active';

ALTER TABLE channel_project_binding
    ALTER COLUMN workspace_id SET NOT NULL,
    ALTER COLUMN project_id SET NOT NULL,
    ALTER COLUMN installation_id SET NOT NULL,
    ALTER COLUMN created_by_user_id SET NOT NULL,
    DROP CONSTRAINT IF EXISTS channel_project_binding_state_check,
    ADD CONSTRAINT channel_project_binding_state_check
        CHECK (state IN ('pending_group', 'active', 'unbound', 'bot_revoked', 'bot_removed')),
    DROP CONSTRAINT IF EXISTS channel_project_binding_active_requires_chat,
    ADD CONSTRAINT channel_project_binding_active_requires_chat
        CHECK (state <> 'active' OR (channel_chat_id IS NOT NULL AND channel_chat_name IS NOT NULL));

ALTER TABLE channel_issue_topic_binding
    ALTER COLUMN project_binding_id DROP NOT NULL,
    ALTER COLUMN project_id DROP NOT NULL,
    DROP CONSTRAINT IF EXISTS channel_issue_topic_binding_binding_source_check,
    ADD CONSTRAINT channel_issue_topic_binding_binding_source_check
        CHECK (binding_source IN ('issue_created_by_multica', 'issue_created_in_topic', 'manual_topic_bind', 'project_backfill')),
    DROP CONSTRAINT IF EXISTS channel_issue_topic_binding_state_check,
    ADD CONSTRAINT channel_issue_topic_binding_state_check
        CHECK (state IN ('active', 'manual_unbound', 'project_unbound', 'orphaned', 'replaced', 'bot_revoked')),
    -- installation_id is intentionally NOT forced NOT NULL here: unresolved legacy
    -- rows retired to 'orphaned' above may still be NULL, and dropping them is not
    -- allowed. This CHECK instead guarantees the invariant that actually matters —
    -- every ACTIVE route has an installation — while letting orphaned history keep
    -- a NULL installation. New/active rows are always written with an installation
    -- by the application.
    DROP CONSTRAINT IF EXISTS channel_issue_topic_binding_active_requires_installation,
    ADD CONSTRAINT channel_issue_topic_binding_active_requires_installation
        CHECK (state <> 'active' OR installation_id IS NOT NULL);

-- When (and only when) no unresolved legacy rows remain — e.g. fresh installs and
-- this deployment's production DB, where installation_id is already fully
-- populated — tighten the column to NOT NULL so it matches the canonical schema.
-- Databases that still carry orphaned NULL rows keep the column nullable and rely
-- on the active-route CHECK above; this is a no-op when already NOT NULL.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM channel_issue_topic_binding WHERE installation_id IS NULL
    ) THEN
        ALTER TABLE channel_issue_topic_binding
            ALTER COLUMN installation_id SET NOT NULL;
    END IF;
END;
$$;

ALTER TABLE channel_notification_outbox
    ALTER COLUMN project_id DROP NOT NULL,
    DROP CONSTRAINT IF EXISTS channel_notification_outbox_status_check,
    ADD CONSTRAINT channel_notification_outbox_status_check
        CHECK (status IN ('pending', 'sending', 'sent', 'dead')),
    DROP CONSTRAINT IF EXISTS channel_notification_outbox_attempts_check,
    ADD CONSTRAINT channel_notification_outbox_attempts_check
        CHECK (attempts >= 0),
    DROP CONSTRAINT IF EXISTS channel_notification_outbox_route_check,
    ADD CONSTRAINT channel_notification_outbox_route_check
        CHECK (project_binding_id IS NOT NULL OR issue_topic_binding_id IS NOT NULL),
    DROP CONSTRAINT IF EXISTS channel_notification_outbox_event_type_check,
    ADD CONSTRAINT channel_notification_outbox_event_type_check
        CHECK (event_type IN (
            'issue_created',
            'issue_status_changed',
            'comment_created',
            'comment_updated',
            'task_started',
            'task_completed',
            'completed',
            'task_result',
            'task_failed',
            'task_cancelled',
            'assignee_changed',
            'priority_changed',
            'blocked_reason_changed'
        ));

CREATE OR REPLACE FUNCTION enqueue_channel_issue_notification()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    target_project_binding_id UUID;
    target_topic_binding_id UUID;
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.project_id IS DISTINCT FROM OLD.project_id THEN
        UPDATE channel_issue_topic_binding
        SET state = 'project_unbound',
            unbound_at = now(),
            updated_at = now()
        WHERE workspace_id = NEW.workspace_id
          AND issue_id = NEW.id
          AND project_binding_id IS NOT NULL
          AND project_id IS DISTINCT FROM NEW.project_id
          AND state = 'active';
    END IF;

    SELECT id, project_binding_id
    INTO target_topic_binding_id, target_project_binding_id
    FROM channel_issue_topic_binding
    WHERE workspace_id = NEW.workspace_id
      AND issue_id = NEW.id
      AND state = 'active'
    LIMIT 1;

    IF (TG_OP = 'INSERT' OR NEW.project_id IS DISTINCT FROM OLD.project_id)
       AND target_topic_binding_id IS NULL
       AND NEW.project_id IS NOT NULL THEN
        SELECT id
        INTO target_project_binding_id
        FROM channel_project_binding
        WHERE workspace_id = NEW.workspace_id
          AND project_id = NEW.project_id
          AND state = 'active'
        LIMIT 1;

        IF target_project_binding_id IS NOT NULL THEN
            INSERT INTO channel_notification_outbox (
                event_id, workspace_id, project_id, project_binding_id,
                issue_topic_binding_id, issue_id, event_type, payload
            ) VALUES (
                gen_random_uuid(), NEW.workspace_id, NEW.project_id,
                target_project_binding_id, NULL, NEW.id, 'issue_created',
                jsonb_build_object(
                    'issue_id', NEW.id,
                    'number', NEW.number,
                    'title', NEW.title,
                    'status', NEW.status,
                    'assignee_type', NEW.assignee_type,
                    'assignee_id', NEW.assignee_id,
                    'creator_type', NEW.creator_type,
                    'creator_id', NEW.creator_id,
                    'occurred_at', now()
                )
            );
        END IF;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        IF target_topic_binding_id IS NULL AND NEW.project_id IS NOT NULL THEN
            SELECT id
            INTO target_project_binding_id
            FROM channel_project_binding
            WHERE workspace_id = NEW.workspace_id
              AND project_id = NEW.project_id
              AND state = 'active'
            LIMIT 1;
        END IF;

        IF target_topic_binding_id IS NOT NULL OR target_project_binding_id IS NOT NULL THEN
            IF NEW.status IS DISTINCT FROM OLD.status THEN
                INSERT INTO channel_notification_outbox (
                    event_id, workspace_id, project_id, project_binding_id,
                    issue_topic_binding_id, issue_id, event_type, payload
                ) VALUES (
                    gen_random_uuid(), NEW.workspace_id, NEW.project_id,
                    target_project_binding_id, target_topic_binding_id, NEW.id,
                    'issue_status_changed',
                    jsonb_build_object(
                        'issue_id', NEW.id,
                        'number', NEW.number,
                        'title', NEW.title,
                        'previous_status', OLD.status,
                        'status', NEW.status,
                        'occurred_at', now()
                    )
                );
            END IF;

            IF NEW.assignee_type IS DISTINCT FROM OLD.assignee_type
               OR NEW.assignee_id IS DISTINCT FROM OLD.assignee_id THEN
                INSERT INTO channel_notification_outbox (
                    event_id, workspace_id, project_id, project_binding_id,
                    issue_topic_binding_id, issue_id, event_type, payload
                ) VALUES (
                    gen_random_uuid(), NEW.workspace_id, NEW.project_id,
                    target_project_binding_id, target_topic_binding_id, NEW.id,
                    'assignee_changed',
                    jsonb_build_object(
                        'issue_id', NEW.id,
                        'number', NEW.number,
                        'title', NEW.title,
                        'previous_assignee_type', OLD.assignee_type,
                        'previous_assignee_id', OLD.assignee_id,
                        'assignee_type', NEW.assignee_type,
                        'assignee_id', NEW.assignee_id,
                        'occurred_at', now()
                    )
                );
            END IF;

            IF NEW.priority IS DISTINCT FROM OLD.priority THEN
                INSERT INTO channel_notification_outbox (
                    event_id, workspace_id, project_id, project_binding_id,
                    issue_topic_binding_id, issue_id, event_type, payload
                ) VALUES (
                    gen_random_uuid(), NEW.workspace_id, NEW.project_id,
                    target_project_binding_id, target_topic_binding_id, NEW.id,
                    'priority_changed',
                    jsonb_build_object(
                        'issue_id', NEW.id,
                        'number', NEW.number,
                        'title', NEW.title,
                        'previous_priority', OLD.priority,
                        'priority', NEW.priority,
                        'occurred_at', now()
                    )
                );
            END IF;

            IF (NEW.metadata -> 'blocked_reason') IS DISTINCT FROM
               (OLD.metadata -> 'blocked_reason') THEN
                INSERT INTO channel_notification_outbox (
                    event_id, workspace_id, project_id, project_binding_id,
                    issue_topic_binding_id, issue_id, event_type, payload
                ) VALUES (
                    gen_random_uuid(), NEW.workspace_id, NEW.project_id,
                    target_project_binding_id, target_topic_binding_id, NEW.id,
                    'blocked_reason_changed',
                    jsonb_build_object(
                        'issue_id', NEW.id,
                        'number', NEW.number,
                        'title', NEW.title,
                        'previous_blocked_reason', OLD.metadata -> 'blocked_reason',
                        'blocked_reason', NEW.metadata -> 'blocked_reason',
                        'occurred_at', now()
                    )
                );
            END IF;
        END IF;
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_channel_issue_created_notification ON issue;
CREATE TRIGGER trg_channel_issue_created_notification
AFTER INSERT ON issue
FOR EACH ROW
EXECUTE FUNCTION enqueue_channel_issue_notification();

DROP TRIGGER IF EXISTS trg_channel_issue_updated_notification ON issue;
CREATE TRIGGER trg_channel_issue_updated_notification
AFTER UPDATE OF status, project_id, assignee_type, assignee_id, priority, metadata ON issue
FOR EACH ROW
EXECUTE FUNCTION enqueue_channel_issue_notification();

CREATE OR REPLACE FUNCTION enqueue_channel_comment_notification()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    target_issue issue%ROWTYPE;
    target_project_binding_id UUID;
    target_topic_binding_id UUID;
    notification_type TEXT;
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.content IS NOT DISTINCT FROM OLD.content THEN
        RETURN NEW;
    END IF;

    SELECT scoped_issue.*
    INTO target_issue
    FROM issue AS scoped_issue
    WHERE scoped_issue.id = NEW.issue_id;

    IF target_issue.id IS NULL THEN
        RETURN NEW;
    END IF;

    SELECT id, project_binding_id
    INTO target_topic_binding_id, target_project_binding_id
    FROM channel_issue_topic_binding
    WHERE workspace_id = target_issue.workspace_id
      AND issue_id = target_issue.id
      AND state = 'active'
    LIMIT 1;

    IF target_topic_binding_id IS NULL AND target_issue.project_id IS NOT NULL THEN
        SELECT id
        INTO target_project_binding_id
        FROM channel_project_binding
        WHERE workspace_id = target_issue.workspace_id
          AND project_id = target_issue.project_id
          AND state = 'active'
        LIMIT 1;
    END IF;

    IF target_topic_binding_id IS NULL AND target_project_binding_id IS NULL THEN
        RETURN NEW;
    END IF;

    notification_type := CASE
        WHEN TG_OP = 'INSERT' THEN 'comment_created'
        ELSE 'comment_updated'
    END;

    INSERT INTO channel_notification_outbox (
        event_id, workspace_id, project_id, project_binding_id,
        issue_topic_binding_id, issue_id, event_type, payload
    ) VALUES (
        gen_random_uuid(), target_issue.workspace_id, target_issue.project_id,
        target_project_binding_id, target_topic_binding_id,
        target_issue.id, notification_type,
        jsonb_build_object(
            'issue_id', target_issue.id,
            'number', target_issue.number,
            'title', target_issue.title,
            'comment_id', NEW.id,
            'comment_type', NEW.type,
            'author_type', NEW.author_type,
            'author_id', NEW.author_id,
            'content', NEW.content,
            'previous_content', CASE WHEN TG_OP = 'UPDATE' THEN OLD.content ELSE NULL END,
            'occurred_at', now()
        )
    );

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_channel_comment_created_notification ON comment;
CREATE TRIGGER trg_channel_comment_created_notification
AFTER INSERT ON comment
FOR EACH ROW
EXECUTE FUNCTION enqueue_channel_comment_notification();

DROP TRIGGER IF EXISTS trg_channel_comment_updated_notification ON comment;
CREATE TRIGGER trg_channel_comment_updated_notification
AFTER UPDATE OF content ON comment
FOR EACH ROW
EXECUTE FUNCTION enqueue_channel_comment_notification();

CREATE OR REPLACE FUNCTION enqueue_channel_task_notification()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    target_issue issue%ROWTYPE;
    target_project_binding_id UUID;
    target_topic_binding_id UUID;
    notification_type TEXT;
BEGIN
    IF NEW.issue_id IS NULL THEN
        RETURN NEW;
    END IF;

    IF NEW.status IS NOT DISTINCT FROM OLD.status
       AND NEW.result IS NOT DISTINCT FROM OLD.result THEN
        RETURN NEW;
    END IF;

    SELECT scoped_issue.*
    INTO target_issue
    FROM issue AS scoped_issue
    JOIN agent AS scoped_agent
      ON scoped_agent.id = NEW.agent_id
     AND scoped_agent.workspace_id = scoped_issue.workspace_id
    WHERE scoped_issue.id = NEW.issue_id;

    IF target_issue.id IS NULL THEN
        RETURN NEW;
    END IF;

    SELECT id, project_binding_id
    INTO target_topic_binding_id, target_project_binding_id
    FROM channel_issue_topic_binding
    WHERE workspace_id = target_issue.workspace_id
      AND issue_id = target_issue.id
      AND state = 'active'
    LIMIT 1;

    IF target_topic_binding_id IS NULL AND target_issue.project_id IS NOT NULL THEN
        SELECT id
        INTO target_project_binding_id
        FROM channel_project_binding
        WHERE workspace_id = target_issue.workspace_id
          AND project_id = target_issue.project_id
          AND state = 'active'
        LIMIT 1;
    END IF;

    IF target_topic_binding_id IS NULL AND target_project_binding_id IS NULL THEN
        RETURN NEW;
    END IF;

    IF NEW.status IS DISTINCT FROM OLD.status
       AND NEW.status IN ('running', 'completed', 'failed', 'cancelled') THEN
        notification_type := CASE NEW.status
            WHEN 'running' THEN 'task_started'
            WHEN 'completed' THEN 'task_completed'
            WHEN 'failed' THEN 'task_failed'
            ELSE 'task_cancelled'
        END;

        INSERT INTO channel_notification_outbox (
            event_id, workspace_id, project_id, project_binding_id,
            issue_topic_binding_id, issue_id, task_id, event_type, payload
        ) VALUES (
            gen_random_uuid(), target_issue.workspace_id, target_issue.project_id,
            target_project_binding_id, target_topic_binding_id,
            target_issue.id, NEW.id, notification_type,
            jsonb_build_object(
                'issue_id', target_issue.id,
                'number', target_issue.number,
                'title', target_issue.title,
                'issue_status', target_issue.status,
                'task_id', NEW.id,
                'agent_id', NEW.agent_id,
                'task_status', NEW.status,
                'reason', CASE
                    WHEN NEW.status = 'failed' THEN COALESCE(NULLIF(NEW.error, ''), 'Task execution failed')
                    WHEN NEW.status = 'cancelled' THEN 'Task execution was stopped'
                    ELSE NULL
                END,
                'occurred_at', now()
            )
        );
    END IF;

    IF NEW.result IS DISTINCT FROM OLD.result AND NEW.result IS NOT NULL THEN
        INSERT INTO channel_notification_outbox (
            event_id, workspace_id, project_id, project_binding_id,
            issue_topic_binding_id, issue_id, task_id, event_type, payload
        ) VALUES (
            gen_random_uuid(), target_issue.workspace_id, target_issue.project_id,
            target_project_binding_id, target_topic_binding_id,
            target_issue.id, NEW.id, 'task_result',
            jsonb_build_object(
                'issue_id', target_issue.id,
                'number', target_issue.number,
                'title', target_issue.title,
                'issue_status', target_issue.status,
                'task_id', NEW.id,
                'agent_id', NEW.agent_id,
                'task_status', NEW.status,
                'result', NEW.result,
                'occurred_at', now()
            )
        );
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_channel_task_notification ON agent_task_queue;
CREATE TRIGGER trg_channel_task_notification
AFTER UPDATE OF status, result ON agent_task_queue
FOR EACH ROW
EXECUTE FUNCTION enqueue_channel_task_notification();
