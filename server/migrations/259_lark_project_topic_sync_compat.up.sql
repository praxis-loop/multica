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
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
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
    unbound_at TIMESTAMPTZ
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
    event_order BIGINT GENERATED ALWAYS AS IDENTITY
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

UPDATE channel_issue_topic_binding citb
SET installation_id = cpb.installation_id
FROM channel_project_binding cpb
WHERE citb.installation_id IS NULL
  AND cpb.id = citb.project_binding_id
  AND cpb.workspace_id = citb.workspace_id;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM channel_issue_topic_binding
        WHERE installation_id IS NULL
    ) THEN
        RAISE EXCEPTION 'channel_issue_topic_binding.installation_id contains NULL rows; audit legacy data before applying lark project/topic sync compat migration';
    END IF;
END;
$$;

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
    ALTER COLUMN installation_id SET NOT NULL,
    ALTER COLUMN project_binding_id DROP NOT NULL,
    ALTER COLUMN project_id DROP NOT NULL,
    DROP CONSTRAINT IF EXISTS channel_issue_topic_binding_binding_source_check,
    ADD CONSTRAINT channel_issue_topic_binding_binding_source_check
        CHECK (binding_source IN ('issue_created_by_multica', 'issue_created_in_topic', 'manual_topic_bind', 'project_backfill')),
    DROP CONSTRAINT IF EXISTS channel_issue_topic_binding_state_check,
    ADD CONSTRAINT channel_issue_topic_binding_state_check
        CHECK (state IN ('active', 'manual_unbound', 'project_unbound', 'orphaned', 'replaced', 'bot_revoked'));

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

CREATE UNIQUE INDEX IF NOT EXISTS channel_project_binding_pkey
    ON channel_project_binding (id);

CREATE UNIQUE INDEX IF NOT EXISTS channel_issue_topic_binding_pkey
    ON channel_issue_topic_binding (id);

CREATE UNIQUE INDEX IF NOT EXISTS channel_notification_outbox_pkey
    ON channel_notification_outbox (id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'channel_project_binding'::regclass
          AND contype = 'p'
    ) THEN
        ALTER TABLE channel_project_binding
            ADD CONSTRAINT channel_project_binding_pkey PRIMARY KEY USING INDEX channel_project_binding_pkey;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'channel_issue_topic_binding'::regclass
          AND contype = 'p'
    ) THEN
        ALTER TABLE channel_issue_topic_binding
            ADD CONSTRAINT channel_issue_topic_binding_pkey PRIMARY KEY USING INDEX channel_issue_topic_binding_pkey;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'channel_notification_outbox'::regclass
          AND contype = 'p'
    ) THEN
        ALTER TABLE channel_notification_outbox
            ADD CONSTRAINT channel_notification_outbox_pkey PRIMARY KEY USING INDEX channel_notification_outbox_pkey;
    END IF;
END;
$$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_channel_project_binding_bind_token
    ON channel_project_binding (bind_token_hash)
    WHERE state = 'pending_group' AND bind_token_hash IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_channel_project_binding_project
    ON channel_project_binding (project_id)
    WHERE state IN ('pending_group', 'active');

CREATE UNIQUE INDEX IF NOT EXISTS uq_channel_project_binding_workspace_project
    ON channel_project_binding (workspace_id, project_id)
    WHERE state IN ('pending_group', 'active');

CREATE UNIQUE INDEX IF NOT EXISTS uq_channel_project_binding_bot_group
    ON channel_project_binding (installation_id, channel_chat_id)
    WHERE state = 'active';

CREATE UNIQUE INDEX IF NOT EXISTS uq_channel_issue_topic_issue
    ON channel_issue_topic_binding (issue_id)
    WHERE state = 'active';

CREATE UNIQUE INDEX IF NOT EXISTS uq_channel_issue_topic_workspace_issue
    ON channel_issue_topic_binding (workspace_id, issue_id)
    WHERE state = 'active';

CREATE UNIQUE INDEX IF NOT EXISTS uq_channel_issue_topic_route
    ON channel_issue_topic_binding (installation_id, channel_chat_id, topic_root_message_id)
    WHERE state = 'active';

CREATE UNIQUE INDEX IF NOT EXISTS uq_channel_notification_outbox_event
    ON channel_notification_outbox (event_id);

CREATE INDEX IF NOT EXISTS idx_channel_notification_outbox_pending
    ON channel_notification_outbox (next_attempt_at, created_at)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_channel_notification_outbox_issue_order
    ON channel_notification_outbox (issue_id, created_at)
    WHERE status IN ('pending', 'sending');

CREATE INDEX IF NOT EXISTS idx_channel_notification_outbox_issue_event_order
    ON channel_notification_outbox (issue_id, created_at, event_order)
    WHERE status IN ('pending', 'sending');

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
