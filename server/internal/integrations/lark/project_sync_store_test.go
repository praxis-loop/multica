package lark

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/util"
)

func requireProjectSyncTables(t *testing.T, q projectSyncDB) {
	t.Helper()
	ctx := context.Background()
	for _, table := range []string{"channel_project_binding", "channel_issue_topic_binding", "channel_notification_outbox"} {
		var present bool
		if err := q.QueryRow(ctx, "SELECT to_regclass('public."+table+"') IS NOT NULL").Scan(&present); err != nil || !present {
			t.Skipf("%s not present (database not migrated)", table)
		}
	}
}

func TestProjectSyncStoreAllowsOnlyOneActiveTopicPerIssue(t *testing.T) {
	pool := channelScopeTestDB(t)
	requireProjectSyncTables(t, pool)
	ctx := context.Background()
	store := newProjectSyncStore(pool)

	const (
		workspaceID    = "a1110000-0000-4000-8000-000000000001"
		installationID = "a1110000-0000-4000-8000-000000000002"
		issueID        = "a1110000-0000-4000-8000-000000000003"
		userID         = "a1110000-0000-4000-8000-000000000004"
	)
	cleanProjectSyncRows(t, pool, workspaceID)

	first, err := store.createIssueTopic(ctx, pool,
		util.MustParseUUID(workspaceID),
		util.MustParseUUID(installationID),
		pgtype.UUID{},
		pgtype.UUID{},
		util.MustParseUUID(issueID),
		"oc_topic_once",
		"om_root_once",
		"",
		"manual_topic_bind",
		util.MustParseUUID(userID),
	)
	if err != nil {
		t.Fatalf("create first topic: %v", err)
	}
	if _, err := store.createIssueTopic(ctx, pool,
		util.MustParseUUID(workspaceID),
		util.MustParseUUID(installationID),
		pgtype.UUID{},
		pgtype.UUID{},
		util.MustParseUUID(issueID),
		"oc_topic_once",
		"om_root_two",
		"",
		"manual_topic_bind",
		util.MustParseUUID(userID),
	); err == nil {
		t.Fatal("second active topic for one issue succeeded")
	}

	if _, err := store.manualUnbindIssueTopic(ctx, pool, util.MustParseUUID(workspaceID), first.ID, util.MustParseUUID(userID)); err != nil {
		t.Fatalf("manual unbind first topic: %v", err)
	}
	second, err := store.createIssueTopic(ctx, pool,
		util.MustParseUUID(workspaceID),
		util.MustParseUUID(installationID),
		pgtype.UUID{},
		pgtype.UUID{},
		util.MustParseUUID(issueID),
		"oc_topic_once",
		"om_root_two",
		"",
		"manual_topic_bind",
		util.MustParseUUID(userID),
	)
	if err != nil {
		t.Fatalf("create replacement topic after manual unbind: %v", err)
	}
	if second.ID == first.ID {
		t.Fatal("replacement topic reused the old binding row")
	}
}

func TestProjectUnbindDoesNotDeactivateDirectIssueTopics(t *testing.T) {
	pool := channelScopeTestDB(t)
	requireProjectSyncTables(t, pool)
	ctx := context.Background()
	store := newProjectSyncStore(pool)

	const (
		workspaceID      = "a1120000-0000-4000-8000-000000000001"
		installationID   = "a1120000-0000-4000-8000-000000000002"
		projectBindingID = "a1120000-0000-4000-8000-000000000003"
		projectID        = "a1120000-0000-4000-8000-000000000004"
		manualIssueID    = "a1120000-0000-4000-8000-000000000005"
		createdIssueID   = "a1120000-0000-4000-8000-000000000006"
		ownedIssueID     = "a1120000-0000-4000-8000-000000000007"
		userID           = "a1120000-0000-4000-8000-000000000008"
		chatID           = "oc_direct_project_unbind"
	)
	cleanProjectSyncRows(t, pool, workspaceID)

	manualTopic, err := store.createIssueTopic(ctx, pool,
		util.MustParseUUID(workspaceID),
		util.MustParseUUID(installationID),
		pgtype.UUID{},
		pgtype.UUID{},
		util.MustParseUUID(manualIssueID),
		chatID,
		"om_manual_direct_project_unbind",
		"",
		"manual_topic_bind",
		util.MustParseUUID(userID),
	)
	if err != nil {
		t.Fatalf("create manual direct topic: %v", err)
	}
	createdTopic, err := store.createIssueTopic(ctx, pool,
		util.MustParseUUID(workspaceID),
		util.MustParseUUID(installationID),
		pgtype.UUID{},
		pgtype.UUID{},
		util.MustParseUUID(createdIssueID),
		chatID,
		"om_created_direct_project_unbind",
		"",
		"issue_created_in_topic",
		util.MustParseUUID(userID),
	)
	if err != nil {
		t.Fatalf("create issue-created direct topic: %v", err)
	}
	ownedTopic, err := store.createIssueTopic(ctx, pool,
		util.MustParseUUID(workspaceID),
		util.MustParseUUID(installationID),
		util.MustParseUUID(projectBindingID),
		util.MustParseUUID(projectID),
		util.MustParseUUID(ownedIssueID),
		chatID,
		"om_owned_project_unbind",
		"",
		"issue_created_by_multica",
		util.MustParseUUID(userID),
	)
	if err != nil {
		t.Fatalf("create project-owned topic: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO channel_notification_outbox (
			event_id, workspace_id, project_id, project_binding_id,
			issue_id, event_type, payload, status, locked_at, locked_by
		) VALUES
			(gen_random_uuid(), $1, $2, $3, $4, 'issue_status_changed', '{}', 'pending', NULL, NULL),
			(gen_random_uuid(), $1, $2, $3, $4, 'comment_created', '{}', 'sending', now(), 'owned-worker')`,
		workspaceID, projectID, projectBindingID, ownedIssueID); err != nil {
		t.Fatalf("insert project-owned outbox: %v", err)
	}

	if err := store.finishProjectUnbind(ctx, pool, ChannelProjectBinding{
		ID:             util.MustParseUUID(projectBindingID),
		WorkspaceID:    util.MustParseUUID(workspaceID),
		InstallationID: util.MustParseUUID(installationID),
		ChannelChatID:  pgtype.Text{String: chatID, Valid: true},
	}, "project_unbound"); err != nil {
		t.Fatalf("finish project unbind: %v", err)
	}

	for name, topic := range map[string]ChannelIssueTopicBinding{
		"manual direct":        manualTopic,
		"issue-created direct": createdTopic,
	} {
		reloaded, err := store.getIssueTopicByID(ctx, pool, util.MustParseUUID(workspaceID), topic.ID)
		if err != nil {
			t.Fatalf("reload %s topic: %v", name, err)
		}
		if reloaded.State != "active" {
			t.Fatalf("%s topic state = %q, want active", name, reloaded.State)
		}
	}
	ownedReloaded, err := store.getIssueTopicByID(ctx, pool, util.MustParseUUID(workspaceID), ownedTopic.ID)
	if err != nil {
		t.Fatalf("reload project-owned topic: %v", err)
	}
	if ownedReloaded.State != "project_unbound" {
		t.Fatalf("project-owned topic state = %q, want project_unbound", ownedReloaded.State)
	}
	var liveOwnedOutbox int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM channel_notification_outbox
		WHERE workspace_id = $1 AND project_binding_id = $2
		  AND (status <> 'dead' OR locked_at IS NOT NULL OR locked_by IS NOT NULL)`,
		workspaceID, projectBindingID).Scan(&liveOwnedOutbox); err != nil {
		t.Fatalf("check project-owned outbox: %v", err)
	}
	if liveOwnedOutbox != 0 {
		t.Fatalf("project-owned outbox rows not fully dead-lettered = %d", liveOwnedOutbox)
	}
}

func TestTerminalNotificationFailureFencesLeaseOwner(t *testing.T) {
	pool := channelScopeTestDB(t)
	requireProjectSyncTables(t, pool)
	ctx := context.Background()
	store := newProjectSyncStore(pool)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	tests := []struct {
		name       string
		workspace  string
		issue      string
		attempts   int32
		processErr error
	}{
		{
			name:       "permanent failure",
			workspace:  "a1140000-0000-4000-8000-000000000001",
			issue:      "a1140000-0000-4000-8000-000000000002",
			processErr: permanentSyncError{reason: "permanent_failure"},
		},
		{
			name:       "max-attempt failure",
			workspace:  "a1150000-0000-4000-8000-000000000001",
			issue:      "a1150000-0000-4000-8000-000000000002",
			attempts:   projectSyncMaxAttempts - 1,
			processErr: errors.New("attempts exhausted"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cleanProjectSyncRows(t, pool, tt.workspace)
			if _, err := pool.Exec(ctx, `
				INSERT INTO channel_notification_outbox (
					event_id, workspace_id, project_binding_id, issue_id,
					event_type, payload, attempts
				) VALUES (gen_random_uuid(), $1, gen_random_uuid(), $2,
				          'issue_status_changed', '{}', $3)`,
				tt.workspace, tt.issue, tt.attempts); err != nil {
				t.Fatalf("insert notification: %v", err)
			}

			claimedByA, err := store.claimNotifications(ctx, "worker-a", time.Now().Add(-time.Minute), 1)
			if err != nil || len(claimedByA) != 1 {
				t.Fatalf("worker A claim = %d items, err %v", len(claimedByA), err)
			}
			if _, err := pool.Exec(ctx, `
				UPDATE channel_notification_outbox
				SET locked_at = now() - interval '3 minutes'
				WHERE id = $1`, claimedByA[0].ID); err != nil {
				t.Fatalf("expire worker A lease: %v", err)
			}
			claimedByB, err := store.claimNotifications(ctx, "worker-b", time.Now().Add(-2*time.Minute), 1)
			if err != nil || len(claimedByB) != 1 {
				t.Fatalf("worker B reclaim = %d items, err %v", len(claimedByB), err)
			}

			workerA := &ProjectIssueSyncWorker{store: store, workerID: "worker-a", logger: logger}
			workerA.handleFailure(ctx, claimedByA[0], tt.processErr)
			assertNotificationLeaseState(t, pool, claimedByA[0].ID, "sending", tt.attempts, "worker-b")

			workerB := &ProjectIssueSyncWorker{store: store, workerID: "worker-b", logger: logger}
			workerB.handleFailure(ctx, claimedByB[0], tt.processErr)
			assertNotificationLeaseState(t, pool, claimedByB[0].ID, "dead", tt.attempts+1, "")
		})
	}
}

func assertNotificationLeaseState(t *testing.T, q projectSyncDB, id pgtype.UUID, wantStatus string, wantAttempts int32, wantWorker string) {
	t.Helper()
	var (
		status   string
		attempts int32
		lockedBy pgtype.Text
	)
	if err := q.QueryRow(context.Background(), `
		SELECT status, attempts, locked_by
		FROM channel_notification_outbox
		WHERE id = $1`, id).Scan(&status, &attempts, &lockedBy); err != nil {
		t.Fatalf("load notification state: %v", err)
	}
	if status != wantStatus || attempts != wantAttempts || lockedBy.String != wantWorker || lockedBy.Valid != (wantWorker != "") {
		t.Fatalf(
			"notification state = status %q, attempts %d, locked_by %v; want status %q, attempts %d, worker %q",
			status, attempts, lockedBy, wantStatus, wantAttempts, wantWorker,
		)
	}
}

func TestProjectBindingConflictPreventsDuplicateActiveGroup(t *testing.T) {
	pool := channelScopeTestDB(t)
	requireProjectSyncTables(t, pool)
	ctx := context.Background()
	store := newProjectSyncStore(pool)

	const (
		workspaceID    = "a1130000-0000-4000-8000-000000000001"
		projectAID     = "a1130000-0000-4000-8000-000000000002"
		projectBID     = "a1130000-0000-4000-8000-000000000003"
		installationID = "a1130000-0000-4000-8000-000000000004"
		userID         = "a1130000-0000-4000-8000-000000000005"
	)
	cleanProjectSyncRows(t, pool, workspaceID)

	if _, err := store.createActiveProjectBinding(ctx, pool,
		util.MustParseUUID(workspaceID),
		util.MustParseUUID(projectAID),
		util.MustParseUUID(installationID),
		"oc_same_group",
		"Same Group",
		util.MustParseUUID(userID),
	); err != nil {
		t.Fatalf("create first project binding: %v", err)
	}
	if _, err := store.createActiveProjectBinding(ctx, pool,
		util.MustParseUUID(workspaceID),
		util.MustParseUUID(projectBID),
		util.MustParseUUID(installationID),
		"oc_same_group",
		"Same Group",
		util.MustParseUUID(userID),
	); err == nil || errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("duplicate active group binding err = %v, want constraint error", err)
	}
}

func cleanProjectSyncRows(t *testing.T, q projectSyncDB, workspaceID string) {
	t.Helper()
	clean := func() {
		ctx := context.Background()
		_, _ = q.Exec(ctx, `DELETE FROM channel_notification_outbox WHERE workspace_id = $1`, workspaceID)
		_, _ = q.Exec(ctx, `DELETE FROM channel_issue_topic_binding WHERE workspace_id = $1`, workspaceID)
		_, _ = q.Exec(ctx, `DELETE FROM channel_project_binding WHERE workspace_id = $1`, workspaceID)
	}
	clean()
	t.Cleanup(clean)
}
