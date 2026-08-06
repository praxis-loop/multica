package lark

import (
	"context"
	"errors"
	"testing"

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
		issueID          = "a1120000-0000-4000-8000-000000000004"
		userID           = "a1120000-0000-4000-8000-000000000005"
	)
	cleanProjectSyncRows(t, pool, workspaceID)

	topic, err := store.createIssueTopic(ctx, pool,
		util.MustParseUUID(workspaceID),
		util.MustParseUUID(installationID),
		pgtype.UUID{},
		pgtype.UUID{},
		util.MustParseUUID(issueID),
		"oc_direct_project_unbind",
		"om_direct_project_unbind",
		"",
		"manual_topic_bind",
		util.MustParseUUID(userID),
	)
	if err != nil {
		t.Fatalf("create direct topic: %v", err)
	}

	if err := store.finishProjectUnbind(ctx, pool, ChannelProjectBinding{
		ID:             util.MustParseUUID(projectBindingID),
		WorkspaceID:    util.MustParseUUID(workspaceID),
		InstallationID: util.MustParseUUID(installationID),
		ChannelChatID:  pgtype.Text{String: "different_chat", Valid: true},
	}, "project_unbound"); err != nil {
		t.Fatalf("finish unrelated project unbind: %v", err)
	}

	reloaded, err := store.getIssueTopicByID(ctx, pool, util.MustParseUUID(workspaceID), topic.ID)
	if err != nil {
		t.Fatalf("reload direct topic: %v", err)
	}
	if reloaded.State != "active" {
		t.Fatalf("direct topic state = %q, want active", reloaded.State)
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
