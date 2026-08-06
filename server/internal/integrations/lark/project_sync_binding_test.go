package lark

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type projectBindingFixture struct {
	t              *testing.T
	pool           *pgxpool.Pool
	ctx            context.Context
	service        *ProjectSyncService
	workspaceID    pgtype.UUID
	projectID      pgtype.UUID
	installationID pgtype.UUID
	userID         pgtype.UUID
	installation   Installation
}

func newProjectBindingFixture(t *testing.T, namespace string, ids [5]string) *projectBindingFixture {
	t.Helper()
	pool := channelScopeTestDB(t)
	requireProjectSyncTables(t, pool)
	ctx := context.Background()
	workspaceID := util.MustParseUUID(ids[0])
	projectID := util.MustParseUUID(ids[1])
	installationID := util.MustParseUUID(ids[2])
	userID := util.MustParseUUID(ids[3])
	agentID := util.MustParseUUID(ids[4])

	clean := func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM channel_notification_outbox WHERE workspace_id = $1`, workspaceID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM channel_issue_topic_binding WHERE workspace_id = $1`, workspaceID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM channel_project_binding WHERE workspace_id = $1`, workspaceID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM channel_installation WHERE workspace_id = $1`, workspaceID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM project WHERE workspace_id = $1`, workspaceID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM member WHERE workspace_id = $1`, workspaceID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM "user" WHERE id = $1`, userID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM workspace WHERE id = $1`, workspaceID)
	}
	clean()
	t.Cleanup(clean)

	seed := func(query string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, query, args...); err != nil {
			t.Fatalf("seed project binding fixture: %v", err)
		}
	}
	seed(`INSERT INTO workspace (id, name, slug) VALUES ($1, $2, $3)`, workspaceID, namespace, namespace)
	seed(`INSERT INTO "user" (id, name, email) VALUES ($1, $2, $3)`, userID, namespace, fmt.Sprintf("%s@example.test", namespace))
	seed(`INSERT INTO member (workspace_id, user_id, role) VALUES ($1, $2, 'admin')`, workspaceID, userID)
	seed(`INSERT INTO project (id, workspace_id, title) VALUES ($1, $2, $3)`, projectID, workspaceID, namespace)
	seed(`
		INSERT INTO channel_installation (
			id, workspace_id, agent_id, channel_type, config,
			installer_user_id, status
		) VALUES ($1, $2, $3, 'feishu', jsonb_build_object('app_id', $4::text), $5, 'active')`,
		installationID, workspaceID, agentID, namespace, userID,
	)

	return &projectBindingFixture{
		t:              t,
		pool:           pool,
		ctx:            ctx,
		service:        &ProjectSyncService{store: newProjectSyncStore(pool), queries: db.New(pool)},
		workspaceID:    workspaceID,
		projectID:      projectID,
		installationID: installationID,
		userID:         userID,
		installation: Installation{
			ID:          installationID,
			WorkspaceID: workspaceID,
			Status:      string(InstallationActive),
		},
	}
}

func (f *projectBindingFixture) begin() (ChannelProjectBinding, string, error) {
	f.t.Helper()
	return f.service.BeginProjectBinding(f.ctx, f.workspaceID, f.projectID, f.installationID, f.userID)
}

func (f *projectBindingFixture) confirm(code, chatID string) (ChannelProjectBinding, bool, error) {
	f.t.Helper()
	return f.service.confirmCodeInGroup(f.ctx, engine.ProjectCommandContext{
		Installation: engine.ResolvedInstallation{
			ID:          f.installationID,
			WorkspaceID: f.workspaceID,
			Active:      true,
			Platform:    f.installation,
		},
		UserID: f.userID,
		Message: channel.InboundMessage{Source: channel.Source{
			ChannelType: channel.TypeFeishu,
			ChatID:      chatID,
			ChatType:    channel.ChatTypeGroup,
		}},
	}, code)
}

func TestBeginProjectBindingRenewsExpiredPendingToken(t *testing.T) {
	f := newProjectBindingFixture(t, "binding-expired-renewal", [5]string{
		"b1010000-0000-4000-8000-000000000001",
		"b1010000-0000-4000-8000-000000000002",
		"b1010000-0000-4000-8000-000000000003",
		"b1010000-0000-4000-8000-000000000004",
		"b1010000-0000-4000-8000-000000000005",
	})

	first, firstCode, err := f.begin()
	if err != nil {
		t.Fatalf("begin initial binding: %v", err)
	}
	if _, err := f.pool.Exec(f.ctx, `
		UPDATE channel_project_binding
		SET bind_token_expires_at = now() - interval '1 minute'
		WHERE id = $1`, first.ID); err != nil {
		t.Fatalf("expire initial confirmation code: %v", err)
	}

	renewed, renewedCode, err := f.begin()
	if err != nil {
		t.Fatalf("renew expired binding: %v", err)
	}
	if renewed.ID != first.ID {
		t.Fatalf("renewed binding id = %v, want %v", renewed.ID, first.ID)
	}
	if renewedCode == "" || renewedCode == firstCode {
		t.Fatalf("renewed confirmation code = %q, want fresh non-empty code", renewedCode)
	}
	if !renewed.BindTokenExpiresAt.Valid || !renewed.BindTokenExpiresAt.Time.After(time.Now()) {
		t.Fatalf("renewed token expiration = %v, want future timestamp", renewed.BindTokenExpiresAt)
	}

	if _, handled, err := f.confirm(firstCode, "oc_expired_old"); err != nil || handled {
		t.Fatalf("previous code confirmation = handled %v, err %v; want unhandled", handled, err)
	}
	confirmed, handled, err := f.confirm(renewedCode, "oc_expired_fresh")
	if err != nil || !handled {
		t.Fatalf("fresh code confirmation = handled %v, err %v", handled, err)
	}
	if confirmed.ID != first.ID || confirmed.State != "active" {
		t.Fatalf("confirmed binding = id %v state %q", confirmed.ID, confirmed.State)
	}
}

func TestBeginProjectBindingActiveIsIdempotent(t *testing.T) {
	f := newProjectBindingFixture(t, "binding-active-idempotent", [5]string{
		"b1020000-0000-4000-8000-000000000001",
		"b1020000-0000-4000-8000-000000000002",
		"b1020000-0000-4000-8000-000000000003",
		"b1020000-0000-4000-8000-000000000004",
		"b1020000-0000-4000-8000-000000000005",
	})

	pending, code, err := f.begin()
	if err != nil {
		t.Fatalf("begin pending binding: %v", err)
	}
	active, handled, err := f.confirm(code, "oc_active_idempotent")
	if err != nil || !handled {
		t.Fatalf("confirm binding = handled %v, err %v", handled, err)
	}

	same, retryCode, err := f.begin()
	if err != nil {
		t.Fatalf("begin active binding again: %v", err)
	}
	if same.ID != pending.ID || same.ID != active.ID {
		t.Fatalf("active retry binding id = %v, want %v", same.ID, active.ID)
	}
	if same.State != "active" || retryCode != "" {
		t.Fatalf("active retry = state %q code %q, want active with empty code", same.State, retryCode)
	}
}

func TestBeginProjectBindingConcurrentPendingRetriesKeepOneCurrentToken(t *testing.T) {
	f := newProjectBindingFixture(t, "binding-concurrent-retry", [5]string{
		"b1030000-0000-4000-8000-000000000001",
		"b1030000-0000-4000-8000-000000000002",
		"b1030000-0000-4000-8000-000000000003",
		"b1030000-0000-4000-8000-000000000004",
		"b1030000-0000-4000-8000-000000000005",
	})

	initial, initialCode, err := f.begin()
	if err != nil {
		t.Fatalf("begin initial binding: %v", err)
	}

	type beginResult struct {
		binding ChannelProjectBinding
		code    string
		err     error
	}
	start := make(chan struct{})
	results := make(chan beginResult, 2)
	for range 2 {
		go func() {
			<-start
			binding, code, err := f.begin()
			results <- beginResult{binding: binding, code: code, err: err}
		}()
	}
	close(start)
	first := <-results
	second := <-results

	for i, result := range []beginResult{first, second} {
		if result.err != nil {
			t.Fatalf("concurrent begin %d: %v", i+1, result.err)
		}
		if result.binding.ID != initial.ID || result.code == "" {
			t.Fatalf("concurrent begin %d = id %v code %q", i+1, result.binding.ID, result.code)
		}
	}
	if first.code == second.code {
		t.Fatal("concurrent retries returned the same confirmation code")
	}

	current, err := f.service.store.getCurrentProjectBinding(f.ctx, f.pool, f.workspaceID, f.projectID)
	if err != nil {
		t.Fatalf("load binding after concurrent retries: %v", err)
	}
	currentMatches := []bool{
		current.BindTokenHash.String == projectBindingCodeHash(first.code),
		current.BindTokenHash.String == projectBindingCodeHash(second.code),
	}
	if currentMatches[0] == currentMatches[1] {
		t.Fatalf("current token matches concurrent results = %v, want exactly one match", currentMatches)
	}
	winnerCode, loserCode := first.code, second.code
	if currentMatches[1] {
		winnerCode, loserCode = second.code, first.code
	}
	for name, code := range map[string]string{"initial": initialCode, "superseded": loserCode} {
		if _, handled, err := f.confirm(code, "oc_concurrent_"+name); err != nil || handled {
			t.Fatalf("%s code confirmation = handled %v, err %v; want unhandled", name, handled, err)
		}
	}
	if _, handled, err := f.confirm(winnerCode, "oc_concurrent_winner"); err != nil || !handled {
		t.Fatalf("current code confirmation = handled %v, err %v", handled, err)
	}
}
