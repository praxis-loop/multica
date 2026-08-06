package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/multica-ai/multica/server/internal/integrations/lark"
)

type agentCreateFeishuTopicFixture struct {
	agentID        string
	taskID         string
	projectID      string
	installationID string
	projectBinding string
	chatSessionID  string
	chatID         string
	rootMessageID  string
	threadID       string
}

func setupAgentCreateFeishuTopicFixture(t *testing.T, bindingConfig string) agentCreateFeishuTopicFixture {
	t.Helper()
	ctx := context.Background()

	var schemaReady bool
	if err := testPool.QueryRow(ctx, `
		SELECT to_regclass('channel_issue_topic_binding') IS NOT NULL
		   AND to_regclass('channel_notification_outbox') IS NOT NULL
	`).Scan(&schemaReady); err != nil {
		t.Fatalf("check Feishu project sync schema: %v", err)
	}
	if !schemaReady {
		t.Skip("Feishu project sync migration not applied")
	}

	fx := agentCreateFeishuTopicFixture{
		agentID:       createHandlerTestAgent(t, "agent-create-feishu-topic-"+t.Name(), nil),
		chatID:        "oc_agent_create_source",
		rootMessageID: "om_agent_create_root",
		threadID:      "omt_agent_create_topic",
	}

	if err := testPool.QueryRow(ctx, `
		INSERT INTO project (workspace_id, title)
		VALUES ($1, $2)
		RETURNING id
	`, testWorkspaceID, "Agent create Feishu topic "+t.Name()).Scan(&fx.projectID); err != nil {
		t.Fatalf("create source Project: %v", err)
	}
	if err := testPool.QueryRow(ctx, `
		INSERT INTO channel_installation (
			workspace_id, agent_id, channel_type, config, installer_user_id, status
		) VALUES ($1, $2, 'feishu', '{}'::jsonb, $3, 'active')
		RETURNING id
	`, testWorkspaceID, fx.agentID, testUserID).Scan(&fx.installationID); err != nil {
		t.Fatalf("create Feishu installation: %v", err)
	}
	if err := testPool.QueryRow(ctx, `
		INSERT INTO channel_project_binding (
			workspace_id, project_id, installation_id, channel_type,
			channel_chat_id, channel_chat_name, state,
			created_by_user_id, bound_by_user_id, bound_at
		) VALUES ($1, $2, $3, 'feishu', $4, 'Agent create source group',
		          'active', $5, $5, now())
		RETURNING id
	`, testWorkspaceID, fx.projectID, fx.installationID, fx.chatID, testUserID).Scan(&fx.projectBinding); err != nil {
		t.Fatalf("create active Project route: %v", err)
	}
	if err := testPool.QueryRow(ctx, `
		INSERT INTO chat_session (workspace_id, agent_id, creator_id, title)
		VALUES ($1, $2, $3, 'Agent create source topic')
		RETURNING id
	`, testWorkspaceID, fx.agentID, testUserID).Scan(&fx.chatSessionID); err != nil {
		t.Fatalf("create source chat session: %v", err)
	}
	if _, err := testPool.Exec(ctx, `
		INSERT INTO channel_chat_session_binding (
			chat_session_id, installation_id, channel_type, channel_chat_id,
			chat_type, last_message_id, last_thread_id, config
		) VALUES ($1, $2, 'feishu', $3, 'group', $4, $5, $6::jsonb)
	`, fx.chatSessionID, fx.installationID, fx.chatID+":"+fx.threadID,
		fx.rootMessageID, fx.threadID, bindingConfig); err != nil {
		t.Fatalf("create source topic session binding: %v", err)
	}
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (
			agent_id, runtime_id, chat_session_id, status, priority,
			originator_user_id, accountable_user_id, started_at
		) VALUES ($1, (SELECT runtime_id FROM agent WHERE id = $1), $2,
		          'running', 0, $3, $3, now())
		RETURNING id
	`, fx.agentID, fx.chatSessionID, testUserID).Scan(&fx.taskID); err != nil {
		t.Fatalf("create acting topic task: %v", err)
	}

	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = testPool.Exec(ctx, `DELETE FROM channel_notification_outbox WHERE workspace_id = $1 AND project_binding_id = $2`, testWorkspaceID, fx.projectBinding)
		_, _ = testPool.Exec(ctx, `DELETE FROM channel_issue_topic_binding WHERE workspace_id = $1 AND installation_id = $2`, testWorkspaceID, fx.installationID)
		_, _ = testPool.Exec(ctx, `DELETE FROM issue WHERE workspace_id = $1 AND project_id = $2`, testWorkspaceID, fx.projectID)
		_, _ = testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE id = $1`, fx.taskID)
		_, _ = testPool.Exec(ctx, `DELETE FROM channel_chat_session_binding WHERE chat_session_id = $1`, fx.chatSessionID)
		_, _ = testPool.Exec(ctx, `DELETE FROM chat_session WHERE id = $1`, fx.chatSessionID)
		_, _ = testPool.Exec(ctx, `DELETE FROM channel_project_binding WHERE id = $1`, fx.projectBinding)
		_, _ = testPool.Exec(ctx, `DELETE FROM channel_installation WHERE id = $1`, fx.installationID)
		_, _ = testPool.Exec(ctx, `DELETE FROM project WHERE id = $1`, fx.projectID)
	})

	return fx
}

func installHandlerProjectSyncForTest(t *testing.T) {
	t.Helper()
	syncService, err := lark.NewProjectSyncService(lark.ProjectSyncServiceConfig{
		Pool:    testPool,
		Queries: testHandler.Queries,
		Issues:  testHandler.IssueService,
		Tasks:   testHandler.TaskService,
	})
	if err != nil {
		t.Fatalf("initialize Project sync service: %v", err)
	}
	previous := testHandler.LarkProjectSync
	testHandler.LarkProjectSync = syncService
	t.Cleanup(func() { testHandler.LarkProjectSync = previous })
}

// TestCreateIssue_AgentCreate_StampsActingTaskOrigin locks the MUL-4305 fix at
// the HTTP boundary: when an agent creates an issue through the ordinary POST
// /api/issues path (no explicit origin, not quick_create), the handler stamps
// origin_type='agent_create' + origin_id=<acting task>, resolved from the
// SERVER-trusted X-Task-ID (resolveActor only grants "agent" once the
// agent/task pair is validated). That link is what lets
// resolveOriginatorForIssueTask recover the top-of-chain human for any
// downstream assignment / squad-leader run and keep A2A mentions authorized.
func TestCreateIssue_AgentCreate_StampsActingTaskOrigin(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	var agentID, runtimeID string
	if err := testPool.QueryRow(ctx,
		`SELECT id, runtime_id FROM agent WHERE workspace_id = $1 AND name = $2`,
		testWorkspaceID, "Handler Test Agent",
	).Scan(&agentID, &runtimeID); err != nil {
		t.Fatalf("find test agent: %v", err)
	}

	// Acting task carrying the human originator (testUserID). resolveActor
	// validates this (agent, task) pair before the handler trusts X-Task-ID.
	var taskID string
	if err := testPool.QueryRow(ctx,
		`INSERT INTO agent_task_queue (agent_id, runtime_id, status, priority, originator_user_id, accountable_user_id)
		 VALUES ($1, $2, 'running', 0, $3, $3) RETURNING id`,
		agentID, runtimeID, testUserID,
	).Scan(&taskID); err != nil {
		t.Fatalf("seed acting task: %v", err)
	}
	t.Cleanup(func() { testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE id = $1`, taskID) })

	w := httptest.NewRecorder()
	req := newRequest("POST", "/api/issues?workspace_id="+testWorkspaceID, map[string]any{
		"title": "Agent-created via normal create (MUL-4305)",
	})
	req.Header.Set("X-Agent-ID", agentID)
	req.Header.Set("X-Task-ID", taskID)
	testHandler.CreateIssue(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("CreateIssue: expected 201, got %d: %s", w.Code, w.Body.String())
	}
	var created IssueResponse
	if err := json.NewDecoder(w.Body).Decode(&created); err != nil {
		t.Fatalf("decode issue: %v", err)
	}
	t.Cleanup(func() {
		cleanup := withURLParam(newRequest("DELETE", "/api/issues/"+created.ID, nil), "id", created.ID)
		testHandler.DeleteIssue(httptest.NewRecorder(), cleanup)
	})

	var originType, originID string
	if err := testPool.QueryRow(ctx,
		`SELECT COALESCE(origin_type, ''), COALESCE(origin_id::text, '') FROM issue WHERE id = $1`, created.ID,
	).Scan(&originType, &originID); err != nil {
		t.Fatalf("load issue origin: %v", err)
	}
	if originType != "agent_create" {
		t.Fatalf("origin_type = %q, want agent_create", originType)
	}
	if originID != taskID {
		t.Fatalf("origin_id = %q, want acting task %q", originID, taskID)
	}
}

func TestCreateIssue_AgentCreate_BindsSourceFeishuTopicInCreateTransaction(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	fx := setupAgentCreateFeishuTopicFixture(t,
		`{"chat_id":"oc_agent_create_source","topic_root_message_id":"om_agent_create_root"}`)
	installHandlerProjectSyncForTest(t)

	w := httptest.NewRecorder()
	req := newRequest("POST", "/api/issues?workspace_id="+testWorkspaceID, map[string]any{
		"title":      "Agent-created from a Feishu source topic",
		"project_id": fx.projectID,
	})
	req.Header.Set("X-Agent-ID", fx.agentID)
	req.Header.Set("X-Task-ID", fx.taskID)
	testHandler.CreateIssue(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("CreateIssue: expected 201, got %d: %s", w.Code, w.Body.String())
	}
	var created IssueResponse
	if err := json.NewDecoder(w.Body).Decode(&created); err != nil {
		t.Fatalf("decode issue: %v", err)
	}

	ctx := context.Background()
	var activeCount int
	if err := testPool.QueryRow(ctx, `
		SELECT count(*)
		FROM channel_issue_topic_binding
		WHERE workspace_id = $1 AND issue_id = $2 AND state = 'active'
	`, testWorkspaceID, created.ID).Scan(&activeCount); err != nil {
		t.Fatalf("count active source topic bindings: %v", err)
	}
	if activeCount != 1 {
		t.Fatalf("active source topic bindings = %d, want exactly 1", activeCount)
	}

	var installationID, projectBindingID, projectID, chatID, rootID, threadID, source string
	if err := testPool.QueryRow(ctx, `
		SELECT installation_id::text, project_binding_id::text, project_id::text,
		       channel_chat_id, topic_root_message_id,
		       COALESCE(channel_thread_id, ''), binding_source
		FROM channel_issue_topic_binding
		WHERE workspace_id = $1 AND issue_id = $2 AND state = 'active'
	`, testWorkspaceID, created.ID).Scan(
		&installationID, &projectBindingID, &projectID,
		&chatID, &rootID, &threadID, &source,
	); err != nil {
		t.Fatalf("load active source topic binding: %v", err)
	}
	if installationID != fx.installationID || projectBindingID != fx.projectBinding ||
		projectID != fx.projectID || chatID != fx.chatID || rootID != fx.rootMessageID ||
		threadID != fx.threadID || source != "issue_created_in_topic" {
		t.Fatalf("source topic binding = installation %q project_binding %q project %q chat %q root %q thread %q source %q",
			installationID, projectBindingID, projectID, chatID, rootID, threadID, source)
	}

	var staleProjectRoutes int
	if err := testPool.QueryRow(ctx, `
		SELECT count(*)
		FROM channel_notification_outbox
		WHERE workspace_id = $1 AND issue_id = $2
		  AND event_type = 'issue_created'
		  AND project_binding_id = $3
		  AND issue_topic_binding_id IS NULL
	`, testWorkspaceID, created.ID, fx.projectBinding).Scan(&staleProjectRoutes); err != nil {
		t.Fatalf("count stale Project issue-created routes: %v", err)
	}
	if staleProjectRoutes != 0 {
		t.Fatalf("stale Project issue-created routes = %d, want 0", staleProjectRoutes)
	}
}

func TestCreateIssue_AgentCreate_TopicHookResolutionErrorFailsBeforeCreate(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	fx := setupAgentCreateFeishuTopicFixture(t,
		`{"chat_id":123,"topic_root_message_id":"om_agent_create_root"}`)
	installHandlerProjectSyncForTest(t)

	const title = "Agent create must fail with invalid Feishu topic route"
	w := httptest.NewRecorder()
	req := newRequest("POST", "/api/issues?workspace_id="+testWorkspaceID, map[string]any{
		"title":      title,
		"project_id": fx.projectID,
	})
	req.Header.Set("X-Agent-ID", fx.agentID)
	req.Header.Set("X-Task-ID", fx.taskID)
	testHandler.CreateIssue(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("CreateIssue: expected 500, got %d: %s", w.Code, w.Body.String())
	}

	var issueCount int
	if err := testPool.QueryRow(context.Background(), `
		SELECT count(*) FROM issue
		WHERE workspace_id = $1 AND project_id = $2 AND title = $3
	`, testWorkspaceID, fx.projectID, title).Scan(&issueCount); err != nil {
		t.Fatalf("count issues after hook resolution failure: %v", err)
	}
	if issueCount != 0 {
		t.Fatalf("issues created after hook resolution failure = %d, want 0", issueCount)
	}
}

// TestCreateIssue_NoAgentCreateStampForMemberOrForgedAgent is the security
// regression for MUL-4305: the agent_create stamp must only ride a genuine
// agent actor. A plain member create carries no origin, and a member who
// forges X-Agent-ID without a valid X-Task-ID is demoted to "member" by
// resolveActor — so it must NOT smuggle an agent_create origin (which would
// later let a downstream run inherit a human identity the caller never had).
func TestCreateIssue_NoAgentCreateStampForMemberOrForgedAgent(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	var agentID string
	if err := testPool.QueryRow(ctx,
		`SELECT id FROM agent WHERE workspace_id = $1 AND name = $2`,
		testWorkspaceID, "Handler Test Agent",
	).Scan(&agentID); err != nil {
		t.Fatalf("find test agent: %v", err)
	}

	assertNoAgentOrigin := func(t *testing.T, mutate func(*http.Request)) {
		t.Helper()
		w := httptest.NewRecorder()
		req := newRequest("POST", "/api/issues?workspace_id="+testWorkspaceID, map[string]any{
			"title": "No agent_create stamp expected (MUL-4305)",
		})
		if mutate != nil {
			mutate(req)
		}
		testHandler.CreateIssue(w, req)
		if w.Code != http.StatusCreated {
			t.Fatalf("CreateIssue: expected 201, got %d: %s", w.Code, w.Body.String())
		}
		var created IssueResponse
		if err := json.NewDecoder(w.Body).Decode(&created); err != nil {
			t.Fatalf("decode issue: %v", err)
		}
		t.Cleanup(func() {
			cleanup := withURLParam(newRequest("DELETE", "/api/issues/"+created.ID, nil), "id", created.ID)
			testHandler.DeleteIssue(httptest.NewRecorder(), cleanup)
		})
		var originType string
		if err := testPool.QueryRow(ctx,
			`SELECT COALESCE(origin_type, '') FROM issue WHERE id = $1`, created.ID,
		).Scan(&originType); err != nil {
			t.Fatalf("load issue origin: %v", err)
		}
		if originType != "" {
			t.Fatalf("origin_type = %q, want empty (no agent_create stamp)", originType)
		}
	}

	t.Run("plain member create", func(t *testing.T) {
		assertNoAgentOrigin(t, nil)
	})

	t.Run("forged X-Agent-ID without X-Task-ID", func(t *testing.T) {
		// resolveActor refuses to trust X-Agent-ID without a paired, valid
		// X-Task-ID, so this stays a member create and gets no origin stamp.
		assertNoAgentOrigin(t, func(req *http.Request) {
			req.Header.Set("X-Agent-ID", agentID)
		})
	})
}
