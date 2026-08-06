package lark

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// countingRootClient records how many times SendTextMessage is called — i.e.
// how many Lark topic root messages get created — and slows each call down to
// widen the concurrency window. Every other APIClient method is left unset:
// ensureIssueTopic must not call them, and a nil-method panic would surface it
// immediately if it ever did.
type countingRootClient struct {
	APIClient
	mu    sync.Mutex
	calls int
}

func (c *countingRootClient) SendTextMessage(ctx context.Context, p SendTextParams) (string, error) {
	c.mu.Lock()
	c.calls++
	n := c.calls
	c.mu.Unlock()
	// Sleep inside the "create the root" call so that, without per-issue
	// serialization, several racing goroutines would all get here and create
	// duplicate root messages.
	time.Sleep(40 * time.Millisecond)
	return fmt.Sprintf("root-msg-%d", n), nil
}

// TestEnsureIssueTopicCreatesSingleRootUnderConcurrency proves the core
// "never create a topic twice" invariant: when many outbox items for the SAME
// issue reach ensureIssueTopic concurrently — each with a DISTINCT item id, so
// Lark's per-item idempotency key does NOT dedupe them — exactly one Lark root
// message and one active binding are produced, and every caller observes that
// same binding. This is the regression guard for the topic-root race: the
// per-issue advisory lock must serialize the check -> Lark -> insert sequence.
func TestEnsureIssueTopicCreatesSingleRootUnderConcurrency(t *testing.T) {
	pool := channelScopeTestDB(t)
	requireProjectSyncTables(t, pool)
	ctx := context.Background()

	const (
		workspaceID    = "cafe0000-0000-4000-8000-000000000001"
		installationID = "cafe0000-0000-4000-8000-000000000002"
		projectID      = "cafe0000-0000-4000-8000-000000000003"
		bindingID      = "cafe0000-0000-4000-8000-000000000004"
		issueID        = "cafe0000-0000-4000-8000-000000000005"
	)
	cleanProjectSyncRows(t, pool, workspaceID)
	t.Cleanup(func() { cleanProjectSyncRows(t, pool, workspaceID) })

	client := &countingRootClient{}
	svc := &ProjectSyncService{
		queries: db.New(pool),
		store:   newProjectSyncStore(pool),
		client:  client,
	}
	worker := NewProjectIssueSyncWorker(svc, "topic-race-test")

	projectBinding := ChannelProjectBinding{
		ID:              util.MustParseUUID(bindingID),
		WorkspaceID:     util.MustParseUUID(workspaceID),
		ProjectID:       util.MustParseUUID(projectID),
		InstallationID:  util.MustParseUUID(installationID),
		ChannelType:     "feishu",
		ChannelChatID:   pgtype.Text{String: "oc_race_chat", Valid: true},
		ChannelChatName: pgtype.Text{String: "Race Chat", Valid: true},
		State:           "active",
	}
	issue := db.Issue{
		ID:          util.MustParseUUID(issueID),
		WorkspaceID: util.MustParseUUID(workspaceID),
		Title:       "Race issue",
		Status:      "todo",
		Number:      42,
	}

	const n = 8
	var wg sync.WaitGroup
	results := make([]ChannelIssueTopicBinding, n)
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Distinct item id per goroutine => distinct Lark idempotency key.
			item := ChannelNotificationOutbox{
				ID:               util.MustParseUUID(fmt.Sprintf("cafe0000-0000-4000-8000-1000000000%02d", i)),
				WorkspaceID:      util.MustParseUUID(workspaceID),
				IssueID:          util.MustParseUUID(issueID),
				ProjectBindingID: util.MustParseUUID(bindingID),
			}
			<-start
			b, _, err := worker.ensureIssueTopic(ctx, item, projectBinding, issue, projectNotificationPayload{}, InstallationCredentials{})
			results[i] = b
			errs[i] = err
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("ensureIssueTopic goroutine %d failed: %v", i, err)
		}
	}

	if client.calls != 1 {
		t.Fatalf("Lark root message created %d times, want exactly 1", client.calls)
	}

	var active int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM channel_issue_topic_binding WHERE workspace_id=$1 AND issue_id=$2 AND state='active'`,
		workspaceID, issueID).Scan(&active); err != nil {
		t.Fatalf("count active topic bindings: %v", err)
	}
	if active != 1 {
		t.Fatalf("active topic bindings = %d, want 1", active)
	}

	var total int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM channel_issue_topic_binding WHERE workspace_id=$1 AND issue_id=$2`,
		workspaceID, issueID).Scan(&total); err != nil {
		t.Fatalf("count all topic bindings: %v", err)
	}
	if total != 1 {
		t.Fatalf("total topic bindings = %d, want 1 (no orphaned reservations)", total)
	}

	first := results[0].ID
	for i, b := range results {
		if b.ID != first {
			t.Fatalf("goroutine %d observed binding %s, want the single shared binding %s",
				i, util.UUIDToString(b.ID), util.UUIDToString(first))
		}
	}
}
