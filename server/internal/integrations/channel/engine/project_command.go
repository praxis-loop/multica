package engine

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
)

// ProjectCommand is a deterministic control command handled before a channel
// message becomes an agent chat turn.
type ProjectCommand struct {
	Resource     string
	Action       string
	Arguments    []string
	RawArguments string
}

type ProjectCommandContext struct {
	Installation ResolvedInstallation
	UserID       pgtype.UUID
	Message      channel.InboundMessage
	Command      ProjectCommand
}

type ProjectCommandResult struct {
	ReplyText       string
	IssueID         pgtype.UUID
	IssueNumber     int32
	IssueIdentifier string
	IssueTitle      string
}

type ProjectCommandHandler interface {
	HandleProjectCommand(ctx context.Context, p ProjectCommandContext) (ProjectCommandResult, error)
}

func ParseProjectCommand(body string) (ProjectCommand, bool) {
	lines := strings.Split(body, "\n")
	first := -1
	for i, line := range lines {
		if strings.TrimSpace(line) != "" {
			first = i
			break
		}
	}
	if first < 0 {
		return ProjectCommand{}, false
	}

	line := strings.TrimSpace(lines[first])
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return ProjectCommand{}, false
	}

	switch fields[0] {
	case "/help":
		if fields[0] != line {
			return ProjectCommand{}, false
		}
		return ProjectCommand{Resource: "help", Action: "show"}, true
	case "/project", "/issue":
	default:
		return ProjectCommand{}, false
	}

	resource := strings.TrimPrefix(fields[0], "/")
	if len(fields) == 1 {
		return ProjectCommand{Resource: resource, Action: "help"}, true
	}

	action := fields[1]
	argStart := 2
	if resource == "issue" && !isIssueProjectCommandAction(action) {
		action = "bind"
		argStart = 1
	}

	args := append([]string(nil), fields[argStart:]...)
	raw := strings.Join(args, " ")
	if first+1 < len(lines) {
		tail := strings.TrimRight(strings.Join(lines[first+1:], "\n"), " \t\r\n")
		if tail != "" {
			if raw != "" {
				raw += "\n"
			}
			raw += tail
		}
	}

	return ProjectCommand{
		Resource:     resource,
		Action:       action,
		Arguments:    args,
		RawArguments: raw,
	}, true
}

func isIssueProjectCommandAction(action string) bool {
	switch action {
	case "create", "bind", "unbind", "status", "stop", "show", "help":
		return true
	default:
		return false
	}
}
