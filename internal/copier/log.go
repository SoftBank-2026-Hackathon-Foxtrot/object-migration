package copier

import (
	"context"
	"log/slog"
	"strings"

	"github.com/rclone/rclone/fs"
)

// SetLogger sends rclone's own log lines to h. rclone logs every object a
// comparison finds missing as an error; Check returns those as counts, so
// they are dropped here instead of filling the log during a copy.
func SetLogger(h slog.Handler) {
	fs.SetLogger(rcloneHandler{h})
}

type rcloneHandler struct{ slog.Handler }

func (h rcloneHandler) Handle(ctx context.Context, r slog.Record) error {
	if strings.HasPrefix(r.Message, "file not in ") {
		return nil
	}
	return h.Handler.Handle(ctx, r)
}

func (h rcloneHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return rcloneHandler{h.Handler.WithAttrs(attrs)}
}

func (h rcloneHandler) WithGroup(name string) slog.Handler {
	return rcloneHandler{h.Handler.WithGroup(name)}
}
