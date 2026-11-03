package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rclone/rclone/fs"

	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/copier"
	"github.com/SoftBank-2026-Hackathon-Foxtrot/object-migration/internal/migration"
)

// Rclone copies objects with rclone. The initial sync copies the whole
// source and each sync round copies what is new, so the target catches up
// until a round finds nothing to copy. Freeze, switch and resume do nothing
// yet: the app keeps using the source until switching lands.
type Rclone struct {
	Options copier.Options
	// PreflightTimeout bounds each preflight check.
	PreflightTimeout time.Duration

	// open connects to an endpoint; tests replace it.
	open func(ctx context.Context, name string, e migration.Endpoint) (fs.Fs, error)
}

func NewRclone(opt copier.Options) *Rclone {
	return &Rclone{Options: opt, PreflightTimeout: 15 * time.Second, open: copier.Open}
}

func (r *Rclone) Name() string { return "rclone" }

// ReverseSync is false until switching is implemented: nothing writes to the
// target, so there is nothing to copy back.
func (r *Rclone) ReverseSync() bool { return false }

func (r *Rclone) Preflight(ctx context.Context, source, target migration.Endpoint) []migration.Check {
	var checks []migration.Check
	check := func(name string, fn func(context.Context) error) bool {
		ctx, cancel := context.WithTimeout(ctx, r.PreflightTimeout)
		defer cancel()
		c := migration.Check{Name: name, OK: true}
		if err := fn(ctx); err != nil {
			c.OK, c.Message = false, err.Error()
		}
		checks = append(checks, c)
		return c.OK
	}

	check("source_read", func(ctx context.Context) error {
		f, err := r.open(ctx, "source", source)
		if err != nil {
			return err
		}
		return copier.ProbeList(ctx, f)
	})
	var dst fs.Fs
	if check("target_read", func(ctx context.Context) (err error) {
		if dst, err = r.open(ctx, "target", target); err != nil {
			return err
		}
		return copier.ProbeList(ctx, dst)
	}) {
		check("target_write", func(ctx context.Context) error { return copier.ProbeWrite(ctx, dst) })
	}
	return checks
}

func (r *Rclone) InitialSync(ctx context.Context, m migration.Migration, report func(migration.Progress)) error {
	src, dst, err := r.pair(ctx, m)
	if err != nil {
		return err
	}
	total, totalBytes, err := copier.Count(ctx, src)
	if err != nil {
		return fmt.Errorf("count source objects: %w", err)
	}
	progress := func(res copier.Result) migration.Progress {
		done, bytes := res.Copied+res.Matched, res.Bytes+res.MatchedBytes
		// Objects written to the source during the copy, and retried
		// transfers, can push past the count taken before it.
		return migration.Progress{
			Completed: done, Total: max(total, done), Unit: "objects",
			Bytes: bytes, TotalBytes: max(totalBytes, bytes),
		}
	}
	report(progress(copier.Result{}))
	res, err := copier.Copy(ctx, src, dst, r.Options, func(res copier.Result) { report(progress(res)) })
	if err != nil {
		return fmt.Errorf("copy: %w", err)
	}
	report(progress(res))
	return nil
}

// Sync copies what is new since the last round. Cutover may start once a
// round copies nothing and the target holds every source object.
func (r *Rclone) Sync(ctx context.Context, m migration.Migration) (SyncReport, error) {
	src, dst, err := r.pair(ctx, m)
	if err != nil {
		return SyncReport{}, err
	}
	res, err := copier.Copy(ctx, src, dst, r.Options, nil)
	if err != nil {
		return SyncReport{}, fmt.Errorf("copy: %w", err)
	}
	idle := migration.Check{
		Name:    "no_new_objects",
		OK:      res.Copied == 0,
		Message: fmt.Sprintf("last round copied %d objects", res.Copied),
	}
	if !idle.OK {
		return SyncReport{Cutover: migration.Decide(idle), Backlog: res.Copied}, nil
	}
	same, d, err := r.compare(ctx, src, dst)
	if err != nil {
		return SyncReport{}, err
	}
	return SyncReport{Cutover: migration.Decide(idle, same), Backlog: d.Missing + d.Differ}, nil
}

func (r *Rclone) Verify(ctx context.Context, m migration.Migration) (VerifyReport, error) {
	src, dst, err := r.pair(ctx, m)
	if err != nil {
		return VerifyReport{}, err
	}
	same, _, err := r.compare(ctx, src, dst)
	if err != nil {
		return VerifyReport{}, err
	}
	return VerifyReport{
		Advance: migration.Decide(same),
		Rollback: migration.Decide(migration.Check{
			Name:    "source_unchanged",
			OK:      true,
			Message: "the app still uses the source; switching is not implemented yet",
		}),
	}, nil
}

func (r *Rclone) compare(ctx context.Context, src, dst fs.Fs) (migration.Check, copier.Diff, error) {
	d, err := copier.Check(ctx, src, dst, r.Options)
	if err != nil {
		return migration.Check{}, d, fmt.Errorf("check: %w", err)
	}
	return migration.Check{
		Name:    "target_matches_source",
		OK:      d.Same(),
		Message: fmt.Sprintf("%d missing, %d different, %d identical", d.Missing, d.Differ, d.Matched),
	}, d, nil
}

// pair connects to both sides of a migration. A credentials reference that
// cannot be resolved is permanent: retrying cannot fix it.
func (r *Rclone) pair(ctx context.Context, m migration.Migration) (src, dst fs.Fs, err error) {
	if src, err = r.open(ctx, "source", m.Source); err != nil {
		return nil, nil, openError("source", err)
	}
	if dst, err = r.open(ctx, "target", m.Target); err != nil {
		return nil, nil, openError("target", err)
	}
	return src, dst, nil
}

func openError(side string, err error) error {
	if errors.Is(err, copier.ErrBadRef) {
		return fmt.Errorf("%w: open %s: %w", ErrPermanent, side, err)
	}
	return fmt.Errorf("open %s: %w", side, err)
}

// Switching is not implemented yet, so these steps have nothing to do.

func (r *Rclone) Freeze(context.Context, migration.Migration) error   { return nil }
func (r *Rclone) Switch(context.Context, migration.Migration) error   { return nil }
func (r *Rclone) Resume(context.Context, migration.Migration) error   { return nil }
func (r *Rclone) Finalize(context.Context, migration.Migration) error { return nil }
func (r *Rclone) Rollback(context.Context, migration.Migration) error { return nil }
func (r *Rclone) Abort(context.Context, migration.Migration) error    { return nil }
