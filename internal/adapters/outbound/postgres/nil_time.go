package postgres

import (
	"database/sql"
	"time"
)

// nilTime is a small wrapper around sql.NullTime that exposes the same
// "did the column have a value" semantics with a tighter API. We use it
// directly with pgx's Row.Scan because pgx accepts the underlying
// sql.Scanner interface and we want consumed_at IS NULL to round-trip
// to a typed absent value rather than the time.Time zero value, which
// is indistinguishable from "1 January Year 1" data in the wild.
type nilTime struct {
	t   time.Time
	set bool
}

// Scan implements sql.Scanner.
func (n *nilTime) Scan(src any) error {
	var nt sql.NullTime
	if err := nt.Scan(src); err != nil {
		return err
	}
	n.t = nt.Time
	n.set = nt.Valid
	return nil
}
