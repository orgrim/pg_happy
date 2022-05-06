// Copyright (c) 2022, Nicolas Thauvin All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

package pg

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/orgrim/pg_happy/store"
	"time"
)

type DB struct {
	Conn *pgx.Conn
}

func NewDB(ctx context.Context, connstring string) (*DB, error) {
	c, err := pgx.Connect(ctx, connstring)
	if err != nil {
		return nil, err
	}

	return &DB{Conn: c}, nil
}

func (d *DB) CloseWithTimeout(ctx context.Context, timeout time.Duration) error {
	closeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return d.Conn.Close(closeCtx)
}

func runXact(ctx context.Context, d *DB, sqlCommands []string) error {
	tx, err := d.Conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("could not start transaction: %w", err)
	}

	for _, q := range sqlCommands {
		_, err := tx.Exec(ctx, q)
		if err != nil {
			werr := fmt.Errorf("query failed: %w, query: %s", err, q)
			cerr := tx.Rollback(ctx)
			if cerr != nil {
				return fmt.Errorf("xact rollback failed: %w", werr)
			}

			return werr
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("xact commit failed: %w", err)
	}

	return nil
}

func CreateSchema(ctx context.Context, d *DB) error {
	sqlCommands := []string{
		"create schema if not exists happy",
		"create table if not exists happy.stamps ( id int primary key, ts timestamptz not null )",
		"create unlogged table if not exists happy.store ( id int primary key, ts timestamptz not null )",
	}

	return runXact(ctx, d, sqlCommands)
}

func TruncateTables(ctx context.Context, d *DB) error {
	sqlCommands := []string{
		"truncate happy.stamps",
		"truncate happy.store",
	}

	return runXact(ctx, d, sqlCommands)
}

func InsertData(ctx context.Context, d *DB, timeout time.Duration, id int, ts time.Time) error {
	insCtx, cancel := context.WithTimeout(ctx, timeout)
	tx, err := d.Conn.Begin(insCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("could not start transaction: %w", err)
	}

	insCtx, cancel = context.WithTimeout(ctx, timeout)
	_, err = tx.Exec(insCtx, "insert into happy.stamps (id, ts) values ($1, $2)", id, ts)
	cancel()
	if err != nil {
		werr := fmt.Errorf("query failed: %w", err)

		if insCtx.Err() == nil {
			insCtx, cancel := context.WithTimeout(ctx, timeout)
			cerr := tx.Rollback(insCtx)
			cancel()
			if cerr != nil {
				return fmt.Errorf("xact rollback failed: %w and %s: %w", cerr, werr)
			}
		}

		return werr
	}

	insCtx, cancel = context.WithTimeout(ctx, timeout)
	err = tx.Commit(insCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("xact commit failed: %w", err)
	}

	return nil
}

func GetNextId(ctx context.Context, d *DB) (int, error) {
	var id int

	err := d.Conn.QueryRow(ctx, "select coalesce(max(id)+1, 1) from happy.stamps").Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("query failed: %w", err)
	}

	return id, nil
}

func CopyStore(ctx context.Context, d *DB, st *store.Store) (int64, error) {
	count, err := d.Conn.CopyFrom(ctx, pgx.Identifier{"happy", "store"}, []string{"id", "ts"}, st)
	if err != nil {
		return 0, fmt.Errorf("could not load store contents to database: %w", err)
	}

	return count, nil
}

func Compare(ctx context.Context, d *DB) ([]store.StoreData, error) {
	rows, err := d.Conn.Query(ctx, "select r.id, r.ts from happy.stamps s full join happy.store r using (id) where s.id is null;")
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	data := make([]store.StoreData, 0)
	for rows.Next() {
		var (
			i int
			t time.Time
		)

		err = rows.Scan(&i, &t)
		if err != nil {
			return data, err
		}

		data = append(data, store.StoreData{Id: i, Ts: t})
	}

	if rows.Err() != nil {
		return data, rows.Err()
	}

	return data, nil
}
