// Copyright (c) 2022, Nicolas Thauvin All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

package cmd

import (
	"context"
	"fmt"
	"github.com/orgrim/pg_happy/pg"
	"github.com/orgrim/pg_happy/store"
	"github.com/spf13/cobra"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	// Global options
	ConnString string
	LocalStore string

	// Load options
	Timeout  string
	Pause    string
	Truncate bool
	Size     int

	// Compare options
	NoLoad bool

	rootCmd = &cobra.Command{
		Use:   "pg_happy",
		Short: "A test application for HA setups of PostgreSQL",
		Long: `pg_happy is a small application to help test high available setups of PostgreSQL
by sending data and making possible to know if some data were lost during
failover.`,
		Version:      "0.1.0",
		SilenceUsage: true,
	}

	initCmd = &cobra.Command{
		Use:   "init",
		Short: "Initialize the schema of the application",
		Long:  "Initialize the schema of the application in the database",
		RunE:  initDB,
	}

	loadCmd = &cobra.Command{
		Use:   "load",
		Short: "Insert data into the database",
		Long:  "Insert data into the database and save it locally to allow comparison",
		RunE:  loadDB,
	}

	compareCmd = &cobra.Command{
		Use:   "compare",
		Short: "Compare the local store with the database",
		RunE:  compareDB,
	}
)

func init() {
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(loadCmd)
	rootCmd.AddCommand(compareCmd)

	rootCmd.PersistentFlags().StringVarP(&ConnString, "db-url", "d", "", "connection string or URL to PostgreSQL")
	rootCmd.PersistentFlags().StringVarP(&LocalStore, "store", "s", "/tmp/pg_happy.data", "path to the local file storing data send to PostgreSQL")

	loadCmd.Flags().StringVarP(&Timeout, "timeout", "t", "5s", "timeout when interacting with PostgreSQL")
	loadCmd.Flags().StringVarP(&Pause, "pause", "p", "500ms", "pause between transactions")
	loadCmd.Flags().BoolVarP(&Truncate, "truncate", "T", false, "truncate tables and files before sending data")
	loadCmd.Flags().IntVarP(&Size, "size", "S", 10, "payload size in bytes")

	compareCmd.Flags().BoolVarP(&NoLoad, "no-load", "n", false, "do not load local file to database")
}

// Execute run the application through cobra
func Execute(ctx context.Context) error {
	return rootCmd.ExecuteContext(ctx)
}

func initDB(cmd *cobra.Command, args []string) error {
	baseCtx := cmd.Context()
	ctx, cancel := context.WithTimeout(baseCtx, 5*time.Second)

	// connect
	db, err := pg.NewDB(ctx, ConnString)
	cancel()
	if err != nil {
		return fmt.Errorf("could not connect: %w", err)
	}

	defer db.CloseWithTimeout(baseCtx, 5*time.Second)

	// ddl
	ctx, cancel = context.WithTimeout(baseCtx, 5*time.Second)
	defer cancel()
	if err := pg.CreateSchema(ctx, db); err != nil {
		return err
	}

	log.Println("Database schema initialized successfully")

	return nil
}

func loadDB(cmd *cobra.Command, args []string) error {
	// process and validate options
	timeout, err := time.ParseDuration(Timeout)
	if err != nil {
		return fmt.Errorf("invalid timeout value: %w", err)
	}

	pause, err := time.ParseDuration(Pause)
	if err != nil {
		return fmt.Errorf("invalid pause value: %w", err)
	}

	baseCtx, baseCancel := context.WithCancel(cmd.Context())

	if Size <= 0 {
		return fmt.Errorf("invalid size for payload: too small")
	}

	// Setup
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)

	var (
		db        *pg.DB
		id        int
		connected bool
	)

	st, err := store.NewStore(LocalStore, Truncate)
	if err != nil {
		return err
	}

	log.Println("generating random payload")
	payload := make([]byte, Size)
	for i := 0; i < Size; i++ {
		// generate a number between 32 and 126, the ascii visible
		// characters
		c := (rand.Uint32() & 94) + 32
		payload[i] = byte(c)
	}

mainLoop:
	for {
		// end the loop if requested
		select {
		case sig := <-sigC:
			log.Printf("received signal %s, exiting", sig)
			baseCancel()
			break mainLoop
		default:
		}

		// get a connection
		if !connected {
			var err error

			ctx, cancel := context.WithTimeout(baseCtx, timeout)
			log.Println("connecting to PostgreSQL")
			db, err = pg.NewDB(ctx, ConnString)
			if err != nil {
				if ctx.Err() == context.Canceled {
					return nil
				}

				log.Printf("could not connect: %s", err)
				cancel()
				time.Sleep(pause)
				continue
			}

			connected = true

			defer db.CloseWithTimeout(baseCtx, timeout)
		}

		// avoid unicity violation by getting the greatest id on first run
		if id == 0 {
			var err error

			ctx, cancel := context.WithTimeout(baseCtx, timeout)

			if Truncate {
				log.Println("truncating tables")
				err = pg.TruncateTables(ctx, db)
				cancel()
				if err != nil {
					log.Printf("could not truncate tables: %s\n", err)
				}

				id = 1
			} else {
				log.Println("getting next id")
				id, err = pg.GetNextId(ctx, db)
				cancel()
				if err != nil {
					log.Printf("could not get next id: %s", err)
				}
			}

			if err != nil {
				if db.Conn.IsClosed() {
					connected = false
				}

				time.Sleep(pause)
				continue
			}

			log.Printf("next id is: %d\n", id)
		}

		ts := time.Now()

		// save locally
		if err := st.Append(id, ts); err != nil {
			return fmt.Errorf("could not store data: %w", err)
		}

		// insert the same data into the database
		log.Printf("insert data: id=%d\n", id)
		err := pg.InsertData(baseCtx, db, timeout, id, ts, string(payload))
		if err != nil {
			log.Printf("could not insert (%v, %v): %s", id, ts, err)
		}

		// Force reconnection
		if db.Conn.IsClosed() {
			connected = false
		}

		id++

		time.Sleep(pause)
	}

	return nil
}

func compareDB(cmd *cobra.Command, args []string) error {
	baseCtx := cmd.Context()
	timeout := 5 * time.Second

	st, err := store.NewStore(LocalStore, false)
	if err != nil {
		return err
	}

	defer st.Close()

	ctx, cancel := context.WithTimeout(baseCtx, timeout)
	log.Println("connecting to PostgreSQL")
	db, err := pg.NewDB(ctx, ConnString)
	cancel()
	if err != nil {
		return fmt.Errorf("could not connect: %w", err)
	}

	defer db.CloseWithTimeout(baseCtx, timeout)

	// copy all the data from the file to the database
	if !NoLoad {
		log.Println("copying store to database")
		ctx, cancel = context.WithTimeout(baseCtx, timeout)
		count, err := pg.CopyStore(ctx, db, st)
		cancel()
		if err != nil {
			return err
		}

		log.Printf("copied %d rows", count)
	}

	// query with a full join
	ctx, cancel = context.WithTimeout(baseCtx, timeout)
	diff, err := pg.Compare(ctx, db)
	cancel()
	if err != nil {
		return err
	}

	// show stats
	log.Printf("differences: %d\n", len(diff))
	for _, d := range diff {
		log.Printf("%+v\n", d)
	}

	return nil
}
