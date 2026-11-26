// Command migrate applies SQL migrations from MIGRATIONS_DIR (default
// ./migrations) to DATABASE_URL.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/avaargsh/compute-control-panel/internal/migrate"
)

func printUsage(w io.Writer) {
	fmt.Fprintln(w, `Usage: migrate
       migrate --help

Applies forward-only SQL migrations.
Set DATABASE_URL to the PostgreSQL connection string.
MIGRATIONS_DIR defaults to ./migrations.`)
}

func main() {
	if args := os.Args[1:]; len(args) != 0 {
		if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
			printUsage(os.Stdout)
			return
		}
		fmt.Fprintln(os.Stderr, "migrate does not accept arguments other than -h or --help.")
		printUsage(os.Stderr)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		slog.Error("DATABASE_URL is required")
		os.Exit(2)
	}
	dir := os.Getenv("MIGRATIONS_DIR")
	if dir == "" {
		dir = "migrations"
	}
	migrations, err := migrate.Load(os.DirFS(dir))
	if err != nil {
		slog.Error("load migrations", "error", err)
		os.Exit(1)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		slog.Error("open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	applied, err := migrate.Apply(ctx, db, migrations)
	if err != nil {
		slog.Error("apply migrations", "error", err, "applied", applied)
		os.Exit(1)
	}
	slog.Info("migrations complete", "applied", applied)
}
