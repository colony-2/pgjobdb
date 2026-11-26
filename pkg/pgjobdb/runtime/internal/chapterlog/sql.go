package chapterlog

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync/atomic"
)

type creationKey struct{}

type creation struct {
	route      *connector
	tx         *sql.Tx
	rolledBack atomic.Bool
}

// connector adapts JobDB's *sql.DB-only store to the caller's pool. The bridge
// is unbounded and retains no idle connections: each SQL operation (or Begin)
// gets a connection for its own context. Only Create carries a transaction;
// ordinary calls borrow a connection and pass through to the caller's pool.
type connector struct{ db *sql.DB }

func (route *connector) Driver() driver.Driver { return route }

func (*connector) Open(string) (driver.Conn, error) {
	return nil, errors.New("postgres chapter log: use connector")
}

func (route *connector) Connect(ctx context.Context) (driver.Conn, error) {
	if operation, ok := ctx.Value(creationKey{}).(*creation); ok && operation.route == route {
		if operation.tx == nil {
			tx, err := route.db.BeginTx(ctx, nil)
			if err != nil {
				return nil, err
			}
			operation.tx = tx
		}
		return &conn{creation: operation}, nil
	}
	dbConn, err := route.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &conn{db: dbConn}, nil
}

type conn struct {
	db       *sql.Conn
	tx       *sql.Tx
	creation *creation
}

func (*conn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("postgres chapter log: expected a direct query")
}

func (connection *conn) Close() error {
	if connection.db != nil {
		return connection.db.Close()
	}
	return nil
}

func (connection *conn) Ping(ctx context.Context) error {
	if connection.creation != nil {
		return ctx.Err()
	}
	return connection.db.PingContext(ctx)
}

func (connection *conn) Begin() (driver.Tx, error) {
	return connection.BeginTx(context.Background(), driver.TxOptions{})
}

func (connection *conn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	if operation := connection.creation; operation != nil {
		if options != (driver.TxOptions{}) {
			return nil, errors.New("postgres chapter log: unexpected initial chapter transaction options")
		}
		return createAppend{operation: operation}, nil
	}
	tx, err := connection.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.IsolationLevel(options.Isolation), ReadOnly: options.ReadOnly})
	if err != nil {
		return nil, err
	}
	connection.tx = tx
	return tx, nil
}

// JobDB uses a separate default transaction to append the initial chapter.
// Defer its commit to Create, and never publish if its append rolls back.
type createAppend struct{ operation *creation }

func (createAppend) Commit() error { return nil }
func (appendTx createAppend) Rollback() error {
	appendTx.operation.rolledBack.Store(true)
	return nil
}

type executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func (connection *conn) executor() executor {
	if connection.creation != nil {
		return connection.creation.tx
	}
	if connection.tx != nil {
		return connection.tx
	}
	return connection.db
}

func arguments(values []driver.NamedValue) []any {
	args := make([]any, len(values))
	for index, item := range values {
		if item.Name != "" {
			args[index] = sql.Named(item.Name, item.Value)
		} else {
			args[index] = item.Value
		}
	}
	return args
}

func (connection *conn) ExecContext(ctx context.Context, query string, values []driver.NamedValue) (driver.Result, error) {
	return connection.executor().ExecContext(ctx, query, arguments(values)...)
}

func (connection *conn) QueryContext(ctx context.Context, query string, values []driver.NamedValue) (driver.Rows, error) {
	result, err := connection.executor().QueryContext(ctx, query, arguments(values)...)
	if err != nil {
		return nil, err
	}
	cols, err := result.Columns()
	if err != nil {
		_ = result.Close()
		return nil, err
	}
	return &rows{result: result, cols: cols, scan: make([]any, len(cols))}, nil
}

type rows struct {
	result *sql.Rows
	cols   []string
	scan   []any
}

func (result *rows) Columns() []string { return result.cols }
func (result *rows) Close() error      { return result.result.Close() }

func (result *rows) Next(dest []driver.Value) error {
	if !result.result.Next() {
		if err := result.result.Err(); err != nil {
			return err
		}
		return io.EOF
	}
	for index := range dest {
		result.scan[index] = (*any)(&dest[index])
	}
	return result.result.Scan(result.scan...)
}
