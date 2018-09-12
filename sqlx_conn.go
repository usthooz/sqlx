package sqlx

import (
	"context"
	"database/sql"
)

// Conn
type Conn struct {
	*sql.Conn
	db *DB
}

// Conn
func (db *DB) Conn(ctx context.Context) (*Conn, error) {
	conn, err := db.DB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &Conn{
		Conn: conn,
		db:   db,
	}, nil
}

// Close
func (c *Conn) Close() {
	c.Conn.Close()
}

// DriverName
func (c *Conn) DriverName() string {
	return c.db.DriverName()
}

// Rebind
func (c *Conn) Rebind(query string) string {
	return c.db.Rebind(query)
}

// BindNamed
func (c *Conn) BindNamed(query string, arg interface{}) (string, []interface{}, error) {
	return c.db.BindNamed(query, arg)
}

// BeginTxx
func (c *Conn) BeginTxx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := c.Conn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{Tx: tx, driverName: c.db.driverName, unsafe: c.db.unsafe, Mapper: c.db.Mapper}, err
}

// Beginx
func (c *Conn) Beginx() (*Tx, error) {
	return c.BeginTxx(context.Background(), nil)
}

// PrepareNamedContext
func (c *Conn) PrepareNamedContext(ctx context.Context, query string) (*NamedStmt, error) {
	return prepareNamedContext(ctx, c, query)
}

// NamedQueryContext
func (c *Conn) NamedQueryContext(ctx context.Context, query string, arg interface{}) (*Rows, error) {
	return NamedQueryContext(ctx, c, query, arg)
}

// NamedExecContext
func (c *Conn) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	return NamedExecContext(ctx, c, query, arg)
}

// PreparexContext
func (c *Conn) PreparexContext(ctx context.Context, query string) (*Stmt, error) {
	return PreparexContext(ctx, c, query)
}

// SelectContext
func (c *Conn) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return SelectContext(ctx, c, dest, query, args...)
}

// GetContext
func (c *Conn) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return GetContext(ctx, c, dest, query, args...)
}

// QueryxContext
func (c *Conn) QueryxContext(ctx context.Context, query string, args ...interface{}) (*Rows, error) {
	r, err := c.Conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: r, unsafe: c.db.unsafe, Mapper: c.db.Mapper}, err
}

// QueryRowxContext
func (c *Conn) QueryRowxContext(ctx context.Context, query string, args ...interface{}) *Row {
	rows, err := c.Conn.QueryContext(ctx, query, args...)
	return &Row{rows: rows, err: err, unsafe: c.db.unsafe, Mapper: c.db.Mapper}
}
