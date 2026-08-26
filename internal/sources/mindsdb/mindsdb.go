// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mindsdb

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/goccy/go-yaml"
	"github.com/googleapis/mcp-toolbox/internal/sources"
	"github.com/googleapis/mcp-toolbox/internal/tools/mysql/mysqlcommon"
	"go.opentelemetry.io/otel/trace"
)

const SourceType string = "mindsdb"

// validate interface
var _ sources.SourceConfig = Config{}

func init() {
	if !sources.Register(SourceType, newConfig) {
		panic(fmt.Sprintf("source type %q already registered", SourceType))
	}
}

func newConfig(ctx context.Context, name string, decoder *yaml.Decoder) (sources.SourceConfig, error) {
	actual := Config{Name: name}
	if err := decoder.DecodeContext(ctx, &actual); err != nil {
		return nil, err
	}
	return actual, nil
}

type Config struct {
	Name         string `yaml:"name" validate:"required"`
	Type         string `yaml:"type" validate:"required"`
	Host         string `yaml:"host" validate:"required"`
	Port         string `yaml:"port" validate:"required"`
	User         string `yaml:"user" validate:"required"`
	Password     string `yaml:"password"`
	Database     string `yaml:"database" validate:"required"`
	QueryTimeout string `yaml:"queryTimeout"`
}

func (r Config) SourceConfigType() string {
	return SourceType
}

func (r Config) Initialize(ctx context.Context, tracer trace.Tracer, lazy bool) (sources.Source, error) {
	s := r.newSource(ctx, tracer)
	if lazy {
		return s, nil
	}
	if _, err := s.pool(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func (r Config) newSource(ctx context.Context, tracer trace.Tracer) *Source {
	return &Source{
		Config: r,
		tracer: tracer,
		conn:   sources.NewConnectOnce[*sql.DB](ctx, r.Name, SourceType, tracer),
	}
}

var _ sources.Source = &Source{}

type Source struct {
	Config
	tracer trace.Tracer
	conn   *sources.ConnectOnce[*sql.DB]
}

// pool returns the connection pool, creating it on first use.
func (s *Source) pool(ctx context.Context) (*sql.DB, error) {
	return s.conn.Do(ctx, func(ctx context.Context) (*sql.DB, error) {
		pool, err := initMindsDBConnectionPool(ctx, s.tracer, s.Name, s.Host, s.Port, s.User, s.Password, s.Database, s.QueryTimeout)
		if err != nil {
			return nil, fmt.Errorf("unable to create pool: %w", err)
		}
		if err := pool.PingContext(ctx); err != nil {
			return nil, fmt.Errorf("unable to connect successfully: %w", err)
		}
		return pool, nil
	})
}

func (s *Source) IsReadOnly() bool {
	return false
}

func (s *Source) SourceType() string {
	return SourceType
}

func (s *Source) ToConfig() sources.SourceConfig {
	return s.Config
}

// MindsDBPool reports the pool if one has been made. It is the type
// discriminator tools assert on; a deferred source has not connected yet, so
// callers that need a live pool must go through a method that takes a context.
func (s *Source) MindsDBPool() *sql.DB {
	pool, _ := s.conn.Get()
	return pool
}

// MySQLPool reports the pool if one has been made. Like MindsDBPool it is a
// type discriminator, here for the MySQL tools.
func (s *Source) MySQLPool() *sql.DB {
	pool, _ := s.conn.Get()
	return pool
}

func (s *Source) RunSQL(ctx context.Context, statement string, params []any) (any, error) {
	pool, err := s.pool(ctx)
	if err != nil {
		return nil, err
	}
	// MindsDB now supports MySQL prepared statements natively
	results, err := pool.QueryContext(ctx, statement, params...)
	if err != nil {
		return nil, fmt.Errorf("unable to execute query: %w", err)
	}

	cols, err := results.Columns()
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve rows column name: %w", err)
	}

	// create an array of values for each column, which can be re-used to scan each row
	rawValues := make([]any, len(cols))
	values := make([]any, len(cols))
	for i := range rawValues {
		values[i] = &rawValues[i]
	}
	defer results.Close()

	colTypes, err := results.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("unable to get column types: %w", err)
	}

	out := []any{}
	for results.Next() {
		err := results.Scan(values...)
		if err != nil {
			return nil, fmt.Errorf("unable to parse row: %w", err)
		}
		vMap := make(map[string]any)
		for i, name := range cols {
			val := rawValues[i]
			if val == nil {
				vMap[name] = nil
				continue
			}

			// MindsDB uses mysql driver
			vMap[name], err = mysqlcommon.ConvertToType(colTypes[i], val)
			if err != nil {
				return nil, fmt.Errorf("errors encountered when converting values: %w", err)
			}
		}
		out = append(out, vMap)
	}

	if err := results.Err(); err != nil {
		return nil, fmt.Errorf("errors encountered during row iteration: %w", err)
	}

	return out, nil
}

func initMindsDBConnectionPool(ctx context.Context, tracer trace.Tracer, name, host, port, user, pass, dbname, queryTimeout string) (*sql.DB, error) {
	//nolint:all // Reassigned ctx
	ctx, span := sources.InitConnectionSpan(ctx, tracer, SourceType, name)
	defer span.End()

	// Configure the driver to connect to the database
	var dsn string
	if pass == "" {
		// Connect without password
		dsn = fmt.Sprintf("%s@tcp(%s:%s)/%s?parseTime=true", user, host, port, dbname)
	} else {
		// Connect with password
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", user, pass, host, port, dbname)
	}

	// Add query timeout to DSN if specified
	if queryTimeout != "" {
		timeout, err := time.ParseDuration(queryTimeout)
		if err != nil {
			return nil, fmt.Errorf("invalid queryTimeout %q: %w", queryTimeout, err)
		}
		dsn += "&readTimeout=" + timeout.String()
	}

	// Interact with the driver directly as you normally would
	pool, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("sql.Open: %w", err)
	}
	return pool, nil
}
