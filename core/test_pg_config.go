package core

import (
	"fmt"
	"net/url"
	"os"
)

// EnvTestPostgresURL is the environment variable name used to configure
// the PostgreSQL connection string for the test suite.
const EnvTestPostgresURL = "TEST_POSTGRES_URL"

// defaultTestPostgresURL is the fallback connection string used when
// the TEST_POSTGRES_URL environment variable is not set.
const defaultTestPostgresURL = "postgres://postgres:postgres@127.0.0.1:5432/pb-pg-test?sslmode=disable"

// TestPostgresURL returns the PostgreSQL connection string to use for
// tests, reading from the TEST_POSTGRES_URL env var with a fallback
// to defaultTestPostgresURL.
func TestPostgresURL() string {
	if v := os.Getenv(EnvTestPostgresURL); v != "" {
		return v
	}
	return defaultTestPostgresURL
}

// TestPostgresCreds holds parsed PostgreSQL connection components
// extracted from the test connection string.
type TestPostgresCreds struct {
	URL      string
	User     string
	Password string
	Host     string
	Port     string
	Database string
}

// TestPostgresCredentials parses TestPostgresURL() and returns the
// individual connection components used for shell commands.
func TestPostgresCredentials() TestPostgresCreds {
	raw := TestPostgresURL()
	u, err := url.Parse(raw)
	if err != nil {
		panic(fmt.Sprintf("invalid %s: %s", EnvTestPostgresURL, err))
	}

	creds := TestPostgresCreds{
		URL:      raw,
		User:     u.User.Username(),
		Password: "",
		Host:     u.Hostname(),
		Port:     u.Port(),
		Database: u.Path,
	}
	if p, ok := u.User.Password(); ok {
		creds.Password = p
	}
	if creds.Port == "" {
		creds.Port = "5432"
	}
	if len(creds.Database) > 0 && creds.Database[0] == '/' {
		creds.Database = creds.Database[1:]
	}

	return creds
}

// PgEnvCommand returns a shell prefix string with PGPASSWORD set.
func (c TestPostgresCreds) PgEnvCommand() string {
	return fmt.Sprintf("PGPASSWORD=%s ", c.Password)
}

// CreatedbCommand returns a shell command string to create the given database.
func (c TestPostgresCreds) CreatedbCommand(dbName string) string {
	return fmt.Sprintf("%screatedb -h %s -p %s -U %s %s", c.PgEnvCommand(), c.Host, c.Port, c.User, dbName)
}

// DropdbCommand returns a shell command string to drop the given database (if exists).
func (c TestPostgresCreds) DropdbCommand(dbName string) string {
	return fmt.Sprintf("%sdropdb --if-exists -h %s -p %s -U %s %s", c.PgEnvCommand(), c.Host, c.Port, c.User, dbName)
}

// PsqlCommand returns a shell command string to run a psql file import.
func (c TestPostgresCreds) PsqlCommand(dbName, file string) string {
	return fmt.Sprintf("%spsql -h %s -p %s -U %s -d %s < %s", c.PgEnvCommand(), c.Host, c.Port, c.User, dbName, file)
}
