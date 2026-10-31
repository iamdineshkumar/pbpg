package tests

import "github.com/postgresbase/pocketbase/core"

// Re-export the PostgreSQL test config helpers from core so that
// test files importing the tests package can use them directly.
//
// The canonical definitions live in core/test_pg_config.go to avoid
// a circular import (core cannot import tests).

type TestPostgresCreds = core.TestPostgresCreds

func TestPostgresURL() string {
	return core.TestPostgresURL()
}

func TestPostgresCredentials() core.TestPostgresCreds {
	return core.TestPostgresCredentials()
}
