package neontestdb

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"gotest.tools/assert"

	_ "github.com/joho/godotenv/autoload"
)

func init() {
	SetDefaultBranch("main")
}

func TestConnectionWithBranchNew(t *testing.T) {
	for i := 0; i < 2; i++ {
		LoadClient().UsingTestBranch(t, func(uri ConnectionURI) {
			ctx := context.Background()
			db, err := pgx.Connect(ctx, uri.ConnectionURI)
			assert.NilError(t, err)
			defer db.Close(ctx)
			err = db.Ping(ctx)
			assert.NilError(t, err)
		})
	}
}
