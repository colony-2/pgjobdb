package installer

import (
	"context"
	"database/sql"
	"testing"
)

func TestInstallerValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	inst := Installer{}
	if err := inst.Apply(ctx); err == nil {
		t.Fatalf("expected error for nil DB apply")
	}
	inst.DB = new(sql.DB)
	if err := inst.Apply(nil); err == nil {
		t.Fatalf("expected error for nil context apply")
	}
	inst.DB = nil
	if err := inst.Verify(ctx); err == nil {
		t.Fatalf("expected error for nil DB verify")
	}
}
