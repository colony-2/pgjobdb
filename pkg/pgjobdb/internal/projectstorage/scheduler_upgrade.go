package projectstorage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
)

const leaseActivityFormat = 16

const certificateTable = `CREATE TABLE pgjobdb.project_scheduler_upgrade (
 singleton integer PRIMARY KEY CHECK (singleton=1),
 source_format integer NOT NULL CHECK (source_format BETWEEN 13 AND 15),
 target_format integer NOT NULL CHECK (target_format=16),
 source_receipt_sha256 text NOT NULL,
 source_structure_sha256 text NOT NULL,
 target_structure_sha256 text NOT NULL,
 binding_sha256 text NOT NULL
)`

func schedulerFormat(ctx context.Context, q Queryer) (int, error) {
	var format int
	err := q.QueryRowContext(ctx, `SELECT format_version FROM pgjobdb.installation WHERE name='pgjobdb'`).Scan(&format)
	return format, err
}

const schedulerClockCheck = `SELECT count(*)=1 AND count(*) FILTER (WHERE
 p.pronargs=0 AND p.prokind='f' AND p.prorettype='pg_catalog.timestamptz'::regtype AND NOT p.proretset
 AND l.lanname='sql' AND p.provolatile='v' AND NOT p.proisstrict AND NOT p.prosecdef
 AND NOT p.proleakproof AND p.proparallel='u' AND p.proconfig IS NULL
 AND p.prosrc=' SELECT clock_timestamp(); ' AND p.prosqlbody IS NULL)=1
 FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang
 WHERE p.pronamespace='pgjobdb'::regnamespace AND p.proname='scheduler_now'`

const schedulerClockError = "pgjobdb: missing or altered scheduler clock pgjobdb.scheduler_now()"

func VerifySchedulerClock(ctx context.Context, q Queryer) error {
	var valid bool
	err := q.QueryRowContext(ctx, schedulerClockCheck).Scan(&valid)
	if err != nil {
		return err
	}
	if !valid {
		return fmt.Errorf("%s", schedulerClockError)
	}
	return nil
}

func historicalStructure(receipt Receipt, priorStructure sql.NullString, format int) (string, bool) {
	if format < 13 || format >= leaseActivityFormat || receipt.SchedulerFormat != 0 || (format == 13 && priorStructure.Valid) {
		return "", false
	}
	digest := receipt.Structure
	if priorStructure.Valid {
		digest = priorStructure.String
	}
	value, err := hex.DecodeString(digest)
	return digest, err == nil && len(value) == sha256.Size
}

func upgradeBinding(source, target int, receipt, sourceStructure, targetStructure string) string {
	return Digest([]byte(fmt.Sprintf("pgjobdb.scheduler-upgrade\n%d\n%d\n%s\n%s\n%s", source, target, receipt, sourceStructure, targetStructure)))
}

func verifySchedulerBinding(ctx context.Context, q Queryer, receipt Receipt, receiptBytes []byte, format int, certified bool, priorStructure sql.NullString) error {
	want, historical := historicalStructure(receipt, priorStructure, format)
	switch {
	case historical && !certified:
	case format == leaseActivityFormat && receipt.SchedulerFormat == leaseActivityFormat && !certified && !priorStructure.Valid:
		want = receipt.Structure
	case format == leaseActivityFormat && receipt.SchedulerFormat == 0 && certified:
		var source, target int
		var originalReceipt, originalStructure, binding string
		err := q.QueryRowContext(ctx, `SELECT source_format,target_format,source_receipt_sha256,source_structure_sha256,target_structure_sha256,binding_sha256 FROM pgjobdb.project_scheduler_upgrade WHERE singleton=1`).Scan(&source, &target, &originalReceipt, &originalStructure, &want, &binding)
		expectedSource, validSource := historicalStructure(receipt, priorStructure, source)
		if err != nil || !validSource || target != leaseActivityFormat || originalReceipt != Digest(receiptBytes) || originalStructure != expectedSource || binding != upgradeBinding(source, target, originalReceipt, originalStructure, want) {
			return fmt.Errorf("invalid project scheduler upgrade certification")
		}
	default:
		return fmt.Errorf("missing or unexpected project scheduler upgrade certification for format %d", format)
	}
	got, err := structure(ctx, q, format)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("project storage definitions or guard bindings changed")
	}
	return nil
}

type UpgradeSource struct {
	format          int
	receiptSHA256   string
	structureSHA256 string
}

func VerifyUpgradeSource(ctx context.Context, tx *sql.Tx) (UpgradeSource, error) {
	if err := Verify(ctx, tx); err != nil {
		return UpgradeSource{}, err
	}
	receipt, payload, err := readReceipt(ctx, tx)
	if err != nil {
		return UpgradeSource{}, err
	}
	var format int
	var priorStructure sql.NullString
	if err = tx.QueryRowContext(ctx, `SELECT m.format_version,row_to_json(i)->>'scheduler_structure' FROM pgjobdb.project_installation i CROSS JOIN pgjobdb.installation m WHERE i.singleton=1 AND m.name='pgjobdb'`).Scan(&format, &priorStructure); err != nil {
		return UpgradeSource{}, err
	}
	verified, valid := historicalStructure(receipt, priorStructure, format)
	if !valid {
		return UpgradeSource{}, fmt.Errorf("project upgrade requires a verified historical receipt")
	}
	return UpgradeSource{format, Digest(payload), verified}, nil
}

func (source UpgradeSource) Certify(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, certificateTable); err != nil {
		return err
	}
	target, err := structure(ctx, tx, leaseActivityFormat)
	if err != nil {
		return err
	}
	binding := upgradeBinding(source.format, leaseActivityFormat, source.receiptSHA256, source.structureSHA256, target)
	_, err = tx.ExecContext(ctx, `INSERT INTO pgjobdb.project_scheduler_upgrade VALUES(1,$1,$2,$3,$4,$5,$6)`, source.format, leaseActivityFormat, source.receiptSHA256, source.structureSHA256, target, binding)
	return err
}

func VerifyTargetGuards(ctx context.Context, q Queryer) error {
	if err := CheckResolution(ctx, q); err != nil {
		return err
	}
	for _, table := range TableNames() {
		var valid bool
		err := q.QueryRowContext(ctx, `SELECT count(*)=2 FROM pg_trigger WHERE tgrelid=to_regclass($1) AND NOT tgisinternal AND tgenabled='O' AND tgqual IS NULL AND (
 (tgname='project_admission_row' AND tgtype=31 AND tgfoid=to_regprocedure('pgjobdb._project_row_guard()') AND tgnargs=1 AND tgargs=$2) OR
 (tgname='project_admission_truncate' AND tgtype=34 AND tgfoid=to_regprocedure('pgjobdb._project_no_truncate()') AND tgnargs=0))`, table, []byte(Tables[table]+"\x00")).Scan(&valid)
		if err != nil {
			return err
		}
		if !valid {
			return fmt.Errorf("invalid project guard bindings on %s", table)
		}
	}
	for name := range entryGuards {
		rows, err := q.QueryContext(ctx, `SELECT prosrc FROM pg_proc WHERE pronamespace='pgjobdb'::regnamespace AND proname=$1`, name)
		if err != nil {
			return err
		}
		count := 0
		for rows.Next() {
			var body string
			if err = rows.Scan(&body); err != nil {
				break
			}
			count++
			if expected, guardErr := GuardedBody(name, body); guardErr != nil || body != expected {
				err = fmt.Errorf("invalid project entry guard on %s", name)
				break
			}
		}
		if err == nil {
			err = rows.Err()
		}
		rows.Close()
		if err != nil {
			return err
		}
		if count == 0 {
			return fmt.Errorf("missing project entry guard on %s", name)
		}
	}
	var release string
	if err := q.QueryRowContext(ctx, `SELECT prosrc FROM pg_proc WHERE oid='pgjobdb.release_append(text,text,text)'::regprocedure`).Scan(&release); err != nil {
		return err
	}
	if strings.TrimSpace(release) != strings.TrimSpace(guardedReleaseBody) {
		return fmt.Errorf("invalid project entry guard on release_append")
	}
	return nil
}
