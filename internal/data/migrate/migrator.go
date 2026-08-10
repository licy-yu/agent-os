// Package migrate 提供最小、确定性的 PostgreSQL schema migration。
package migrate

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// migrations 在编译时进入二进制，因此生产容器不依赖外部 SQL 文件路径。
//
//go:embed sql/*.sql
var migrations embed.FS

const advisoryLockID int64 = 0x535741524D4F53 // ASCII "SWARMOS"

// Up 按文件名顺序应用尚未执行的迁移。
// 同名已执行迁移的 SHA-256 如果改变，进程会拒绝启动，防止历史迁移被静默篡改。
func Up(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("获取 migration 数据库连接: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", advisoryLockID); err != nil {
		return fmt.Errorf("获取 migration advisory lock: %w", err)
	}
	defer func() { _, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", advisoryLockID) }()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			name TEXT PRIMARY KEY,
			checksum CHAR(64) NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("创建 schema_migrations: %w", err)
	}

	entries, err := fs.Glob(migrations, "sql/*.sql")
	if err != nil {
		return fmt.Errorf("枚举内嵌 migration: %w", err)
	}
	sort.Strings(entries)
	for _, name := range entries {
		if err := applyOne(ctx, conn.Conn(), name); err != nil {
			return err
		}
	}
	return nil
}

func applyOne(ctx context.Context, conn *pgx.Conn, name string) error {
	raw, err := migrations.ReadFile(name)
	if err != nil {
		return fmt.Errorf("读取 migration %s: %w", name, err)
	}
	digest := sha256.Sum256(raw)
	checksum := hex.EncodeToString(digest[:])
	shortName := strings.TrimPrefix(name, "sql/")

	var existing string
	err = conn.QueryRow(ctx, "SELECT checksum FROM schema_migrations WHERE name=$1", shortName).Scan(&existing)
	switch {
	case err == nil:
		if existing != checksum {
			return fmt.Errorf("migration %s 校验和变化：已执行=%s 当前=%s", shortName, existing, checksum)
		}
		return nil
	case err != pgx.ErrNoRows:
		return fmt.Errorf("查询 migration %s 状态: %w", shortName, err)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("开始 migration %s 事务: %w", shortName, err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, string(raw)); err != nil {
		return fmt.Errorf("执行 migration %s: %w", shortName, err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations(name, checksum) VALUES($1,$2)", shortName, checksum); err != nil {
		return fmt.Errorf("记录 migration %s: %w", shortName, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("提交 migration %s: %w", shortName, err)
	}
	return nil
}
