package domain

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	// DefaultPageSize 防止调用方不传分页参数时一次读取全部资源。
	DefaultPageSize = 50
	// MaxPageSize 是 API 和仓储共同遵守的硬上限。
	MaxPageSize = 200
)

// Cursor 使用创建时间和 UUID 构成稳定的 keyset 游标。
// 与 offset 相比，它不会因为并发插入导致重复或跳过记录。
type Cursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        uuid.UUID `json:"id"`
}

// Page 是仓储层使用的分页条件。
type Page struct {
	Size   int
	Cursor *Cursor
}

// NormalizePageSize 将外部输入限制在安全范围内。
func NormalizePageSize(size int32) int {
	if size <= 0 {
		return DefaultPageSize
	}
	if size > MaxPageSize {
		return MaxPageSize
	}
	return int(size)
}

// EncodeCursor 把内部游标编码为不透明 URL-safe token。
func EncodeCursor(cursor Cursor) (string, error) {
	raw, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf("序列化分页游标: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// DecodeCursor 解码客户端回传的 token；非法 token 必须返回参数错误，不能静默回到第一页。
func DecodeCursor(token string) (*Cursor, error) {
	if token == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("分页 token 不是合法 Base64: %w", err)
	}
	var cursor Cursor
	if err := json.Unmarshal(raw, &cursor); err != nil {
		return nil, fmt.Errorf("分页 token 内容无效: %w", err)
	}
	if cursor.CreatedAt.IsZero() || cursor.ID == uuid.Nil {
		return nil, fmt.Errorf("分页 token 缺少 created_at 或 id")
	}
	return &cursor, nil
}
