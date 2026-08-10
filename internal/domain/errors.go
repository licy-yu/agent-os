// Package domain 定义跨资源共享的领域基础类型。
package domain

import "errors"

var (
	// ErrNotFound 表示资源在当前一致性快照中不存在。
	ErrNotFound = errors.New("资源不存在")
	// ErrConflict 表示唯一键、乐观锁或资源状态发生冲突。
	ErrConflict = errors.New("资源冲突")
	// ErrInvalidTransition 表示调用方尝试绕过领域状态机。
	ErrInvalidTransition = errors.New("非法状态转换")
)
