#!/usr/bin/env python3
"""安全、原子地补齐 SwarmOS 单机生产部署所需环境变量。

脚本不会 source `.env`，因此文件内容不会被当作 Shell 代码执行；也不会在输出中
显示 API Key、数据库密码或完整连接串。已有长度合格的密钥会被保留，只有缺失或
明显不合格的值才会使用操作系统 CSPRNG 重新生成。
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import secrets
import stat


FIXED_VALUES = {
    "SWARMOS_ENVIRONMENT": "production",
    "SWARMOS_TEMPORAL_ENABLED": "true",
    "SWARMOS_TEMPORAL_ADDRESS": "127.0.0.1:7233",
    "SWARMOS_TEMPORAL_NAMESPACE": "default",
    "SWARMOS_TEMPORAL_TASK_QUEUE": "swarmos-runs-v1-5",
    "SWARMOS_TENANT_ID": "00000000-0000-0000-0000-000000000001",
    "SWARMOS_API_SUBJECT": "api-operator",
}
REQUIRED_EXISTING = (
    "SWARMOS_DATABASE_DSN",
    "SWARMOS_REDIS_ADDR",
    "SWARMOS_NATS_URL",
)
SECRET_KEYS = ("SWARMOS_API_KEY", "SWARMOS_TEMPORAL_DB_PASSWORD")


def parse_env(lines: list[str]) -> dict[str, str]:
    """只解析简单 KEY=VALUE；注释、空行和未知行会在写回时原样保留。"""

    values: dict[str, str] = {}
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key:
            values[key] = value.strip()
    return values


def rewrite(lines: list[str], updates: dict[str, str]) -> list[str]:
    """保持原文件顺序，只替换目标键；不存在的键追加到文件末尾。"""

    output: list[str] = []
    written: set[str] = set()
    for line in lines:
        if "=" in line and not line.lstrip().startswith("#"):
            key = line.split("=", 1)[0].strip()
            if key in updates:
                if key not in written:
                    output.append(f"{key}={updates[key]}\n")
                    written.add(key)
                # 重复定义会造成“最后一个值”歧义，写回时直接去重。
                continue
        output.append(line if line.endswith("\n") else line + "\n")
    if output and output[-1].strip():
        output.append("\n")
    for key, value in updates.items():
        if key not in written:
            output.append(f"{key}={value}\n")
    return output


def atomic_write(path: Path, content: str) -> None:
    """同目录写临时文件、fsync 后 os.replace，避免断电留下半个 `.env`。"""

    temporary = path.with_name(path.name + ".incoming")
    if temporary.exists():
        raise RuntimeError(f"临时文件已存在，拒绝覆盖：{temporary}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
        # POSIX 的 fsync(文件) 只保证文件内容落盘；目录项替换还需要同步父目录，
        # 才能在主机突然断电后仍保证 rename 的持久性。Windows 没有 O_DIRECTORY，
        # 本脚本在本地语法/幂等测试时跳过这一步，目标 Linux VM 会执行它。
        if os.name == "posix":
            directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description="原子配置 SwarmOS 生产 .env")
    parser.add_argument("--path", default="/srv/projects/agent-os/.env", help="目标 .env 绝对路径")
    args = parser.parse_args()
    path = Path(args.path)
    if not path.is_absolute() or not path.is_file() or path.is_symlink():
        raise SystemExit("目标必须是已存在、非符号链接的绝对路径普通文件")

    # utf-8-sig 同时接受普通 UTF-8 与 Windows 工具可能写入的 BOM；写回统一为无 BOM UTF-8。
    lines = path.read_text(encoding="utf-8-sig").splitlines(keepends=True)
    existing = parse_env(lines)
    missing = [key for key in REQUIRED_EXISTING if not existing.get(key, "").strip()]
    if missing:
        raise SystemExit("缺少既有基础设施变量：" + ", ".join(missing))

    updates = dict(FIXED_VALUES)
    generated: list[str] = []
    for key in SECRET_KEYS:
        current = existing.get(key, "").strip()
        if len(current) >= 32:
            updates[key] = current
        else:
            updates[key] = secrets.token_hex(32)
            generated.append(key)

    atomic_write(path, "".join(rewrite(lines, updates)))
    print("production_env=ready")
    print("generated_secrets=" + (",".join(generated) if generated else "none"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
