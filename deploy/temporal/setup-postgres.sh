#!/bin/sh
# 为 Temporal Server 初始化并升级主库与 Visibility 库。
#
# Job 会在每次 Compose 发布时运行。create/setup-schema 在数据库已存在时可能返回非零，
# 但随后的 update-schema 必须成功，否则 set -e 会阻止 Temporal Server 启动，避免新二进制
# 在旧 Schema 上“带病运行”。数据库密码只通过环境变量传给官方工具，不写入命令行或日志。
set -eu

: "${POSTGRES_SEEDS:?POSTGRES_SEEDS 不能为空}"
: "${POSTGRES_USER:?POSTGRES_USER 不能为空}"
: "${SQL_PASSWORD:?SQL_PASSWORD 不能为空}"

DB_PORT="${DB_PORT:-5432}"

echo "等待 Temporal PostgreSQL 就绪……"
nc -z -w 10 "${POSTGRES_SEEDS}" "${DB_PORT}"

upgrade_database() {
  database="$1"
  schema_dir="$2"

  # 首次部署需要 create/setup-schema；滚动升级时库和初始版本已经存在，允许这两步
  # 返回“already exists”，最终 update-schema 仍负责验证连接并迁移到镜像目标版本。
  temporal-sql-tool --plugin postgres12 --ep "${POSTGRES_SEEDS}" -u "${POSTGRES_USER}" \
    -p "${DB_PORT}" --db "${database}" create || true
  temporal-sql-tool --plugin postgres12 --ep "${POSTGRES_SEEDS}" -u "${POSTGRES_USER}" \
    -p "${DB_PORT}" --db "${database}" setup-schema -v 0.0 || true
  temporal-sql-tool --plugin postgres12 --ep "${POSTGRES_SEEDS}" -u "${POSTGRES_USER}" \
    -p "${DB_PORT}" --db "${database}" update-schema -d "${schema_dir}"
}

upgrade_database temporal /etc/temporal/schema/postgresql/v12/temporal/versioned
upgrade_database temporal_visibility /etc/temporal/schema/postgresql/v12/visibility/versioned

echo "Temporal PostgreSQL Schema 已就绪。"
