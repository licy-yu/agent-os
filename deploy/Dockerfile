# Alpine 镜像从 AWS 官方公共镜像仓库拉取，适配目标服务器不能访问 Docker Hub 的网络环境。
FROM public.ecr.aws/docker/library/alpine:3.22

# CA 根证书供 OpenAI/MCP HTTPS 调用使用；运行进程固定为无登录权限的非 root 用户。
RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 10001 swarmos \
    && adduser -S -D -H -u 10001 -G swarmos swarmos

WORKDIR /app
COPY --chown=swarmos:swarmos bin/control-plane /app/bin/control-plane
COPY --chown=swarmos:swarmos bin/worker /app/bin/worker
COPY --chown=swarmos:swarmos configs/config.yaml /app/configs/config.yaml
COPY --chown=swarmos:swarmos web/dist /app/web/dist

USER 10001:10001

# Compose 会为 Worker 覆盖 command；默认命令保持控制面，方便单容器诊断。
CMD ["/app/bin/control-plane", "-config", "/app/configs/config.yaml"]
