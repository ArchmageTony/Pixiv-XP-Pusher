#!/bin/bash
# Pixiv-XP-Pusher Docker 部署脚本

set -e

ACTION=${1:-start}

if docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(docker-compose)
else
  echo "❌ 未找到 Docker Compose，请安装 Docker Compose v2。"
  exit 1
fi

case "$ACTION" in
  start)
    echo "🚀 首次启动，运行一次任务并启动调度器..."
    
    echo "🚀 构建镜像并启动服务..."
    
    # 由于 Dockerfile CMD 已包含 --now，容器启动后会自动先跑一次
    "${COMPOSE_CMD[@]}" up -d --build
    
    echo "✅ 启动完成！"
    echo "📋 查看日志: docker compose logs -f"
    ;;
    
  stop)
    echo "🛑 停止服务..."
    "${COMPOSE_CMD[@]}" down
    ;;
    
  restart)
    echo "🔄 重启服务..."
    "${COMPOSE_CMD[@]}" restart
    ;;
    
  logs)
    "${COMPOSE_CMD[@]}" logs -f --tail=100
    ;;
    
  once)
    echo "▶️ 手动执行一次任务..."
    "${COMPOSE_CMD[@]}" run --rm pixiv-xp python main.py --once
    ;;
    
  reset-xp)
    echo "🗑️ 重置 XP 数据..."
    "${COMPOSE_CMD[@]}" run --rm pixiv-xp python main.py --reset-xp
    ;;
    
  *)
    echo "用法: $0 {start|stop|restart|logs|once|reset-xp}"
    exit 1
    ;;
esac
