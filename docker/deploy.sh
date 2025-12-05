#!/bin/bash

# 净值分析系统一键部署脚本

set -e

cd "$(dirname "$0")"

echo "================================"
echo "净值分析系统 - 部署脚本"
echo "================================"
echo ""

# 检查 Docker 是否安装
if ! command -v docker &> /dev/null; then
    echo "❌ Docker 未安装！"
    echo "请先安装 Docker: https://docs.docker.com/get-docker/"
    exit 1
fi

# 检查 Docker Compose 是否安装
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose 未安装！"
    echo "请先安装 Docker Compose: https://docs.docker.com/compose/install/"
    exit 1
fi

echo "✅ Docker 和 Docker Compose 已安装"
echo ""

# 检查 .env 文件（在项目根目录）
if [ ! -f ../.env ]; then
    echo "⚠️  未找到 .env 文件，正在创建..."
    if [ -f ./env.example ]; then
        cp ./env.example ../.env
        echo "✅ 已创建 .env 文件（从 docker/env.example 复制到根目录）"
        echo ""
        echo "⚠️  重要：请编辑 .env 文件，修改数据库密码！"
        echo "   nano ../.env"
        echo ""
        read -p "按 Enter 继续，或 Ctrl+C 退出..."
    else
        echo "❌ 未找到 env.example 文件！"
        exit 1
    fi
else
    echo "✅ 找到 .env 文件"
fi

echo ""
echo "开始部署..."
echo ""

# 停止旧容器（如果存在）
echo "1️⃣  停止旧容器..."
docker-compose down 2>/dev/null || true

# 构建镜像
echo ""
echo "2️⃣  构建 Docker 镜像..."
docker-compose build

# 启动服务
echo ""
echo "3️⃣  启动服务..."
docker-compose up -d

# 等待服务启动
echo ""
echo "4️⃣  等待服务启动..."
sleep 10

# 检查服务状态
echo ""
echo "5️⃣  检查服务状态..."
docker-compose ps

# 测试服务
echo ""
echo "6️⃣  测试服务..."

WEB_PORT=$(grep WEB_PORT ../.env | cut -d '=' -f2 || echo "5000")
API_PORT=$(grep API_PORT ../.env | cut -d '=' -f2 || echo "8080")

echo ""
echo "测试 Web 服务 (端口 $WEB_PORT)..."
if curl -s http://localhost:$WEB_PORT > /dev/null; then
    echo "✅ Web 服务运行正常"
else
    echo "❌ Web 服务测试失败"
fi

echo ""
echo "测试 API 服务 (端口 $API_PORT)..."
if curl -s http://localhost:$API_PORT/netvalue/intervals > /dev/null; then
    echo "✅ API 服务运行正常"
else
    echo "❌ API 服务测试失败"
fi

echo ""
echo "================================"
echo "✅ 部署完成！"
echo "================================"
echo ""
echo "访问地址："
echo "  Web 界面: http://localhost:$WEB_PORT"
echo "  API 接口: http://localhost:$API_PORT"
echo ""
echo "常用命令（在 docker 目录下执行）："
echo "  查看日志: docker-compose logs -f"
echo "  停止服务: docker-compose stop"
echo "  重启服务: docker-compose restart"
echo "  删除容器: docker-compose down"
echo ""
echo "详细文档: 查看 DEPLOYMENT.md"
echo ""

