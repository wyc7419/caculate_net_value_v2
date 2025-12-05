# Docker 部署文件

本目录包含所有 Docker 相关的配置文件。

## 📁 文件说明

| 文件 | 说明 |
|------|------|
| `Dockerfile` | Docker 镜像构建文件 |
| `docker-compose.yml` | 多容器编排配置 |
| `.dockerignore` | Docker 构建时忽略的文件 |
| `deploy.sh` | 一键部署脚本 |
| `env.example` | 环境变量模板 |

## 🚀 快速开始

### 方法1: 在项目根目录运行（推荐）

```bash
# 给脚本添加执行权限
chmod +x docker-start.sh

# 一键启动
./docker-start.sh
```

### 方法2: 在 docker 目录中运行

```bash
cd docker

# 给脚本添加执行权限
chmod +x deploy.sh

# 运行部署脚本
./deploy.sh
```

### 方法3: 手动运行

```bash
cd docker

# 1. 复制环境变量（首次运行）
cp env.example ../.env
nano ../.env  # 编辑配置

# 2. 启动服务
docker-compose up -d

# 3. 查看日志
docker-compose logs -f
```

## ⚙️ 常用命令

**注意：以下命令需要在 `docker` 目录下运行**

```bash
# 进入 docker 目录
cd docker

# 启动服务
docker-compose up -d

# 停止服务
docker-compose stop

# 重启服务
docker-compose restart

# 查看服务状态
docker-compose ps

# 查看日志
docker-compose logs -f

# 查看特定服务的日志
docker-compose logs -f web
docker-compose logs -f api
docker-compose logs -f timescaledb

# 停止并删除容器
docker-compose down

# 停止并删除容器和数据（危险！）
docker-compose down -v

# 重新构建镜像
docker-compose build

# 重新构建并启动
docker-compose up -d --build
```

## 📝 配置说明

### 环境变量（.env 文件）

环境变量文件位于**项目根目录**（`../.env`），而不是 docker 目录。

首次部署时，从 `env.example` 复制：
```bash
cp env.example ../.env
```

**重要配置项：**
```bash
# 数据库密码（必须修改！）
POSTGRES_PASSWORD=your_very_secure_password_here

# 服务端口
WEB_PORT=5000
API_PORT=8080
```

### 服务端口

| 服务 | 默认端口 | 说明 |
|------|----------|------|
| Web 界面 | 5000 | 主应用 |
| API 服务 | 8080 | 第三方 API |
| 数据库 | 5432 | TimescaleDB |

## 🔧 目录结构

```
caculate_net_value/
├── docker/                      # Docker 配置目录 ← 你在这里
│   ├── Dockerfile              # 镜像构建文件
│   ├── docker-compose.yml      # 服务编排
│   ├── .dockerignore           # 构建忽略
│   ├── deploy.sh              # 部署脚本
│   ├── env.example            # 环境变量模板
│   └── README.md              # 本文件
├── .env                        # 环境变量（需要自己创建）
├── docker-start.sh             # 根目录快捷启动脚本
├── requirements.txt
├── start_web.py
└── ... (其他项目文件)
```

## 🌐 访问服务

启动成功后：
- **Web 界面**: http://localhost:5000
- **API 接口**: http://localhost:8080
- **数据库**: localhost:5432

## 📊 查看资源使用

```bash
# 查看容器资源使用
docker stats netvalue_web netvalue_api netvalue_timescaledb

# 查看磁盘使用
docker system df
```

## 💾 数据备份

```bash
# 备份数据库
docker exec netvalue_timescaledb pg_dump -U netvalue_user net_value_db > ../backup.sql

# 恢复数据库
docker exec -i netvalue_timescaledb psql -U netvalue_user net_value_db < ../backup.sql
```

## 🆘 故障排查

### 问题1: 端口被占用

修改 `../.env` 文件中的端口：
```bash
WEB_PORT=5001
API_PORT=8081
```

然后重启：
```bash
docker-compose down
docker-compose up -d
```

### 问题2: 容器启动失败

查看详细日志：
```bash
docker-compose logs web
```

### 问题3: 数据库连接失败

检查数据库状态：
```bash
docker-compose ps timescaledb
docker-compose logs timescaledb
```

## 📚 更多文档

- **完整部署指南**: 查看项目根目录的 `DEPLOYMENT.md`
- **快速开始**: 查看 `DOCKER_QUICKSTART.md`
- **API 文档**: 查看 `net_value_api/README.md`

## 💡 小贴士

1. **首次运行必须配置 .env**
   ```bash
   cp env.example ../.env
   nano ../.env  # 修改数据库密码
   ```

2. **查看所有容器**
   ```bash
   docker ps -a
   ```

3. **清理未使用的资源**
   ```bash
   docker system prune -a
   ```

4. **进入容器调试**
   ```bash
   docker exec -it netvalue_web bash
   ```

5. **修改代码后重启**
   ```bash
   docker-compose restart web
   ```

---

**祝使用愉快！** 🎉

有问题请查看详细文档或提出 Issue。

