# StockAgent 部署指南

## 架构概览

```
┌─────────────────────────────────────────────────────────────┐
│                        Frontend (Vue3)                       │
│                        Nginx / CDN                           │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                     Web Node (FastAPI)                       │
│                     Port: 8000                               │
└─────────────────────────┬───────────────────────────────────┘
                          │ RPC
        ┌─────────────────┼─────────────────┐
        ▼                 ▼                 ▼
┌───────────────┐ ┌───────────────┐ ┌───────────────┐
│  Data Sync    │ │   Inference   │ │   Backtest    │
│    Node       │ │     Node      │ │     Node      │
└───────┬───────┘ └───────┬───────┘ └───────┬───────┘
        │                 │                 │
        └─────────────────┼─────────────────┘
                          ▼
        ┌─────────────────────────────────────┐
        │         MongoDB + Redis             │
        └─────────────────────────────────────┘
```

## 部署方式

### 方式一：Docker Compose 全栈部署（推荐）

适合单机或小规模部署。

```bash
# 1. 准备配置文件
cd AgentServer
cp .env.example .env
# 编辑 .env 配置 TUSHARE_TOKEN、LLM API Key 等

# 2. 启动所有服务
cd deploy
docker compose up -d

# 3. 查看状态
docker compose ps
docker compose logs -f
```

### 方式二：分层部署

适合将基础设施和应用分开管理。

```bash
# 1. 先启动基础设施
cd deploy/mongodb && docker compose up -d
cd ../redis && docker compose up -d

# 2. 再启动应用节点
cd .. && docker compose up -d web data-sync inference backtest
```

### 方式三：手动部署（开发/调试）

```bash
cd AgentServer

# 安装依赖
pip install -r requirements.txt

# 启动各节点（不同终端）
NODE_TYPE=web python main.py
NODE_TYPE=data_sync python main.py
NODE_TYPE=inference python main.py
NODE_TYPE=backtest python main.py
```

## 环境配置

在 `AgentServer/.env` 中配置：

```env
# ===== 基础设施 =====
MONGO_HOST=localhost          # Docker 内部用 mongodb
MONGO_PORT=27017
MONGO_USERNAME=admin
MONGO_PASSWORD=password123
MONGO_DATABASE=stock_agent

REDIS_HOST=localhost          # Docker 内部用 redis
REDIS_PORT=6379
REDIS_DB=0

# ===== API 密钥 =====
TUSHARE_TOKEN=your_token_here

# LLM (按需配置)
OPENAI_API_KEY=sk-xxx
DEEPSEEK_API_KEY=sk-xxx

# ===== 安全 =====
JWT_SECRET_KEY=your-production-secret-key
```

## 服务端口

| 服务 | 端口 | 说明 |
|------|------|------|
| Web Node | 8000 | HTTP API |
| MongoDB | 27017 | 数据库 |
| Redis | 6379 | 缓存 |
| Milvus | 19530 | 向量数据库（可选） |

## 常用命令

```bash
# 查看所有容器状态
docker compose ps

# 查看日志
docker compose logs -f web
docker compose logs -f data-sync

# 重启单个服务
docker compose restart web

# 停止所有服务
docker compose down

# 停止并删除数据
docker compose down -v

# 重新构建镜像
docker compose build --no-cache

# 进入容器调试
docker exec -it stock-agent-web bash
```

## 前端部署

```bash
cd frontend

# 安装依赖
npm install

# 构建生产版本
npm run build

# 输出在 dist/ 目录，可部署到 Nginx
```

### Nginx 配置示例

```nginx
server {
    listen 80;
    server_name your-domain.com;

    # 前端静态文件
    location / {
        root /var/www/stock-agent/dist;
        try_files $uri $uri/ /index.html;
    }

    # API 代理
    location /api/ {
        proxy_pass http://localhost:8000/api/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }

    # WebSocket
    location /ws/ {
        proxy_pass http://localhost:8000/ws/;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

## 健康检查

```bash
# Web 节点
curl http://localhost:8000/health

# MongoDB
docker exec stock-agent-mongo mongosh --eval "db.adminCommand('ping')"

# Redis
docker exec stock-agent-redis redis-cli ping
```

## 常见问题

### 1. 容器启动失败
```bash
# 查看详细日志
docker compose logs <service_name>
```

### 2. 连接数据库失败
- Docker 内部服务间通信使用服务名（如 `mongodb`）
- 外部访问使用 `localhost` 或服务器 IP

### 3. 内存不足
```yaml
# 在 docker-compose.yml 中限制内存
services:
  web:
    deploy:
      resources:
        limits:
          memory: 512M
```

## 生产环境建议

1. **安全**
   - 修改所有默认密码
   - 使用 HTTPS
   - 配置防火墙

2. **性能**
   - 根据负载调整 inference 节点数量
   - 配置 MongoDB 副本集
   - 使用 Redis 集群

3. **监控**
   - 取消注释 Grafana/Loki 配置
   - 接入日志收集系统
