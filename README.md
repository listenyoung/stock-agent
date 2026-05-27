# StockAgent

StockAgent 是一个面向 A 股数据分析的智能体系统。项目由 FastAPI 后端、React 前端和多个可独立启动的后端节点组成，支持股票数据同步、市场分析、智能体推理、策略监听、回测以及 Web 端交互。

## 功能特性

- 股票基础信息、日线行情、资金流、涨跌停、财务指标等数据同步
- 基于 LangGraph 的多轮股票分析智能体
- MCP 工具节点，用于股票数据、新闻、研报和相似信息检索
- Web API、WebSocket 实时推送和 JWT 用户认证
- 策略订阅、行情监听、回测任务和评估反馈
- MongoDB、Redis、Milvus 向量库和可选 Loki/Phoenix 可观测性配置
- React + Vite 前端工作台

## 项目结构

```text
.
├── AgentServer/              # Python 后端与各类节点
│   ├── agent/                # Agent 运行时、规划、工具、记忆、评估等
│   ├── core/                 # 配置、协议、管理器、通用核心能力
│   ├── nodes/                # web/data_sync/mcp/inference/listener/backtest 节点
│   ├── scripts/              # 数据同步、初始化、检查和维护脚本
│   ├── deploy/               # Docker Compose、Dockerfile、K8s、Redis、Milvus 配置
│   ├── requirements.txt
│   └── pyproject.toml
├── frontend-react/           # React + Vite 前端
├── manager.ps1               # Windows 管理脚本
├── backup.ps1
└── .gitignore
```

## 环境要求

- Python 3.11+
- Node.js 18+
- Docker / Docker Compose
- MongoDB、Redis、Milvus
- Tushare Token
- 至少一个 LLM API Key，例如 OpenAI、DashScope、智谱、DeepSeek 或本地 Ollama

## 配置

先复制后端环境变量模板：

```powershell
cd AgentServer
Copy-Item .env.example .env
```

至少需要检查并配置这些变量：

```env
JWT_SECRET=replace-with-a-long-random-secret
MONGO_PASSWORD=replace-with-your-mongo-password
REDIS_PASSWORD=replace-with-your-redis-password
TUSHARE_TOKEN=your-tushare-token
LLM_PROVIDER=deepseek
LLM_API_KEY=your-llm-api-key
LLM_MODEL_NAME=deepseek-chat
LLM_EMBEDDING_PROVIDER=zhipu
LLM_EMBEDDING_API_KEY=your-embedding-api-key
```

`.env` 已在 `.gitignore` 中忽略，不要把真实密钥、数据库密码或 Webhook 提交到仓库。

## 本地开发

### 1. 启动基础设施

如果只需要基础设施，可以使用后端部署目录：

```powershell
cd AgentServer\deploy
docker compose up -d mongodb redis milvus
```

也可以直接启动全栈后端节点：

```powershell
cd AgentServer\deploy
docker compose up -d
```

### 2. 启动后端节点

开发调试时可以在不同终端启动不同节点：

```powershell
cd AgentServer
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

$env:NODE_TYPE="web"; python main.py
$env:NODE_TYPE="data_sync"; python main.py
$env:NODE_TYPE="mcp"; python main.py
$env:NODE_TYPE="inference"; python main.py
$env:NODE_TYPE="listener"; python main.py
$env:NODE_TYPE="backtest"; python main.py
```

Web API 默认地址：

```text
http://localhost:8000
```

接口文档：

```text
http://localhost:8000/docs
```

### 3. 启动前端

```powershell
cd frontend-react
npm install
npm run dev
```

前端默认地址：

```text
http://localhost:5174
```

如需指定后端地址，可在前端环境变量中设置：

```env
VITE_API_BASE_URL=http://localhost:8000/api/v1
```

## 常用脚本

```powershell
cd AgentServer

python scripts\sync_stock_basic.py
python scripts\sync_stock_daily.py
python scripts\sync_daily_basic.py
python scripts\sync_moneyflow.py
python scripts\sync_limit_list.py
python scripts\check_sync_status.py
python scripts\set_admin.py
```

## 节点说明

| NODE_TYPE | 说明 |
| --- | --- |
| web | FastAPI 网关、认证、REST API、WebSocket、任务派发 |
| data_sync | 定时同步股票、指数、资金流、新闻、财务等数据 |
| mcp | 为智能体提供股票、行情、新闻、搜索等工具 |
| inference | 执行股票分析智能体与多轮推理流程 |
| listener | 交易时段监听策略信号并推送通知 |
| backtest | 量化回测与因子选择任务 |

## 主要接口

- `POST /api/v1/auth/register`
- `POST /api/v1/auth/login`
- `GET /api/v1/users/me`
- `GET /api/v1/stocks/search`
- `GET /api/v1/market/overview`
- `POST /api/v1/tasks/analyze/stock`
- `POST /api/v1/agents/runs`
- `GET /api/v1/backtest/history`
- `WS /ws`

完整接口以 `http://localhost:8000/docs` 为准。

## 安全检查

- `.env`、`.env.local`、日志、数据库文件、`node_modules`、`__pycache__` 已在 `.gitignore` 中忽略。
- `AgentServer/.env.example` 只放占位值，可以提交。
- 当前本地目录没有 `.git` 元数据时，无法用 `git status` 或提交历史确认是否已经推送过密钥。推送前请在真实仓库中执行：

```powershell
git status --short
git ls-files AgentServer/.env
git log --all -- AgentServer/.env
```

如果 `.env` 曾经被提交过，请立刻撤销相关 API Key、数据库密码和 Webhook，并清理 Git 历史后再公开仓库。

## 许可证

本项目使用 MIT License。
"# stock-agent" 
"# stock-agent" 
