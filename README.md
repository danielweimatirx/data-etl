# 智能 ETL 助手

通过自然语言对话完成数据建模与加工：连接数据库 → 选择基表 → 定义目标表 → 字段映射 → 校验与溯源。真实连接 MySQL，DDL/DML 先展示后确认执行。

## 功能概览

- **对话式流程**：连接库、选基表、建表、映射、验证、溯源，全程自然语言引导
- **真实执行**：后端连接你的 MySQL 执行 SQL，结果以表格 + 返回码形式回显，不编造
- **安全确认**：所有 DDL、DML 先展示 SQL，用户确认后才执行
- **自我纠正**：执行失败（如列不存在）时自动查表结构并给出修正 SQL

## 技术栈

- 前端：React + TypeScript + Vite + Tailwind CSS + Zustand
- 后端：Node.js + Express + mysql2
- 模型：通义千问 qwen3-max（意图解析 + 对话回复），通过 OpenAI 兼容接口调用
- 数据持久化：MatrixOne 数据库（应用数据共享）

## 本地运行

### 1. 安装依赖

```bash
npm install
```

### 2. 启动

LLM 和数据库配置已内置，无需额外配置即可直接启动：

```bash
# 同时启动后端 + 前端（推荐）
npm run dev:all

# 或分别启动
npm run dev:server   # 后端 http://localhost:3001
npm run dev          # 前端 Vite 开发服务器
```

浏览器访问前端地址（Vite 默认如 `http://localhost:5173`），在对话中输入 MySQL 连接串即可开始。连接串示例：`mysql://username:password@host:3306/database_name`。

### 可选：自定义配置

如需覆盖默认配置，创建 `.env` 文件：

```
LLM_API_KEY=your_api_key
LLM_BASE_URL=https://your-llm-endpoint/v1
LLM_MODEL=your-model-name
PORT=3001
```

## 项目结构

```
├── server/          # 后端 Express API、意图解析、数据库执行
│   ├── server.js    # 主服务
│   └── appDb.js     # 应用数据持久化层（MatrixOne）
├── src/              # 前端 React 应用
│   ├── components/  # 对话、输入、消息展示等
│   ├── store.ts     # 对话状态与演示流程
│   └── types.ts     # ETL 步骤与类型定义
├── .env              # 环境变量（可选，已有内置默认值）
└── package.json
```

## License

ISC
