# 设计文档：FastAPI + LangGraph 架构迁移

## 概述

本设计将现有 Express.js 后端（`server/server.js`，约 1733 行）迁移为 Python FastAPI + LangGraph 架构。核心原则：**所有 prompt、所有业务逻辑、所有 API 请求/响应格式保持完全不变**，仅替换底层技术栈。

### 技术栈映射

| 现有技术 | 迁移目标 |
|---------|---------|
| Express.js | FastAPI |
| mysql2/promise | aiomysql |
| node-fetch | openai Python SDK (OpenAI 兼容模式) |
| 手动状态管理 | LangGraph StateGraph |
| dotenv (Node) | python-dotenv |
| CORS middleware | fastapi.middleware.cors |

### 关键约束

- 所有 LLM prompt **逐字复制**，不做任何修改
- 所有 API 端点路径、HTTP 方法、请求体/响应体 JSON 结构完全一致
- 所有 HTTP 状态码（200/400/500/503）保持一致
- 所有 LLM 调用参数（temperature、max_tokens、model）保持一致
- LLM 客户端使用 OpenAI 兼容接口（`openai` Python SDK），通过 `LLM_BASE_URL` 和 `LLM_API_KEY` 环境变量配置，可切换任意兼容模型

## 架构

### 项目结构

```
server_py/
├── main.py                  # FastAPI 应用入口，uvicorn 启动
├── config.py                # 环境变量与常量
├── routers/
│   ├── __init__.py
│   ├── chat.py              # POST /api/chat（ETL 对话）
│   ├── metric_chat.py       # POST /api/metric-chat（指标对话）
│   ├── mapping.py           # POST /api/mapping
│   ├── dml.py               # POST /api/dml, POST /api/dml/optimize
│   ├── tables.py            # POST /api/tables
│   ├── metric.py            # POST /api/metric/match, /generate, /query
│   ├── lineage.py           # POST /api/lineage, POST /api/metric-lineage
│   └── debug.py             # GET /api/debug-deepseek
├── graphs/
│   ├── __init__.py
│   ├── etl_chat_graph.py    # ETL 对话 LangGraph 状态图
│   └── metric_chat_graph.py # 指标对话 LangGraph 状态图
├── db/
│   ├── __init__.py
│   ├── connection.py        # MySQL 连接解析与管理
│   └── operations.py        # 数据库操作执行引擎
├── llm/
│   ├── __init__.py
│   ├── client.py            # OpenAI 兼容 LLM 客户端封装
│   └── intent.py            # 意图解析
├── utils/
│   ├── __init__.py
│   ├── formatters.py        # Markdown 表格格式化等
│   └── sql_parser.py        # SQL 表引用提取
├── requirements.txt
└── .env.example
```

### 架构图

```mermaid
graph TB
    subgraph "FastAPI Server (main.py)"
        Router[API Router]
    end

    subgraph "Routers"
        ChatR[chat.py]
        MetricChatR[metric_chat.py]
        MappingR[mapping.py]
        DmlR[dml.py]
        TablesR[tables.py]
        MetricR[metric.py]
        LineageR[lineage.py]
        DebugR[debug.py]
    end

    subgraph "LangGraph Graphs"
        ETLGraph[etl_chat_graph.py]
        MetricGraph[metric_chat_graph.py]
    end

    subgraph "Core Modules"
        LLMClient[llm/client.py]
        IntentParser[llm/intent.py]
        DBConn[db/connection.py]
        DBOps[db/operations.py]
        Fmt[utils/formatters.py]
        SqlP[utils/sql_parser.py]
    end

    Router --> ChatR & MetricChatR & MappingR & DmlR & TablesR & MetricR & LineageR & DebugR
    ChatR --> ETLGraph
    MetricChatR --> MetricGraph
    ETLGraph --> IntentParser & DBOps & LLMClient & Fmt
    MetricGraph --> IntentParser & DBOps & LLMClient & Fmt
    MappingR & DmlR & MetricR & LineageR & DebugR --> LLMClient
    TablesR & MetricR --> DBOps
    DBOps --> DBConn
    IntentParser --> LLMClient
```


## 组件与接口

### 1. 应用入口 (`main.py`)

**对应原始代码**：`server.js` 第 290-310 行（Express 初始化 + CORS + JSON limit + 根路由 + listen）

```python
# main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from config import PORT
from routers import chat, metric_chat, mapping, dml, tables, metric, lineage, debug

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
# FastAPI 默认无 body size limit，通过自定义中间件或 Starlette 配置限制 2MB

app.include_router(chat.router)
app.include_router(metric_chat.router)
app.include_router(mapping.router)
app.include_router(dml.router)
app.include_router(tables.router)
app.include_router(metric.router)
app.include_router(lineage.router)
app.include_router(debug.router)

@app.get("/")
async def root():
    return {
        "name": "ETL API",
        "message": "后端已运行",
        "endpoints": {
            "POST /api/chat": "ETL 六步对话",
            "POST /api/mapping": "字段映射",
            "POST /api/dml": "生成 DML",
            "GET /api/debug-deepseek": "测试 DeepSeek",
        },
    }

# uvicorn main:app --host 0.0.0.0 --port {PORT}
```

### 2. 配置模块 (`config.py`)

**对应原始代码**：`server.js` 第 1-5 行（dotenv）+ 第 296-298 行（常量）

```python
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

LLM_API_KEY = os.getenv("LLM_API_KEY", os.getenv("DEEPSEEK_API_KEY", ""))
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://api.deepseek.com/v1")
LLM_MODEL = os.getenv("LLM_MODEL", "deepseek-chat")
PORT = int(os.getenv("PORT", "3001"))
```

### 3. MySQL 连接解析 (`db/connection.py`)

**对应原始代码**：`server.js` 第 13-95 行（5 个函数）

| JS 函数 | Python 函数 | 说明 |
|---------|------------|------|
| `looksLikeConnectionString(str)` | `looks_like_connection_string(s: str) -> bool` | 判断字符串是否为连接串 |
| `parseConnectionStringUrl(str)` | `parse_connection_string_url(s: str) -> Optional[dict]` | 解析 URL 格式连接串 |
| `parseMysqlCliConnectionString(str)` | `parse_mysql_cli_connection_string(s: str) -> Optional[dict]` | 解析 CLI 格式连接串 |
| `getConnectionConfig(connectionString)` | `get_connection_config(connection_string: str) -> Optional[dict]` | 统一入口 |
| `testConnection(connectionString)` | `async test_connection(connection_string: str) -> dict` | 连接测试（8秒超时） |

连接配置字典结构：`{"host": str, "port": int, "user": str, "password": str, "database": Optional[str]}`

`get_mysql_connection` 使用 `aiomysql.connect()` 替代 `mysql2.createConnection()`，超时 10 秒。

### 4. 数据库操作引擎 (`db/operations.py`)

**对应原始代码**：`server.js` 第 97-260 行

| JS 函数 | Python 函数 |
|---------|------------|
| `safeIdentifier(name)` | `safe_identifier(name: str) -> Optional[str]` |
| `extractDatabaseFromCreateTable(ddl)` | `extract_database_from_create_table(ddl: str) -> Optional[str]` |
| `getMysqlConnection(connectionString)` | `async get_mysql_connection(connection_string: str) -> aiomysql.Connection` |
| `runDatabaseOperation(connectionString, intent, params)` | `async run_database_operation(connection_string: str, intent: str, params: dict) -> dict` |

`VALID_INTENTS` 列表保持不变：`['createDatabase', 'listDatabases', 'listTables', 'describeTable', 'previewData', 'createTable', 'executeSQL', 'analyzeNulls']`

`executeSQL` 中的禁止关键字正则保持不变：`r'\b(DROP|TRUNCATE|DELETE|UPDATE)\b'`（忽略大小写）

### 5. LLM 客户端 (`llm/client.py`)

**对应原始代码**：所有 `fetch(DEEPSEEK_CHAT_URL, ...)` 调用

使用 `openai` Python SDK 的 `AsyncOpenAI` 客户端，通过 `base_url` 和 `api_key` 参数实现 OpenAI 兼容接口，可对接 DeepSeek、本地模型等任意兼容服务。

```python
from openai import AsyncOpenAI
from config import LLM_API_KEY, LLM_BASE_URL, LLM_MODEL

_client = AsyncOpenAI(base_url=LLM_BASE_URL, api_key=LLM_API_KEY)

async def call_llm(messages: list, temperature: float, max_tokens: int) -> dict:
    """
    统一的 LLM API 调用（OpenAI 兼容接口）。
    返回 {"ok": bool, "content": str, "error": str}
    """
    try:
        response = await _client.chat.completions.create(
            model=LLM_MODEL,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            stream=False,
        )
        content = response.choices[0].message.content
        return {"ok": True, "content": content, "error": ""}
    except Exception as e:
        return {"ok": False, "content": "", "error": str(e)}
```

### 6. 意图解析 (`llm/intent.py`)

**对应原始代码**：`server.js` 第 262-290 行 `extractDbIntentFromModel`

```python
async def extract_db_intent_from_model(conversation: list, api_key: str, chat_url: str) -> dict:
    """
    返回 {"intent": str|None, "params": dict}
    - system prompt 与 JS 版完全相同（逐字复制）
    - 仅取 conversation 最后 8 条消息
    - temperature=0.1, max_tokens=1024
    - 解析失败返回 {"intent": None, "params": {}}
    """
```

意图解析 system prompt **逐字复制**自 `server.js` 第 263-288 行，不做任何修改。

### 7. 工具函数

#### `utils/formatters.py`

**对应原始代码**：`server.js` 第 140-155 行

| JS 函数 | Python 函数 |
|---------|------------|
| `rowsToMarkdownTable(rows, columns)` | `rows_to_markdown_table(rows: list, columns: list = None) -> str` |

#### `utils/sql_parser.py`

**对应原始代码**：`server.js` 第 120-140 行

| JS 函数 | Python 函数 |
|---------|------------|
| `extractTableRefsFromSql(sql)` | `extract_table_refs_from_sql(sql: str) -> list[dict]` |

返回 `[{"database": str|None, "table": str}]`，正则逻辑与 JS 版完全一致。



### 8. ETL 对话 LangGraph 状态图 (`graphs/etl_chat_graph.py`)

**对应原始代码**：`server.js` 第 305-640 行（`POST /api/chat` 完整处理逻辑）

#### 8.1 状态类型定义

```python
from typing import TypedDict, Optional

class ETLChatState(TypedDict):
    # 输入
    conversation: list          # [{role, content}, ...]
    context: dict               # {connectionString, currentStep, selectedTables}

    # 中间状态
    connection_string: Optional[str]     # 解析后的有效连接串
    should_test_connection: bool         # 是否需要测试连接
    connection_test_note: str            # 连接测试结果文本
    connection_test_ok: bool             # 连接测试是否成功
    db_intent: dict                      # {intent, params}
    db_operation_note: str               # 数据库操作结果文本
    selected_tables_note: str            # 用户已选中库表的提示文本

    # 输出
    llm_response: dict                   # {reply, connectionReceived, connectionTestOk, currentStep}
```

#### 8.2 节点定义

| 节点名 | 功能 | 对应 JS 代码行 |
|--------|------|---------------|
| `parse_input` | 从 conversation/context 中提取 connectionString、lastUserContent、selectedTables，判断 shouldTestConnection | 第 308-325 行 |
| `test_connection` | 调用 `test_connection()` 测试连接，写入 `connection_test_note` 和 `connection_test_ok` | 第 327-340 行 |
| `extract_intent` | 调用 `extract_db_intent_from_model()` 解析数据库操作意图 | 第 343-347 行 |
| `execute_db_operation` | 根据 `db_intent` 执行数据库操作，格式化结果为 Markdown 注入 `db_operation_note`。包含 describeTable 的双查询逻辑（schema + preview）、selectedTables 过滤逻辑、列名错误自动纠正逻辑 | 第 349-500 行 |
| `build_prompt_and_call_llm` | 组装完整 system prompt（逐字复制自 JS），拼接 connectionTestNote + dbOperationNote + selectedTablesNote，调用 LLM（temperature=0.3, max_tokens=4096），解析 JSON 响应 | 第 502-640 行 |

#### 8.3 条件边与流程

```mermaid
graph TD
    START([开始]) --> parse_input
    parse_input --> check_connection{需要测试连接?}
    check_connection -->|是| test_connection
    check_connection -->|否| check_intent{有连接串且非纯连接串?}
    test_connection --> check_intent
    check_intent -->|是| extract_intent
    check_intent -->|否| build_prompt_and_call_llm
    extract_intent --> check_db_op{有有效意图?}
    check_db_op -->|是| execute_db_operation
    check_db_op -->|否| build_prompt_and_call_llm
    execute_db_operation --> build_prompt_and_call_llm
    build_prompt_and_call_llm --> END([结束])
```

#### 8.4 关键逻辑说明

**连接测试触发条件**（与 JS 完全一致）：
- `lastUserContent` 本身是连接串，或
- `lastUserContent` 包含"测试/试一下/验证/检查"且包含"连接/连接串/连通"，且历史消息中有连接串

**意图解析触发条件**：
- 存在有效 `connectionString`
- `lastUserContent` 非空
- `lastUserContent` 不是纯连接串（长度 < 500 的连接串视为纯连接串）

**describeTable 双查询**：当意图为 `describeTable` 时，同时执行 `describeTable` 和 `previewData`（limit=10），将两者结果合并注入 prompt。

**selectedTables 过滤**：当 `selectedTables` 非空时：
- `listDatabases` 结果仅保留选中库
- `listTables` 结果仅保留选中表
- 在 system prompt 中注入选中库表限制说明

**列名错误自动纠正**：当 `executeSQL` 失败且错误信息匹配列名错误模式时，自动查询涉及表的结构并注入 prompt，要求 LLM 给出修正后的 SQL。

**system prompt**：逐字复制自 `server.js` 第 502-612 行，包含：
- 六步引导逻辑（1-连接数据库 → 2-选择基表 → 3-定义目标表 → 4-字段映射 → 5-数据验证 → 6-异常溯源）
- 硬性约定（DDL/DML 需确认、MySQL 语法、禁止 JSON 展示数据、失败如实报告）
- 各步骤引导说明
- 通用回复规则
- 输出格式要求

**LLM 响应解析**：从 LLM 返回内容中提取 JSON（正则 `\{[\s\S]*\}`），解析 `reply`、`connectionReceived`、`currentStep` 字段。若解析失败，使用原始内容作为 reply。

#### 8.5 图构建

```python
from langgraph.graph import StateGraph, END

def build_etl_chat_graph():
    graph = StateGraph(ETLChatState)
    graph.add_node("parse_input", parse_input)
    graph.add_node("test_connection", test_connection_node)
    graph.add_node("extract_intent", extract_intent_node)
    graph.add_node("execute_db_operation", execute_db_operation_node)
    graph.add_node("build_prompt_and_call_llm", build_prompt_and_call_llm_node)

    graph.set_entry_point("parse_input")
    graph.add_conditional_edges("parse_input", should_test_connection,
        {True: "test_connection", False: "extract_intent"})
    graph.add_conditional_edges("test_connection", should_extract_intent,
        {True: "extract_intent", False: "build_prompt_and_call_llm"})
    graph.add_conditional_edges("extract_intent", has_valid_intent,
        {True: "execute_db_operation", False: "build_prompt_and_call_llm"})
    graph.add_edge("execute_db_operation", "build_prompt_and_call_llm")
    graph.add_edge("build_prompt_and_call_llm", END)

    return graph.compile()
```

### 9. 指标对话 LangGraph 状态图 (`graphs/metric_chat_graph.py`)

**对应原始代码**：`server.js` 第 1480-1733 行（`POST /api/metric-chat` 完整处理逻辑）

#### 9.1 状态类型定义

```python
class MetricChatState(TypedDict):
    # 输入
    conversation: list          # [{role, content}, ...]
    connection_string: Optional[str]
    selected_tables: list       # ["db.table", ...]

    # 中间状态
    db_operation_note: str      # 数据库操作结果文本
    schema_context: str         # 可用表结构信息文本

    # 输出
    llm_response: dict          # {reply, metricDef?}
```

#### 9.2 节点定义

| 节点名 | 功能 | 对应 JS 代码行 |
|--------|------|---------------|
| `extract_intent_and_execute` | 调用意图解析 + 执行数据库操作（逻辑与 ETL chat 一致，但 dbOperationNote 格式略有不同） | 第 1490-1600 行 |
| `fetch_schema_context` | 获取可用表结构：若 selectedTables 非空则只查选中表；否则全量扫描（每库最多 20 表，最多 10 库），排除系统库 | 第 1602-1650 行 |
| `build_prompt_and_call_llm` | 组装 system prompt（逐字复制自 JS），拼接 schemaContext + dbOperationNote + selectedTablesRestriction，调用 LLM（temperature=0.3, max_tokens=4096），解析 JSON 响应 | 第 1652-1733 行 |

#### 9.3 条件边与流程

```mermaid
graph TD
    START([开始]) --> extract_intent_and_execute
    extract_intent_and_execute --> fetch_schema_context
    fetch_schema_context --> build_prompt_and_call_llm
    build_prompt_and_call_llm --> END([结束])
```

指标对话图比 ETL 图简单：无连接测试节点（连接串由前端直接传入），意图解析和数据库操作合并为一个节点，始终执行 schema 获取。

#### 9.4 关键逻辑说明

**数据库操作能力**：与 ETL chat 完全一致，支持所有 8 种意图。区别在于：
- 无连接测试逻辑（不判断用户消息是否为连接串）
- `listDatabases` 和 `listTables` 的 selectedTables 过滤逻辑使用 `Set` 而非 `Map`
- `dbOperationNote` 格式略有简化（无"验证 SQL"前缀）

**schema 获取策略**：
- 若 `selectedTables` 非空：仅查询选中表的结构（`DESCRIBE` 每张表）
- 若 `selectedTables` 为空：全量扫描，`SHOW DATABASES` → 排除系统库 → 每库 `SHOW TABLES`（最多 20 张）→ `DESCRIBE` 每张表，最多扫描 10 个库

**system prompt**：逐字复制自 `server.js` 第 1652-1710 行，包含：
- 双能力说明（数据加工 + 指标定义）
- 数据库操作规则
- 指标定义流程
- 意图判断规则
- 回复规则
- 输出格式（普通回复 vs 指标确认回复含 `metricDef`）

**LLM 响应解析**：提取 JSON，解析 `reply` 和可选的 `metricDef`（包含 name、definition、tables、aggregation、measureField）。

**selectedTables 限制注入**：当 `selectedTables` 非空时，在 system prompt 中注入严格限制说明，要求 LLM 只使用选中的表。

### 10. 路由层设计

所有路由使用 `fastapi.APIRouter`，每个路由文件对应一个功能模块。所有路由在请求处理前检查 `LLM_API_KEY` 是否配置（需要 LLM 的端点），未配置返回 HTTP 503。

#### 10.1 ETL 对话路由 (`routers/chat.py`)

**对应原始代码**：`server.js` 第 305-640 行

```
POST /api/chat
```

| 字段 | 说明 |
|------|------|
| 请求体 | `{conversation: [{role, content}], context: {connectionString?, currentStep?, selectedTables?}}` |
| 响应体 | `{reply: str, connectionReceived: bool, connectionTestOk: bool, currentStep: int}` |
| 错误码 | 400（conversation 缺失）、500（LLM 错误）、503（API key 未配置） |

路由处理：
1. 校验 `conversation` 非空数组
2. 构建 `ETLChatState` 初始状态
3. 调用 `build_etl_chat_graph()` 编译的图执行
4. 从图输出的 `llm_response` 中提取响应字段返回

#### 10.2 指标对话路由 (`routers/metric_chat.py`)

**对应原始代码**：`server.js` 第 1480-1733 行

```
POST /api/metric-chat
```

| 字段 | 说明 |
|------|------|
| 请求体 | `{conversation: [{role, content}], connectionString?: str, selectedTables?: [str]}` |
| 响应体 | `{reply: str, metricDef?: {name, definition, tables, aggregation, measureField}}` |
| 错误码 | 400（conversation 缺失）、500（LLM 错误）、503（API key 未配置） |

路由处理：
1. 校验 `conversation` 非空数组
2. 构建 `MetricChatState` 初始状态
3. 调用 `build_metric_chat_graph()` 编译的图执行
4. 从图输出中提取 `reply` 和可选的 `metricDef` 返回

#### 10.3 字段映射路由 (`routers/mapping.py`)

**对应原始代码**：`server.js` 第 650-730 行

```
POST /api/mapping
```

| 字段 | 说明 |
|------|------|
| 请求体 | `{message: str, conversation?: [{role, content}], targetTableName: str, targetFields: [{name, type, comment?}], existingMappings?: [{targetField, status, ...}]}` |
| 响应体 | `{mappings: [{targetField, source, logic, sql}]}` |
| 错误码 | 400（缺少 message/targetTableName/targetFields）、500（LLM 错误）、503 |
| LLM 参数 | temperature=0.2, max_tokens=4096 |

关键逻辑：
- `buildSystemPrompt(targetTableName, targetFields, existingMappings)` 函数逐字复制自 JS
- conversation 预处理：过滤空内容、移除开头的 assistant 消息
- 若 conversation 为空或预处理后为空，仅发送 `[system, user(message)]`
- 从 LLM 响应中提取 JSON，解析 `mappings` 数组

#### 10.4 DML 生成与优化路由 (`routers/dml.py`)

**对应原始代码**：`server.js` 第 732-830 行

```
POST /api/dml
POST /api/dml/optimize
```

**POST /api/dml**：

| 字段 | 说明 |
|------|------|
| 请求体 | `{targetTableFullName: str, mappings: [{targetField, source?, logic?, sql?}]}` |
| 响应体 | `{dml: str}` |
| 错误码 | 400（缺少 targetTableFullName 或 mappings 为空）、500、503 |
| LLM 参数 | temperature=0.1, max_tokens=4096 |

关键逻辑：
- 过滤 mappings：仅保留有 `targetField` 和 `sql` 的项
- system prompt 逐字复制自 JS（TRUNCATE + INSERT INTO ... SELECT 格式要求）
- 用户消息固定为 `"请用 JOIN 写法生成 DML。"`
- 响应中去除 markdown 代码块标记（```` ```sql ````、```` ``` ````）

**POST /api/dml/optimize**：

| 字段 | 说明 |
|------|------|
| 请求体 | `{dml: str}` |
| 响应体 | `{dml: str}` |
| 错误码 | 400（缺少 dml）、500、503 |
| LLM 参数 | temperature=0.1, max_tokens=4096 |

关键逻辑：
- system prompt 逐字复制自 JS（标量子查询改 JOIN）
- 用户消息为 `"请优化：\n\n{dml}"`
- 响应中去除 markdown 代码块标记

#### 10.5 表列表路由 (`routers/tables.py`)

**对应原始代码**：`server.js` 第 860-890 行

```
POST /api/tables
```

| 字段 | 说明 |
|------|------|
| 请求体 | `{connectionString: str}` |
| 响应体 | `{databases: [{database: str, tables: [str]}]}` |
| 错误码 | 400（connectionString 缺失或连接失败）、500 |

关键逻辑：
- 不需要 LLM，纯数据库操作
- 调用 `listDatabases` 获取所有库
- 排除系统库：`information_schema, mysql, performance_schema, sys, mo_catalog, system, system_metrics`
- 对每个非系统库调用 `listTables` 获取表列表
- 组装树形结构返回

#### 10.6 指标 API 路由 (`routers/metric.py`)

**对应原始代码**：`server.js` 第 892-1200 行

```
POST /api/metric/match
POST /api/metric/generate
POST /api/metric/query
```

**POST /api/metric/match**：

| 字段 | 说明 |
|------|------|
| 请求体 | `{description: str, metricDefs: [{name, definition, aggregation, measureField, tables}]}` |
| 响应体 | `{matches: [{name, reason}], suggestion: str}` |
| 错误码 | 400（缺少 description 或 metricDefs）、500、503 |
| LLM 参数 | temperature=0.2, max_tokens=1024 |

**POST /api/metric/generate**：

| 字段 | 说明 |
|------|------|
| 请求体 | `{metricName: str, definition?: str, description: str, metricDefs?: [...], connectionString?: str}` |
| 响应体 | `{sql: str, chartType: str, explanation: str, derivedMetricDef?: {name, definition, tables, aggregation, measureField}}` |
| 错误码 | 400（缺少 metricName 或 description）、500、503 |
| LLM 参数 | temperature=0.2, max_tokens=2048 |

关键逻辑：
- 从 `metricDefs` 收集涉及的表，获取表结构作为上下文
- system prompt 逐字复制自 JS（SQL 兼容性要求、可视化类型选择规则、派生指标建议逻辑）
- **SQL 验证重试机制**：生成 SQL 后在用户数据库上执行验证，失败时将错误信息反馈给 LLM 重新生成，最多重试 3 次
- **派生指标自动检测**：若 LLM 未返回 `derivedMetricDef`，服务端使用规则检测：
  - 比率/比值模式匹配（毛利率、转化率、占比、客单价、同比/环比、完成率）
  - SQL 中的除法运算检测（`SUM(...)/SUM(...)` 模式）
- `chartType` 限制为 `number|bar|line|pie|table`，不在列表中默认 `table`

**POST /api/metric/query**：

| 字段 | 说明 |
|------|------|
| 请求体 | `{sql: str, connectionString: str}` |
| 响应体 | `{rows: list, rowCount: int}` |
| 错误码 | 400（缺少参数或 SQL 含禁止关键字或执行失败）、500 |

关键逻辑：
- 不需要 LLM，纯数据库操作
- 禁止关键字列表比 `executeSQL` 更严格：`DROP|TRUNCATE|DELETE|UPDATE|INSERT|CREATE|ALTER`
- 通过 `runDatabaseOperation` 的 `executeSQL` 意图执行

#### 10.7 血缘分析路由 (`routers/lineage.py`)

**对应原始代码**：`server.js` 第 1210-1478 行

```
POST /api/lineage
POST /api/metric-lineage
```

**POST /api/lineage**：

| 字段 | 说明 |
|------|------|
| 请求体 | `{sql: str, connectionString?: str, targetTable?: str}` |
| 响应体 | `{targetTable, sourceTables, fieldMappings, joinRelations, groupBy, filters}` |
| 错误码 | 400（缺少 sql）、500、503 |
| LLM 参数 | temperature=0.1, max_tokens=4096 |

关键逻辑：
- 使用 `extractTableRefsFromSql` 从 SQL 中提取涉及的表
- 若有 `connectionString`，获取每张表的结构作为上下文
- system prompt 逐字复制自 JS（血缘分析规则、输出 JSON 格式）

**POST /api/metric-lineage**：

| 字段 | 说明 |
|------|------|
| 请求体 | `{metricDef: {name, definition, aggregation, measureField, tables}, processedTables?: [{database, table, sourceTables, fieldMappings, insertSql}], connectionString?: str}` |
| 响应体 | `{layers: [{level, label, tables}], edges: [{from, to, transform}], summary: str}` |
| 错误码 | 400（缺少 metricDef）、500、503 |
| LLM 参数 | temperature=0.1, max_tokens=4096 |

关键逻辑：
- 从 `metricDef.tables` 和 `processedTables` 收集所有相关表
- 匹配策略：先直接匹配指标涉及的表，若无匹配则使用所有传入的加工表
- 获取所有相关表的结构作为上下文
- 收集 ETL 加工信息（来源表、字段映射、加工 SQL）
- system prompt 逐字复制自 JS（全链路血缘分析规则、三层结构 source→processed→metric）

#### 10.8 调试路由 (`routers/debug.py`)

**对应原始代码**：`server.js` 第 832-858 行

```
GET /api/debug-deepseek
```

| 字段 | 说明 |
|------|------|
| 响应体（成功） | `{ok: true, reply: str}` |
| 响应体（未配置） | `{ok: false, reason: "DEEPSEEK_API_KEY 未设置"}` |
| 响应体（失败） | `{ok: false, status?: int, body?: any, error?: str, code?: str}` |

关键逻辑：
- 使用 OpenAI 兼容客户端发送测试消息：`[{role: "system", content: "You are a helpful assistant."}, {role: "user", content: "Say hello in one word."}]`
- 注意：错误响应中的 `reason` 文本保持原有的 `"DEEPSEEK_API_KEY 未设置"`（不改为 LLM_API_KEY），确保前端兼容

## 数据模型

### 请求/响应模型（Pydantic）

```python
from pydantic import BaseModel
from typing import Optional

# /api/chat
class ChatRequest(BaseModel):
    conversation: list  # [{role: str, content: str}]
    context: Optional[dict] = None  # {connectionString?, currentStep?, selectedTables?}

class ChatResponse(BaseModel):
    reply: str
    connectionReceived: bool = False
    connectionTestOk: bool = False
    currentStep: int = 1

# /api/metric-chat
class MetricChatRequest(BaseModel):
    conversation: list
    connectionString: Optional[str] = None
    selectedTables: Optional[list] = None

class MetricDef(BaseModel):
    name: str
    definition: str
    tables: list
    aggregation: str
    measureField: str

class MetricChatResponse(BaseModel):
    reply: str
    metricDef: Optional[MetricDef] = None

# /api/mapping
class MappingRequest(BaseModel):
    message: str
    conversation: Optional[list] = None
    targetTableName: str
    targetFields: list  # [{name, type, comment?}]
    existingMappings: Optional[list] = None

class MappingResponse(BaseModel):
    mappings: list  # [{targetField, source, logic, sql}]

# /api/dml
class DmlRequest(BaseModel):
    targetTableFullName: str
    mappings: list  # [{targetField, source?, logic?, sql?}]

class DmlOptimizeRequest(BaseModel):
    dml: str

class DmlResponse(BaseModel):
    dml: str

# /api/tables
class TablesRequest(BaseModel):
    connectionString: str

# /api/metric/match
class MetricMatchRequest(BaseModel):
    description: str
    metricDefs: list

# /api/metric/generate
class MetricGenerateRequest(BaseModel):
    metricName: str
    definition: Optional[str] = None
    description: str
    metricDefs: Optional[list] = None
    connectionString: Optional[str] = None

# /api/metric/query
class MetricQueryRequest(BaseModel):
    sql: str
    connectionString: str

# /api/lineage
class LineageRequest(BaseModel):
    sql: str
    connectionString: Optional[str] = None
    targetTable: Optional[str] = None

# /api/metric-lineage
class MetricLineageRequest(BaseModel):
    metricDef: dict
    processedTables: Optional[list] = None
    connectionString: Optional[str] = None
```

### 内部数据结构

| 结构 | 字段 | 说明 |
|------|------|------|
| 连接配置 | `{host, port, user, password, database}` | MySQL 连接参数 |
| 意图解析结果 | `{intent: str\|None, params: dict}` | 数据库操作意图 |
| 数据库操作结果 | `{ok: bool, data?: dict, error?: str}` | 统一操作返回 |
| LLM 调用结果 | `{ok: bool, content: str, error: str}` | 统一 LLM 返回 |



## 正确性属性

*属性（Property）是指在系统所有有效执行中都应成立的特征或行为——本质上是对系统应做什么的形式化陈述。属性是人类可读规格说明与机器可验证正确性保证之间的桥梁。*

### Property 1: 连接串解析正确性

*For any* 有效的 MySQL URL 格式连接串（`mysql://user:pass@host:port/db`），解析后应正确提取 host、port、user、password、database 五个字段，且 user 和 password 中的 URL 编码字符应被正确解码。同理，*for any* 有效的 MySQL CLI 格式连接串（`mysql -h host -u user -p pass`），解析后也应正确提取所有字段。

**Validates: Requirements 2.1, 2.2, 2.6**

### Property 2: 无效连接串返回 null

*For any* 不符合 URL 格式且不符合 CLI 格式的随机字符串，`get_connection_config` 应返回 `None`。

**Validates: Requirements 2.3**

### Property 3: 连接串解析往返一致性

*For any* 有效的连接配置对象 `{host, port, user, password, database}`，将其格式化为 `mysql://user:pass@host:port/db` URL 字符串后再解析，应得到与原始配置等价的对象。

**Validates: Requirements 2.7**

### Property 4: SQL 禁止关键字拒绝

*For any* 包含 `DROP`、`TRUNCATE`、`DELETE`、`UPDATE` 关键字的 SQL 字符串，`executeSQL` 操作应返回失败结果。*For any* 包含 `DROP`、`TRUNCATE`、`DELETE`、`UPDATE`、`INSERT`、`CREATE`、`ALTER` 关键字的 SQL 字符串，`/api/metric/query` 端点应返回 HTTP 400 错误。

**Validates: Requirements 3.7, 10.2**

### Property 5: 安全标识符验证

*For any* 仅包含字母、数字、下划线的非空字符串，`safe_identifier` 应返回该字符串本身。*For any* 包含其他字符的字符串（或空字符串），`safe_identifier` 应返回 `None`。

**Validates: Requirements 3.9**

### Property 6: ETL 对话条件路由

*For any* 用户消息，若该消息被 `looks_like_connection_string` 判定为连接串，则 ETL 图应走连接测试路径。*For any* 非纯连接串的用户消息（在存在有效连接的前提下），ETL 图应走意图解析路径。

**Validates: Requirements 4.2, 4.3**

### Property 7: Markdown 表格格式化

*For any* 非空的行数据列表（`list[dict]`），`rows_to_markdown_table` 应返回包含表头行、分隔行和数据行的有效 Markdown 表格字符串，且列数与输入字典的键数一致，行数与输入列表长度一致。

**Validates: Requirements 4.5**

### Property 8: selectedTables 过滤

*For any* 非空的 `selectedTables` 列表和 `listDatabases`/`listTables` 的原始结果，过滤后的结果应仅包含 `selectedTables` 中指定的库和表，且不包含任何未选中的库表。

**Validates: Requirements 4.8**

### Property 9: 意图解析优雅降级

*For any* 不在 `VALID_INTENTS` 列表中的意图字符串，意图解析应返回 `null` 意图。*For any* 格式异常的 LLM 响应（非 JSON、空内容、缺少字段），意图解析应返回 `{intent: None, params: {}}` 而非抛出异常。

**Validates: Requirements 5.4, 5.5**

### Property 10: 意图解析消息截断

*For any* 长度为 N 的对话列表，传递给 LLM 的用户消息数量应为 `min(N, 8)`，即仅取最后 8 条消息。

**Validates: Requirements 5.2**

### Property 11: 请求校验返回 400

*For any* 缺少必要字段的请求体（如 `/api/mapping` 缺少 message/targetTableName/targetFields，`/api/dml` 缺少 targetTableFullName/mappings），对应端点应返回 HTTP 400 错误。

**Validates: Requirements 6.5, 7.5**

### Property 12: 派生指标自动检测

*For any* 包含比率/比值关键词（毛利率、转化率、占比、客单价、同比/环比、完成率）的指标名称或描述，当 LLM 未返回 `derivedMetricDef` 时，服务端规则应自动检测并生成派生指标定义。*For any* SQL 中包含 `SUM(...)/SUM(...)` 除法模式的情况，也应触发派生指标检测。

**Validates: Requirements 9.5**

### Property 13: LLM 未配置时返回 503

*For any* 需要 LLM 调用的 API 端点（chat、metric-chat、mapping、dml、dml/optimize、metric/match、metric/generate、lineage、metric-lineage、debug-deepseek），当 `LLM_API_KEY` 未配置时，应返回 HTTP 503 错误（debug-deepseek 返回 `{ok: false}`）。

**Validates: Requirements 14.5**

### Property 14: LangGraph 状态隔离

*For any* 两个并发的图执行请求，它们的状态应完全独立，一个请求的 `conversation`、`db_intent`、`connection_string` 等状态不应泄漏到另一个请求。

**Validates: Requirements 15.3**

## 错误处理

### 错误处理策略

| 错误场景 | HTTP 状态码 | 响应格式 | 对应 JS 行为 |
|----------|------------|---------|-------------|
| LLM_API_KEY 未配置 | 503 | `{error: "DEEPSEEK_API_KEY not configured"}` | 完全一致 |
| 请求体缺少必要字段 | 400 | `{error: "Missing ..."}` | 完全一致 |
| LLM API 调用失败 | LLM 返回的状态码 | `{error: "错误信息"}` | 透传 LLM 错误 |
| LLM 返回格式异常 | 500 | `{error: "DeepSeek 返回格式异常"}` | 完全一致 |
| 数据库连接失败 | 400 | `{error: "具体错误信息"}` | 完全一致 |
| SQL 执行失败 | 400 | `{error: "具体错误信息"}` | 完全一致 |
| 连接串解析失败 | 200（在 chat 响应中） | 连接测试失败信息注入 reply | 完全一致 |
| 意图解析失败 | 不报错 | 返回 `{intent: null}` | 完全一致 |
| 未知异常 | 500 | `{error: "错误信息"}` | 完全一致 |

### 错误处理原则

1. **所有错误响应格式与 JS 版完全一致**：错误消息文本、HTTP 状态码、JSON 结构均保持不变
2. **数据库连接必须在 finally 中释放**：使用 `try/finally` 确保 `conn.close()` 被调用
3. **LLM 调用错误透传**：将 LLM API 返回的 HTTP 状态码和错误信息直接透传给前端
4. **意图解析永不抛异常**：任何解析失败均返回 `{intent: None, params: {}}`
5. **JSON 解析失败降级**：LLM 返回内容无法解析为 JSON 时，使用原始文本作为 reply

## 测试策略

### 测试框架

- **单元测试**：`pytest` + `pytest-asyncio`（异步测试支持）
- **属性测试**：`hypothesis`（Python 属性测试库）
- **HTTP 测试**：`httpx` + FastAPI `TestClient`
- **Mock**：`unittest.mock` / `pytest-mock`（Mock LLM 调用和数据库连接）

### 属性测试配置

- 每个属性测试最少运行 **100 次迭代**（`@settings(max_examples=100)`）
- 每个属性测试必须通过注释引用设计文档中的属性编号
- 标签格式：`# Feature: fastapi-langgraph-migration, Property {N}: {property_text}`
- 每个正确性属性由**单个**属性测试实现

### 单元测试范围

单元测试聚焦于具体示例、边界情况和集成点：

1. **配置模块**：环境变量读取、默认值（Property 相关的 example 测试）
2. **API 端点注册**：验证 12 个端点路径和 HTTP 方法正确注册
3. **响应格式**：验证各端点的响应 JSON 结构符合契约
4. **system prompt 一致性**：逐字比对 Python 版和 JS 版的 prompt 文本
5. **LLM 参数配置**：验证各端点使用正确的 temperature 和 max_tokens
6. **LangGraph 图结构**：验证节点和边的定义正确
7. **debug-deepseek 未配置响应**：验证返回 `{ok: false, reason: "DEEPSEEK_API_KEY 未设置"}`

### 属性测试范围

属性测试覆盖所有正确性属性（Property 1-14），使用 `hypothesis` 生成随机输入：

1. **Property 1-3**：连接串解析（生成随机 host/port/user/password/database 组合）
2. **Property 4**：SQL 禁止关键字（生成包含禁止关键字的随机 SQL）
3. **Property 5**：安全标识符（生成随机字符串，含特殊字符）
4. **Property 6**：条件路由（生成随机用户消息，含/不含连接串模式）
5. **Property 7**：Markdown 表格（生成随机行数据列表）
6. **Property 8**：selectedTables 过滤（生成随机库表列表和选中列表）
7. **Property 9-10**：意图解析（生成随机意图字符串和对话列表）
8. **Property 11**：请求校验（生成缺少不同字段组合的请求体）
9. **Property 12**：派生指标检测（生成包含比率关键词的随机指标名称）
10. **Property 13**：503 响应（遍历所有 LLM 依赖端点）
11. **Property 14**：状态隔离（并发执行多个图实例）

### 集成测试（需要外部依赖）

集成测试需要真实的 MySQL 数据库和 LLM API，不在属性测试范围内：

- 数据库操作的 8 种意图执行（需求 3.2-3.6, 3.8, 3.10）
- ETL 对话完整流程（需求 4.4, 4.6）
- 指标对话完整流程（需求 12.2-12.5）
- LLM API 健康检查（需求 13.1, 13.3, 13.4）
- 血缘分析（需求 11.1-11.2）
