# 实施计划：FastAPI + LangGraph 架构迁移

## 概述

将现有 Express.js 后端（`server/server.js`）逐步迁移为 Python FastAPI + LangGraph 架构。所有 prompt 逐字复制，所有 API 契约保持不变。按模块从底层工具到上层路由逐步构建，每步确保可测试。

## 任务

- [ ] 1. 项目基础结构与配置
  - [x] 1.1 创建项目目录结构和依赖文件
    - 创建 `server_py/` 目录及所有子目录（routers/、graphs/、db/、llm/、utils/）
    - 创建 `requirements.txt`，包含 fastapi、uvicorn、aiomysql、langgraph、langchain-core、langchain-openai、python-dotenv、openai、httpx、pydantic、pytest、pytest-asyncio、hypothesis、httpx 依赖
    - 创建所有 `__init__.py` 文件
    - 创建 `.env.example`，包含 LLM_API_KEY、LLM_BASE_URL、LLM_MODEL、PORT、DEEPSEEK_API_KEY 变量
    - _需求: 16.1, 16.2, 16.3, 16.4_

  - [ ] 1.2 实现配置模块 (`config.py`)
    - 从 `.env` 读取 LLM_API_KEY（向后兼容 DEEPSEEK_API_KEY）、LLM_BASE_URL（默认 `https://api.deepseek.com/v1`）、LLM_MODEL、PORT（默认 3001）
    - _需求: 1.4, 1.5_

  - [ ] 1.3 实现 FastAPI 应用入口 (`main.py`)
    - 创建 FastAPI app，启用 CORS 中间件（allow_origins=["*"]）
    - 配置 2MB 请求体大小限制
    - 实现 `GET /` 根路由，返回服务名称、状态消息和端点列表
    - 添加 uvicorn 启动入口
    - _需求: 1.1, 1.2, 1.3, 1.6_

- [ ] 2. 工具函数层
  - [ ] 2.1 实现 Markdown 表格格式化 (`utils/formatters.py`)
    - 从 `server.js` 第 141-153 行逐字迁移 `rowsToMarkdownTable` 逻辑为 `rows_to_markdown_table(rows, columns=None)`
    - _需求: 4.5_

  - [ ]* 2.2 编写 Markdown 表格格式化属性测试
    - **Property 7: Markdown 表格格式化**
    - 使用 hypothesis 生成随机 list[dict] 数据，验证输出包含表头行、分隔行、数据行，列数和行数一致
    - **验证: 需求 4.5**

  - [ ] 2.3 实现 SQL 表引用提取 (`utils/sql_parser.py`)
    - 从 `server.js` 第 113-139 行逐字迁移正则逻辑为 `extract_table_refs_from_sql(sql) -> list[dict]`
    - 返回 `[{"database": str|None, "table": str}]`
    - _需求: 11.4_

- [ ] 3. MySQL 连接解析与管理
  - [ ] 3.1 实现连接串解析 (`db/connection.py`)
    - 从 `server.js` 第 12-95 行逐字迁移以下函数：
      - `looks_like_connection_string(s) -> bool`
      - `parse_connection_string_url(s) -> Optional[dict]`（含 URL 解码）
      - `parse_mysql_cli_connection_string(s) -> Optional[dict]`
      - `get_connection_config(connection_string) -> Optional[dict]`
      - `async test_connection(connection_string) -> dict`（8 秒超时）
    - _需求: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_

  - [ ]* 3.2 编写连接串解析属性测试
    - **Property 1: 连接串解析正确性**
    - 使用 hypothesis 生成随机 host/port/user/password/database，构造 URL 和 CLI 格式连接串，验证解析结果正确
    - **验证: 需求 2.1, 2.2, 2.6**

  - [ ]* 3.3 编写无效连接串属性测试
    - **Property 2: 无效连接串返回 null**
    - 使用 hypothesis 生成不符合 URL/CLI 格式的随机字符串，验证 `get_connection_config` 返回 None
    - **验证: 需求 2.3**

  - [ ]* 3.4 编写连接串往返一致性属性测试
    - **Property 3: 连接串解析往返一致性**
    - 使用 hypothesis 生成随机连接配置，格式化为 URL 后再解析，验证等价
    - **验证: 需求 2.7**

- [ ] 4. 数据库操作引擎
  - [ ] 4.1 实现数据库操作模块 (`db/operations.py`)
    - 从 `server.js` 第 82-260 行逐字迁移以下函数：
      - `safe_identifier(name) -> Optional[str]`
      - `extract_database_from_create_table(ddl) -> Optional[str]`
      - `async get_mysql_connection(connection_string) -> aiomysql.Connection`（10 秒超时）
      - `async run_database_operation(connection_string, intent, params) -> dict`（支持 8 种意图）
    - executeSQL 中禁止 DROP/TRUNCATE/DELETE/UPDATE 关键字
    - 确保 finally 中释放数据库连接
    - _需求: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 3.10_

  - [ ]* 4.2 编写安全标识符属性测试
    - **Property 5: 安全标识符验证**
    - 使用 hypothesis 生成随机字符串，验证仅字母/数字/下划线的非空字符串返回自身，其他返回 None
    - **验证: 需求 3.9**

  - [ ]* 4.3 编写 SQL 禁止关键字属性测试
    - **Property 4: SQL 禁止关键字拒绝**
    - 使用 hypothesis 生成包含 DROP/TRUNCATE/DELETE/UPDATE 的随机 SQL，验证 executeSQL 返回失败
    - **验证: 需求 3.7, 10.2**

- [ ] 5. 检查点 - 基础模块验证
  - 确保所有测试通过，如有问题请向用户确认。

- [ ] 6. LLM 客户端与意图解析
  - [ ] 6.1 实现 LLM 客户端 (`llm/client.py`)
    - 使用 `openai` Python SDK 的 `AsyncOpenAI` 客户端
    - 通过 `LLM_BASE_URL` 和 `LLM_API_KEY` 配置
    - 实现 `call_llm(messages, temperature, max_tokens) -> dict`，返回 `{ok, content, error}`
    - _需求: 1.4, 1.5_

  - [ ] 6.2 实现意图解析 (`llm/intent.py`)
    - 从 `server.js` 第 289-340 行逐字复制意图解析 system prompt
    - 实现 `extract_db_intent_from_model(conversation, api_key, chat_url) -> dict`
    - 仅取最后 8 条消息，temperature=0.1，max_tokens=1024
    - 解析失败返回 `{intent: None, params: {}}`
    - VALID_INTENTS 列表与 JS 版完全一致
    - _需求: 5.1, 5.2, 5.3, 5.4, 5.5_

  - [ ]* 6.3 编写意图解析属性测试
    - **Property 9: 意图解析优雅降级**
    - 验证无效意图字符串返回 null，格式异常的 LLM 响应返回 `{intent: None, params: {}}` 而非抛出异常
    - **验证: 需求 5.4, 5.5**

  - [ ]* 6.4 编写意图解析消息截断属性测试
    - **Property 10: 意图解析消息截断**
    - 使用 hypothesis 生成不同长度的对话列表，验证传递给 LLM 的消息数量为 min(N, 8)
    - **验证: 需求 5.2**

- [ ] 7. Pydantic 请求/响应模型
  - [ ] 7.1 定义所有 API 的 Pydantic 请求/响应模型
    - 在各 router 文件或独立 models 文件中定义 ChatRequest、ChatResponse、MetricChatRequest、MetricChatResponse、MappingRequest、MappingResponse、DmlRequest、DmlOptimizeRequest、DmlResponse、TablesRequest、MetricMatchRequest、MetricGenerateRequest、MetricQueryRequest、LineageRequest、MetricLineageRequest
    - 所有字段名与现有 JS API 的 JSON 字段名完全一致（camelCase）
    - _需求: 14.2, 14.3_

- [ ] 8. ETL 对话 LangGraph 状态图
  - [ ] 8.1 实现 ETL 对话状态图 (`graphs/etl_chat_graph.py`)
    - 定义 `ETLChatState` TypedDict 状态类型
    - 实现 5 个节点：`parse_input`、`test_connection`、`extract_intent`、`execute_db_operation`、`build_prompt_and_call_llm`
    - 从 `server.js` 第 305-640 行逐字复制所有 system prompt 内容（六步引导逻辑、硬性约定、各步骤引导说明）
    - 实现条件边：连接测试判断 → 意图解析判断 → 有效意图判断
    - 实现 describeTable 双查询逻辑（schema + preview）
    - 实现 selectedTables 过滤逻辑
    - 实现列名错误自动纠正逻辑
    - LLM 调用参数：temperature=0.3, max_tokens=4096
    - LLM 响应 JSON 解析（正则 `\{[\s\S]*\}`），解析失败使用原始文本作为 reply
    - 实现 `build_etl_chat_graph()` 返回编译后的图
    - _需求: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.9, 15.1, 15.2, 15.3, 15.4_

  - [ ]* 8.2 编写 ETL 对话条件路由属性测试
    - **Property 6: ETL 对话条件路由**
    - 验证连接串消息走连接测试路径，非纯连接串消息（有有效连接时）走意图解析路径
    - **验证: 需求 4.2, 4.3**

  - [ ]* 8.3 编写 selectedTables 过滤属性测试
    - **Property 8: selectedTables 过滤**
    - 使用 hypothesis 生成随机库表列表和选中列表，验证过滤结果仅包含选中项
    - **验证: 需求 4.8**

- [ ] 9. 指标对话 LangGraph 状态图
  - [ ] 9.1 实现指标对话状态图 (`graphs/metric_chat_graph.py`)
    - 定义 `MetricChatState` TypedDict 状态类型
    - 实现 3 个节点：`extract_intent_and_execute`、`fetch_schema_context`、`build_prompt_and_call_llm`
    - 从 `server.js` 第 1480-1733 行逐字复制所有 system prompt 内容（双能力说明、指标定义流程）
    - 实现 schema 获取策略（selectedTables 非空查选中表，否则全量扫描，每库最多 20 表，最多 10 库）
    - 实现 selectedTables 限制注入
    - LLM 响应解析：提取 reply 和可选 metricDef
    - 实现 `build_metric_chat_graph()` 返回编译后的图
    - _需求: 12.1, 12.2, 12.3, 12.4, 12.5, 12.6_

- [ ] 10. 检查点 - 核心引擎验证
  - 确保所有测试通过，如有问题请向用户确认。

- [ ] 11. API 路由层 — 对话与调试
  - [ ] 11.1 实现 ETL 对话路由 (`routers/chat.py`)
    - POST /api/chat，校验 conversation 非空
    - 构建 ETLChatState 初始状态，调用图执行
    - 返回 `{reply, connectionReceived, connectionTestOk, currentStep}`
    - 错误码：400（conversation 缺失）、500（LLM 错误）、503（API key 未配置）
    - _需求: 4.9, 14.1, 14.2, 14.3, 14.4, 14.5_

  - [ ] 11.2 实现指标对话路由 (`routers/metric_chat.py`)
    - POST /api/metric-chat，校验 conversation 非空
    - 构建 MetricChatState 初始状态，调用图执行
    - 返回 `{reply, metricDef?}`
    - 错误码：400、500、503
    - _需求: 12.6, 14.1, 14.2, 14.3, 14.4, 14.5_

  - [ ] 11.3 实现调试路由 (`routers/debug.py`)
    - GET /api/debug-deepseek
    - LLM_API_KEY 未配置返回 `{ok: false, reason: "DEEPSEEK_API_KEY 未设置"}`（保持原文本）
    - 成功返回 `{ok: true, reply: "..."}`
    - 失败返回 `{ok: false, status?, body?, error?, code?}`
    - _需求: 13.1, 13.2, 13.3, 13.4_

  - [ ]* 11.4 编写 LLM 未配置返回 503 属性测试
    - **Property 13: LLM 未配置时返回 503**
    - 遍历所有需要 LLM 的端点，验证 LLM_API_KEY 未配置时返回 503（debug-deepseek 返回 {ok: false}）
    - **验证: 需求 14.5**

- [ ] 12. API 路由层 — 字段映射与 DML
  - [ ] 12.1 实现字段映射路由 (`routers/mapping.py`)
    - POST /api/mapping
    - 从 `server.js` 第 650-730 行逐字复制 `buildSystemPrompt` 函数和 system prompt
    - conversation 预处理：过滤空内容、移除开头 assistant 消息
    - temperature=0.2, max_tokens=4096
    - 返回 `{mappings: [{targetField, source, logic, sql}]}`
    - 错误码：400（缺少 message/targetTableName/targetFields）、500、503
    - _需求: 6.1, 6.2, 6.3, 6.4, 6.5_

  - [ ] 12.2 实现 DML 生成与优化路由 (`routers/dml.py`)
    - POST /api/dml：从 `server.js` 第 732-790 行逐字复制 system prompt，过滤 mappings，temperature=0.1, max_tokens=4096，去除 markdown 代码块标记
    - POST /api/dml/optimize：从 `server.js` 第 792-830 行逐字复制 system prompt，temperature=0.1, max_tokens=4096
    - 错误码：400、500、503
    - _需求: 7.1, 7.2, 7.3, 7.4, 7.5_

  - [ ]* 12.3 编写请求校验返回 400 属性测试
    - **Property 11: 请求校验返回 400**
    - 使用 hypothesis 生成缺少不同必要字段组合的请求体，验证 /api/mapping 和 /api/dml 返回 HTTP 400
    - **验证: 需求 6.5, 7.5**

- [ ] 13. API 路由层 — 表列表与指标
  - [ ] 13.1 实现表列表路由 (`routers/tables.py`)
    - POST /api/tables，纯数据库操作（不需要 LLM）
    - 排除系统库（information_schema, mysql, performance_schema, sys, mo_catalog, system, system_metrics）
    - 返回 `{databases: [{database, tables: []}]}`
    - 错误码：400（connectionString 缺失或连接失败）
    - _需求: 8.1, 8.2, 8.3_

  - [ ] 13.2 实现指标 API 路由 (`routers/metric.py`)
    - POST /api/metric/match：从 `server.js` 逐字复制 system prompt，temperature=0.2, max_tokens=1024
    - POST /api/metric/generate：从 `server.js` 逐字复制 system prompt，temperature=0.2, max_tokens=2048，实现 SQL 验证重试机制（最多 3 次），实现派生指标自动检测（比率关键词 + SUM/SUM 除法模式）
    - POST /api/metric/query：纯数据库操作，禁止 DROP/TRUNCATE/DELETE/UPDATE/INSERT/CREATE/ALTER，返回 `{rows, rowCount}`
    - _需求: 9.1, 9.2, 9.3, 9.4, 9.5, 10.1, 10.2, 10.3, 10.4_

  - [ ]* 13.3 编写派生指标自动检测属性测试
    - **Property 12: 派生指标自动检测**
    - 使用 hypothesis 生成包含比率/比值关键词的随机指标名称，验证自动检测触发
    - 验证 SQL 中 SUM(...)/SUM(...) 模式也触发检测
    - **验证: 需求 9.5**

- [ ] 14. API 路由层 — 血缘分析
  - [ ] 14.1 实现血缘分析路由 (`routers/lineage.py`)
    - POST /api/lineage：从 `server.js` 第 1210-1350 行逐字复制 system prompt，使用 `extract_table_refs_from_sql` 提取表引用，获取表结构上下文，temperature=0.1, max_tokens=4096
    - POST /api/metric-lineage：从 `server.js` 第 1352-1478 行逐字复制 system prompt，实现表匹配策略（先直接匹配，无匹配则使用所有加工表），收集 ETL 加工信息，temperature=0.1, max_tokens=4096
    - 错误码：400（缺少 sql/metricDef）、500、503
    - _需求: 11.1, 11.2, 11.3, 11.4_

- [ ] 15. 路由注册与集成
  - [ ] 15.1 在 `main.py` 中注册所有路由
    - 将所有 8 个 router 模块注册到 FastAPI app
    - 验证 12 个 API 端点路径和 HTTP 方法正确注册
    - _需求: 14.1_

  - [ ]* 15.2 编写 LangGraph 状态隔离属性测试
    - **Property 14: LangGraph 状态隔离**
    - 并发执行多个图实例，验证状态不泄漏
    - **验证: 需求 15.3**

- [ ] 16. 最终检查点 - 全量验证
  - 确保所有测试通过，如有问题请向用户确认。

## 备注

- 标记 `*` 的子任务为可选，可跳过以加速 MVP
- 每个任务引用具体需求编号以确保可追溯性
- 所有 prompt 必须从 `server.js` 逐字复制，禁止任何修改
- 属性测试验证设计文档中的正确性属性
- 检查点确保增量验证
