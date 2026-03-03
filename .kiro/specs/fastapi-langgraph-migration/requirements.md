# 需求文档：FastAPI + LangGraph 架构迁移

## 简介

将现有的 Express.js 后端（`server/server.js`，约 1733 行）迁移为 Python FastAPI + LangGraph 架构。所有 API 端点、业务逻辑、LLM prompt 和前端接口契约保持完全不变，仅改变底层技术栈。现有系统是一个智能数据 ETL 助手，通过 DeepSeek LLM 驱动对话式数据加工流程，包含 12 个 API 端点。

### 迁移原则

1. **纯架构迁移，零功能变更**：所有 prompt 文本、LLM 参数、业务逻辑、错误处理行为必须与现有 `server.js` 完全一致，逐字照搬 prompt 内容，不做任何改写、优化或"改进"
2. **LLM 客户端使用 OpenAI 兼容接口**：通过 `openai` Python SDK（OpenAI 兼容模式）调用 LLM，支持通过环境变量 `LLM_BASE_URL` 和 `LLM_API_KEY` 配置任意 OpenAI 兼容模型（如 DeepSeek、本地模型等），不硬编码特定厂商 SDK

## 术语表

- **FastAPI_Server**：基于 Python FastAPI 框架的新后端服务，替代现有 Express.js 服务
- **LangGraph_Engine**：基于 LangGraph 框架的 LLM 编排引擎，管理多步对话状态与 LLM 调用链
- **LLM_Client**：基于 OpenAI 兼容接口的 LLM 客户端模块，通过 `LLM_BASE_URL` 和 `LLM_API_KEY` 环境变量配置，可对接 DeepSeek 或任意 OpenAI 兼容模型
- **MySQL_Connector**：负责连接用户 MySQL 数据库并执行 SQL 操作的模块
- **Intent_Parser**：从对话上下文中提取用户数据库操作意图的 LangGraph 节点
- **ETL_Chat_Graph**：处理 `/api/chat` 端点的 LangGraph 状态图，包含意图解析、数据库操作、prompt 组装和 LLM 调用
- **Metric_Chat_Graph**：处理 `/api/metric-chat` 端点的 LangGraph 状态图
- **API_Router**：FastAPI 路由模块，定义所有 HTTP 端点
- **Connection_Parser**：解析 MySQL 连接串（URL 格式和 CLI 格式）的工具模块

## 需求

### 需求 1：FastAPI 服务基础架构

**用户故事：** 作为开发者，我希望后端服务从 Express.js 迁移到 FastAPI，以便使用 Python 生态和 LangGraph 框架。

#### 验收标准

1. THE FastAPI_Server SHALL 在与现有 Express.js 相同的端口（默认 3001，可通过 PORT 环境变量配置）上启动 HTTP 服务
2. THE FastAPI_Server SHALL 启用 CORS 中间件，允许所有来源的跨域请求
3. THE FastAPI_Server SHALL 支持最大 2MB 的 JSON 请求体
4. THE FastAPI_Server SHALL 从 `.env` 文件读取 `LLM_API_KEY`、`LLM_BASE_URL` 和 `PORT` 环境变量
5. WHEN `LLM_BASE_URL` 未配置时，THE FastAPI_Server SHALL 默认使用 `https://api.deepseek.com/v1` 作为 LLM 端点
6. WHEN 收到 `GET /` 请求时，THE FastAPI_Server SHALL 返回包含服务名称、状态消息和所有可用端点列表的 JSON 响应

### 需求 2：MySQL 连接串解析与连接管理

**用户故事：** 作为数据分析师，我希望系统能解析我提供的各种格式的 MySQL 连接串，以便连接到我的数据库。

#### 验收标准

1. WHEN 收到 URL 格式的连接串（如 `mysql://user:pass@host:3306/db`）时，THE Connection_Parser SHALL 解析出 host、port、user、password 和 database 字段
2. WHEN 收到 MySQL CLI 格式的连接串（如 `mysql -h host -u user -p pass`）时，THE Connection_Parser SHALL 解析出 host、port、user、password 和 database 字段
3. IF 连接串格式无法识别，THEN THE Connection_Parser SHALL 返回 null
4. WHEN 执行连接测试时，THE MySQL_Connector SHALL 在 8 秒超时内完成连接并执行 ping 操作
5. IF 连接测试失败，THEN THE MySQL_Connector SHALL 返回包含具体错误信息的失败结果
6. THE Connection_Parser SHALL 对解析后的连接串进行 URL 解码处理（用户名和密码中的特殊字符）
7. FOR ALL 有效的 MySQL 连接串，解析后再格式化再解析 SHALL 产生等价的连接配置对象（往返一致性）

### 需求 3：数据库操作执行引擎

**用户故事：** 作为数据分析师，我希望系统能在我的 MySQL 数据库上执行各种操作，以便完成 ETL 流程。

#### 验收标准

1. THE MySQL_Connector SHALL 支持以下 8 种操作意图：createDatabase、listDatabases、listTables、describeTable、previewData、createTable、executeSQL、analyzeNulls
2. WHEN 执行 listDatabases 操作时，THE MySQL_Connector SHALL 返回所有数据库名称及每个库的表数量
3. WHEN 执行 listTables 操作时，THE MySQL_Connector SHALL 返回指定数据库下的所有表名
4. WHEN 执行 describeTable 操作时，THE MySQL_Connector SHALL 返回表的完整列信息（Field、Type、Null、Key、Default、Extra）
5. WHEN 执行 previewData 操作时，THE MySQL_Connector SHALL 返回指定表的前 N 条数据（N 上限为 50）
6. WHEN 执行 createTable 操作时，THE MySQL_Connector SHALL 执行用户提供的 CREATE TABLE DDL，并在目标库不存在时自动创建数据库
7. WHEN 执行 executeSQL 操作时，THE MySQL_Connector SHALL 拒绝包含 DROP、TRUNCATE、DELETE、UPDATE 关键字的 SQL 语句
8. WHEN 执行 analyzeNulls 操作时，THE MySQL_Connector SHALL 返回指定表每列的空值数量和空值比例
9. THE MySQL_Connector SHALL 使用参数化的安全标识符（仅允许字母、数字、下划线）防止 SQL 注入
10. IF 数据库操作执行失败，THEN THE MySQL_Connector SHALL 返回包含具体错误信息的失败结果，并确保数据库连接被正确释放

### 需求 4：LangGraph 对话状态图 — ETL 主对话

**用户故事：** 作为数据分析师，我希望通过对话完成 ETL 六步流程，系统能自动管理对话状态和步骤转换。

#### 验收标准

1. THE ETL_Chat_Graph SHALL 包含以下节点：连接测试、意图解析、数据库操作执行、prompt 组装、LLM 调用、响应解析
2. WHEN 用户消息包含 MySQL 连接串时，THE ETL_Chat_Graph SHALL 自动触发连接测试节点
3. WHEN 用户消息不是纯连接串且存在有效连接时，THE ETL_Chat_Graph SHALL 调用 Intent_Parser 节点解析数据库操作意图
4. WHEN Intent_Parser 返回有效意图时，THE ETL_Chat_Graph SHALL 执行对应的数据库操作并将结果注入 system prompt
5. THE ETL_Chat_Graph SHALL 将数据库操作结果格式化为 Markdown 表格（禁止 JSON 格式）注入到 system prompt 中
6. THE ETL_Chat_Graph SHALL 保持与现有 Express.js 实现完全相同的 system prompt 内容（包括六步引导逻辑、硬性约定、各步骤引导说明）
7. THE ETL_Chat_Graph SHALL 在 LLM 调用时使用 temperature=0.3、max_tokens=4096 的参数配置
8. WHEN 用户已选择库表（selectedTables 非空）时，THE ETL_Chat_Graph SHALL 在数据库操作中过滤结果，仅展示已选中的库表
9. THE ETL_Chat_Graph SHALL 返回与现有 API 完全相同的 JSON 响应格式：`{reply, connectionReceived, connectionTestOk, currentStep}`

### 需求 5：LangGraph 意图解析节点

**用户故事：** 作为系统，我需要从对话上下文中准确识别用户的数据库操作意图，以便执行正确的操作。

#### 验收标准

1. THE Intent_Parser SHALL 使用与现有实现完全相同的意图解析 system prompt（包含 8 种意图的判断规则和特殊场景处理）
2. THE Intent_Parser SHALL 仅分析对话的最后 8 条消息作为上下文
3. THE Intent_Parser SHALL 使用 temperature=0.1、max_tokens=1024 的 LLM 参数配置
4. WHEN LLM 返回的意图不在有效意图列表中时，THE Intent_Parser SHALL 返回 null 意图
5. IF LLM 调用失败或返回格式异常，THEN THE Intent_Parser SHALL 返回 null 意图而非抛出异常

### 需求 6：字段映射 API

**用户故事：** 作为数据分析师，我希望通过自然语言描述字段映射关系，系统自动生成 SQL 表达式。

#### 验收标准

1. WHEN 收到 `POST /api/mapping` 请求时，THE FastAPI_Server SHALL 接受包含 message、conversation、targetTableName、targetFields 和 existingMappings 的请求体
2. THE FastAPI_Server SHALL 使用与现有实现完全相同的字段映射 system prompt（包含 JOIN 写法要求、MySQL 语法限制）
3. THE FastAPI_Server SHALL 返回 `{mappings: [{targetField, source, logic, sql}]}` 格式的 JSON 响应
4. THE FastAPI_Server SHALL 使用 temperature=0.2、max_tokens=4096 的 LLM 参数配置
5. IF 请求缺少必要字段（message、targetTableName 或 targetFields），THEN THE FastAPI_Server SHALL 返回 HTTP 400 错误

### 需求 7：DML 生成与优化 API

**用户故事：** 作为数据分析师，我希望系统根据字段映射自动生成 INSERT INTO ... SELECT 的 DML 语句，并支持 SQL 优化。

#### 验收标准

1. WHEN 收到 `POST /api/dml` 请求时，THE FastAPI_Server SHALL 根据目标表名和字段映射列表生成标准 MySQL DML 语句
2. THE FastAPI_Server SHALL 使用与现有实现完全相同的 DML 生成 system prompt（包含 TRUNCATE + INSERT INTO ... SELECT 格式要求）
3. WHEN 收到 `POST /api/dml/optimize` 请求时，THE FastAPI_Server SHALL 将标量子查询优化为 JOIN 写法
4. THE FastAPI_Server SHALL 对 DML 生成和优化均使用 temperature=0.1 的 LLM 参数配置
5. IF 请求缺少必要字段，THEN THE FastAPI_Server SHALL 返回 HTTP 400 错误

### 需求 8：表列表查询 API

**用户故事：** 作为数据分析师，我希望能获取数据库连接中所有库和表的树形结构，以便在界面上选择源表。

#### 验收标准

1. WHEN 收到 `POST /api/tables` 请求时，THE FastAPI_Server SHALL 返回所有非系统库（排除 information_schema、mysql、performance_schema、sys、mo_catalog、system、system_metrics）的库表树形结构
2. THE FastAPI_Server SHALL 返回 `{databases: [{database, tables: []}]}` 格式的 JSON 响应
3. IF 连接串缺失或连接失败，THEN THE FastAPI_Server SHALL 返回 HTTP 400 错误及具体错误信息

### 需求 9：指标匹配与生成 API

**用户故事：** 作为数据分析师，我希望系统能根据我的描述匹配已有指标，并生成带维度的查询 SQL。

#### 验收标准

1. WHEN 收到 `POST /api/metric/match` 请求时，THE FastAPI_Server SHALL 从已有指标定义中找出与用户描述最匹配的指标
2. WHEN 收到 `POST /api/metric/generate` 请求时，THE FastAPI_Server SHALL 生成查询 SQL、推荐可视化类型，并在需要时建议派生指标
3. THE FastAPI_Server SHALL 在 SQL 生成后自动验证执行，失败时最多重试 3 次并让 LLM 自动修正
4. THE FastAPI_Server SHALL 使用与现有实现完全相同的指标生成 system prompt（包含 SQL 兼容性要求、可视化类型选择规则、派生指标建议逻辑）
5. IF LLM 未返回派生指标建议，THEN THE FastAPI_Server SHALL 使用服务端规则自动检测比率/比值类指标并生成派生指标定义

### 需求 10：指标查询执行 API

**用户故事：** 作为数据分析师，我希望能执行指标 SQL 查询并获取数据结果。

#### 验收标准

1. WHEN 收到 `POST /api/metric/query` 请求时，THE FastAPI_Server SHALL 执行指定的 SQL 查询并返回结果行
2. THE FastAPI_Server SHALL 仅允许 SELECT 语句，拒绝包含 DROP、TRUNCATE、DELETE、UPDATE、INSERT、CREATE、ALTER 关键字的 SQL
3. THE FastAPI_Server SHALL 返回 `{rows, rowCount}` 格式的 JSON 响应
4. IF SQL 执行失败，THEN THE FastAPI_Server SHALL 返回 HTTP 400 错误及具体错误信息

### 需求 11：数据血缘分析 API

**用户故事：** 作为数据分析师，我希望系统能分析 SQL 的数据血缘关系，追踪字段从源表到目标表的流转路径。

#### 验收标准

1. WHEN 收到 `POST /api/lineage` 请求时，THE FastAPI_Server SHALL 分析 INSERT INTO ... SELECT SQL 的数据血缘，返回源表、字段映射、JOIN 关系等信息
2. WHEN 收到 `POST /api/metric-lineage` 请求时，THE FastAPI_Server SHALL 分析指标的全链路血缘（基表 → 加工表 → 指标）
3. THE FastAPI_Server SHALL 使用与现有实现完全相同的血缘分析 system prompt
4. THE FastAPI_Server SHALL 在分析前自动获取涉及表的结构信息作为上下文

### 需求 12：LangGraph 对话状态图 — 指标定义对话

**用户故事：** 作为数据分析师，我希望通过对话定义指标，系统能同时支持数据库操作和指标定义两种能力。

#### 验收标准

1. THE Metric_Chat_Graph SHALL 包含与 ETL_Chat_Graph 相同的数据库操作能力（意图解析、操作执行、结果注入）
2. THE Metric_Chat_Graph SHALL 在对话开始时自动获取用户选中表的结构信息作为上下文
3. WHEN 用户未选择特定表时，THE Metric_Chat_Graph SHALL 扫描所有非系统库的表结构（每库最多 20 张表，最多 10 个库）
4. THE Metric_Chat_Graph SHALL 使用与现有实现完全相同的指标对话 system prompt（包含 ETL 能力和指标定义能力的双模式说明）
5. WHEN 用户确认指标定义时，THE Metric_Chat_Graph SHALL 在响应中包含 metricDef 对象（name、definition、tables、aggregation、measureField）
6. THE Metric_Chat_Graph SHALL 返回 `{reply, metricDef?}` 格式的 JSON 响应

### 需求 13：LLM API 健康检查

**用户故事：** 作为开发者，我希望能快速验证 LLM API 的连通性和配置是否正确。

#### 验收标准

1. WHEN 收到 `GET /api/debug-deepseek` 请求时，THE FastAPI_Server SHALL 通过 OpenAI 兼容客户端向配置的 LLM 端点发送一条简单测试消息
2. IF LLM_API_KEY 未配置，THEN THE FastAPI_Server SHALL 返回 `{ok: false, reason: "DEEPSEEK_API_KEY 未设置"}`（保持原有响应文本不变）
3. IF LLM API 调用成功，THEN THE FastAPI_Server SHALL 返回 `{ok: true, reply: "模型回复内容"}`
4. IF LLM API 调用失败，THEN THE FastAPI_Server SHALL 返回包含 HTTP 状态码和错误详情的失败结果

### 需求 14：API 接口契约兼容性

**用户故事：** 作为前端开发者，我希望后端迁移后所有 API 的请求和响应格式保持不变，前端无需任何修改。

#### 验收标准

1. THE FastAPI_Server SHALL 保持与现有 Express.js 服务完全相同的 12 个 API 端点路径和 HTTP 方法
2. THE FastAPI_Server SHALL 保持与现有服务完全相同的请求体 JSON 结构
3. THE FastAPI_Server SHALL 保持与现有服务完全相同的响应体 JSON 结构
4. THE FastAPI_Server SHALL 保持与现有服务完全相同的 HTTP 状态码（200、400、500、503）
5. IF LLM_API_KEY 未配置，THEN THE FastAPI_Server SHALL 对所有需要 LLM 的端点返回 HTTP 503 错误

### 需求 15：LangGraph 状态管理

**用户故事：** 作为系统，我需要在 LangGraph 图中正确管理对话状态，确保各节点间数据流转正确。

#### 验收标准

1. THE LangGraph_Engine SHALL 定义包含以下字段的状态类型：conversation、context（connectionString、currentStep、selectedTables）、connectionTestNote、dbOperationNote、dbIntent、llmResponse
2. THE LangGraph_Engine SHALL 通过条件边（conditional edges）控制节点执行顺序：先判断是否需要连接测试，再判断是否需要意图解析，再判断是否需要数据库操作
3. THE LangGraph_Engine SHALL 确保每次请求创建独立的图执行实例，不在请求间共享状态
4. WHEN 数据库操作结果包含列名错误时，THE LangGraph_Engine SHALL 自动查询涉及表的结构并将真实列名注入 prompt 以支持自我纠正

### 需求 16：项目结构与依赖管理

**用户故事：** 作为开发者，我希望新的 Python 后端有清晰的项目结构和依赖管理。

#### 验收标准

1. THE FastAPI_Server SHALL 使用 `requirements.txt` 或 `pyproject.toml` 管理 Python 依赖
2. THE FastAPI_Server SHALL 至少依赖以下包：fastapi、uvicorn、aiomysql（或等效异步 MySQL 驱动）、langgraph、langchain-core、langchain-openai、python-dotenv、openai（用于 OpenAI 兼容 LLM 调用）、httpx（或等效异步 HTTP 客户端）
3. THE FastAPI_Server SHALL 将代码组织为模块化结构：路由层、LangGraph 图定义层、数据库操作层、工具函数层
4. THE FastAPI_Server SHALL 提供与现有 `.env.example` 兼容的环境变量配置，新增 `LLM_BASE_URL` 和 `LLM_API_KEY` 变量（同时保持 `DEEPSEEK_API_KEY` 向后兼容）
