# 智能数据 ETL 平台 - 功能详细文档

> 版本：v1.0 | 文档生成日期：2026-03-03

---

## 一、产品概述

### 1.1 产品定位

智能数据 ETL 平台是一款基于自然语言对话的数据建模与加工工具，旨在帮助数据分析师和数据工程师通过对话方式完成复杂的 ETL（Extract-Transform-Load）流程，无需编写复杂 SQL。

### 1.2 核心价值

- **对话式交互**：通过自然语言描述需求，系统自动生成 SQL
- **真实执行**：所有 SQL 在用户的 MySQL 数据库上真实执行，结果实时反馈
- **安全确认机制**：所有写操作（DDL/DML）需用户确认后才执行
- **自我纠正能力**：SQL 执行失败时自动分析原因并给出修正建议
- **指标管理**：支持定义业务指标并生成可视化监控数据

### 1.3 技术架构

| 层级 | 技术栈 |
|------|--------|
| 前端 | React 19 + TypeScript + Vite + Tailwind CSS 4 + Zustand |
| 后端 | Node.js + Express + mysql2 |
| AI 模型 | DeepSeek API（意图解析 + 对话回复 + SQL 生成） |
| 数据存储 | 用户自有 MySQL 数据库 + 浏览器 LocalStorage（状态持久化） |

---

## 二、系统架构

### 2.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              前端应用                                    │
├─────────────┬─────────────┬─────────────┬─────────────┬─────────────────┤
│   Sidebar   │ Dashboard   │ AgentPanel  │   Modals    │   状态管理       │
│   侧边栏     │  Content    │  对话面板    │   弹窗组件   │   (Zustand)     │
└─────────────┴─────────────┴─────────────┴─────────────┴─────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           后端 API 服务                                  │
├─────────────┬─────────────┬─────────────┬─────────────┬─────────────────┤
│  /api/chat  │ /api/tables │/api/metric/*│ /api/lineage│ /api/metric-chat│
│  ETL 对话   │  表列表      │  指标相关    │  血缘分析    │   指标对话       │
└─────────────┴─────────────┴─────────────┴─────────────┴─────────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    ▼                               ▼
            ┌─────────────┐                 ┌─────────────┐
            │ DeepSeek AI │                 │ MySQL 数据库 │
            │  意图解析    │                 │  用户数据源   │
            └─────────────┘                 └─────────────┘
```

### 2.2 前端状态管理架构

系统使用 Zustand 进行状态管理，共有 8 个独立的 Store：

| Store 名称 | 文件 | 职责 |
|-----------|------|------|
| useStore | store.ts | ETL 对话主状态（消息、步骤、连接串） |
| useDashboardStore | dashboardStore.ts | Dashboard 管理（创建、删除、切换） |
| useMetricStore | metricStore.ts | 监控数据管理（生成、查询、刷新） |
| useMetricDefStore | metricDefStore.ts | 指标定义管理（添加、删除） |
| useMetricChatStore | metricChatStore.ts | 指标对话状态 |
| useSchemaStore | schemaStore.ts | 库表树状态（加载、选择、展开） |
| useProcessedTableStore | processedTableStore.ts | 已加工表记录 |
| useChatModeStore | chatModeStore.ts | 对话模式切换（ETL/指标） |

---

## 三、核心功能模块

### 3.1 Dashboard 管理

#### 3.1.1 功能描述

Dashboard 是系统的顶层组织单元，每个 Dashboard 代表一个独立的数据分析项目或业务场景。

#### 3.1.2 功能清单

| 功能 | 描述 | 操作入口 |
|------|------|---------|
| 创建 Dashboard | 输入名称创建新的 Dashboard | 侧边栏「新建 Dashboard」按钮 |
| 删除 Dashboard | 删除 Dashboard 及其关联的所有数据 | 侧边栏列表项悬停显示删除按钮 |
| 切换 Dashboard | 点击切换到不同的 Dashboard | 侧边栏列表项点击 |
| 查看指标列表 | 展开 Dashboard 查看其下的指标定义 | 侧边栏列表项展开箭头 |

#### 3.1.3 数据持久化

- Dashboard 列表存储在 `localStorage` 的 `etl-dashboards` 键中
- 删除 Dashboard 时会同步清理：
  - 对应的聊天记录（`etl-chat-{dashboardId}`）
  - 对应的已加工表记录

---

### 3.2 ETL 对话系统（核心功能）

#### 3.2.1 功能概述

ETL 对话系统是产品的核心功能，通过自然语言对话引导用户完成完整的数据加工流程。

#### 3.2.2 六步流程

系统将 ETL 流程分为 6 个步骤，每个步骤有明确的目标和引导：

| 步骤 | 名称 | 描述 | 触发条件 |
|------|------|------|---------|
| 1 | 连接数据库 | 用户提供 MySQL 连接串，系统验证连通性 | 初始状态 |
| 2 | 选择基表 | 浏览库表结构，选定要加工的基表 | 连接成功后 |
| 3 | 定义目标表 | 描述目标表字段，生成 CREATE TABLE SQL | 选定基表后 |
| 4 | 字段映射 | 描述字段来源与加工逻辑，生成 INSERT SQL | 目标表创建后 |
| 5 | 数据验证 | 分析目标表数据质量（空值率、异常值） | 数据写入后 |
| 6 | 异常溯源 | 追溯基表数据，定位问题根源 | 发现异常后 |

#### 3.2.3 支持的数据库操作

| 操作类型 | Intent | 描述 | 是否需确认 |
|---------|--------|------|-----------|
| 列出数据库 | listDatabases | 显示所有数据库及表数量 | 否 |
| 列出表 | listTables | 显示指定库下的所有表 | 否 |
| 查看表结构 | describeTable | 显示表的字段定义 | 否 |
| 预览数据 | previewData | 显示表的前 N 条数据 | 否 |
| 创建数据库 | createDatabase | 创建新数据库 | 是 |
| 创建表 | createTable | 执行 CREATE TABLE DDL | 是 |
| 执行 SQL | executeSQL | 执行 SELECT/INSERT 等 | 写操作需确认 |
| 空值分析 | analyzeNulls | 分析表各列的空值率 | 否 |

#### 3.2.4 连接串格式支持

系统支持两种 MySQL 连接串格式：

**URL 格式**：
```
mysql://username:password@host:3306/database_name
```

**命令行格式**：
```
mysql -h host -P 3306 -u username -p password -D database_name
```

#### 3.2.5 安全确认机制

所有会修改数据库的操作都需要用户明确确认：

1. 系统先展示完整的 SQL 语句
2. 提示用户「请确认后说『确认』或『执行』」
3. 用户回复确认后才真正执行
4. 执行结果（成功/失败）如实反馈

#### 3.2.6 自我纠正能力

当 SQL 执行失败时，系统会：

1. 如实输出失败原因
2. 自动查询涉及表的真实结构
3. 根据真实列名生成修正后的 SQL
4. 提示用户再次执行

#### 3.2.7 操作指南演示

首次使用时，用户可输入「操作指南」触发自问自答演示，展示完整的 ETL 流程。

---

### 3.3 库表树浏览器

#### 3.3.1 功能描述

库表树浏览器提供可视化的数据库结构浏览和表选择功能。

#### 3.3.2 功能清单

| 功能 | 描述 |
|------|------|
| 自动加载 | 连接成功后自动加载库表结构 |
| 展开/收起 | 点击数据库名展开或收起表列表 |
| 单表选择 | 勾选单个表 |
| 批量选择 | 勾选数据库名选中该库下所有表 |
| 刷新 | 手动刷新库表结构 |
| 选择摘要 | 收起状态下显示已选表的摘要 |

#### 3.3.3 选择限制

- 已选中的表会影响对话中的操作范围
- 当用户说「看看有哪些表」时，只展示选中范围内的表
- 选择状态按 Dashboard 独立持久化

---

### 3.4 指标定义系统

#### 3.4.1 功能概述

指标定义系统允许用户通过对话定义业务指标（纯度量，不含维度），为后续的监控数据生成提供基础。

#### 3.4.2 指标定义结构

| 字段 | 类型 | 描述 |
|------|------|------|
| id | string | 唯一标识 |
| dashboardId | string | 所属 Dashboard |
| name | string | 指标名称（如「收入」「订单量」） |
| definition | string | 计算逻辑描述 |
| tables | string[] | 涉及的表 |
| aggregation | string | 聚合方式（SUM/COUNT/AVG/COUNT_DISTINCT/MAX/MIN/自定义） |
| measureField | string | 度量字段 |
| createdAt | number | 创建时间戳 |

#### 3.4.3 指标定义流程

1. 用户在「添加指标」模式下描述想要的指标
2. 系统分析可用表结构，推荐聚合方式和度量字段
3. 用户确认后，指标定义自动保存
4. 指标定义可在侧边栏查看和管理

#### 3.4.4 指标详情弹窗

点击侧边栏的指标可打开详情弹窗，包含三个标签页：

| 标签页 | 内容 |
|--------|------|
| 指标信息 | 基本信息、计算逻辑、涉及表、关联的监控数据 |
| SQL | 指标公式、关联查询 SQL |
| 数据血缘 | 指标的全链路血缘可视化图 |

---

### 3.5 监控数据管理

#### 3.5.1 功能概述

监控数据是基于指标定义生成的带维度的查询结果，以可视化图表形式展示在 Dashboard 中。

#### 3.5.2 监控数据结构

| 字段 | 类型 | 描述 |
|------|------|------|
| id | string | 唯一标识 |
| dashboardId | string | 所属 Dashboard |
| name | string | 监控数据名称 |
| definition | string | 描述 |
| tables | string[] | 涉及的表 |
| sql | string | 查询 SQL |
| chartType | ChartType | 图表类型 |
| data | Record[] | 查询结果数据 |
| createdAt | number | 创建时间戳 |

#### 3.5.3 添加监控数据流程

1. 点击「添加监控数据」按钮
2. 输入数据名称和自然语言描述
3. 系统根据已有指标定义自动生成 SQL
4. 预览 SQL 和图表类型，可手动修改
5. 确认执行，数据写入 Dashboard

#### 3.5.4 支持的图表类型

| 类型 | 标识 | 适用场景 |
|------|------|---------|
| 数值 | number | 单个数值（总数、平均值、求和） |
| 柱状图 | bar | 分类对比（按类目/地区分组） |
| 折线图 | line | 时间序列趋势（按日/月变化） |
| 饼图 | pie | 占比分布（分组数 ≤ 8） |
| 表格 | table | 多列明细或复杂结构 |

#### 3.5.5 图表功能

| 功能 | 描述 |
|------|------|
| 切换类型 | 悬停显示切换按钮，可在 5 种图表类型间切换 |
| 刷新数据 | 重新执行 SQL 获取最新数据 |
| 删除 | 删除该监控数据 |
| 查看详情 | 点击卡片查看详细信息和 SQL |
| 拖拽排序 | 拖拽卡片调整显示顺序 |

#### 3.5.6 数值图表增强

数值类型图表支持显示同比/环比：

- 当指标描述中包含「同比」时，显示同比变化
- 当指标描述中包含「环比」时，显示环比变化
- 自动从查询结果中提取对应字段

#### 3.5.7 派生指标建议

添加监控数据时，系统会智能分析是否需要创建派生指标：

- 涉及比率/比值计算（毛利率、转化率等）
- 涉及增长率/变化率（同比、环比）
- 涉及复合计算（客单价 = 收入/订单数）

如果检测到，会提示用户同时创建派生指标定义。

---

### 3.6 已加工表管理

#### 3.6.1 功能描述

系统自动记录通过 ETL 对话创建的业务表，包括字段映射关系和加工 SQL。

#### 3.6.2 已加工表结构

| 字段 | 类型 | 描述 |
|------|------|------|
| id | string | 唯一标识（database.table） |
| dashboardId | string | 所属 Dashboard |
| database | string | 数据库名 |
| table | string | 表名 |
| sourceTables | string[] | 基表来源 |
| fieldMappings | FieldMapping[] | 字段映射关系 |
| insertSql | string | 加工 SQL |
| processedAt | number | 加工时间戳 |

#### 3.6.3 字段映射结构

| 字段 | 描述 |
|------|------|
| targetField | 目标字段名 |
| sourceTable | 来源表（database.table） |
| sourceExpr | 来源字段或表达式 |
| transform | 加工逻辑（SUM、COUNT、直接映射等） |

#### 3.6.4 已加工表展示

- 在 Dashboard 内容区顶部显示已加工表列表
- 点击可打开血缘分析弹窗
- 显示加工时间

---

### 3.7 数据血缘分析

#### 3.7.1 功能概述

数据血缘分析功能帮助用户理解数据的来源和加工过程，支持两种血缘分析：

1. **表级血缘**：分析已加工表的数据来源
2. **指标血缘**：分析指标的全链路数据流转

#### 3.7.2 表级血缘分析

点击已加工表打开血缘弹窗，展示：

| 内容 | 描述 |
|------|------|
| 血缘图 | SVG 可视化图，展示源表到目标表的数据流 |
| 源表列表 | 基表、维表、关联表及其角色 |
| 字段映射明细 | 每个目标字段的来源和加工逻辑 |
| JOIN 关系 | 表间关联条件 |
| GROUP BY | 分组字段 |
| WHERE 条件 | 过滤条件 |

#### 3.7.3 指标血缘分析

在指标详情弹窗的「数据血缘」标签页，展示：

| 层级 | 描述 |
|------|------|
| source | 基表/维表层（最底层数据源） |
| processed | 加工业务表层（ETL 产出的表） |
| metric | 指标层（最终的度量） |

#### 3.7.4 血缘图可视化

- 使用 SVG 绘制三层架构图
- 不同层级使用不同颜色区分
- 连线显示数据流向和加工逻辑
- 支持横向滚动查看大图

---

### 3.8 对话模式切换

#### 3.8.1 功能描述

系统支持两种对话模式，用户可在输入框上方切换：

| 模式 | 标识 | 功能 |
|------|------|------|
| 业务表加工 | etl | 完整的 ETL 六步流程 |
| 添加指标 | metric | 指标定义 + 数据库操作 |

#### 3.8.2 模式差异

| 特性 | ETL 模式 | 指标模式 |
|------|---------|---------|
| 步骤引导 | 有（1-6 步） | 无 |
| 数据库操作 | 完整支持 | 完整支持 |
| 指标定义 | 不支持 | 支持 |
| 对话历史 | 独立存储 | 独立存储 |

---

## 四、后端 API 接口

### 4.1 接口总览

| 接口 | 方法 | 描述 |
|------|------|------|
| /api/chat | POST | ETL 六步对话主入口 |
| /api/tables | POST | 获取所有库表列表 |
| /api/mapping | POST | 字段映射生成 |
| /api/dml | POST | DML 语句生成 |
| /api/dml/optimize | POST | SQL 优化（子查询转 JOIN） |
| /api/metric/match | POST | 指标匹配 |
| /api/metric/generate | POST | 监控数据 SQL 生成 |
| /api/metric/query | POST | 执行指标查询 |
| /api/lineage | POST | 表级血缘分析 |
| /api/metric-lineage | POST | 指标全链路血缘 |
| /api/metric-chat | POST | 指标定义对话 |
| /api/debug-deepseek | GET | DeepSeek 连通性测试 |

### 4.2 /api/chat - ETL 对话

**请求参数**：

```json
{
  "conversation": [
    {"role": "user", "content": "用户消息"},
    {"role": "assistant", "content": "助手回复"}
  ],
  "context": {
    "connectionString": "mysql://...",
    "currentStep": 1,
    "selectedTables": ["db.table1", "db.table2"]
  }
}
```

**响应格式**：

```json
{
  "reply": "助手回复内容（支持 Markdown）",
  "connectionReceived": true,
  "connectionTestOk": true,
  "currentStep": 2
}
```

### 4.3 /api/tables - 获取库表列表

**请求参数**：

```json
{
  "connectionString": "mysql://..."
}
```

**响应格式**：

```json
{
  "databases": [
    {
      "database": "db_name",
      "tables": ["table1", "table2"]
    }
  ]
}
```

### 4.4 /api/metric/generate - 监控数据 SQL 生成

**请求参数**：

```json
{
  "metricName": "月度收入趋势",
  "description": "按月统计各行业的总收入，最近12个月",
  "metricDefs": [
    {
      "name": "收入",
      "definition": "订单金额汇总",
      "tables": ["db.orders"],
      "aggregation": "SUM",
      "measureField": "amount"
    }
  ],
  "connectionString": "mysql://..."
}
```

**响应格式**：

```json
{
  "sql": "SELECT ... FROM ...",
  "chartType": "line",
  "explanation": "查询逻辑说明",
  "derivedMetricDef": {
    "name": "派生指标名",
    "definition": "计算逻辑",
    "tables": ["db.table"],
    "aggregation": "自定义",
    "measureField": "field"
  }
}
```

**特性**：
- 自动验证生成的 SQL，最多重试 3 次
- 智能检测是否需要建议派生指标
- 兼容 MySQL 5.7+ 语法

### 4.5 /api/lineage - 表级血缘分析

**请求参数**：

```json
{
  "sql": "INSERT INTO target SELECT ... FROM source ...",
  "connectionString": "mysql://...",
  "targetTable": "db.target_table"
}
```

**响应格式**：

```json
{
  "targetTable": "db.target_table",
  "sourceTables": [
    {
      "name": "db.source_table",
      "alias": "s",
      "role": "基表",
      "joinType": "无（主表）",
      "joinCondition": ""
    }
  ],
  "fieldMappings": [
    {
      "targetField": "total_amount",
      "sourceTable": "db.orders",
      "sourceField": "amount",
      "transform": "SUM聚合",
      "expression": "SUM(o.amount)"
    }
  ],
  "joinRelations": [...],
  "groupBy": "user_id",
  "filters": "status = 'completed'"
}
```

### 4.6 /api/metric-lineage - 指标全链路血缘

**请求参数**：

```json
{
  "metricDef": {
    "name": "收入",
    "definition": "订单金额汇总",
    "tables": ["db.order_summary"],
    "aggregation": "SUM",
    "measureField": "total_amount"
  },
  "processedTables": [
    {
      "database": "db",
      "table": "order_summary",
      "sourceTables": ["db.orders", "db.users"],
      "fieldMappings": [...],
      "insertSql": "INSERT INTO ..."
    }
  ],
  "connectionString": "mysql://..."
}
```

**响应格式**：

```json
{
  "layers": [
    {
      "level": "source",
      "label": "基表/维表",
      "tables": [{"name": "db.orders", "role": "基表", "fields": ["amount"]}]
    },
    {
      "level": "processed",
      "label": "加工业务表",
      "tables": [{"name": "db.order_summary", "role": "业务表", "fields": ["total_amount"]}]
    },
    {
      "level": "metric",
      "label": "指标",
      "tables": [{"name": "收入", "role": "指标", "fields": ["SUM(total_amount)"]}]
    }
  ],
  "edges": [
    {
      "from": {"table": "db.orders", "field": "amount"},
      "to": {"table": "db.order_summary", "field": "total_amount"},
      "transform": "SUM聚合"
    }
  ],
  "summary": "收入指标来源于 orders 表的 amount 字段，经过 order_summary 表汇总后计算得出"
}
```

### 4.7 /api/metric-chat - 指标定义对话

**请求参数**：

```json
{
  "conversation": [...],
  "connectionString": "mysql://...",
  "selectedTables": ["db.table1"]
}
```

**响应格式**：

```json
{
  "reply": "助手回复",
  "metricDef": {
    "name": "收入",
    "definition": "订单金额汇总",
    "tables": ["db.orders"],
    "aggregation": "SUM",
    "measureField": "amount"
  }
}
```

**说明**：`metricDef` 仅在用户确认指标定义后返回。

---

## 五、前端组件详解

### 5.1 组件架构

```
App.tsx
├── Sidebar.tsx              # 侧边栏（Dashboard 列表 + 指标列表）
├── DashboardContent.tsx     # Dashboard 主内容区
│   ├── MetricCard.tsx       # 监控数据卡片
│   └── AddMetricModal.tsx   # 添加监控数据弹窗
├── AgentPanel.tsx           # 对话面板
│   ├── SchemaTree.tsx       # 库表树浏览器
│   ├── ChatMessage.tsx      # 消息气泡
│   └── ChatInput.tsx        # 输入框
├── LineageModal.tsx         # 表级血缘弹窗
└── MetricDefDetailModal.tsx # 指标详情弹窗
```

### 5.2 Sidebar 侧边栏

**功能**：
- 显示 Logo 和产品名称
- 新建 Dashboard 按钮和输入框
- Dashboard 列表（支持展开查看指标）
- 指标列表（支持删除和查看详情）

**交互**：
- 悬停显示删除按钮
- 点击 Dashboard 切换
- 点击展开箭头显示指标
- 点击指标打开详情弹窗

### 5.3 DashboardContent 主内容区

**功能**：
- 显示 Dashboard 标题和描述
- 「添加监控数据」按钮
- 已加工表列表（可点击查看血缘）
- 监控数据网格（支持拖拽排序）

**空状态**：
- 未选择 Dashboard 时显示引导
- 无监控数据时显示添加引导

### 5.4 AgentPanel 对话面板

**功能**：
- 收起/展开按钮
- 重置会话按钮
- 库表树浏览器
- 消息列表（支持滚动）
- 输入框和模式切换

**状态同步**：
- 切换 Dashboard 时自动加载对应的对话历史
- ETL 连接串自动同步到指标对话

### 5.5 ChatMessage 消息组件

**功能**：
- 区分用户消息和系统消息
- 支持 Markdown 渲染
- 支持代码块高亮
- 支持表格渲染
- 支持列表渲染

**Markdown 支持**：
- 粗体（`**text**`）
- 行内代码（`` `code` ``）
- 代码块（```sql）
- 表格（| col1 | col2 |）
- 有序/无序列表
- 标题（#、##、###）

### 5.6 MetricCard 监控数据卡片

**功能**：
- 显示指标名称和图表类型图标
- 渲染 5 种图表类型
- 悬停显示操作按钮
- 点击打开详情弹窗
- 支持拖拽排序

**图表组件**：
- NumberChart：数值显示 + 同比/环比
- BarChartViz：柱状图（SVG）
- LineChartViz：折线图（SVG）
- PieChartViz：饼图（SVG）
- TableChartViz：表格

### 5.7 AddMetricModal 添加监控数据弹窗

**三步流程**：
1. **form**：输入名称和描述，显示可用指标
2. **preview**：预览 SQL 和图表类型，可编辑
3. **done**：完成提示

**特性**：
- 显示当前 Dashboard 的指标定义列表
- 派生指标建议（可选择是否同时创建）
- SQL 验证失败时显示错误提示

### 5.8 LineageModal 血缘分析弹窗

**功能**：
- SVG 血缘图可视化
- 源表列表（角色、JOIN 类型）
- 字段映射明细表格
- JOIN 关系和过滤条件
- 加工 SQL 折叠显示
- 「基于此表添加监控」按钮

### 5.9 MetricDefDetailModal 指标详情弹窗

**三个标签页**：
1. **指标信息**：基本信息、计算逻辑、涉及表、关联监控数据
2. **SQL**：指标公式、关联查询 SQL（支持复制）
3. **数据血缘**：全链路血缘 SVG 图

### 5.10 SchemaTree 库表树

**功能**：
- 可折叠的面板
- 数据库列表（显示表数量）
- 表列表（支持勾选）
- 批量选择（勾选数据库名）
- 刷新按钮
- 选择摘要显示

---

## 六、数据持久化

### 6.1 LocalStorage 键值说明

| 键名 | 内容 |
|------|------|
| etl-dashboards | Dashboard 列表 |
| etl-chat-{dashboardId} | ETL 对话历史 |
| etl-metric-chat-{dashboardId} | 指标对话历史 |
| etl-metrics | 监控数据列表 |
| etl-metric-defs | 指标定义列表 |
| etl-processed-tables | 已加工表列表 |
| etl-schema-sel-{dashboardId} | 库表选择状态 |

### 6.2 数据清理策略

- 删除 Dashboard 时清理关联的对话历史和已加工表
- 监控数据和指标定义按 dashboardId 过滤
- 库表选择状态按 Dashboard 独立存储

---

## 七、环境配置

### 7.1 环境变量

| 变量名 | 必填 | 描述 | 默认值 |
|--------|------|------|--------|
| DEEPSEEK_API_KEY | 是 | DeepSeek API 密钥 | - |
| PORT | 否 | 后端服务端口 | 3001 |

### 7.2 配置文件

**.env.example**：
```
DEEPSEEK_API_KEY=sk-your-key-here
PORT=3001
```

### 7.3 启动命令

```bash
# 安装依赖
npm install

# 同时启动前后端（推荐）
npm run dev:all

# 或分别启动
npm run dev:server   # 后端 http://localhost:3001
npm run dev          # 前端 Vite 开发服务器
```

---

## 八、AI 能力详解

### 8.1 意图解析

系统使用 DeepSeek 模型进行意图解析，支持以下意图：

| 意图 | 触发示例 |
|------|---------|
| listDatabases | 「有哪些库」「看看数据库」 |
| listTables | 「有哪些表」「show tables」 |
| describeTable | 「看看这张表」「表结构」「用 xxx 表」 |
| previewData | 「看前几条数据」「preview」 |
| createDatabase | 「建库 xxx」（需确认） |
| createTable | 「确认建表」（需先展示 DDL） |
| executeSQL | 「执行」「确认」（需先展示 SQL） |
| analyzeNulls | 「分析空值」「数据质量」 |

### 8.2 SQL 生成规则

- 所有 SQL 必须符合 MySQL 5.7+ 语法
- 使用 JOIN 写法，禁止标量子查询
- 表名/列名使用反引号包裹
- 中文别名必须使用反引号
- 避免使用高级函数（QUARTER、STR_TO_DATE 等）

### 8.3 自动验证与修正

监控数据 SQL 生成时：
1. 生成 SQL 后自动执行验证
2. 如果执行失败，分析错误原因
3. 让模型根据错误修正 SQL
4. 最多重试 3 次
5. 仍失败则返回 SQL 和错误信息供用户手动修改

### 8.4 上下文管理

- 对话历史最多保留最近 8 轮
- 系统提示词包含当前步骤、已选表、操作结果
- 数据库操作结果注入到系统提示词中

---

## 九、安全机制

### 9.1 SQL 执行限制

| 操作类型 | 限制 |
|---------|------|
| SELECT | 允许直接执行 |
| SHOW/DESCRIBE | 允许直接执行 |
| CREATE TABLE | 需用户确认 |
| CREATE DATABASE | 需用户确认 |
| INSERT INTO | 需用户确认 |
| DROP/TRUNCATE/DELETE/UPDATE | 禁止执行 |

### 9.2 指标查询限制

`/api/metric/query` 接口仅允许 SELECT 语句，禁止：
- DROP
- TRUNCATE
- DELETE
- UPDATE
- INSERT
- CREATE
- ALTER

### 9.3 连接安全

- 连接串仅在内存中使用，不持久化到服务端
- 连接超时设置为 8-10 秒
- 每次操作后立即销毁连接

---

## 十、错误处理

### 10.1 前端错误处理

- API 请求失败时显示错误消息
- 对话中显示错误提示
- 生成失败时允许重试

### 10.2 后端错误处理

- DeepSeek API 异常时返回具体错误信息
- 数据库连接失败时返回连接错误
- SQL 执行失败时返回错误原因并尝试自动纠正

### 10.3 自我纠正机制

当 SQL 执行失败且错误为「列不存在」时：
1. 自动查询涉及表的真实结构
2. 将真实列名注入到系统提示词
3. 让模型根据真实列名生成修正 SQL
4. 提示用户再次执行

---

## 十一、扩展性设计

### 11.1 图表类型扩展

在 `MetricCard.tsx` 中添加新的图表组件：
1. 创建新的图表渲染函数
2. 在 `CHART_ICONS` 中添加图标
3. 在 `SWITCHABLE_TYPES` 中添加选项
4. 在 `renderChart` 中添加 case

### 11.2 数据库类型扩展

当前仅支持 MySQL，扩展其他数据库需要：
1. 修改 `getConnectionConfig` 支持新的连接串格式
2. 修改 `getMysqlConnection` 支持新的驱动
3. 调整 SQL 语法生成规则

### 11.3 指标聚合方式扩展

在 `metricDefStore.ts` 和后端 API 中添加新的聚合方式：
- 前端：更新 `AGG_LABELS` 映射
- 后端：更新系统提示词中的聚合方式说明

---

## 十二、已知限制

1. **数据库支持**：当前仅支持 MySQL
2. **连接串存储**：连接串不持久化，刷新页面需重新输入
3. **并发限制**：单用户单会话，不支持多用户协作
4. **数据量限制**：预览数据最多 50 条，查询结果最多 100 条
5. **图表数据量**：大数据量时图表渲染可能较慢

---

## 十三、版本历史

| 版本 | 日期 | 主要更新 |
|------|------|---------|
| v1.0 | 2026-03 | 初始版本，完整 ETL 流程 + 指标管理 + 血缘分析 |

---

*文档结束*
