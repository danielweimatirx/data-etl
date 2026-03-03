from fastapi import APIRouter
from fastapi.responses import JSONResponse
from config import LLM_API_KEY
from llm.client import call_llm
from db.operations import run_database_operation
from utils.sql_parser import extract_table_refs_from_sql
import re
import json

router = APIRouter()


# ────────── POST /api/lineage — 解析 SQL 返回数据血缘 ──────────

@router.post("/api/lineage")
async def lineage(request_body: dict):
    if not LLM_API_KEY:
        return JSONResponse(status_code=503, content={"error": "DEEPSEEK_API_KEY not configured"})

    sql = request_body.get('sql')
    connection_string = request_body.get('connectionString')
    target_table = request_body.get('targetTable')

    if not sql:
        return JSONResponse(status_code=400, content={"error": "Missing sql"})

    # 获取涉及表的结构
    schema_info = ''
    if connection_string:
        table_refs = extract_table_refs_from_sql(sql)
        for ref in table_refs:
            result = await run_database_operation(
                connection_string, 'describeTable',
                {'database': ref['database'], 'table': ref['table']},
            )
            if result['ok']:
                cols = result['data'].get('columns') or []
                full_name = f'{ref["database"]}.{ref["table"]}' if ref['database'] else ref['table']
                col_list = '\n'.join(
                    f'  {c["Field"]} {c["Type"]}{" -- " + c["Comment"] if c.get("Comment") else ""}'
                    for c in cols
                )
                schema_info += f'\n\u8868 {full_name}:\n{col_list}\n'

    system_prompt = f'''\u4f60\u662f\u4e00\u4e2a SQL \u8840\u7f18\u5206\u6790\u4e13\u5bb6\u3002\u5206\u6790\u4ee5\u4e0b INSERT INTO ... SELECT SQL\uff0c\u63d0\u53d6\u5b8c\u6574\u7684\u6570\u636e\u8840\u7f18\u5173\u7cfb\u3002

**SQL**:
```sql
{sql}
```

**\u6d89\u53ca\u8868\u7684\u7ed3\u6784**:
{schema_info or '\uff08\u672a\u63d0\u4f9b\uff09'}

**\u76ee\u6807\u8868**: {target_table or '\u4ece SQL \u4e2d\u63d0\u53d6'}

\u8bf7\u5206\u6790\u5e76\u8fd4\u56de JSON\uff08\u4e0d\u8981 markdown \u4ee3\u7801\u5757\uff09\uff0c\u683c\u5f0f\u5982\u4e0b\uff1a
{{
  "targetTable": "\u5e93\u540d.\u8868\u540d",
  "sourceTables": [
    {{
      "name": "\u5e93\u540d.\u8868\u540d",
      "alias": "SQL\u4e2d\u7684\u522b\u540d\uff08\u5982\u6709\uff09",
      "role": "\u57fa\u8868|\u7ef4\u8868|\u5173\u8054\u8868",
      "joinType": "LEFT JOIN|INNER JOIN|\u65e0\uff08\u4e3b\u8868\uff09",
      "joinCondition": "ON \u6761\u4ef6\uff08\u5982\u6709\uff09"
    }}
  ],
  "fieldMappings": [
    {{
      "targetField": "\u76ee\u6807\u5b57\u6bb5\u540d",
      "sourceTable": "\u6765\u6e90\u8868\u5168\u540d\uff08\u5e93\u540d.\u8868\u540d\uff09",
      "sourceField": "\u6765\u6e90\u5b57\u6bb5\u540d",
      "transform": "\u52a0\u5de5\u903b\u8f91\u63cf\u8ff0\uff0c\u5982\uff1a\u76f4\u63a5\u6620\u5c04\u3001SUM\u805a\u5408\u3001COUNT\u8ba1\u6570\u3001CASE WHEN\u6761\u4ef6\u8f6c\u6362\u3001LEFT JOIN\u5173\u8054\u53d6\u503c\u3001COALESCE\u7a7a\u503c\u5904\u7406 \u7b49",
      "expression": "\u539f\u59cbSQL\u8868\u8fbe\u5f0f\u7247\u6bb5"
    }}
  ],
  "joinRelations": [
    {{
      "leftTable": "\u5e93\u540d.\u8868\u540d",
      "rightTable": "\u5e93\u540d.\u8868\u540d",
      "joinType": "LEFT JOIN|INNER JOIN",
      "condition": "ON \u6761\u4ef6"
    }}
  ],
  "groupBy": "GROUP BY \u5b57\u6bb5\u5217\u8868\uff08\u5982\u6709\uff09",
  "filters": "WHERE \u6761\u4ef6\uff08\u5982\u6709\uff09"
}}

**\u8981\u6c42**\uff1a
- \u6bcf\u4e2a\u76ee\u6807\u5b57\u6bb5\u90fd\u5fc5\u987b\u8ffd\u6eaf\u5230\u5177\u4f53\u7684\u6765\u6e90\u8868\u548c\u6765\u6e90\u5b57\u6bb5
- \u5982\u679c\u4e00\u4e2a\u76ee\u6807\u5b57\u6bb5\u6d89\u53ca\u591a\u4e2a\u6765\u6e90\u8868/\u5b57\u6bb5\uff08\u5982 JOIN \u540e\u53d6\u503c\uff09\uff0c\u5217\u51fa\u4e3b\u8981\u6765\u6e90
- transform \u8981\u7528\u4e2d\u6587\u63cf\u8ff0\u6e05\u695a\u52a0\u5de5\u903b\u8f91
- \u7ef4\u8868\uff08\u901a\u8fc7 JOIN \u5173\u8054\u7684\u67e5\u627e\u8868\uff09\u7684 role \u6807\u8bb0\u4e3a\u201c\u7ef4\u8868\u201d
- \u4e3b\u8981\u6570\u636e\u6765\u6e90\u8868\u7684 role \u6807\u8bb0\u4e3a\u201c\u57fa\u8868\u201d
- sourceTables \u5fc5\u987b\u5305\u542b SQL \u4e2d FROM \u548c\u6240\u6709 JOIN \u6d89\u53ca\u7684\u8868'''

    try:
        llm_result = await call_llm(
            messages=[
                {'role': 'system', 'content': system_prompt},
                {'role': 'user', 'content': '\u8bf7\u5206\u6790\u8fd9\u6761 SQL \u7684\u6570\u636e\u8840\u7f18\u5173\u7cfb\u3002'},
            ],
            temperature=0.1,
            max_tokens=4096,
        )
        if not llm_result['ok']:
            return JSONResponse(status_code=500, content={"error": llm_result['error'] or "\u8840\u7f18\u5206\u6790\u5931\u8d25"})

        content = (llm_result.get('content') or '').strip()
        json_match = re.search(r'\{[\s\S]*\}', content)
        if not json_match:
            return JSONResponse(status_code=500, content={"error": "\u6a21\u578b\u672a\u8fd4\u56de\u6709\u6548 JSON"})
        parsed = json.loads(json_match.group(0))
        return parsed
    except Exception as e:
        print(f'[/api/lineage] {e}')
        return JSONResponse(status_code=500, content={"error": str(e) or "\u8840\u7f18\u5206\u6790\u5931\u8d25"})


# ────────── POST /api/metric-lineage — 指标全链路血缘 ──────────

@router.post("/api/metric-lineage")
async def metric_lineage(request_body: dict):
    if not LLM_API_KEY:
        return JSONResponse(status_code=503, content={"error": "DEEPSEEK_API_KEY not configured"})

    metric_def = request_body.get('metricDef')
    processed_tables = request_body.get('processedTables', [])
    connection_string = request_body.get('connectionString')

    if not metric_def:
        return JSONResponse(status_code=400, content={"error": "Missing metricDef"})

    # 收集所有相关的加工 SQL
    relevant_tables = metric_def.get('tables') or []
    all_processed = processed_tables or []

    # 直接匹配指标涉及的表
    direct_match = [
        pt for pt in all_processed
        if any(t == f'{pt["database"]}.{pt["table"]}' for t in relevant_tables)
    ]
    # 如果直接匹配为空，使用所有传入的加工表（前端已按 dashboard 过滤）
    relevant_processed = direct_match if len(direct_match) > 0 else all_processed

    # 获取表结构
    schema_info = ''
    if connection_string:
        all_tbls = set(relevant_tables)
        for pt in relevant_processed:
            for s in (pt.get('sourceTables') or []):
                all_tbls.add(s)
        for tbl in all_tbls:
            parts = tbl.split('.')
            if len(parts) == 2:
                result = await run_database_operation(
                    connection_string, 'describeTable',
                    {'database': parts[0], 'table': parts[1]},
                )
                if result['ok']:
                    cols_list = result['data'].get('columns') or []
                    col_str = '\n'.join(
                        f'  {c["Field"]} {c["Type"]}{" -- " + c["Comment"] if c.get("Comment") else ""}'
                        for c in cols_list
                    )
                    schema_info += f'\n\u8868 {tbl}:\n{col_str}\n'

    # 收集加工 SQL 和字段映射信息
    etl_info_parts = []
    for pt in relevant_processed:
        mapping_info = ''
        if isinstance(pt.get('fieldMappings'), list) and len(pt['fieldMappings']) > 0:
            mapping_lines = '\n'.join(
                f'    {fm["targetField"]} \u2190 {fm["sourceTable"]}.{fm["sourceExpr"]} ({fm["transform"]})'
                for fm in pt['fieldMappings']
            )
            mapping_info = f'  \u5b57\u6bb5\u6620\u5c04:\n{mapping_lines}'
        source_tables_str = ', '.join(pt.get('sourceTables') or [])
        entry = f'\u52a0\u5de5\u8868 {pt["database"]}.{pt["table"]}:\n  \u6765\u6e90\u8868: {source_tables_str}'
        if mapping_info:
            entry += '\n' + mapping_info
        entry += f'\n  \u52a0\u5de5SQL: {pt.get("insertSql") or "\u65e0"}'
        etl_info_parts.append(entry)
    etl_info = '\n\n'.join(etl_info_parts)

    metric_name = metric_def.get('name', '')
    metric_definition = metric_def.get('definition', '')
    metric_aggregation = metric_def.get('aggregation', '')
    metric_measure_field = metric_def.get('measureField', '')

    system_prompt = f'''\u4f60\u662f\u4e00\u4e2a\u6570\u636e\u8840\u7f18\u5206\u6790\u4e13\u5bb6\u3002\u8bf7\u5206\u6790\u6307\u6807\u7684\u5168\u94fe\u8def\u8840\u7f18\uff0c\u4ece\u6700\u5e95\u5c42\u7684\u57fa\u8868/\u7ef4\u8868\u5230\u52a0\u5de5\u540e\u7684\u4e1a\u52a1\u8868\uff0c\u518d\u5230\u6307\u6807\u672c\u8eab\u3002

**\u6307\u6807\u4fe1\u606f**\uff1a
- \u540d\u79f0\uff1a{metric_name}
- \u5b9a\u4e49\uff1a{metric_definition}
- \u805a\u5408\u65b9\u5f0f\uff1a{metric_aggregation}
- \u5ea6\u91cf\u5b57\u6bb5\uff1a{metric_measure_field}
- \u6d89\u53ca\u8868\uff1a{', '.join(relevant_tables)}

**ETL \u52a0\u5de5\u4fe1\u606f\uff08\u8fd9\u662f\u771f\u5b9e\u7684\u52a0\u5de5\u8bb0\u5f55\uff0c\u5fc5\u987b\u4e25\u683c\u4f9d\u636e\u6b64\u4fe1\u606f\u786e\u5b9a\u57fa\u8868\u548c\u7ef4\u8868\uff09**\uff1a
{etl_info or '\uff08\u65e0\u52a0\u5de5\u4fe1\u606f\uff09'}

**\u91cd\u8981\u89c4\u5219**\uff1a
- source \u5c42\u7684\u57fa\u8868/\u7ef4\u8868**\u5fc5\u987b**\u6765\u81ea\u4e0a\u65b9 ETL \u52a0\u5de5\u4fe1\u606f\u4e2d\u7684\u300c\u6765\u6e90\u8868\u300d\u548c\u300c\u5b57\u6bb5\u6620\u5c04\u300d\uff0c\u4e0d\u5f97\u81ea\u884c\u7f16\u9020\u3002
- \u5982\u679c ETL \u52a0\u5de5\u4fe1\u606f\u4e2d\u6709\u5b57\u6bb5\u6620\u5c04\uff0c\u57fa\u8868\u662f\u63d0\u4f9b\u4e3b\u8981\u6570\u636e\u7684\u6765\u6e90\u8868\uff0c\u7ef4\u8868\u662f\u901a\u8fc7 JOIN \u5173\u8054\u7684\u67e5\u627e\u8868\u3002
- \u5982\u679c ETL \u52a0\u5de5\u4fe1\u606f\u4e2d\u6709\u52a0\u5de5 SQL\uff08INSERT INTO ... SELECT ... FROM ... JOIN ...\uff09\uff0c\u4ece SQL \u4e2d\u7684 FROM \u786e\u5b9a\u57fa\u8868\uff0c\u4ece JOIN \u786e\u5b9a\u7ef4\u8868\u3002
- \u5982\u679c\u6ca1\u6709 ETL \u52a0\u5de5\u4fe1\u606f\uff0c\u5219\u6307\u6807\u6d89\u53ca\u7684\u8868\u672c\u8eab\u5c31\u662f\u57fa\u8868\uff0c\u4e0d\u8981\u7f16\u9020\u4e0d\u5b58\u5728\u7684\u8868\u3002

**\u8868\u7ed3\u6784**\uff1a
{schema_info or '\uff08\u672a\u63d0\u4f9b\uff09'}

\u8bf7\u5206\u6790\u5e76\u8fd4\u56de\u8be5\u6307\u6807\u7684\u5168\u94fe\u8def\u8840\u7f18\uff0c**\u53ea\u5305\u542b\u4e0e\u8be5\u6307\u6807\u76f8\u5173\u7684\u5b57\u6bb5**\u3002

\u8fd4\u56de JSON\uff08\u4e0d\u8981 markdown \u4ee3\u7801\u5757\uff09\uff1a
{{
  "layers": [
    {{
      "level": "source",
      "label": "\u57fa\u8868/\u7ef4\u8868",
      "tables": [
        {{
          "name": "db.table",
          "role": "\u57fa\u8868|\u7ef4\u8868",
          "fields": ["\u53ea\u5217\u51fa\u4e0e\u6307\u6807\u76f8\u5173\u7684\u5b57\u6bb5"]
        }}
      ]
    }},
    {{
      "level": "processed",
      "label": "\u52a0\u5de5\u4e1a\u52a1\u8868",
      "tables": [
        {{
          "name": "db.table",
          "role": "\u4e1a\u52a1\u8868",
          "fields": ["\u53ea\u5217\u51fa\u4e0e\u6307\u6807\u76f8\u5173\u7684\u5b57\u6bb5"]
        }}
      ]
    }},
    {{
      "level": "metric",
      "label": "\u6307\u6807",
      "tables": [
        {{
          "name": "{metric_name}",
          "role": "\u6307\u6807",
          "fields": ["{metric_aggregation}({metric_measure_field})"]
        }}
      ]
    }}
  ],
  "edges": [
    {{
      "from": {{"table": "\u6e90\u8868\u540d", "field": "\u6e90\u5b57\u6bb5"}},
      "to": {{"table": "\u76ee\u6807\u8868\u540d", "field": "\u76ee\u6807\u5b57\u6bb5"}},
      "transform": "\u52a0\u5de5\u903b\u8f91\uff08\u5982\u76f4\u63a5\u6620\u5c04\u3001SUM\u3001JOIN\u7b49\uff09"
    }}
  ],
  "summary": "\u4e00\u53e5\u8bdd\u603b\u7ed3\u8be5\u6307\u6807\u7684\u6570\u636e\u6d41\u8f6c\u8def\u5f84"
}}'''

    try:
        llm_result = await call_llm(
            messages=[
                {'role': 'system', 'content': system_prompt},
                {'role': 'user', 'content': f'\u8bf7\u5206\u6790\u6307\u6807\u300c{metric_name}\u300d\u7684\u5168\u94fe\u8def\u8840\u7f18'},
            ],
            temperature=0.1,
            max_tokens=4096,
        )
        if not llm_result['ok']:
            return JSONResponse(status_code=500, content={"error": llm_result['error'] or "\u6307\u6807\u8840\u7f18\u5206\u6790\u5931\u8d25"})

        content = (llm_result.get('content') or '').strip()
        json_match = re.search(r'\{[\s\S]*\}', content)
        if not json_match:
            return JSONResponse(status_code=500, content={"error": "\u6a21\u578b\u672a\u8fd4\u56de\u6709\u6548 JSON"})
        parsed = json.loads(json_match.group(0))
        return parsed
    except Exception as e:
        print(f'[/api/metric-lineage] {e}')
        return JSONResponse(status_code=500, content={"error": str(e) or "\u6307\u6807\u8840\u7f18\u5206\u6790\u5931\u8d25"})
