from fastapi import APIRouter
from fastapi.responses import JSONResponse
from config import LLM_API_KEY
from llm.client import call_llm
import re

router = APIRouter()


@router.post("/api/dml")
async def generate_dml(request_body: dict):
    if not LLM_API_KEY:
        return JSONResponse(status_code=503, content={"error": "DEEPSEEK_API_KEY not configured"})

    target_table_full_name = request_body.get('targetTableFullName')
    mappings = request_body.get('mappings')

    if not target_table_full_name or not isinstance(mappings, list) or len(mappings) == 0:
        return JSONResponse(status_code=400, content={"error": "Missing targetTableFullName or mappings"})

    mapping_list = [
        {
            'targetField': m['targetField'],
            'source': m.get('source', ''),
            'logic': m.get('logic', ''),
            'sql': m.get('sql') or m.get('targetField'),
        }
        for m in mappings
    ]
    mapping_list = [m for m in mapping_list if m['targetField'] and m['sql']]

    mapping_text = '\n'.join(
        f'- {m["targetField"]}: source={m["source"]}, logic={m["logic"]}, sql={m["sql"]}'
        for m in mapping_list
    )

    system_prompt = f'''你是 MySQL SQL 专家。生成**标准 MySQL 语法**的 DML：仅使用 MySQL 支持的类型与函数，表名/列名可用反引号。用多表 JOIN，禁止标量子查询。

**目标表**：{target_table_full_name}

**字段映射**：
{mapping_text}

**输出**：1) TRUNCATE TABLE 目标表; 2) INSERT INTO 目标表 (列...) SELECT ... FROM 主表 LEFT JOIN ... 只输出 MySQL 可执行的 SQL，无 markdown。'''

    try:
        result = await call_llm(
            [
                {'role': 'system', 'content': system_prompt},
                {'role': 'user', 'content': '请用 JOIN 写法生成 DML。'},
            ],
            temperature=0.1,
            max_tokens=4096,
        )
        if not result['ok']:
            return JSONResponse(status_code=500, content={"error": result['error'] or "DeepSeek 请求失败"})

        content = (result.get('content') or '').strip()
        dml = re.sub(r'^```\w*\n?|```\s*$', '', content).strip()
        return {"dml": dml or content}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e) or "DeepSeek 请求失败"})


@router.post("/api/dml/optimize")
async def optimize_dml(request_body: dict):
    if not LLM_API_KEY:
        return JSONResponse(status_code=503, content={"error": "DEEPSEEK_API_KEY not configured"})

    dml = request_body.get('dml')
    if not dml or not isinstance(dml, str):
        return JSONResponse(status_code=400, content={"error": "Missing dml"})

    system_prompt = '你是 MySQL SQL 优化专家。将标量子查询改为 JOIN，保持语义不变。输出必须为**标准 MySQL 语法**、可直接在 MySQL 中执行。只输出优化后的完整 SQL，不要 markdown 或解释。'

    try:
        result = await call_llm(
            [
                {'role': 'system', 'content': system_prompt},
                {'role': 'user', 'content': f'请优化：\n\n{dml}'},
            ],
            temperature=0.1,
            max_tokens=4096,
        )
        if not result['ok']:
            return JSONResponse(status_code=500, content={"error": result['error'] or "DeepSeek 请求失败"})

        content = (result.get('content') or '').strip()
        optimized = re.sub(r'^```\w*\n?|```\s*$', '', content).strip()
        return {"dml": optimized or content}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e) or "DeepSeek 请求失败"})
