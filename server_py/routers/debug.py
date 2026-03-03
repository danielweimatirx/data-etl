from fastapi import APIRouter
from config import LLM_API_KEY
from llm.client import call_llm

router = APIRouter()


@router.get("/api/debug-deepseek")
async def debug_deepseek():
    if not LLM_API_KEY:
        return {"ok": False, "reason": "DEEPSEEK_API_KEY 未设置"}

    try:
        result = await call_llm(
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Say hello in one word."},
            ],
            temperature=0.7,
            max_tokens=100,
        )
        if result['ok']:
            return {"ok": True, "reply": result['content']}
        else:
            return {"ok": False, "error": result['error']}
    except Exception as e:
        return {"ok": False, "error": str(e), "code": getattr(e, 'code', None)}
