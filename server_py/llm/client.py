from openai import AsyncOpenAI, APIStatusError
from config import LLM_API_KEY, LLM_BASE_URL, LLM_MODEL

_client = AsyncOpenAI(base_url=LLM_BASE_URL, api_key=LLM_API_KEY)


async def call_llm(messages: list, temperature: float = None, max_tokens: int = None) -> dict:
    """
    统一的 LLM API 调用（OpenAI 兼容接口）。
    返回 {"ok": bool, "content": str, "error": str, "status": int|None}
    """
    kwargs = {"model": LLM_MODEL, "messages": messages, "stream": False}
    if temperature is not None:
        kwargs["temperature"] = temperature
    if max_tokens is not None:
        kwargs["max_tokens"] = max_tokens
    try:
        response = await _client.chat.completions.create(**kwargs)
        content = response.choices[0].message.content
        return {"ok": True, "content": content, "error": ""}
    except APIStatusError as e:
        return {"ok": False, "content": "", "error": e.message, "status": e.status_code}
    except Exception as e:
        return {"ok": False, "content": "", "error": str(e)}
