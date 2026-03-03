from fastapi import APIRouter
from fastapi.responses import JSONResponse
from config import LLM_API_KEY
from graphs.etl_chat_graph import build_etl_chat_graph

router = APIRouter()


@router.post("/api/chat")
async def chat(request_body: dict):
    if not LLM_API_KEY:
        return JSONResponse(status_code=503, content={"error": "DEEPSEEK_API_KEY not configured"})

    conversation = request_body.get('conversation')
    context = request_body.get('context') or {}

    if not isinstance(conversation, list) or len(conversation) == 0:
        return JSONResponse(status_code=400, content={"error": "Missing or invalid conversation"})

    try:
        graph = build_etl_chat_graph()
        initial_state = {
            'conversation': conversation,
            'context': context,
            'connection_string': None,
            'should_test_connection': False,
            'connection_test_note': '',
            'connection_test_ok': False,
            'db_intent': {'intent': None, 'params': {}},
            'db_operation_note': '',
            'selected_tables_note': '',
            'llm_response': {},
        }
        result = await graph.ainvoke(initial_state)
        response = result.get('llm_response', {})
        return response
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e) or "DeepSeek 请求失败"})
