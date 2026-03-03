from fastapi import APIRouter
from fastapi.responses import JSONResponse
from db.operations import run_database_operation

router = APIRouter()


@router.post("/api/tables")
async def list_tables(request_body: dict):
    connection_string = request_body.get('connectionString')
    if not connection_string:
        return JSONResponse(status_code=400, content={"error": "Missing connectionString"})

    try:
        db_result = await run_database_operation(connection_string, 'listDatabases', {})
        if not db_result['ok']:
            return JSONResponse(status_code=400, content={"error": db_result['error']})

        system_dbs = {'information_schema', 'mysql', 'performance_schema', 'sys', 'mo_catalog', 'system', 'system_metrics'}
        databases = [
            d['database']
            for d in (db_result['data']['databases'] or [])
            if d['database'].lower() not in system_dbs
        ]

        tree = []
        for db in databases:
            tbl_result = await run_database_operation(connection_string, 'listTables', {'database': db})
            if tbl_result['ok']:
                tree.append({'database': db, 'tables': tbl_result['data']['tables'] or []})

        return {"databases": tree}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e) or "获取表列表失败"})
