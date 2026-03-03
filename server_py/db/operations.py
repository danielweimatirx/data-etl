import re
import asyncio
import aiomysql
from db.connection import get_connection_config
from utils.formatters import rows_to_markdown_table


VALID_INTENTS = [
    'createDatabase', 'listDatabases', 'listTables', 'describeTable',
    'previewData', 'createTable', 'executeSQL', 'analyzeNulls',
]


def safe_identifier(name):
    if not name or not isinstance(name, str):
        return None
    s = name.strip()
    if re.match(r'^[a-zA-Z0-9_]+$', s):
        return s
    return None


def extract_database_from_create_table(ddl):
    s = str(ddl).strip()
    after_create = re.sub(r'^\s*CREATE\s+TABLE\s+', '', s, flags=re.IGNORECASE).strip()
    m = re.match(r'^(`[^`]+`|\w+)\s*\.\s*(`[^`]+`|\w+)', after_create)
    if not m:
        return None
    db = m.group(1)
    if db.startswith('`'):
        db = db[1:-1]
    db = db.strip()
    return db if safe_identifier(db) else None


def _esc(name):
    return '`' + str(name).replace('`', '``') + '`'


async def get_mysql_connection(connection_string):
    parsed = get_connection_config(connection_string)
    if not parsed:
        raise Exception('连接串格式无法解析')
    try:
        conn = await asyncio.wait_for(
            aiomysql.connect(
                host=parsed['host'],
                port=parsed['port'],
                user=parsed['user'],
                password=parsed['password'],
                db=parsed.get('database') or '',
                connect_timeout=10,
            ),
            timeout=10,
        )
        return conn
    except asyncio.TimeoutError:
        raise Exception('连接超时')


async def run_database_operation(connection_string, intent, params=None):
    if params is None:
        params = {}
    conn = None
    try:
        conn = await get_mysql_connection(connection_string)
    except Exception as e:
        return {'ok': False, 'error': str(e)}

    parsed = get_connection_config(connection_string)
    database = parsed.get('database') if parsed else None

    try:
        cur = await conn.cursor(aiomysql.DictCursor)

        if intent == 'listDatabases':
            await cur.execute('SHOW DATABASES')
            rows = await cur.fetchall()
            db_list = [r.get('Database') or list(r.values())[0] for r in rows if r]
            with_counts = []
            for db in db_list:
                if not safe_identifier(db):
                    continue
                try:
                    await cur.execute(f'SHOW TABLES FROM {_esc(db)}')
                    tbl_rows = await cur.fetchall()
                    with_counts.append({'database': db, 'tableCount': len(tbl_rows) if tbl_rows else 0})
                except Exception:
                    with_counts.append({'database': db, 'tableCount': 0})
            return {'ok': True, 'data': {'databases': with_counts, 'totalDatabases': len(with_counts)}}

        if intent == 'listTables':
            db = (safe_identifier(params.get('database')) if params.get('database') else None) or (safe_identifier(database) if database else None)
            sql = f'SHOW TABLES FROM {_esc(db)}' if db else 'SHOW TABLES'
            await cur.execute(sql)
            rows = await cur.fetchall()
            tables = [list(r.values())[0] for r in rows if r]
            return {'ok': True, 'data': {'database': db or database or '(当前库)', 'tables': tables}}

        if intent == 'createDatabase' and params.get('name'):
            name = safe_identifier(params['name'])
            if not name:
                return {'ok': False, 'error': '无效的数据库名（仅允许字母、数字、下划线）'}
            await cur.execute(f'CREATE DATABASE IF NOT EXISTS {_esc(name)}')
            return {'ok': True, 'data': {'message': f'数据库 {name} 已创建'}}

        if intent == 'describeTable' and params.get('table'):
            db = (safe_identifier(params.get('database')) if params.get('database') else None) or (safe_identifier(database) if database else None)
            tbl = safe_identifier(params['table'])
            if not tbl:
                return {'ok': False, 'error': '无效的表名'}
            full_name = f'{_esc(db)}.{_esc(tbl)}' if db else _esc(tbl)
            await cur.execute(f'DESCRIBE {full_name}')
            cols = await cur.fetchall()
            return {'ok': True, 'data': {'database': db or database, 'table': tbl, 'columns': cols}}

        if intent == 'previewData' and params.get('table'):
            db = (safe_identifier(params.get('database')) if params.get('database') else None) or (safe_identifier(database) if database else None)
            tbl = safe_identifier(params['table'])
            if not tbl:
                return {'ok': False, 'error': '无效的表名'}
            full_name = f'{_esc(db)}.{_esc(tbl)}' if db else _esc(tbl)
            limit = min(int(params.get('limit', 10) or 10), 50)
            await cur.execute(f'SELECT * FROM {full_name} LIMIT {limit}')
            rows = await cur.fetchall()
            return {'ok': True, 'data': {'database': db or database, 'table': tbl, 'rows': rows, 'rowCount': len(rows) if rows else 0}}

        if intent == 'createTable' and params.get('ddl'):
            ddl = str(params['ddl']).strip()
            if not re.match(r'^\s*CREATE\s+TABLE', ddl, re.IGNORECASE):
                return {'ok': False, 'error': 'DDL 必须以 CREATE TABLE 开头'}

            database_created = False
            target_db = extract_database_from_create_table(ddl)
            if target_db:
                await cur.execute('SHOW DATABASES')
                db_rows = await cur.fetchall()
                existing = [r.get('Database') or list(r.values())[0] for r in db_rows if r]
                if target_db not in existing:
                    await cur.execute(f'CREATE DATABASE {_esc(target_db)}')
                    database_created = True

            await cur.execute(ddl)
            await conn.commit()
            return {
                'ok': True,
                'data': {
                    'message': '数据库已创建，表已创建' if database_created else '表已创建',
                    'databaseCreated': database_created,
                    'ddl': ddl,
                },
            }

        if intent == 'executeSQL' and params.get('sql'):
            sql = str(params['sql']).strip()
            forbidden = re.compile(r'\b(DROP|TRUNCATE|DELETE|UPDATE)\b', re.IGNORECASE)
            if forbidden.search(sql):
                return {'ok': False, 'error': '仅允许 SELECT / SHOW / DESCRIBE / CREATE / INSERT INTO ... SELECT 语句'}
            await cur.execute(sql)
            is_write = bool(re.match(r'^\s*(INSERT|REPLACE|DELETE|TRUNCATE)', sql, re.IGNORECASE))
            if is_write:
                await conn.commit()
                affected_rows = cur.rowcount
                last_id = cur.lastrowid
                execution_summary = {
                    'executed': True,
                    'affectedRows': affected_rows,
                    'message': f'执行完成。影响行数: {affected_rows if affected_rows is not None else "-"}'
                              + (f'，自增 ID: {last_id}' if last_id else ''),
                }
                if last_id:
                    execution_summary['insertId'] = last_id
                result_data = {
                    'executionSummary': execution_summary,
                }
                if affected_rows is not None:
                    result_data['affectedRows'] = affected_rows
                if last_id:
                    result_data['insertId'] = last_id
                return {'ok': True, 'data': result_data}
            else:
                rows = await cur.fetchall()
                row_count = len(rows) if rows else 0
                execution_summary = {
                    'executed': True,
                    'rowCount': row_count,
                    'message': f'查询完成。返回行数: {row_count}',
                }
                result_data = {
                    'executionSummary': execution_summary,
                    'rows': rows[:100] if rows else [],
                }
                return {'ok': True, 'data': result_data}

        if intent == 'analyzeNulls' and params.get('table'):
            db = (safe_identifier(params.get('database')) if params.get('database') else None) or (safe_identifier(database) if database else None)
            tbl = safe_identifier(params['table'])
            if not tbl:
                return {'ok': False, 'error': '无效的表名'}
            full_name = f'{_esc(db)}.{_esc(tbl)}' if db else _esc(tbl)
            await cur.execute(f'DESCRIBE {full_name}')
            cols = await cur.fetchall()
            col_names = [c['Field'] for c in cols]
            await cur.execute(f'SELECT COUNT(*) AS total FROM {full_name}')
            count_rows = await cur.fetchall()
            total_rows = count_rows[0].get('total', 0) if count_rows else 0
            null_checks = ', '.join(
                f'SUM(CASE WHEN {_esc(c)} IS NULL THEN 1 ELSE 0 END) AS {_esc(c)}'
                for c in col_names
            )
            await cur.execute(f'SELECT {null_checks} FROM {full_name}')
            null_rows = await cur.fetchall()
            null_counts = null_rows[0] if null_rows else {}
            analysis = []
            for c in col_names:
                null_count = int(null_counts.get(c, 0) or 0)
                null_rate = f'{(null_count / total_rows * 100):.2f}%' if total_rows > 0 else '0%'
                analysis.append({'column': c, 'nullCount': null_count, 'nullRate': null_rate})
            return {'ok': True, 'data': {'database': db or database, 'table': tbl, 'totalRows': total_rows, 'columns': analysis}}

        return {'ok': False, 'error': '不支持的操作'}
    except Exception as e:
        return {'ok': False, 'error': str(e)}
    finally:
        if conn:
            conn.close()
