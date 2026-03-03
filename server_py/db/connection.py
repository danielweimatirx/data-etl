"""MySQL connection string parsing and testing utilities."""

import asyncio
import re
from typing import Optional
from urllib.parse import urlparse, unquote


def looks_like_connection_string(s: str) -> bool:
    """Detect whether a string looks like a MySQL connection string (URL or CLI)."""
    if not s or not isinstance(s, str):
        return False
    s = s.strip()

    # URL-style: mysql://... or generic scheme://...@...:port
    if re.match(r"^mysql://", s, re.IGNORECASE):
        return True
    if re.match(r"^[a-z]+://", s, re.IGNORECASE) and "@" in s and re.search(r":\d+", s):
        return True

    # CLI-style: mysql ... -h ... -u ... -p ...
    if re.search(r"mysql\s+.+-h\s+", s, re.IGNORECASE):
        has_user = bool(re.search(r"-u\s", s, re.IGNORECASE) or re.search(r"-u'", s))
        has_pass = bool(re.search(r"-p\s", s, re.IGNORECASE) or re.search(r"-p\S", s))
        if has_user and has_pass:
            return True

    return False


def parse_connection_string_url(s: str) -> Optional[dict]:
    """Parse a mysql:// URL into a connection config dict.

    Returns None if the string is not a valid MySQL URL.
    """
    s = s.strip()
    try:
        u = urlparse(s)
        scheme = (u.scheme or "").lower()
        if "mysql" not in scheme:
            return None
        return {
            "host": u.hostname or "",
            "port": u.port or 3306,
            "user": unquote(u.username or ""),
            "password": unquote(u.password or ""),
            "database": u.path.strip("/") or None,
        }
    except Exception:
        return None


def parse_mysql_cli_connection_string(s: str) -> Optional[dict]:
    """Parse a ``mysql -h ... -u ... -p ...`` CLI string into a connection config dict.

    Returns None if the string cannot be parsed as a MySQL CLI invocation.
    """
    if not s or not isinstance(s, str):
        return None
    s = s.strip()

    if not re.search(r"mysql\s+", s, re.IGNORECASE):
        return None

    host_match = re.search(r"-h\s+(\S+)", s, re.IGNORECASE)
    port_match = re.search(r"-P\s+(\d+)", s, re.IGNORECASE)
    user_match = re.search(r"-u\s*'([^']*)'|-u\s*(\S+)", s, re.IGNORECASE)
    password_match = re.search(r"-p\s*(\S+)", s, re.IGNORECASE) or re.search(r"-p(\S+)", s, re.IGNORECASE)

    if not host_match or not user_match:
        return None

    host = host_match.group(1).strip()
    port = int(port_match.group(1)) if port_match else 3306
    user = (user_match.group(1) or user_match.group(2) or "").strip()

    password = ""
    if password_match:
        raw = password_match.group(1) or (password_match.group(2) if password_match.lastindex and password_match.lastindex >= 2 else None)
        if raw:
            password = raw.strip()

    db_match = re.search(r"-D\s+(\S+)", s, re.IGNORECASE)
    database = db_match.group(1).strip() if db_match else None

    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
    }


def get_connection_config(connection_string: str) -> Optional[dict]:
    """Try to parse *connection_string* as either a URL or a CLI invocation.

    Returns the first successfully parsed config dict, or None.
    """
    if not connection_string:
        return None
    url_parsed = parse_connection_string_url(connection_string)
    if url_parsed is not None:
        return url_parsed
    return parse_mysql_cli_connection_string(connection_string)


async def test_connection(connection_string: str) -> dict:
    """Attempt to connect to a MySQL server described by *connection_string*.

    Returns ``{"ok": True, "message": "连接成功"}`` on success or
    ``{"ok": False, "message": "<reason>"}`` on failure.
    """
    parsed = get_connection_config(connection_string)
    if not parsed:
        return {"ok": False, "message": "连接串格式无法解析"}

    try:
        import aiomysql
    except ImportError:
        return {"ok": False, "message": "服务端未安装 aiomysql 驱动"}

    host = parsed["host"]
    port = parsed["port"]
    user = parsed["user"]
    password = parsed["password"]
    database = parsed["database"]

    try:
        conn = await asyncio.wait_for(
            aiomysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                db=database or None,
                connect_timeout=8,
            ),
            timeout=8,
        )
        try:
            await conn.ping()
        finally:
            conn.close()
        return {"ok": True, "message": "连接成功"}
    except asyncio.TimeoutError:
        return {"ok": False, "message": "连接超时"}
    except Exception as e:
        return {"ok": False, "message": str(e) or repr(e)}
