import re


def extract_table_refs_from_sql(sql: str) -> list[dict]:
    """Extract table references (database.table or table) from a SQL string.

    Parses FROM and JOIN clauses to find table references in these forms:
      - `database`.`table`
      - database.table
      - `table`
      - table

    Returns a deduplicated list of dicts with keys "database" (str | None)
    and "table" (str).
    """
    if not sql or not isinstance(sql, str):
        return []

    refs: list[dict] = []
    pattern = re.compile(
        r"(?:FROM|JOIN)\s+"
        r"(?:"
        r"`([^`]+)`\.`([^`]+)`"    # group 1, 2: `database`.`table`
        r"|"
        r"([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)"  # group 3, 4: database.table
        r"|"
        r"`([^`]+)`"              # group 5: `table`
        r"|"
        r"([a-zA-Z0-9_]+)"       # group 6: table
        r")",
        re.IGNORECASE,
    )

    for m in pattern.finditer(sql):
        database: str | None = None
        table: str | None = None

        if m.group(1) is not None and m.group(2) is not None:
            database = m.group(1).strip()
            table = m.group(2).strip()
        elif m.group(3) is not None and m.group(4) is not None:
            database = m.group(3).strip()
            table = m.group(4).strip()
        elif m.group(5) is not None:
            table = m.group(5).strip()
        elif m.group(6) is not None:
            table = m.group(6).strip()

        if table and re.fullmatch(r"[a-zA-Z0-9_]+", table):
            if database is None or re.fullmatch(r"[a-zA-Z0-9_]+", database):
                key = (database + "." if database else "") + table
                if not any(
                    (r.get("database") or "") + "." + r["table"] == key
                    for r in refs
                ):
                    refs.append({"database": database, "table": table})

    return refs
