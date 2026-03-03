def rows_to_markdown_table(rows: list, columns: list = None) -> str:
    """Convert a list of rows (dicts or lists) into a Markdown table string.

    Logic is a direct port of the JavaScript ``rowsToMarkdownTable`` helper
    found in ``server.js`` (lines 141-152).
    """
    if not rows or len(rows) == 0:
        return ""

    if columns is not None:
        cols = columns
    elif isinstance(rows[0], list):
        cols = None
    else:
        cols = list(rows[0].keys())

    if not cols or len(cols) == 0:
        return ""

    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join("---" for _ in cols) + " |"
    body_lines = []
    for r in rows:
        if isinstance(r, list):
            cells = r
        else:
            cells = [("" if r.get(c) is None else str(r[c])) for c in cols]
        body_lines.append("| " + " | ".join(str(cell) for cell in cells) + " |")
    body = "\n".join(body_lines)

    return header + "\n" + sep + "\n" + body
