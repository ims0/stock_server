from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import sqlite3
from typing import Any

STATUS_LABELS = {
    "draft": "草稿",
    "published": "已发布",
    "archived": "已归档",
}

CATEGORY_LABELS = {
    "technical_summary": "技术总结文档",
    "trading_rules": "交易规则",
    "market_strategy": "市场策略",
    "trend_forecast": "趋势预测",
}


def default_db_path(root_path: str) -> Path:
    return Path(root_path) / "data" / "operation_logs.db"


def ensure_db(db_path: str | Path) -> None:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS operation_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT NOT NULL DEFAULT 'technical_summary',
                title TEXT NOT NULL,
                cover_image_url TEXT NOT NULL DEFAULT '',
                symbol TEXT NOT NULL DEFAULT '',
                action_summary TEXT NOT NULL DEFAULT '',
                content TEXT NOT NULL,
                event_date TEXT NOT NULL,
                published_at TEXT,
                status TEXT NOT NULL,
                created_by TEXT NOT NULL,
                updated_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                deleted_at TEXT,
                deleted_by TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS operation_log_audits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                log_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                username TEXT NOT NULL,
                happened_at TEXT NOT NULL,
                snapshot_json TEXT NOT NULL,
                FOREIGN KEY (log_id) REFERENCES operation_logs(id)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_operation_logs_scope ON operation_logs (deleted_at, status, published_at, event_date)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_operation_log_audits_log_id ON operation_log_audits (log_id, happened_at DESC)"
        )

        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(operation_logs)").fetchall()
        }
        if "category" not in columns:
            conn.execute(
                "ALTER TABLE operation_logs ADD COLUMN category TEXT NOT NULL DEFAULT 'technical_summary'"
            )
        if "cover_image_url" not in columns:
            conn.execute(
                "ALTER TABLE operation_logs ADD COLUMN cover_image_url TEXT NOT NULL DEFAULT ''"
            )


def _connect(db_path: str | Path) -> sqlite3.Connection:
    ensure_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def now_string() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def parse_datetime_input(value: str) -> str | None:
    raw = value.strip()
    if not raw:
        return None
    parsed = datetime.fromisoformat(raw)
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def to_datetime_local(value: str | None) -> str:
    if not value:
        return ""
    parsed = datetime.fromisoformat(value)
    return parsed.strftime("%Y-%m-%dT%H:%M")


def to_display_datetime(value: str | None) -> str:
    if not value:
        return "-"
    parsed = datetime.fromisoformat(value)
    return parsed.strftime("%Y-%m-%d %H:%M")


def _snapshot_payload(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "category": row["category"],
        "title": row["title"],
        "cover_image_url": row["cover_image_url"],
        "symbol": row["symbol"],
        "action_summary": row["action_summary"],
        "event_date": row["event_date"],
        "published_at": row["published_at"],
        "status": row["status"],
        "updated_at": row["updated_at"],
        "deleted_at": row["deleted_at"],
    }


def _insert_audit(conn: sqlite3.Connection, log_id: int, action: str, username: str, snapshot: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO operation_log_audits (log_id, action, username, happened_at, snapshot_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (log_id, action, username, now_string(), json.dumps(snapshot, ensure_ascii=False)),
    )


def list_logs(
    db_path: str | Path,
    *,
    keyword: str = "",
    status: str = "",
    category: str = "",
    scope: str = "active",
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    if scope == "active":
        clauses.append("deleted_at IS NULL")
    elif scope == "deleted":
        clauses.append("deleted_at IS NOT NULL")

    if status:
        clauses.append("status = ?")
        params.append(status)

    if category:
        clauses.append("category = ?")
        params.append(category)

    if keyword:
        clauses.append("(title LIKE ? OR symbol LIKE ? OR action_summary LIKE ? OR content LIKE ? OR published_at LIKE ? OR event_date LIKE ?)")
        fuzzy = f"%{keyword}%"
        params.extend([fuzzy, fuzzy, fuzzy, fuzzy, fuzzy, fuzzy])

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"""
        SELECT *
        FROM operation_logs
        {where_sql}
        ORDER BY COALESCE(published_at, updated_at) DESC, id DESC
    """

    with _connect(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()

    return [dict(row) for row in rows]


def get_log(db_path: str | Path, log_id: int) -> dict[str, Any] | None:
    with _connect(db_path) as conn:
        row = conn.execute("SELECT * FROM operation_logs WHERE id = ?", (log_id,)).fetchone()
    return dict(row) if row else None


def create_log(db_path: str | Path, payload: dict[str, Any], username: str) -> int:
    timestamp = now_string()
    with _connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO operation_logs (
                category,
                title,
                cover_image_url,
                symbol,
                action_summary,
                content,
                event_date,
                published_at,
                status,
                created_by,
                updated_by,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["category"],
                payload["title"],
                payload["cover_image_url"],
                payload["symbol"],
                payload["action_summary"],
                payload["content"],
                payload["event_date"],
                payload["published_at"],
                payload["status"],
                username,
                username,
                timestamp,
                timestamp,
            ),
        )
        log_id = int(cursor.lastrowid)
        row = conn.execute("SELECT * FROM operation_logs WHERE id = ?", (log_id,)).fetchone()
        _insert_audit(conn, log_id, "create", username, _snapshot_payload(row))
    return log_id


def update_log(db_path: str | Path, log_id: int, payload: dict[str, Any], username: str) -> bool:
    timestamp = now_string()
    with _connect(db_path) as conn:
        existing = conn.execute("SELECT * FROM operation_logs WHERE id = ?", (log_id,)).fetchone()
        if existing is None:
            return False

        conn.execute(
            """
            UPDATE operation_logs
            SET category = ?,
                title = ?,
                cover_image_url = ?,
                symbol = ?,
                action_summary = ?,
                content = ?,
                event_date = ?,
                published_at = ?,
                status = ?,
                updated_by = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                payload["category"],
                payload["title"],
                payload["cover_image_url"],
                payload["symbol"],
                payload["action_summary"],
                payload["content"],
                payload["event_date"],
                payload["published_at"],
                payload["status"],
                username,
                timestamp,
                log_id,
            ),
        )
        row = conn.execute("SELECT * FROM operation_logs WHERE id = ?", (log_id,)).fetchone()
        _insert_audit(conn, log_id, "edit", username, _snapshot_payload(row))
    return True


def delete_log(db_path: str | Path, log_id: int, username: str) -> bool:
    timestamp = now_string()
    with _connect(db_path) as conn:
        existing = conn.execute("SELECT * FROM operation_logs WHERE id = ?", (log_id,)).fetchone()
        if existing is None:
            return False
        if existing["deleted_at"]:
            return True

        conn.execute(
            "UPDATE operation_logs SET deleted_at = ?, deleted_by = ?, updated_by = ?, updated_at = ? WHERE id = ?",
            (timestamp, username, username, timestamp, log_id),
        )
        row = conn.execute("SELECT * FROM operation_logs WHERE id = ?", (log_id,)).fetchone()
        _insert_audit(conn, log_id, "delete", username, _snapshot_payload(row))
    return True


def list_audits(db_path: str | Path, log_id: int) -> list[dict[str, Any]]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM operation_log_audits
            WHERE log_id = ?
            ORDER BY happened_at DESC, id DESC
            """,
            (log_id,),
        ).fetchall()

    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["snapshot"] = json.loads(item["snapshot_json"])
        item["snapshot"].setdefault("category", "technical_summary")
        items.append(item)
    return items


def recent_audits(db_path: str | Path, limit: int = 12) -> list[dict[str, Any]]:
    return recent_audits_by_category(db_path, category="", limit=limit)


def recent_audits_by_category(db_path: str | Path, *, category: str = "", limit: int = 12) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    if category:
        clauses.append("l.category = ?")
        params.append(category)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    with _connect(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT a.*, l.title
            FROM operation_log_audits a
            JOIN operation_logs l ON l.id = a.log_id
            {where_sql}
            ORDER BY a.happened_at DESC, a.id DESC
            LIMIT ?
            """,
            [*params, limit],
        ).fetchall()

    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["snapshot"] = json.loads(item["snapshot_json"])
        item["snapshot"].setdefault("category", "technical_summary")
        items.append(item)
    return items


def published_dates_in_month(
    db_path: str | Path,
    year: int,
    month: int,
    *,
    category: str = "",
) -> set[int]:
    """Return set of days (1-31) that have at least one published article in the given month."""
    prefix = f"{year:04d}-{month:02d}-"
    clauses = ["deleted_at IS NULL", "status = 'published'", "published_at LIKE ?"]
    params: list[Any] = [f"{prefix}%"]
    if category:
        clauses.append("category = ?")
        params.append(category)
    where_sql = "WHERE " + " AND ".join(clauses)
    sql = f"SELECT published_at FROM operation_logs {where_sql}"
    with _connect(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
    days: set[int] = set()
    for row in rows:
        val = row[0]
        if val and len(val) >= 10:
            try:
                days.add(int(val[8:10]))
            except ValueError:
                pass
    return days


def dashboard_stats(db_path: str | Path, *, category: str = "") -> dict[str, int]:
    clauses: list[str] = []
    params: list[Any] = []

    if category:
        clauses.append("category = ?")
        params.append(category)

    where_sql = f" AND {' AND '.join(clauses)}" if clauses else ""

    with _connect(db_path) as conn:
        active_total = conn.execute(
            f"SELECT COUNT(*) FROM operation_logs WHERE deleted_at IS NULL{where_sql}",
            params,
        ).fetchone()[0]
        published_total = conn.execute(
            f"SELECT COUNT(*) FROM operation_logs WHERE deleted_at IS NULL AND status = 'published'{where_sql}",
            params,
        ).fetchone()[0]
        draft_total = conn.execute(
            f"SELECT COUNT(*) FROM operation_logs WHERE deleted_at IS NULL AND status = 'draft'{where_sql}",
            params,
        ).fetchone()[0]
        deleted_total = conn.execute(
            f"SELECT COUNT(*) FROM operation_logs WHERE deleted_at IS NOT NULL{where_sql}",
            params,
        ).fetchone()[0]

    return {
        "active_total": int(active_total),
        "published_total": int(published_total),
        "draft_total": int(draft_total),
        "deleted_total": int(deleted_total),
    }


def monthly_archives(
    db_path: str | Path,
    *,
    category: str = "",
) -> list[dict[str, Any]]:
    """Return published doc counts grouped by year-month, newest first."""
    clauses = ["deleted_at IS NULL", "status = 'published'", "published_at IS NOT NULL"]
    params: list[Any] = []
    if category:
        clauses.append("category = ?")
        params.append(category)
    where_sql = "WHERE " + " AND ".join(clauses)
    sql = f"""
        SELECT substr(published_at, 1, 7) AS ym, COUNT(*) AS cnt
        FROM operation_logs
        {where_sql}
        GROUP BY ym
        ORDER BY ym DESC
    """
    with _connect(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
    result = []
    for row in rows:
        ym = row[0] or ""
        if len(ym) == 7:
            year, month = int(ym[:4]), int(ym[5:7])
            result.append({"year": year, "month": month, "ym": ym, "count": int(row[1])})
    return result


def symbol_groups(
    db_path: str | Path,
    *,
    category: str = "",
) -> list[dict[str, Any]]:
    """Return doc counts grouped by symbol (non-empty), sorted by count desc."""
    clauses = ["deleted_at IS NULL", "status = 'published'", "symbol != ''"]
    params: list[Any] = []
    if category:
        clauses.append("category = ?")
        params.append(category)
    where_sql = "WHERE " + " AND ".join(clauses)
    sql = f"""
        SELECT symbol, COUNT(*) AS cnt
        FROM operation_logs
        {where_sql}
        GROUP BY symbol
        ORDER BY cnt DESC, symbol ASC
    """
    with _connect(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [{"symbol": row[0], "count": int(row[1])} for row in rows]