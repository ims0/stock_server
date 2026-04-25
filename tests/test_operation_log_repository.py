from pathlib import Path
from tempfile import TemporaryDirectory
import sqlite3
import unittest

from operation_log.repository import create_log, dashboard_stats, delete_log, ensure_db, get_log, list_audits, list_logs, update_log


class OperationLogRepositoryTestCase(unittest.TestCase):
    def test_crud_and_audit_flow(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "operation_logs.db"

            log_id = create_log(
                db_path,
                {
                    "category": "technical_summary",
                    "title": "首次建仓复盘",
                    "cover_image_url": "",
                    "symbol": "600519",
                    "action_summary": "回踩均线后分批买入",
                    "content": "记录建仓逻辑和风险控制。",
                    "event_date": "2026-04-17",
                    "published_at": "2026-04-17 09:30:00",
                    "status": "published",
                },
                "root",
            )

            created = get_log(db_path, log_id)
            self.assertIsNotNone(created)
            self.assertEqual(created["title"], "首次建仓复盘")
            self.assertEqual(created["category"], "technical_summary")

            updated = update_log(
                db_path,
                log_id,
                {
                    "category": "technical_summary",
                    "title": "首次建仓复盘-修订",
                    "cover_image_url": "https://example.com/cover.png",
                    "symbol": "600519",
                    "action_summary": "补充止损条件",
                    "content": "记录建仓逻辑、风险控制与后续观察点。",
                    "event_date": "2026-04-17",
                    "published_at": "2026-04-17 10:00:00",
                    "status": "published",
                },
                "root",
            )
            self.assertTrue(updated)

            deleted = delete_log(db_path, log_id, "root")
            self.assertTrue(deleted)

            active_logs = list_logs(db_path)
            deleted_logs = list_logs(db_path, scope="deleted")
            technical_logs = list_logs(db_path, scope="deleted", category="technical_summary")
            audits = list_audits(db_path, log_id)
            stats = dashboard_stats(db_path)

            self.assertEqual(len(active_logs), 0)
            self.assertEqual(len(deleted_logs), 1)
            self.assertEqual(len(technical_logs), 1)
            self.assertEqual([item["action"] for item in audits], ["delete", "edit", "create"])
            self.assertEqual(audits[0]["snapshot"]["category"], "technical_summary")
            self.assertEqual(audits[0]["snapshot"]["cover_image_url"], "https://example.com/cover.png")
            self.assertEqual(stats["deleted_total"], 1)

    def test_migrate_existing_database_without_category(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "operation_logs.db"

            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE operation_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        title TEXT NOT NULL,
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

            ensure_db(db_path)

            log_id = create_log(
                db_path,
                {
                    "category": "technical_summary",
                    "title": "指标计算总结",
                    "cover_image_url": "",
                    "symbol": "",
                    "action_summary": "整理均线与买卖线逻辑",
                    "content": "归纳信号计算和前端展示实现。",
                    "event_date": "2026-04-17",
                    "published_at": None,
                    "status": "draft",
                },
                "root",
            )

            created = get_log(db_path, log_id)
            self.assertIsNotNone(created)
            self.assertEqual(created["category"], "technical_summary")
            self.assertEqual(created["cover_image_url"], "")


if __name__ == "__main__":
    unittest.main()