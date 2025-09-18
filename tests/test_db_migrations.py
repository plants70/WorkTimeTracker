from __future__ import annotations

import sqlite3
import unittest

from user_app.db_migrations import apply_migrations


class MigrationTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self._create_base_schema()

    def tearDown(self) -> None:
        self.conn.close()

    def _create_base_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE ActiveSessions (
                Email TEXT,
                SessionID TEXT,
                Status TEXT
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE WorkLog (
                Email TEXT,
                Timestamp TEXT,
                SessionID TEXT
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE ActionLogs (
                Synced INTEGER,
                CreatedAt TEXT
            );
            """
        )
        self.conn.commit()

    def _index_names(self, table: str) -> set[str]:
        cur = self.conn.cursor()
        cur.execute(f"PRAGMA index_list({table});")
        rows = cur.fetchall()
        return {row[1] for row in rows if row and row[1]}

    def test_apply_migrations_creates_expected_indexes(self) -> None:
        apply_migrations(self.conn)

        expected = {
            "ActiveSessions": {
                "idx_active_email_session",
                "idx_active_status",
            },
            "WorkLog": {
                "idx_worklog_email_ts",
                "idx_worklog_session",
            },
            "ActionLogs": {
                "idx_actions_synced",
            },
        }

        for table, indexes in expected.items():
            with self.subTest(table=table):
                self.assertTrue(indexes.issubset(self._index_names(table)))

    def test_apply_migrations_idempotent(self) -> None:
        apply_migrations(self.conn)
        first = {table: self._index_names(table) for table in ("ActiveSessions", "WorkLog", "ActionLogs")}

        apply_migrations(self.conn)
        second = {table: self._index_names(table) for table in ("ActiveSessions", "WorkLog", "ActionLogs")}

        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
