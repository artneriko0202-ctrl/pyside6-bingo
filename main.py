import sys
import json
import random
import sqlite3
from pathlib import Path

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget,
    QPushButton, QGridLayout,
    QFileDialog, QInputDialog,
    QTextEdit, QDialog, QVBoxLayout,
    QLineEdit, QLabel, QScrollArea, QHBoxLayout,
    QMessageBox, QListWidget, QListWidgetItem,
    QSizePolicy, QTableWidget, QTableWidgetItem,
    QHeaderView, QTabWidget, QGroupBox, QFormLayout
)
from PySide6.QtGui import QFont, QIcon, QColor, QBrush, QScreen
from PySide6.QtCore import Qt, QTimer, QPropertyAnimation, QRect

# -----------------------
# データベース管理
# -----------------------
class BingoDatabase:
    def __init__(self, db_path="bingo_data.db"):
        self.db_path = db_path
        self.init_db()

    def init_db(self):
        """データベーステーブルを初期化"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # プレイヤーテーブル
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS players (
                player_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # ビンゴテンプレートテーブル（全プレイヤー共通）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bingo_templates (
                template_id INTEGER PRIMARY KEY AUTOINCREMENT,
                size INTEGER NOT NULL CHECK(size IN (3, 5)),
                title TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # マイグレーション: 旧スキーマ(player_id付き)からの移行
        cursor.execute("PRAGMA table_info(bingo_templates)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'player_id' in columns:
            cursor.execute("ALTER TABLE bingo_templates RENAME TO _bingo_templates_old")
            cursor.execute("""
                CREATE TABLE bingo_templates (
                    template_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    size INTEGER NOT NULL CHECK(size IN (3, 5)),
                    title TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("""
                INSERT INTO bingo_templates (template_id, size, title, created_at, updated_at)
                SELECT template_id, size, title, created_at, updated_at FROM _bingo_templates_old
            """)
            cursor.execute("DROP TABLE _bingo_templates_old")

        # ビンゴセルテーブル
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bingo_cells (
                cell_id INTEGER PRIMARY KEY AUTOINCREMENT,
                template_id INTEGER NOT NULL,
                row INTEGER NOT NULL,
                col INTEGER NOT NULL,
                text TEXT NOT NULL DEFAULT '',
                FOREIGN KEY(template_id) REFERENCES bingo_templates(template_id),
                UNIQUE(template_id, row, col)
            )
        """)

        # ビンゴセッションテーブル
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bingo_sessions (
                session_id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id INTEGER NOT NULL,
                template_id INTEGER NOT NULL,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMP,
                status TEXT DEFAULT 'active' CHECK(status IN ('active', 'completed')),
                FOREIGN KEY(player_id) REFERENCES players(player_id),
                FOREIGN KEY(template_id) REFERENCES bingo_templates(template_id)
            )
        """)

        # ビンゴ結果テーブル
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bingo_results (
                result_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL,
                matched_lines TEXT,
                marked_cells TEXT,
                board_layout TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_deleted BOOLEAN DEFAULT FALSE,
                FOREIGN KEY(session_id) REFERENCES bingo_sessions(session_id)
            )
        """)

        # マイグレーション: board_layout カラムが無ければ追加
        cursor.execute("PRAGMA table_info(bingo_results)")
        columns = [col[1] for col in cursor.fetchall()]
        if "board_layout" not in columns:
            cursor.execute("ALTER TABLE bingo_results ADD COLUMN board_layout TEXT")

        # マイグレーション: players に memo カラムが無ければ追加
        cursor.execute("PRAGMA table_info(players)")
        player_columns = [col[1] for col in cursor.fetchall()]
        if "memo" not in player_columns:
            cursor.execute("ALTER TABLE players ADD COLUMN memo TEXT DEFAULT ''")

        conn.commit()
        conn.close()

    # ----- プレイヤー管理 -----
    def create_player(self, name):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("INSERT INTO players (name) VALUES (?)", (name,))
            conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            return None
        finally:
            conn.close()

    def update_player_name(self, player_id, new_name):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE players SET name = ?, updated_at = CURRENT_TIMESTAMP WHERE player_id = ?",
                (new_name, player_id)
            )
            conn.commit()
            return cursor.rowcount > 0
        except sqlite3.IntegrityError:
            return False
        finally:
            conn.close()

    def get_all_players(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT player_id, name FROM players ORDER BY created_at DESC")
            return [(row[0], row[1]) for row in cursor.fetchall()]
        finally:
            conn.close()

    def get_player_name(self, player_id):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT name FROM players WHERE player_id = ?", (player_id,))
            result = cursor.fetchone()
            return result[0] if result else None
        finally:
            conn.close()

    def delete_player(self, player_id):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            # 関連する結果を論理削除
            cursor.execute("""
                UPDATE bingo_results SET is_deleted = TRUE
                WHERE session_id IN (
                    SELECT session_id FROM bingo_sessions WHERE player_id = ?
                )
            """, (player_id,))
            cursor.execute("DELETE FROM bingo_sessions WHERE player_id = ?", (player_id,))
            # テンプレートは共通なので削除しない
            cursor.execute("DELETE FROM players WHERE player_id = ?", (player_id,))
            conn.commit()
        finally:
            conn.close()

    # ----- テンプレート管理 -----
    def create_template(self, size, title, board):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO bingo_templates (size, title) VALUES (?, ?)",
                (size, title)
            )
            template_id = cursor.lastrowid
            for i in range(size):
                for j in range(size):
                    text = board[i][j] if i < len(board) and j < len(board[i]) else ""
                    cursor.execute(
                        "INSERT INTO bingo_cells (template_id, row, col, text) VALUES (?, ?, ?, ?)",
                        (template_id, i, j, text)
                    )
            conn.commit()
            return template_id
        except Exception:
            conn.rollback()
            return None
        finally:
            conn.close()

    def update_template(self, template_id, title, board):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE bingo_templates SET title = ?, updated_at = CURRENT_TIMESTAMP WHERE template_id = ?",
                (title, template_id)
            )
            cursor.execute("SELECT size FROM bingo_templates WHERE template_id = ?", (template_id,))
            size = cursor.fetchone()[0]
            for i in range(size):
                for j in range(size):
                    text = board[i][j] if i < len(board) and j < len(board[i]) else ""
                    cursor.execute(
                        "UPDATE bingo_cells SET text = ? WHERE template_id = ? AND row = ? AND col = ?",
                        (text, template_id, i, j)
                    )
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            return False
        finally:
            conn.close()

    def get_all_templates(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT template_id, title, size, created_at FROM bingo_templates
                ORDER BY updated_at DESC
            """)
            return cursor.fetchall()  # [(template_id, title, size, created_at), ...]
        finally:
            conn.close()

    def get_template_data(self, template_id):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT size, title FROM bingo_templates WHERE template_id = ?",
                (template_id,)
            )
            info = cursor.fetchone()
            if not info:
                return None
            size, title = info
            cursor.execute(
                "SELECT row, col, text FROM bingo_cells WHERE template_id = ? ORDER BY row, col",
                (template_id,)
            )
            board = [["" for _ in range(size)] for _ in range(size)]
            for row, col, text in cursor.fetchall():
                if row < size and col < size:
                    board[row][col] = text
            return {
                "template_id": template_id,
                "size": size,
                "title": title,
                "board": board
            }
        finally:
            conn.close()

    def delete_template(self, template_id):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE bingo_results SET is_deleted = TRUE
                WHERE session_id IN (
                    SELECT session_id FROM bingo_sessions WHERE template_id = ?
                )
            """, (template_id,))
            cursor.execute("DELETE FROM bingo_sessions WHERE template_id = ?", (template_id,))
            cursor.execute("DELETE FROM bingo_cells WHERE template_id = ?", (template_id,))
            cursor.execute("DELETE FROM bingo_templates WHERE template_id = ?", (template_id,))
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            return False
        finally:
            conn.close()

    # ----- セッション管理 -----
    def create_session(self, player_id, template_id, board_layout=None):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO bingo_sessions (player_id, template_id) VALUES (?, ?)",
                (player_id, template_id)
            )
            session_id = cursor.lastrowid
            board_json = json.dumps(board_layout, ensure_ascii=False) if board_layout else None
            cursor.execute(
                "INSERT INTO bingo_results (session_id, matched_lines, marked_cells, board_layout) VALUES (?, ?, ?, ?)",
                (session_id, "[]", "[]", board_json)
            )
            conn.commit()
            return session_id
        except Exception:
            conn.rollback()
            return None
        finally:
            conn.close()

    def get_active_session(self, player_id, template_id):
        """指定プレイヤー＋テンプレートの最新セッションを取得（完了済み含む）"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT s.session_id, r.matched_lines, r.marked_cells, r.board_layout
                FROM bingo_sessions s
                LEFT JOIN bingo_results r ON s.session_id = r.session_id AND r.is_deleted = FALSE
                WHERE s.player_id = ? AND s.template_id = ?
                ORDER BY s.started_at DESC LIMIT 1
            """, (player_id, template_id))
            result = cursor.fetchone()
            if result:
                session_id, matched_json, marked_json, board_json = result
                return {
                    "session_id": session_id,
                    "matched_lines": json.loads(matched_json) if matched_json else [],
                    "marked_cells": json.loads(marked_json) if marked_json else [],
                    "board_layout": json.loads(board_json) if board_json else None
                }
            return None
        finally:
            conn.close()

    def update_session_board(self, session_id, board_layout):
        """セッションのボードレイアウトを更新（シャッフル時用）"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            board_json = json.dumps(board_layout, ensure_ascii=False)
            cursor.execute("""
                UPDATE bingo_results SET board_layout = ?, updated_at = CURRENT_TIMESTAMP
                WHERE session_id = ? AND is_deleted = FALSE
            """, (board_json, session_id))
            conn.commit()
        finally:
            conn.close()

    def update_session_result(self, session_id, matched_lines, marked_cells):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            matched_json = json.dumps(matched_lines, ensure_ascii=False)
            marked_json = json.dumps(marked_cells, ensure_ascii=False)
            cursor.execute("""
                UPDATE bingo_results SET matched_lines = ?, marked_cells = ?, updated_at = CURRENT_TIMESTAMP
                WHERE session_id = ? AND is_deleted = FALSE
            """, (matched_json, marked_json, session_id))
            conn.commit()
        finally:
            conn.close()

    def complete_session(self, session_id):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE bingo_sessions SET status = 'completed', finished_at = CURRENT_TIMESTAMP WHERE session_id = ?",
                (session_id,)
            )
            conn.commit()
        finally:
            conn.close()

    def get_session_data(self, session_id):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT s.player_id, s.template_id, s.started_at, s.finished_at, s.status,
                       r.matched_lines, r.marked_cells
                FROM bingo_sessions s
                LEFT JOIN bingo_results r ON s.session_id = r.session_id AND r.is_deleted = FALSE
                WHERE s.session_id = ?
            """, (session_id,))
            result = cursor.fetchone()
            if result:
                player_id, template_id, started_at, finished_at, status, matched_json, marked_json = result
                return {
                    "session_id": session_id,
                    "player_id": player_id,
                    "template_id": template_id,
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "status": status,
                    "matched_lines": json.loads(matched_json) if matched_json else [],
                    "marked_cells": json.loads(marked_json) if marked_json else []
                }
            return None
        finally:
            conn.close()

    def get_player_sessions(self, player_id):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT s.session_id, t.title, s.started_at, s.finished_at, s.status
                FROM bingo_sessions s
                JOIN bingo_templates t ON s.template_id = t.template_id
                WHERE s.player_id = ?
                ORDER BY s.started_at DESC
            """, (player_id,))
            return [
                {
                    "session_id": row[0],
                    "title": row[1],
                    "started_at": row[2],
                    "finished_at": row[3],
                    "status": row[4]
                }
                for row in cursor.fetchall()
            ]
        finally:
            conn.close()

    def get_ranking(self):
        """保存されたマーク状態からビンゴライン数を計算してランキング"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT p.player_id, p.name,
                       s.session_id, s.status, s.started_at, s.finished_at,
                       t.size, r.marked_cells
                FROM bingo_sessions s
                JOIN players p ON s.player_id = p.player_id
                JOIN bingo_templates t ON s.template_id = t.template_id
                LEFT JOIN bingo_results r ON s.session_id = r.session_id AND r.is_deleted = FALSE
            """)

            player_data = {}
            for row in cursor.fetchall():
                pid, name, sid, status, started, finished, size, marked_json = row
                marked_cells = json.loads(marked_json) if marked_json else []
                lines = self._count_bingo_lines(marked_cells, size)

                if pid not in player_data:
                    player_data[pid] = {
                        "player_name": name,
                        "total_sessions": 0,
                        "total_bingo_lines": 0,
                        "total_marks": 0,
                        "total_cells": 0,
                        "first_bingo_at": None
                    }
                d = player_data[pid]
                d["total_sessions"] += 1
                d["total_bingo_lines"] += lines
                d["total_marks"] += len(marked_cells)
                d["total_cells"] += size * size
                if lines > 0 and finished:
                    if d["first_bingo_at"] is None or finished < d["first_bingo_at"]:
                        d["first_bingo_at"] = finished

            for d in player_data.values():
                sessions = max(d["total_sessions"], 1)
                cells = max(d["total_cells"], 1)
                bingo_eff = d["total_bingo_lines"] / sessions
                mark_rate = d["total_marks"] / cells
                d["rating"] = round(bingo_eff * 500 + mark_rate * 200 + d["total_bingo_lines"] * 20)

            result = sorted(
                player_data.values(),
                key=lambda x: -x["rating"]
            )
            return result
        finally:
            conn.close()

    def get_ranking_by_template(self, template_id):
        """特定テンプレートごとのランキング"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT p.player_id, p.name,
                       s.session_id, s.status, s.started_at, s.finished_at,
                       t.size, r.marked_cells
                FROM bingo_sessions s
                JOIN players p ON s.player_id = p.player_id
                JOIN bingo_templates t ON s.template_id = t.template_id
                LEFT JOIN bingo_results r ON s.session_id = r.session_id AND r.is_deleted = FALSE
                WHERE s.template_id = ?
            """, (template_id,))

            player_data = {}
            for row in cursor.fetchall():
                pid, name, sid, status, started, finished, size, marked_json = row
                marked_cells = json.loads(marked_json) if marked_json else []
                lines = self._count_bingo_lines(marked_cells, size)

                if pid not in player_data:
                    player_data[pid] = {
                        "player_name": name,
                        "total_sessions": 0,
                        "total_bingo_lines": 0,
                        "total_marks": 0,
                        "total_cells": 0,
                        "first_bingo_at": None
                    }
                d = player_data[pid]
                d["total_sessions"] += 1
                d["total_bingo_lines"] += lines
                d["total_marks"] += len(marked_cells)
                d["total_cells"] += size * size
                if lines > 0 and finished:
                    if d["first_bingo_at"] is None or finished < d["first_bingo_at"]:
                        d["first_bingo_at"] = finished

            for d in player_data.values():
                sessions = max(d["total_sessions"], 1)
                cells = max(d["total_cells"], 1)
                bingo_eff = d["total_bingo_lines"] / sessions
                mark_rate = d["total_marks"] / cells
                d["rating"] = round(bingo_eff * 500 + mark_rate * 200 + d["total_bingo_lines"] * 20)

            result = sorted(
                player_data.values(),
                key=lambda x: -x["rating"]
            )
            return result
        finally:
            conn.close()

    @staticmethod
    def _count_bingo_lines(marked_cells, size):
        """保存済みのmarked_cellsからビンゴライン数を計算"""
        if not marked_cells or size <= 0:
            return 0
        grid = [[False] * size for _ in range(size)]
        for cell in marked_cells:
            r, c = cell[0], cell[1]
            if 0 <= r < size and 0 <= c < size:
                grid[r][c] = True

        lines = 0
        for i in range(size):
            if all(grid[i][j] for j in range(size)):
                lines += 1
        for j in range(size):
            if all(grid[i][j] for i in range(size)):
                lines += 1
        if all(grid[i][i] for i in range(size)):
            lines += 1
        if all(grid[i][size - 1 - i] for i in range(size)):
            lines += 1
        return lines

    def get_player_stats(self, player_id):
        """プレイヤーの統計情報を保存データから計算"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT s.session_id, s.status, t.size, t.title,
                       r.marked_cells
                FROM bingo_sessions s
                JOIN bingo_templates t ON s.template_id = t.template_id
                LEFT JOIN bingo_results r ON s.session_id = r.session_id AND r.is_deleted = FALSE
                WHERE s.player_id = ?
            """, (player_id,))

            total_sessions = 0
            total_bingo_lines = 0
            total_marks = 0
            total_cells = 0

            for row in cursor.fetchall():
                sid, status, size, title, marked_json = row
                marked_cells = json.loads(marked_json) if marked_json else []
                lines = self._count_bingo_lines(marked_cells, size)

                total_sessions += 1
                total_bingo_lines += lines
                total_marks += len(marked_cells)
                total_cells += size * size

            cursor.execute("SELECT COUNT(*) FROM bingo_templates")
            template_count = cursor.fetchone()[0]

            # 登録日を取得
            cursor.execute("SELECT created_at FROM players WHERE player_id = ?", (player_id,))
            row = cursor.fetchone()
            created_at = row[0] if row else None

            return {
                "total_sessions": total_sessions,
                "total_bingo_lines": total_bingo_lines,
                "total_marks": total_marks,
                "total_cells": total_cells,
                "template_count": template_count,
                "created_at": created_at
            }
        finally:
            conn.close()

    def get_player_memo(self, player_id):
        """プレイヤーのメモを取得"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT memo FROM players WHERE player_id = ?", (player_id,))
            row = cursor.fetchone()
            return row[0] if row and row[0] else ""
        finally:
            conn.close()

    def save_player_memo(self, player_id, memo):
        """プレイヤーのメモを保存"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE players SET memo = ?, updated_at = CURRENT_TIMESTAMP WHERE player_id = ?",
                (memo, player_id)
            )
            conn.commit()
        finally:
            conn.close()

    def get_player_marked_texts(self, player_id):
        """プレイヤーがマーク済みの全テキスト一覧（セッション別）"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT s.session_id, t.title, t.size, s.started_at, s.status,
                       r.marked_cells, t.template_id, r.board_layout
                FROM bingo_sessions s
                JOIN bingo_templates t ON s.template_id = t.template_id
                LEFT JOIN bingo_results r ON s.session_id = r.session_id AND r.is_deleted = FALSE
                WHERE s.player_id = ?
                ORDER BY s.started_at DESC
            """, (player_id,))

            results = []
            for row in cursor.fetchall():
                session_id, title, size, started_at, status, marked_json, template_id, board_json = row
                marked_cells = json.loads(marked_json) if marked_json else []

                # マーク済みセルのテキストを取得
                marked_texts = []
                if marked_cells:
                    # 保存済みボードレイアウトがあればそこから取得（シャッフル済み対応）
                    board = json.loads(board_json) if board_json else None
                    if board:
                        for cell in marked_cells:
                            r, c = cell[0], cell[1]
                            if 0 <= r < len(board) and 0 <= c < len(board[0]):
                                text = board[r][c]
                                if text:
                                    marked_texts.append(text)
                    else:
                        # board_layoutがない場合はテンプレートのセルから取得
                        cursor2 = conn.cursor()
                        for cell in marked_cells:
                            r, c = cell[0], cell[1]
                            cursor2.execute(
                                "SELECT text FROM bingo_cells WHERE template_id = ? AND row = ? AND col = ?",
                                (template_id, r, c)
                            )
                            cell_row = cursor2.fetchone()
                            if cell_row and cell_row[0]:
                                marked_texts.append(cell_row[0])

                results.append({
                    "session_id": session_id,
                    "title": title,
                    "size": size,
                    "started_at": started_at,
                    "status": status,
                    "marked_count": len(marked_cells),
                    "total_cells": size * size,
                    "marked_texts": marked_texts,
                    "bingo_lines": self._count_bingo_lines(marked_cells, size)
                })
            return results
        finally:
            conn.close()

    def delete_session(self, session_id):
        """セッションと結果を削除"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE bingo_results SET is_deleted = TRUE WHERE session_id = ?", (session_id,))
            cursor.execute("DELETE FROM bingo_sessions WHERE session_id = ?", (session_id,))
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            return False
        finally:
            conn.close()

    def revert_session_status(self, session_id):
        """セッションのステータスをactiveに戻す"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE bingo_sessions SET status = 'active', finished_at = NULL WHERE session_id = ?",
                (session_id,)
            )
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            return False
        finally:
            conn.close()


# グローバルDB接続
db = BingoDatabase()

def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def load_json(path, fallback):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return fallback


# -----------------------
# テキスト編集ダイアログ（グリッド入力）
# -----------------------
class TextEditDialog(QDialog):
    def __init__(self, size=5, title="ビンゴ", parent=None):
        super().__init__(parent)
        self.size = size
        self.title = title
        self.inputs = []

        self.setWindowTitle(f"ビンゴ内容編集 ({size}x{size})")
        self.setMinimumWidth(500)
        self.setMinimumHeight(400)

        layout = QVBoxLayout()

        grid_widget = QWidget()
        grid_layout = QGridLayout()
        grid_layout.setSpacing(8)

        for i in range(size):
            row = []
            for j in range(size):
                input_field = QTextEdit()
                input_field.setPlaceholderText(f"{i+1}-{j+1}")
                input_field.setMinimumHeight(80)
                input_field.setMinimumWidth(120)
                font = QFont()
                font.setPointSize(10)
                input_field.setFont(font)
                input_field.setStyleSheet("""
                    QTextEdit {
                        padding: 8px;
                        border: 2px solid #4CAF50;
                        border-radius: 5px;
                        background-color: #f9f9f9;
                        color: #333;
                    }
                    QTextEdit:focus {
                        border: 2px solid #45a049;
                        background-color: #ffffff;
                    }
                """)
                grid_layout.addWidget(input_field, i, j)
                row.append(input_field)
            self.inputs.append(row)

        grid_widget.setLayout(grid_layout)

        scroll = QScrollArea()
        scroll.setWidget(grid_widget)
        scroll.setWidgetResizable(True)
        layout.addWidget(scroll)

        btn_layout = QHBoxLayout()
        ok_btn = QPushButton("OK")
        cancel_btn = QPushButton("キャンセル")
        ok_btn.setMinimumHeight(40)
        cancel_btn.setMinimumHeight(40)
        ok_btn.setStyleSheet("""
            QPushButton { background-color: #4CAF50; color: white; font-weight: bold; border-radius: 5px; padding: 8px; }
            QPushButton:hover { background-color: #45a049; }
        """)
        cancel_btn.setStyleSheet("""
            QPushButton { background-color: #f44336; color: white; font-weight: bold; border-radius: 5px; padding: 8px; }
            QPushButton:hover { background-color: #da190b; }
        """)
        ok_btn.clicked.connect(self.accept)
        cancel_btn.clicked.connect(self.reject)
        btn_layout.addWidget(ok_btn)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)

        self.setLayout(layout)

    def set_board_data(self, board):
        for i in range(self.size):
            for j in range(self.size):
                if i < len(board) and j < len(board[i]):
                    self.inputs[i][j].setPlainText(str(board[i][j]))

    def get_board_data(self):
        board = []
        for i in range(self.size):
            row = []
            for j in range(self.size):
                row.append(self.inputs[i][j].toPlainText())
            board.append(row)
        return board


# -----------------------
# プレイヤー選択ダイアログ
# -----------------------
class PlayerSelectDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("プレイヤー選択")
        self.setMinimumWidth(400)

        layout = QVBoxLayout()
        layout.addWidget(QLabel("プレイヤーを選択:"))

        self.player_list = db.get_all_players()
        for player_id, name in self.player_list:
            btn = QPushButton(f"👤 {name}")
            btn.setMinimumHeight(40)
            btn.clicked.connect(lambda checked, pid=player_id: self.select_player(pid))
            layout.addWidget(btn)

        layout.addSpacing(20)

        new_btn = QPushButton("➕ 新しいプレイヤーを作成")
        new_btn.setMinimumHeight(40)
        new_btn.setStyleSheet("""
            QPushButton { background-color: #2196F3; color: white; font-weight: bold; border-radius: 5px; padding: 8px; }
            QPushButton:hover { background-color: #1976D2; }
        """)
        new_btn.clicked.connect(self.create_new_player)
        layout.addWidget(new_btn)

        self.setLayout(layout)
        self.selected_player_id = None

    def select_player(self, player_id):
        self.selected_player_id = player_id
        self.accept()

    def create_new_player(self):
        name, ok = QInputDialog.getText(self, "プレイヤー名", "プレイヤー名を入力:")
        if ok and name.strip():
            player_id = db.create_player(name.strip())
            if player_id:
                self.selected_player_id = player_id
                self.accept()
            else:
                QMessageBox.warning(self, "エラー", "そのプレイヤー名は既に使用されています")


# -----------------------
# テンプレート選択ダイアログ
# -----------------------
class TemplateSelectDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("テンプレート選択")
        self.setMinimumWidth(400)
        self.setMinimumHeight(300)

        self.selected_template_id = None
        self.want_new = False

        layout = QVBoxLayout()
        layout.addWidget(QLabel("使用するテンプレートを選択:"))

        self.list_widget = QListWidget()
        templates = db.get_all_templates()
        for tid, title, size, created_at in templates:
            item = QListWidgetItem(f"{title} ({size}x{size})")
            item.setData(Qt.UserRole, tid)
            self.list_widget.addItem(item)
        self.list_widget.itemDoubleClicked.connect(self._on_double_click)
        layout.addWidget(self.list_widget)

        btn_layout = QHBoxLayout()
        select_btn = QPushButton("選択")
        new_btn = QPushButton("➕ 新規作成")
        cancel_btn = QPushButton("キャンセル")

        select_btn.setMinimumHeight(36)
        new_btn.setMinimumHeight(36)
        cancel_btn.setMinimumHeight(36)

        select_btn.clicked.connect(self._on_select)
        new_btn.clicked.connect(self._on_new)
        cancel_btn.clicked.connect(self.reject)

        btn_layout.addWidget(select_btn)
        btn_layout.addWidget(new_btn)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)

        self.setLayout(layout)

    def _on_select(self):
        item = self.list_widget.currentItem()
        if item:
            self.selected_template_id = item.data(Qt.UserRole)
            self.accept()

    def _on_double_click(self, item):
        self.selected_template_id = item.data(Qt.UserRole)
        self.accept()

    def _on_new(self):
        self.want_new = True
        self.accept()


# -----------------------
# ランキングダイアログ
# -----------------------
class RankingDialog(QDialog):
    RANK_TIERS = [
        (2000, "S+"), (1500, "S"), (1000, "A+"), (700, "A"),
        (500, "B+"), (300, "B"), (150, "C"), (0, "D"),
    ]

    @classmethod
    def get_tier(cls, rating):
        for threshold, tier in cls.RANK_TIERS:
            if rating >= threshold:
                return tier
        return "D"

    def _build_ranking_table(self, ranking):
        if not ranking:
            label = QLabel("まだデータがありません")
            label.setAlignment(Qt.AlignCenter)
            return label
        table = QTableWidget(len(ranking), 6)
        table.setHorizontalHeaderLabels(["順位", "レート", "プレイヤー", "ビンゴライン", "マーク数", "セッション数"])
        table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        table.setEditTriggers(QTableWidget.NoEditTriggers)
        table.setSelectionBehavior(QTableWidget.SelectRows)
        for idx, r in enumerate(ranking):
            rating = r["rating"]
            tier = self.get_tier(rating)
            rank_item = QTableWidgetItem(str(idx + 1))
            rank_item.setTextAlignment(Qt.AlignCenter)
            table.setItem(idx, 0, rank_item)
            rate_item = QTableWidgetItem(f"{tier}  ({rating})")
            rate_item.setTextAlignment(Qt.AlignCenter)
            table.setItem(idx, 1, rate_item)
            table.setItem(idx, 2, QTableWidgetItem(r["player_name"]))
            table.setItem(idx, 3, QTableWidgetItem(str(r["total_bingo_lines"])))
            table.setItem(idx, 4, QTableWidgetItem(str(r["total_marks"])))
            table.setItem(idx, 5, QTableWidgetItem(str(r["total_sessions"])))
        return table

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("🏆 ランキング")
        self.setMinimumWidth(700)
        self.setMinimumHeight(400)

        layout = QVBoxLayout()

        tabs = QTabWidget()

        # 総合タブ
        tabs.addTab(self._build_ranking_table(db.get_ranking()), "総合")

        # テンプレート別タブ
        templates = db.get_all_templates()
        for tid, title, size, _ in templates:
            tab_label = f"{title} ({size}x{size})"
            ranking = db.get_ranking_by_template(tid)
            tabs.addTab(self._build_ranking_table(ranking), tab_label)

        layout.addWidget(tabs)

        close_btn = QPushButton("閉じる")
        close_btn.clicked.connect(self.accept)
        layout.addWidget(close_btn)
        self.setLayout(layout)


# -----------------------
# プレイヤー統計ダイアログ
# -----------------------
class PlayerStatsDialog(QDialog):
    def __init__(self, player_id, player_name, parent=None):
        super().__init__(parent)
        self.player_id = player_id
        self.player_name = player_name
        self.setWindowTitle(f"📊 {player_name} の統計")
        self.setMinimumWidth(900)
        self.setMinimumHeight(550)

        outer_layout = QVBoxLayout()

        # --- 登録日メッセージ（上部に全幅で表示） ---
        stats = db.get_player_stats(player_id)
        created_at = stats.get("created_at")
        if created_at:
            try:
                from datetime import datetime
                dt = datetime.strptime(created_at[:10], "%Y-%m-%d")
                date_str = f"{dt.year}年{dt.month}月{dt.day}日"
            except Exception:
                date_str = created_at[:10]
            greeting = QLabel(f"🎉 はじめて登録したのは {date_str} だよ！")
            greeting.setStyleSheet("font-size: 14px; padding: 6px; color: #FFD700;")
            greeting.setAlignment(Qt.AlignCenter)
            outer_layout.addWidget(greeting)

        # --- ランキング情報取得 ---
        ranking = db.get_ranking()
        player_rank = None
        player_rating = None
        total_players = len(ranking)
        for i, r in enumerate(ranking):
            if r["player_name"] == player_name:
                player_rank = i + 1
                player_rating = r["rating"]
                break

        # --- 分析コメント（上部に全幅で表示） ---
        comment = self._generate_comment(stats, player_rank, total_players, player_rating)
        if comment:
            comment_label = QLabel(comment)
            comment_label.setWordWrap(True)
            comment_label.setStyleSheet(
                "font-size: 13px; padding: 8px; margin: 4px 0;"
                "background: rgba(255,255,255,0.07); border-radius: 8px;"
            )
            comment_label.setAlignment(Qt.AlignCenter)
            outer_layout.addWidget(comment_label)

        # ===== 左右2カラム =====
        columns = QHBoxLayout()

        # --- 左カラム: サマリー ---
        left = QVBoxLayout()
        summary = QGroupBox("サマリー")
        form = QFormLayout()
        form.addRow("セッション数:", QLabel(str(stats["total_sessions"])))
        form.addRow("ビンゴライン数:", QLabel(str(stats["total_bingo_lines"])))
        form.addRow("マーク数:", QLabel(f"{stats['total_marks']} / {stats['total_cells']}"))
        form.addRow("テンプレート数:", QLabel(str(stats["template_count"])))
        if stats["total_cells"] > 0:
            rate = stats["total_marks"] / stats["total_cells"] * 100
            form.addRow("マーク率:", QLabel(f"{rate:.0f}%"))
        if player_rating is not None:
            form.addRow("レーティング:", QLabel(str(player_rating)))
        if player_rank is not None and total_players is not None:
            form.addRow("順位:", QLabel(f"{player_rank} / {total_players} 人中"))
        summary.setLayout(form)
        left.addWidget(summary)

        # メモ欄（左カラム下部）
        memo_group = QGroupBox(f"📝 {player_name} へのメモ")
        memo_layout = QVBoxLayout()
        self.memo_edit = QTextEdit()
        self.memo_edit.setPlaceholderText("このプレイヤーについてメモを残せます…")
        self.memo_edit.setMaximumHeight(100)
        self.memo_edit.setPlainText(db.get_player_memo(player_id))
        memo_layout.addWidget(self.memo_edit)
        save_memo_btn = QPushButton("💾 メモを保存")
        save_memo_btn.clicked.connect(self._save_memo)
        memo_layout.addWidget(save_memo_btn)
        memo_group.setLayout(memo_layout)
        left.addWidget(memo_group)
        left.addStretch()

        # --- 右カラム: マーク済みテキスト + メモ ---
        right = QVBoxLayout()
        sessions_data = db.get_player_marked_texts(player_id)
        if sessions_data:
            tabs = QTabWidget()

            # 「全体」タブ
            all_tab = QWidget()
            all_layout = QVBoxLayout()
            all_texts = []
            for s in sessions_data:
                for t in s["marked_texts"]:
                    all_texts.append(t)
            from collections import Counter
            text_counts = Counter(all_texts)
            if text_counts:
                all_layout.addWidget(QLabel(f"マーク済みテキスト (全{len(all_texts)}件, ユニーク{len(text_counts)}件):"))
                all_list = QListWidget()
                for text, count in text_counts.most_common():
                    if count > 1:
                        all_list.addItem(f"✓ {text}  (×{count})")
                    else:
                        all_list.addItem(f"✓ {text}")
                all_layout.addWidget(all_list)
            else:
                all_layout.addWidget(QLabel("マーク済みテキストなし"))
            all_tab.setLayout(all_layout)
            tabs.addTab(all_tab, f"📊 全体 ({len(all_texts)})")

            # セッション別タブ
            for s in sessions_data:
                tab = QWidget()
                tab_layout = QVBoxLayout()

                status_text = "✅ 達成" if s["status"] == "completed" else "🔄 進行中"
                bingo_info = f"🎯 ライン: {s['bingo_lines']}" if s["bingo_lines"] > 0 else ""
                info = f"{status_text}  |  マーク: {s['marked_count']}/{s['total_cells']}  {bingo_info}  |  開始: {s['started_at']}"
                tab_layout.addWidget(QLabel(info))

                if s["marked_texts"]:
                    text_list = QListWidget()
                    for t in s["marked_texts"]:
                        text_list.addItem(f"✓ {t}")
                    tab_layout.addWidget(QLabel("マーク済みテキスト:"))
                    tab_layout.addWidget(text_list)
                else:
                    tab_layout.addWidget(QLabel("マーク済みテキストなし"))

                tab.setLayout(tab_layout)
                tab_label = f"{s['title']} ({s['size']}x{s['size']})"
                tabs.addTab(tab, tab_label)
            right.addWidget(tabs)
        else:
            right.addWidget(QLabel("セッション履歴がありません"))

        columns.addLayout(left, 2)
        columns.addLayout(right, 3)
        outer_layout.addLayout(columns)

        close_btn = QPushButton("閉じる")
        close_btn.clicked.connect(self.accept)
        outer_layout.addWidget(close_btn)
        self.setLayout(outer_layout)

    def _save_memo(self):
        memo_text = self.memo_edit.toPlainText()
        db.save_player_memo(self.player_id, memo_text)
        QMessageBox.information(self, "保存完了", f"{self.player_name} のメモを保存しました ✏️")

    @staticmethod
    def _generate_comment(stats, rank=None, total_players=None, rating=None):
        sessions = stats["total_sessions"]
        lines = stats["total_bingo_lines"]
        marks = stats["total_marks"]
        cells = stats["total_cells"]
        rate = (marks / cells * 100) if cells > 0 else 0

        comments = []

        # セッション数による分析
        if sessions == 0:
            return "🌱 まだビンゴを遊んだことがないみたい。最初の一歩を踏み出してみよう！きっと楽しいよ！"
        elif sessions == 1:
            comments.append("👶 はじめてのビンゴに挑戦したばかりみたい！ここからどんどんハマっていくかも？")
        elif sessions <= 5:
            comments.append("🔰 何回か遊んでビンゴに慣れてきた頃だよ！そろそろコツが掴めてきたかも？")
        elif sessions <= 15:
            comments.append("🎮 けっこう遊んでるね〜、なかなかのビンゴ好きみたい！この調子で続けていこうだよ！")
        else:
            comments.append("🏆 ビンゴをやり込みまくってるね！もはやビンゴマスターの称号がふさわしいかも？")

        # マーク率による分析
        if rate >= 80:
            comments.append("🔥 マーク率がめちゃくちゃ高い！かなり積極的にチャレンジしてるみたい！このペースは本当にすごいよ！")
        elif rate >= 50:
            comments.append("✨ 半分以上のマスをマークしてるね！なかなかいい調子だよ！もうちょっとで完全制覇かも？")
        elif rate >= 20:
            comments.append("💪 マーク率はまだこれからって感じだけど、伸びしろたっぷりだよ！どんどん塗りつぶしていこう！")
        elif cells > 0:
            comments.append("🌟 マーク率はまだ低めだけど、コツコツ進めていけばきっと伸びるよ！焦らずいこうだよ！")

        # ビンゴライン数による分析
        if lines == 0 and sessions > 0:
            comments.append("🎯 ビンゴラインはまだ1本も揃ってないみたい…でも大丈夫、次こそきっと揃うかも？")
        elif lines >= 10:
            comments.append("🎊 ビンゴライン10本突破してる！これはかなりのやり手だよ！みんなが尊敬する存在かも？")
        elif lines >= 5:
            comments.append("🎉 ビンゴラインが着実に増えてきてるね！この調子でどんどん揃えていけそうだよ！")
        elif lines >= 1:
            comments.append("🙌 ビンゴラインを達成してるね！初ビンゴの感動は忘れられないよ！次のラインも狙っていこう！")

        # レート（レーティング）による分析
        if rating is not None:
            if rating >= 1000:
                comments.append("💎 レートが1000を超えてる！もう伝説級のプレイヤーと言っても過言じゃないかも？")
            elif rating >= 500:
                comments.append("🔥 レートが500を超えてるね！周りと比べてもかなりの実力者だよ！")
            elif rating >= 200:
                comments.append("💫 レートが200を超えてきた！着実に力をつけてるのが数字に表れてるみたい！")
            elif rating >= 50:
                comments.append("🌿 レートが50を超えたところだよ！ここからグングン伸びていく時期かも？")
            else:
                comments.append("🌱 レートはまだ低めだけど、ここからが本番だよ！伸びしろは無限大かも？")

        # 順位による分析
        if rank is not None and total_players is not None and total_players > 0:
            if total_players == 1:
                comments.append("👑 今は唯一のプレイヤーだから、堂々の王者だよ！ライバルが来ても負けないようにしよう！")
            elif rank == 1:
                comments.append("🥇 現在なんと第1位！トップの座に君臨してるよ！この地位を守り抜こう！")
            elif rank == 2:
                comments.append("🥈 現在第2位！あと一歩で王座に届くところまで来てるみたい！逆転も十分ありえるかも？")
            elif rank == 3:
                comments.append("🥉 現在第3位で表彰台に立ってるね！ここからさらに上を目指していけるかも？")
            elif rank <= total_players * 0.3:
                comments.append(f"💪 現在第{rank}位！上位グループに入ってるよ！トップ争いに食い込めるポジションかも？")
            elif rank <= total_players * 0.6:
                comments.append(f"🎯 現在第{rank}位の中堅どころだよ！ここからじわじわ上を目指していけるかも？")
            else:
                comments.append(f"🚀 現在第{rank}位だけど、まだまだこれからだよ！一気に追い上げるチャンスはあるかも？")

        return "\n".join(comments)


# -----------------------
# セッション管理ダイアログ（データ修正用）
# -----------------------
class SessionManagerDialog(QDialog):
    def __init__(self, player_id, player_name, parent=None):
        super().__init__(parent)
        self.player_id = player_id
        self.setWindowTitle(f"📝 {player_name} のセッション管理")
        self.setMinimumWidth(650)
        self.setMinimumHeight(400)

        layout = QVBoxLayout()
        layout.addWidget(QLabel("セッションを選択して操作:"))

        self.table = QTableWidget()
        self.table.setColumnCount(4)
        self.table.setHorizontalHeaderLabels(["テンプレート", "状態", "開始日時", "終了日時"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.SingleSelection)
        layout.addWidget(self.table)

        self.sessions = db.get_player_sessions(player_id)
        self._refresh_table()

        btn_layout = QHBoxLayout()

        revert_btn = QPushButton("🔄 達成を取消")
        revert_btn.setToolTip("completed → active に戻す")
        revert_btn.clicked.connect(self.revert_session)
        btn_layout.addWidget(revert_btn)

        delete_btn = QPushButton("🗑 セッション削除")
        delete_btn.clicked.connect(self.delete_session)
        btn_layout.addWidget(delete_btn)

        close_btn = QPushButton("閉じる")
        close_btn.clicked.connect(self.accept)
        btn_layout.addWidget(close_btn)

        layout.addLayout(btn_layout)
        self.setLayout(layout)

    def _refresh_table(self):
        self.sessions = db.get_player_sessions(self.player_id)
        self.table.setRowCount(len(self.sessions))
        for idx, s in enumerate(self.sessions):
            self.table.setItem(idx, 0, QTableWidgetItem(s["title"]))
            status = "✅ 達成" if s["status"] == "completed" else "🔄 進行中"
            self.table.setItem(idx, 1, QTableWidgetItem(status))
            self.table.setItem(idx, 2, QTableWidgetItem(s["started_at"] or ""))
            self.table.setItem(idx, 3, QTableWidgetItem(s["finished_at"] or ""))

    def _selected_session(self):
        row = self.table.currentRow()
        if row < 0 or row >= len(self.sessions):
            QMessageBox.warning(self, "警告", "セッションを選択してください")
            return None
        return self.sessions[row]

    def revert_session(self):
        s = self._selected_session()
        if not s:
            return
        if s["status"] != "completed":
            QMessageBox.information(self, "情報", "このセッションはまだ達成していません")
            return
        reply = QMessageBox.question(
            self, "確認", f"「{s['title']}」の達成状態を取消しますか？",
            QMessageBox.Yes | QMessageBox.No
        )
        if reply == QMessageBox.Yes:
            db.revert_session_status(s["session_id"])
            self._refresh_table()

    def delete_session(self):
        s = self._selected_session()
        if not s:
            return
        reply = QMessageBox.question(
            self, "確認", f"「{s['title']}」のセッションを削除しますか？\nこの操作は取消できません。",
            QMessageBox.Yes | QMessageBox.No
        )
        if reply == QMessageBox.Yes:
            db.delete_session(s["session_id"])
            self._refresh_table()


# -----------------------
# カードUI
# -----------------------
class BingoCard(QWidget):
    def __init__(self, board, marked, style, session_id=None):
        super().__init__()
        self.board = board
        self.marked = marked
        self.style = style
        self.session_id = session_id
        self.previous_bingo_lines = set()

        self.layout = QGridLayout()
        self.layout.setSpacing(10)
        self.layout.setContentsMargins(20, 20, 20, 20)

        self.buttons = []
        self.build()
        self.setLayout(self.layout)

    def build(self):
        size = len(self.board)
        for i in range(size):
            row = []
            for j in range(size):
                btn = QPushButton()
                btn.clicked.connect(lambda _, x=i, y=j: self.toggle(x, y))
                self.layout.addWidget(btn, i, j)
                row.append(btn)
            self.buttons.append(row)

        self.update_ui()
        self.previous_bingo_lines = self.check_bingo()

    def toggle(self, i, j):
        self.marked[i][j] = not self.marked[i][j]
        self.update_ui()

        # セッション結果を更新
        if self.session_id:
            current_bingo_lines = self.check_bingo()
            marked_cells = [
                [i2, j2]
                for i2 in range(len(self.marked))
                for j2 in range(len(self.marked[i2]))
                if self.marked[i2][j2]
            ]
            bingo_list = [list(line) for line in current_bingo_lines]
            db.update_session_result(self.session_id, bingo_list, marked_cells)

        # ビンゴラインをチェック
        current_bingo_lines = self.check_bingo()
        new_bingo_lines = current_bingo_lines - self.previous_bingo_lines

        if new_bingo_lines:
            self.show_bingo_effect(new_bingo_lines)

        self.previous_bingo_lines = current_bingo_lines

    def check_bingo(self):
        size = len(self.board)
        bingo_lines = set()

        for i in range(size):
            if all(self.marked[i][j] for j in range(size)):
                bingo_lines.add(('row', i))

        for j in range(size):
            if all(self.marked[i][j] for i in range(size)):
                bingo_lines.add(('col', j))

        if all(self.marked[i][i] for i in range(size)):
            bingo_lines.add(('diag', 0))

        if all(self.marked[i][size - 1 - i] for i in range(size)):
            bingo_lines.add(('diag', 1))

        return bingo_lines

    def show_bingo_effect(self, bingo_lines):
        from datetime import datetime
        size = len(self.board)
        theme = self.style["themes"][self.style["theme"]]

        if self.session_id:
            db.complete_session(self.session_id)

        bingo_buttons = set()
        for line_type, line_idx in bingo_lines:
            if line_type == 'row':
                for j in range(size):
                    bingo_buttons.add((line_idx, j))
            elif line_type == 'col':
                for i in range(size):
                    bingo_buttons.add((i, line_idx))
            elif line_type == 'diag' and line_idx == 0:
                for i in range(size):
                    bingo_buttons.add((i, i))
            elif line_type == 'diag' and line_idx == 1:
                for i in range(size):
                    bingo_buttons.add((i, size - 1 - i))

        def flash_buttons(flash_count=0):
            if flash_count < 6:
                color = "#FFD700" if flash_count % 2 == 0 else theme["marked_bg"]
                for i, j in bingo_buttons:
                    btn = self.buttons[i][j]
                    btn.setStyleSheet(btn.styleSheet().split("animation")[0] + f"""
                        background-color: {color} !important;
                    """)
                QTimer.singleShot(150, lambda: flash_buttons(flash_count + 1))
            else:
                self.update_ui()
                self.show_congratulations(len(bingo_lines))

        flash_buttons()

    def update_ui(self):
        theme = self.style["themes"][self.style["theme"]]
        size = len(self.board)

        if size == 3:
            scale = 1.0
        elif size == 5:
            scale = 0.85
        else:
            scale = 0.7

        base_font_size = max(6, int(self.style["font_size"] * scale))

        for i in range(size):
            for j in range(size):
                btn = self.buttons[i][j]
                text = str(self.board[i][j])
                wrapped_text = self._wrap_text(text)
                line_count = len(wrapped_text.split('\n'))

                text_length = len(text)
                if text_length > 40 or line_count > 5:
                    font_size = max(4, base_font_size - 8)
                elif text_length > 32 or line_count > 4:
                    font_size = max(5, base_font_size - 6)
                elif text_length > 24 or line_count > 3:
                    font_size = max(6, base_font_size - 4)
                elif text_length > 16 or line_count > 2:
                    font_size = max(6, base_font_size - 3)
                elif text_length > 12:
                    font_size = max(7, base_font_size - 2)
                elif text_length > 8:
                    font_size = max(8, base_font_size - 1)
                else:
                    font_size = base_font_size

                btn.setText(wrapped_text)

                # 伸縮可能に（ウィンドウサイズに追従）
                btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
                btn.setMinimumHeight(60)
                btn.setMinimumWidth(60)

                font = btn.font()
                font.setPointSize(font_size)
                btn.setFont(font)

                bg = theme["marked_bg"] if self.marked[i][j] else theme["cell_bg"]
                btn.setStyleSheet(f"""
                    QPushButton {{
                        background-color: {bg};
                        color: {theme["text"]};
                        border-radius: 8px;
                        border: 2px solid {theme["border"]};
                        font-weight: bold;
                        font-size: {font_size}pt !important;
                        padding: 8px;
                        text-align: center;
                    }}
                    QPushButton:hover {{
                        border: 2px solid #888;
                    }}
                    QPushButton:pressed {{
                        background-color: {theme["marked_bg"]};
                    }}
                """)

    def _wrap_text(self, text):
        if not text:
            return text

        lines = text.split('\n')
        result_lines = []
        max_width = 4 if len(text) > 20 else 5 if len(text) > 15 else 6

        for line in lines:
            if len(line) <= max_width:
                result_lines.append(line)
            else:
                words = line.split()
                if not words:
                    current_line = ""
                    for char in line:
                        current_line += char
                        if len(current_line) >= max_width:
                            result_lines.append(current_line)
                            current_line = ""
                    if current_line:
                        result_lines.append(current_line)
                else:
                    current_line = ""
                    for word in words:
                        if len(current_line) + len(word) + 1 <= max_width:
                            if current_line:
                                current_line += " " + word
                            else:
                                current_line = word
                        else:
                            if current_line:
                                result_lines.append(current_line)
                            current_line = word
                    if current_line:
                        result_lines.append(current_line)

        return "\n".join(result_lines)

    def show_congratulations(self, bingo_count=1):
        message_text = "おめでとうございます！\n"
        if bingo_count == 1:
            message_text += "ビンゴが揃いました！🎉"
        elif bingo_count == 2:
            message_text += "ダブルビンゴです！！🎊"
        elif bingo_count == 3:
            message_text += "トリプルビンゴ！！！🎆"
        elif bingo_count == 4:
            message_text += "4ラインビンゴ！完璧です！！！🎇"
        else:
            message_text += f"{bingo_count}ラインビンゴ達成！🌟"

        msg = QMessageBox()
        msg.setWindowTitle("🎉 ビンゴ完成！")
        msg.setText(message_text)
        msg.setIcon(QMessageBox.Information)
        msg.setStyleSheet("""
            QMessageBox { background-color: #2d2d2d; }
            QMessageBox QLabel { color: #ffffff; font-size: 14px; }
            QPushButton { background-color: #4CAF50; color: white; border-radius: 5px; padding: 8px 20px; min-width: 80px; font-weight: bold; }
            QPushButton:hover { background-color: #45a049; }
        """)
        msg.exec()


# -----------------------
# メインウィンドウ
# -----------------------
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.config_path = "config.json"
        self.style = load_json(self.config_path, {
            "theme": "dark",
            "font_size": 28,
            "cell_size": 120,
            "window_size": [1200, 700],
            "themes": {
                "light": {
                    "bg": "#f5f5f5",
                    "cell_bg": "#f0f0f0",
                    "marked_bg": "#4CAF50",
                    "text": "#000000",
                    "border": "#cccccc"
                },
                "dark": {
                    "bg": "#1a1a1a",
                    "cell_bg": "#2d2d2d",
                    "marked_bg": "#66BB6A",
                    "text": "#ffffff",
                    "border": "#444"
                }
            }
        })

        self.player_id = None
        self.player_name = None
        self.template_id = None
        self.session_id = None
        self.data = None
        self.widget = None

        # プレイヤー選択
        if not self._select_player():
            return

        # テンプレート選択（なければ新規作成）
        if not self._select_or_create_template():
            return

        # セッション開始＆カード表示
        self._start_session()
        self.init_menu()
        self.setWindowTitle(f"🎉 ビンゴ - {self.player_name}")
        self.update_window_style()

        if "window_size" in self.style:
            w, h = self.style["window_size"]
            self.resize(w, h)
        else:
            self.resize(1200, 700)

    # -----------------------
    # 起動時の選択フロー
    # -----------------------
    def _select_player(self):
        dlg = PlayerSelectDialog(self)
        if dlg.exec():
            self.player_id = dlg.selected_player_id
            self.player_name = db.get_player_name(self.player_id) or "不明"
            return True
        return False

    def _select_or_create_template(self):
        templates = db.get_all_templates()
        if not templates:
            # テンプレートがないので新規作成
            return self._create_new_template()

        dlg = TemplateSelectDialog(self)
        if dlg.exec():
            if dlg.want_new:
                return self._create_new_template()
            if dlg.selected_template_id:
                self.template_id = dlg.selected_template_id
                return True
        return False

    def _create_new_template(self):
        sizes = ["3x3", "5x5"]
        choice, ok = QInputDialog.getItem(
            self, "テンプレート作成", "ビンゴのサイズ:", sizes, 1, False
        )
        if not ok:
            return False
        size = int(choice[0])

        title, ok = QInputDialog.getText(self, "テンプレート作成", "タイトル:")
        if not ok or not title.strip():
            return False

        dlg = TextEditDialog(size, title, self)
        if dlg.exec():
            board = dlg.get_board_data()
            tid = db.create_template(size, title.strip(), board)
            if tid:
                self.template_id = tid
                return True
            QMessageBox.critical(self, "エラー", "テンプレートの作成に失敗しました")
        return False

    def _start_session(self):
        """アクティブセッションがあれば復元、なければ新規開始"""
        template_data = db.get_template_data(self.template_id)
        if not template_data:
            QMessageBox.critical(self, "エラー", "テンプレートデータが見つかりません")
            return
        size = template_data["size"]
        board = template_data["board"]

        # 既存のアクティブセッションを探す
        active = db.get_active_session(self.player_id, self.template_id)
        if active:
            self.session_id = active["session_id"]
            # 保存されたボードレイアウトがあれば復元（シャッフル済みの場合）
            if active["board_layout"]:
                board = active["board_layout"]
            # マーク状態を復元
            marked = [[False] * size for _ in range(size)]
            for cell in active["marked_cells"]:
                r, c = cell[0], cell[1]
                if 0 <= r < size and 0 <= c < size:
                    marked[r][c] = True
        else:
            # 新規セッション
            self.session_id = db.create_session(self.player_id, self.template_id, board)
            marked = [[False] * size for _ in range(size)]

        self.data = {
            "size": size,
            "board": board,
            "marked": marked
        }

        # 旧ウィジェットを安全に破棄
        if self.widget:
            self.widget.deleteLater()
            self.widget = None

        self.widget = BingoCard(
            self.data["board"],
            self.data["marked"],
            self.style,
            self.session_id
        )
        self.setCentralWidget(self.widget)

    def _start_new_session(self):
        """強制的に新規セッションを開始（テンプレート編集後等）"""
        template_data = db.get_template_data(self.template_id)
        if not template_data:
            QMessageBox.critical(self, "エラー", "テンプレートデータが見つかりません")
            return
        size = template_data["size"]
        board = template_data["board"]
        self.session_id = db.create_session(self.player_id, self.template_id, board)
        marked = [[False] * size for _ in range(size)]

        self.data = {
            "size": size,
            "board": board,
            "marked": marked
        }

        if self.widget:
            self.widget.deleteLater()
            self.widget = None

        self.widget = BingoCard(
            self.data["board"],
            self.data["marked"],
            self.style,
            self.session_id
        )
        self.setCentralWidget(self.widget)

    # -----------------------
    # メニュー（プレイヤー / テンプレート のみ）
    # -----------------------
    def init_menu(self):
        menu = self.menuBar()

        # --- プレイヤーメニュー ---
        player_menu = menu.addMenu("👤 プレイヤー")
        player_menu.addAction("プレイヤー変更", self.change_player)
        player_menu.addAction("プレイヤー名変更", self.rename_player)
        player_menu.addAction("プレイヤー削除", self.delete_player)

        # --- テンプレートメニュー ---
        template_menu = menu.addMenu("📋 テンプレート")
        template_menu.addAction("テンプレート選択", self.change_template)
        template_menu.addAction("新規テンプレート", self.new_template)
        template_menu.addAction("テンプレート編集", self.edit_template)
        template_menu.addAction("テンプレート削除", self.delete_template)
        template_menu.addSeparator()
        template_menu.addAction("シャッフル", self.shuffle_board)
        template_menu.addAction("マークリセット", self.reset_marks)

        # --- データメニュー ---
        data_menu = menu.addMenu("📊 データ")
        data_menu.addAction("🏆 ランキング", self.show_ranking)
        data_menu.addAction("📊 プレイヤー統計", self.show_player_stats)
        data_menu.addAction("📝 セッション管理", self.show_session_manager)

        # --- 設定メニュー ---
        settings_menu = menu.addMenu("⚙ 設定")
        settings_menu.addAction("フォントサイズ", self.change_font_size)
        settings_menu.addAction("テーマ切り替え", self.toggle_theme)

    # -----------------------
    # プレイヤー操作
    # -----------------------
    def change_player(self):
        dlg = PlayerSelectDialog(self)
        if dlg.exec():
            self.player_id = dlg.selected_player_id
            self.player_name = db.get_player_name(self.player_id) or "不明"
            self.setWindowTitle(f"🎉 ビンゴ - {self.player_name}")
            # テンプレートは共通なのでそのまま、セッションだけ新規作成
            self._start_session()

    def rename_player(self):
        players = db.get_all_players()
        names = [name for _, name in players]
        choice, ok = QInputDialog.getItem(
            self, "プレイヤー名変更", "変更するプレイヤー:", names, 0, False
        )
        if not ok:
            return
        pid = next(pid for pid, name in players if name == choice)

        new_name, ok = QInputDialog.getText(
            self, "プレイヤー名変更", f"新しい名前 (現在: {choice}):"
        )
        if ok and new_name.strip():
            if db.update_player_name(pid, new_name.strip()):
                # 現在のプレイヤーなら表示も更新
                if pid == self.player_id:
                    self.player_name = new_name.strip()
                    self.setWindowTitle(f"🎉 ビンゴ - {self.player_name}")
                QMessageBox.information(self, "成功", f"名前を「{new_name.strip()}」に変更しました")
            else:
                QMessageBox.warning(self, "エラー", "その名前は既に使用されています")

    def delete_player(self):
        players = db.get_all_players()
        if len(players) <= 1:
            QMessageBox.warning(self, "警告", "最後のプレイヤーは削除できません")
            return

        names = [name for _, name in players]
        choice, ok = QInputDialog.getItem(
            self, "プレイヤー削除", "削除するプレイヤー:", names, 0, False
        )
        if not ok:
            return
        pid = next(pid for pid, name in players if name == choice)

        reply = QMessageBox.question(
            self, "確認",
            f"プレイヤー「{choice}」を削除しますか？\n関連するセッションデータが削除されます。",
            QMessageBox.Yes | QMessageBox.No
        )
        if reply == QMessageBox.Yes:
            db.delete_player(pid)
            # 削除したのが現在のプレイヤーなら強制再選択
            if pid == self.player_id:
                self.player_id = None
                self.session_id = None
                while not self._select_player():
                    QMessageBox.warning(self, "警告", "プレイヤーを選択してください")
                self._start_session()
                self.setWindowTitle(f"🎉 ビンゴ - {self.player_name}")

    # -----------------------
    # テンプレート操作
    # -----------------------
    def change_template(self):
        if self._select_or_create_template():
            self._start_session()

    def new_template(self):
        if self._create_new_template():
            self._start_session()

    def edit_template(self):
        templates = db.get_all_templates()
        if not templates:
            QMessageBox.information(self, "情報", "テンプレートがありません")
            return

        names = [f"{title} ({size}x{size})" for _, title, size, _ in templates]
        choice, ok = QInputDialog.getItem(
            self, "テンプレート編集", "編集するテンプレート:", names, 0, False
        )
        if not ok:
            return
        idx = names.index(choice)
        tid = templates[idx][0]

        template_data = db.get_template_data(tid)
        if not template_data:
            QMessageBox.critical(self, "エラー", "テンプレートデータの読み込みに失敗しました")
            return

        new_title, ok = QInputDialog.getText(
            self, "テンプレート編集", "タイトル:", text=template_data["title"]
        )
        if not ok or not new_title.strip():
            return

        dlg = TextEditDialog(template_data["size"], new_title, self)
        dlg.set_board_data(template_data["board"])
        if dlg.exec():
            new_board = dlg.get_board_data()
            if db.update_template(tid, new_title.strip(), new_board):
                QMessageBox.information(self, "成功", "テンプレートを更新しました")
                # 編集したのが現在のテンプレートならテキストだけ差し替え（マーク保持）
                if tid == self.template_id:
                    self.data["board"] = new_board
                    # セッションのboard_layoutも更新して統計に反映
                    if self.session_id:
                        db.update_session_board(self.session_id, new_board)
                    self.rebuild_card()
            else:
                QMessageBox.critical(self, "エラー", "テンプレートの更新に失敗しました")

    def delete_template(self):
        templates = db.get_all_templates()
        if not templates:
            QMessageBox.information(self, "情報", "テンプレートがありません")
            return

        names = [f"{title} ({size}x{size})" for _, title, size, _ in templates]
        choice, ok = QInputDialog.getItem(
            self, "テンプレート削除", "削除するテンプレート:", names, 0, False
        )
        if not ok:
            return
        idx = names.index(choice)
        tid = templates[idx][0]
        title = templates[idx][1]

        reply = QMessageBox.question(
            self, "確認",
            f"テンプレート「{title}」を削除しますか？",
            QMessageBox.Yes | QMessageBox.No
        )
        if reply == QMessageBox.Yes:
            db.delete_template(tid)
            # 削除したのが現在のテンプレートなら強制再選択
            if tid == self.template_id:
                self.template_id = None
                self.session_id = None
                while not self._select_or_create_template():
                    QMessageBox.warning(self, "警告", "テンプレートを選択してください")
                self._start_session()

    def shuffle_board(self):
        reply = QMessageBox.question(
            self, "確認",
            "カードをシャッフルしますか？\nマークもすべてリセットされます。",
            QMessageBox.Yes | QMessageBox.No
        )
        if reply != QMessageBox.Yes:
            return
        flat = [c for r in self.data["board"] for c in r]
        random.shuffle(flat)
        size = self.data["size"]
        for i in range(size):
            for j in range(size):
                self.data["board"][i][j] = flat[i * size + j]
        self.data["marked"] = [[False] * size for _ in range(size)]
        # シャッフル後のボード配置とマークリセットをDBに保存
        if self.session_id:
            db.update_session_board(self.session_id, self.data["board"])
            db.update_session_result(self.session_id, [], [])
        self.rebuild_card()

    def reset_marks(self):
        reply = QMessageBox.question(
            self, "確認",
            "すべてのマークをリセットしますか？",
            QMessageBox.Yes | QMessageBox.No
        )
        if reply != QMessageBox.Yes:
            return
        size = self.data["size"]
        self.data["marked"] = [[False] * size for _ in range(size)]
        # マークリセットをDBに保存
        if self.session_id:
            db.update_session_result(self.session_id, [], [])
        self.rebuild_card()

    # -----------------------
    # データ閲覧
    # -----------------------
    def show_ranking(self):
        dlg = RankingDialog(self)
        dlg.exec()

    def show_player_stats(self):
        players = db.get_all_players()
        if not players:
            return
        names = [name for _, name in players]
        # 現在のプレイヤーをデフォルト選択
        default_idx = next((i for i, (pid, _) in enumerate(players) if pid == self.player_id), 0)
        choice, ok = QInputDialog.getItem(
            self, "プレイヤー統計", "統計を表示するプレイヤー:", names, default_idx, False
        )
        if ok:
            pid = next(pid for pid, name in players if name == choice)
            dlg = PlayerStatsDialog(pid, choice, self)
            dlg.exec()

    def show_session_manager(self):
        players = db.get_all_players()
        if not players:
            return
        names = [name for _, name in players]
        default_idx = next((i for i, (pid, _) in enumerate(players) if pid == self.player_id), 0)
        choice, ok = QInputDialog.getItem(
            self, "セッション管理", "管理するプレイヤー:", names, default_idx, False
        )
        if ok:
            pid = next(pid for pid, name in players if name == choice)
            dlg = SessionManagerDialog(pid, choice, self)
            dlg.exec()
            # ダイアログ後、現在のセッションがまだ有効か確認して復元
            if self.session_id:
                session_data = db.get_session_data(self.session_id)
                if not session_data:
                    # 現在のセッションが削除されていたら再読込
                    self._start_session()

    # -----------------------
    # 設定
    # -----------------------
    def change_font_size(self):
        v, ok = QInputDialog.getInt(self, "フォントサイズ", "サイズ:", self.style["font_size"], 8, 100)
        if ok:
            self.style["font_size"] = v
            save_json(self.config_path, self.style)
            self.rebuild_card()

    def toggle_theme(self):
        self.style["theme"] = "light" if self.style["theme"] == "dark" else "dark"
        save_json(self.config_path, self.style)
        self.update_window_style()
        self.rebuild_card()

    def update_window_style(self):
        theme = self.style["themes"][self.style["theme"]]
        self.setStyleSheet(f"""
            QMainWindow {{
                background-color: {theme["bg"]};
            }}
            QMenuBar {{
                background-color: {theme["cell_bg"]};
                color: {theme["text"]};
                border-bottom: 1px solid {theme["border"]};
            }}
            QMenuBar::item:selected {{
                background-color: #4CAF50;
            }}
            QMenu {{
                background-color: {theme["cell_bg"]};
                color: {theme["text"]};
            }}
            QMenu::item:selected {{
                background-color: #4CAF50;
            }}
        """)

    # -----------------------
    # カード再構築（マーク状態を保持）
    # -----------------------
    def rebuild_card(self):
        """カード再構築（マーク状態・セッションを保持、設定変更時用）"""
        if self.widget:
            self.widget.deleteLater()
            self.widget = None

        self.widget = BingoCard(
            self.data["board"],
            self.data["marked"],
            self.style,
            self.session_id
        )
        self.setCentralWidget(self.widget)

    def closeEvent(self, event):
        self.style["window_size"] = [self.width(), self.height()]
        save_json(self.config_path, self.style)
        event.accept()


# -----------------------
# 起動
# -----------------------
if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())
