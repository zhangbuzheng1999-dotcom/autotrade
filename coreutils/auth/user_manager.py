# coreutils/auth/user_manager.py
import sqlite3, bcrypt
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]      # 项目根
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_FILE = DATA_DIR / "users.db"

class UserManager:
    def __init__(self, db_file=str(DB_FILE)):
        self.conn = sqlite3.connect(db_file, check_same_thread=False)
        self.create_table()

    def create_table(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              username TEXT UNIQUE,
              password_hash TEXT
            )
        """)
        self.conn.commit()

    def add_user(self, username: str, password: str) -> bool:
        try:
            hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
            self.conn.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)", (username, hashed))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def verify_user(self, username: str, password: str) -> bool:
        cur = self.conn.cursor()
        cur.execute("SELECT password_hash FROM users WHERE username=?", (username,))
        row = cur.fetchone()
        return bool(row and bcrypt.checkpw(password.encode(), row[0].encode()))
