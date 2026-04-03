import sqlite3
import os
import hashlib
from contextlib import contextmanager

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "papers.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS papers (
    _id TEXT PRIMARY KEY,
    paper_url TEXT,
    paper_abstract TEXT,
    paper_authors TEXT,
    paper_name TEXT,
    paper_year TEXT,
    citation TEXT,
    conference TEXT,
    source TEXT DEFAULT 'conference'
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_url ON papers(paper_url);
CREATE INDEX IF NOT EXISTS idx_paper_name ON papers(paper_name);
CREATE INDEX IF NOT EXISTS idx_conference_year ON papers(conference, paper_year);

CREATE TABLE IF NOT EXISTS conferences (
    name TEXT NOT NULL,
    year TEXT NOT NULL,
    deadline TEXT,
    start_date TEXT,
    end_date TEXT,
    status TEXT DEFAULT 'upcoming',
    last_scrape_attempt TEXT,
    scrape_retry_count INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (name, year)
);
"""


def _get_connection(db_path=None):
    if db_path is None:
        db_path = DB_PATH
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


@contextmanager
def get_db(db_path=None):
    """上下文管理器，自动管理连接生命周期。"""
    conn = _get_connection(db_path)
    try:
        yield conn
    finally:
        conn.close()


def init_db(db_path=None):
    with get_db(db_path) as conn:
        conn.executescript(SCHEMA)


def _compute_id(paper_url, paper_name):
    raw = f"{paper_url}|{paper_name}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def insert_paper(paper: dict, db_path=None) -> bool:
    """插入单篇论文，若 paper_url 已存在则跳过；返回是否为新插入。"""
    with get_db(db_path) as conn:
        existing = conn.execute(
            "SELECT 1 FROM papers WHERE paper_url = ?", (paper["paper_url"],)
        ).fetchone()
        if existing:
            return False

        _id = paper.get("_id", "") or _compute_id(
            paper["paper_url"], paper["paper_name"]
        )
        conn.execute(
            """
            INSERT INTO papers
                (_id, paper_url, paper_abstract, paper_authors, paper_name, paper_year, citation, conference, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _id,
                paper["paper_url"],
                paper.get("paper_abstract", ""),
                paper.get("paper_authors", ""),
                paper["paper_name"],
                paper.get("paper_year", ""),
                paper.get("citation", ""),
                paper.get("conference", ""),
                paper.get("source", "conference"),
            ),
        )
        conn.commit()
        return True


def insert_papers(papers: list[dict], db_path=None) -> int:
    """批量插入论文列表，使用事务，返回新插入的条数。"""
    if not papers:
        return 0

    with get_db(db_path) as conn:
        inserted = 0
        for paper in papers:
            existing = conn.execute(
                "SELECT 1 FROM papers WHERE paper_url = ?", (paper["paper_url"],)
            ).fetchone()
            if existing:
                continue

            _id = paper.get("_id", "") or _compute_id(
                paper["paper_url"], paper["paper_name"]
            )
            conn.execute(
                """
                INSERT INTO papers
                    (_id, paper_url, paper_abstract, paper_authors, paper_name, paper_year, citation, conference, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _id,
                    paper["paper_url"],
                    paper.get("paper_abstract", ""),
                    paper.get("paper_authors", ""),
                    paper["paper_name"],
                    paper.get("paper_year", ""),
                    paper.get("citation", ""),
                    paper.get("conference", ""),
                    paper.get("source", "conference"),
                ),
            )
            inserted += 1

        conn.commit()
        return inserted


def query_papers(
    conference: str | None = None,
    year: str | None = None,
    keyword: str | None = None,
    db_path=None,
) -> list[dict]:
    """按条件查询论文。"""
    with get_db(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conditions = []
        params: list = []

        if conference:
            conditions.append("conference = ?")
            params.append(conference)
        if year:
            conditions.append("paper_year = ?")
            params.append(str(year))
        if keyword:
            conditions.append("(paper_name LIKE ? OR paper_abstract LIKE ?)")
            params.extend([f"%{keyword}%", f"%{keyword}%"])

        where = ""
        if conditions:
            where = "WHERE " + " AND ".join(conditions)

        rows = conn.execute(
            f"SELECT * FROM papers {where} ORDER BY paper_year DESC, paper_name", params
        ).fetchall()
        return [dict(r) for r in rows]


def paper_exists(paper_url: str, db_path=None) -> bool:
    """检查某篇论文是否已在数据库中。"""
    with get_db(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM papers WHERE paper_url = ?", (paper_url,)
        ).fetchone()
        return row is not None


def paper_exists_by_name(paper_name: str, conference: str, year: str, db_path=None) -> bool:
    """按论文名+会议+年份检查是否已在数据库中。"""
    with get_db(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM papers WHERE paper_name = ? AND conference = ? AND paper_year = ?",
            (paper_name, conference, str(year)),
        ).fetchone()
        return row is not None


def get_paper_titles(conference: str, year: str, db_path=None) -> list[str]:
    """获取指定会议和年份的所有论文标题列表。"""
    with get_db(db_path) as conn:
        rows = conn.execute(
            "SELECT paper_name FROM papers WHERE conference = ? AND paper_year = ?",
            (conference, str(year)),
        ).fetchall()
        return [r[0] for r in rows]


def update_paper_abstract(paper_url: str, abstract: str, db_path=None) -> bool:
    """更新已有论文的摘要。"""
    with get_db(db_path) as conn:
        cursor = conn.execute(
            "UPDATE papers SET paper_abstract = ? WHERE paper_url = ?",
            (abstract, paper_url),
        )
        conn.commit()
        return cursor.rowcount > 0


def insert_or_update_conference(conf: dict, db_path=None) -> bool:
    """插入或更新会议信息。"""
    with get_db(db_path) as conn:
        existing = conn.execute(
            "SELECT 1 FROM conferences WHERE name = ? AND year = ?",
            (conf["name"], str(conf["year"])),
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE conferences
                SET deadline = ?, start_date = ?, end_date = ?, status = ?
                WHERE name = ? AND year = ?
                """,
                (
                    conf.get("deadline", ""),
                    conf.get("start_date", ""),
                    conf.get("end_date", ""),
                    conf.get("status", "upcoming"),
                    conf["name"],
                    str(conf["year"]),
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO conferences
                    (name, year, deadline, start_date, end_date, status)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    conf["name"],
                    str(conf["year"]),
                    conf.get("deadline", ""),
                    conf.get("start_date", ""),
                    conf.get("end_date", ""),
                    conf.get("status", "upcoming"),
                ),
            )
        conn.commit()
        return True


def get_conferences(status: str | None = None, db_path=None) -> list[dict]:
    """获取会议列表，可按状态过滤。"""
    with get_db(db_path) as conn:
        conn.row_factory = sqlite3.Row
        if status:
            rows = conn.execute(
                "SELECT * FROM conferences WHERE status = ? ORDER BY start_date", (status,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM conferences ORDER BY start_date"
            ).fetchall()
        return [dict(r) for r in rows]


def update_conference_status(name: str, year: str, status: str, db_path=None):
    """更新会议状态。"""
    with get_db(db_path) as conn:
        conn.execute(
            "UPDATE conferences SET status = ? WHERE name = ? AND year = ?",
            (status, name, str(year)),
        )
        conn.commit()


def update_conference_scrape_info(name: str, year: str, attempt_time: str, retry_count: int, db_path=None):
    """更新会议爬尝试信息。"""
    with get_db(db_path) as conn:
        conn.execute(
            "UPDATE conferences SET last_scrape_attempt = ?, scrape_retry_count = ? WHERE name = ? AND year = ?",
            (attempt_time, retry_count, name, str(year)),
        )
        conn.commit()


def get_unscraped_conferences(db_path=None) -> list[dict]:
    """获取已过期但仍未爬取的会议（用于进程重启后恢复）。"""
    with get_db(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT * FROM conferences
            WHERE status IN ('upcoming', 'ongoing')
              AND date(end_date, '+10 days') < date('now')
            ORDER BY end_date
            """
        ).fetchall()
        return [dict(r) for r in rows]


def get_stats(db_path=None) -> dict:
    """返回数据库统计信息。"""
    with get_db(db_path) as conn:
        total = conn.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
        by_conf = dict(
            conn.execute(
                "SELECT conference, COUNT(*) as cnt FROM papers GROUP BY conference ORDER BY cnt DESC"
            ).fetchall()
        )
        by_source = dict(
            conn.execute(
                "SELECT source, COUNT(*) as cnt FROM papers GROUP BY source"
            ).fetchall()
        )
        return {"total": total, "by_conference": by_conf, "by_source": by_source}


# 初始化数据库（模块导入时自动执行）
init_db()
