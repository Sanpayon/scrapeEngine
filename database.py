import sqlite3
import os
import hashlib

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "papers.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS papers (
    _id TEXT PRIMARY KEY,
    paper_url TEXT UNIQUE,
    paper_abstract TEXT,
    paper_authors TEXT,
    paper_name TEXT,
    paper_year TEXT,
    citation TEXT,
    conference TEXT
);
"""


def _get_connection(db_path=None):
    if db_path is None:
        db_path = DB_PATH
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db(db_path=None):
    conn = _get_connection(db_path)
    conn.executescript(SCHEMA)
    conn.close()


def _compute_id(paper_url, paper_name):
    raw = f"{paper_url}|{paper_name}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def insert_paper(paper: dict, db_path=None) -> bool:
    """插入单篇论文，若 paper_url 已存在则跳过；返回是否为新插入。"""
    conn = _get_connection(db_path)
    try:
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
                (_id, paper_url, paper_abstract, paper_authors, paper_name, paper_year, citation, conference)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
            ),
        )
        conn.commit()
        return True
    finally:
        conn.close()


def insert_papers(papers: list[dict], db_path=None) -> int:
    """批量插入论文列表，返回新插入的条数。"""
    count = 0
    for p in papers:
        if insert_paper(p, db_path):
            count += 1
    return count


def query_papers(
    conference: str | None = None,
    year: str | None = None,
    keyword: str | None = None,
    db_path=None,
) -> list[dict]:
    """按条件查询论文。"""
    conn = _get_connection(db_path)
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
    conn.close()

    return [dict(r) for r in rows]


def paper_exists(paper_url: str, db_path=None) -> bool:
    """检查某篇论文是否已在数据库中。"""
    conn = _get_connection(db_path)
    row = conn.execute(
        "SELECT 1 FROM papers WHERE paper_url = ?", (paper_url,)
    ).fetchone()
    conn.close()
    return row is not None


def get_stats(db_path=None) -> dict:
    """返回数据库统计信息。"""
    conn = _get_connection(db_path)
    total = conn.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
    by_conf = dict(
        conn.execute(
            "SELECT conference, COUNT(*) as cnt FROM papers GROUP BY conference ORDER BY cnt DESC"
        ).fetchall()
    )
    conn.close()
    return {"total": total, "by_conference": by_conf}


# 初始化数据库（模块导入时自动执行）
init_db()
