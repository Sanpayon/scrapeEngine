#!/usr/bin/env python3
"""
基于 RapidFuzz（C++ 底层）的两阶段论文标题模糊匹配。

阶段1：词袋 Jaccard 预筛选，快速排除明显不匹配的标题
阶段2：RapidFuzz token_sort_ratio 精准匹配
"""

from rapidfuzz import fuzz, process
from rapidfuzz.utils import default_process


def _tokenize(title: str) -> set[str]:
    """将标题转为小写 token 集合，去除标点。"""
    import re
    cleaned = re.sub(r'[^\w\s]', '', title.lower())
    return set(cleaned.replace("-", " ").replace("_", " ").split())


def _jaccard(a: set[str], b: set[str]) -> float:
    """计算 Jaccard 相似度。"""
    if not a or not b:
        return 0.0
    intersection = len(a & b)
    union = len(a | b)
    return intersection / union if union > 0 else 0.0


def find_new_papers(
    new_titles: list[str],
    db_titles: list[str],
    jaccard_threshold: float = 0.3,
    fuzz_threshold: int = 90,
) -> list[str]:
    """
    从新论文标题列表中筛选出数据库中不存在的论文。

    :param new_titles: 待检查的新论文标题列表
    :param db_titles: 数据库中已有论文标题列表
    :param jaccard_threshold: Jaccard 预筛选阈值
    :param fuzz_threshold: RapidFuzz 匹配阈值（0-100）
    :return: 数据库中不存在的新论文标题列表
    """
    if not db_titles:
        return new_titles

    db_token_sets = [(t, _tokenize(t)) for t in db_titles]

    new_papers = []
    for title in new_titles:
        new_tokens = _tokenize(title)

        candidates = []
        for db_title, db_tokens in db_token_sets:
            if _jaccard(new_tokens, db_tokens) >= jaccard_threshold:
                candidates.append(db_title)

        if not candidates:
            new_papers.append(title)
            continue

        result = process.extractOne(
            default_process(title),
            [default_process(t) for t in candidates],
            scorer=fuzz.token_sort_ratio,
            score_cutoff=fuzz_threshold,
            processor=None,
        )

        if result is None:
            new_papers.append(title)

    return new_papers


def match_paper_to_db(
    title: str,
    db_titles: list[str],
    threshold: int = 90,
) -> tuple[str | None, int]:
    """
    将单篇论文标题与数据库标题匹配。

    :return: (匹配的数据库标题, 分数) 或 (None, 0)
    """
    if not db_titles:
        return None, 0

    processed_title = default_process(title)
    processed_db = [default_process(t) for t in db_titles]

    result = process.extractOne(
        processed_title,
        processed_db,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=threshold,
        processor=None,
    )

    if result:
        _, score, idx = result
        return db_titles[idx], score

    return None, 0
