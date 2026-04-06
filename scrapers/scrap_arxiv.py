#!/usr/bin/env python3
"""
搜索 arXiv 上的论文，
并将结果保存为 JSON 文件，字段包括：
    _id, paper_url, paper_abstract, paper_authors, paper_name, paper_year, citation
"""

import arxiv
import json
import logging

logger = logging.getLogger(__name__)

def crawl_arxiv(max_results=4000, sort_by=arxiv.SortCriterion.Relevance, conference="NeurIPS", year="2026", save_json=False):
    """
    执行搜索并保存结果。
    :param max_results: 最大返回结果数
    :param sort_by: 排序方式
    :param conference: 顶会名称
    :param year: 顶会年度
    """
    query = "{}{} OR {} {}".format(conference, year, conference, year)
    logger.info(f"搜索查询: {query}")

    client = arxiv.Client()
    search = arxiv.Search(
        query=query,
        max_results=max_results,
        sort_by=sort_by,
        sort_order=arxiv.SortOrder.Descending
    )

    logger.info(f"正在搜索 arXiv... (最多 {max_results} 条结果)")
    results = list(client.results(search))

    output_file = "{}{}_papers_arxiv.json".format(conference, year)

    if not results:
        logger.warning("未找到符合条件的论文。")
        return

    papers = []
    for paper in results:
        record = {
            "_id": "",
            "paper_url": paper.entry_id,
            "paper_abstract": paper.summary.strip(),
            "paper_authors": ", ".join([a.name for a in paper.authors]),
            "paper_name": paper.title.strip(),
            "paper_year": str(paper.published.year),
            "citation": "",
            "conference": conference
        }
        papers.append(record)

    if save_json:
        _save_to_json(papers, output_file)

    _save_arxiv_to_dataset(papers, conference, year)

def _save_to_json(data, filename):
    """将数据保存为JSON文件"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        logger.info(f"\n{'='*50}")
        logger.info(f"爬取完成！共获取 {len(data)} 篇论文")
        logger.info(f"所有数据已保存到 {filename}")
        logger.info(f"{'='*50}")
    except Exception as e:
        logger.error(f"JSON保存失败: {e}")

def _save_arxiv_to_dataset(papers, summit, year):
    """将 arXiv 搜索结果保存到 SQLite 数据库"""
    try:
        from database import insert_papers
        for p in papers:
            p["conference"] = summit
            p["paper_year"] = str(year)
        inserted = insert_papers(papers)
        logger.info(f"数据库保存完成！新插入 {inserted} 篇论文（共 {len(papers)} 篇）")
    except Exception as e:
        logger.error(f"数据库保存失败: {e}")
