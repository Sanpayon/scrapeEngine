#!/usr/bin/env python3
"""
搜索 arXiv 上的论文，
并将结果保存为 JSON 文件，字段包括：
    _id, paper_url, paper_abstract, paper_authors, paper_name, paper_year, citation
"""

import arxiv
import json

def crawl_arxiv(max_results=4000, sort_by=arxiv.SortCriterion.Relevance, conference="NeurIPS", year="2026", save_json=False):
    """
    执行搜索并保存结果。
    :param max_results: 最大返回结果数
    :param sort_by: 排序方式
    :param conference: 顶会名称
    :param year: 顶会年度
    """
    query = "{}{} OR {} {}".format(conference, year, conference, year)
    print(f"搜索查询: {query}\n")

    client = arxiv.Client()
    search = arxiv.Search(
        query=query,
        max_results=max_results,
        sort_by=sort_by,
        sort_order=arxiv.SortOrder.Descending
    )

    print(f"正在搜索 arXiv... (最多 {max_results} 条结果)\n")
    results = list(client.results(search))

    output_file = "{}{}_papers_arxiv.json".format(conference, year)

    if not results:
        print("未找到符合条件的论文。")
        return

    papers = []
    for paper in results:
        # 构造每条记录的字典
        record = {
            "_id": "",                                 # 留空
            "paper_url": paper.entry_id,               # arXiv 页面 URL
            "paper_abstract": paper.summary.strip(),   # 去除首尾空白
            "paper_authors": ", ".join([a.name for a in paper.authors]),
            "paper_name": paper.title.strip(),
            "paper_year": str(paper.published.year),
            "citation": "",                             # 留空，可手动补充
            "conference": conference
        }
        papers.append(record)

    if save_json:
        _save_to_json(papers, output_file)

    # 保存到数据库
    _save_arxiv_to_dataset(papers, conference, year)

def _save_to_json(data, filename):
    """将数据保存为JSON文件"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    print(f"\n{'='*50}")
    print(f"爬取完成！共获取 {len(data)} 篇论文")
    print(f"所有数据已保存到 {filename}")
    print(f"{'='*50}")

def _save_arxiv_to_dataset(papers, summit, year):
    """将 arXiv 搜索结果保存到 SQLite 数据库"""
    try:
        from database import insert_papers
        for p in papers:
            p["conference"] = summit
            p["paper_year"] = str(year)
        inserted = insert_papers(papers)
        print(f"数据库保存完成！新插入 {inserted} 篇论文（共 {len(papers)} 篇）")
    except Exception as e:
        print(f"数据库保存失败: {e}")
