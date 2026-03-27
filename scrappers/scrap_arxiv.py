#!/usr/bin/env python3
"""
搜索 arXiv 上的论文，
并将结果保存为 JSON 文件，字段包括：
    _id, paper_url, paper_abstract, paper_authors, paper_name, paper_year, citation
"""

import arxiv
import json

def crawl_arxiv(max_results=4000, sort_by=arxiv.SortCriterion.Relevance, summit="NeurIPS", year="2026"):
    """
    执行搜索并保存结果。
    :param max_results: 最大返回结果数
    :param sort_by: 排序方式
    :param summit: 顶会名称
    :param year: 顶会年度
    """
    query = query = "{}{} OR {} {}".format(summit, year, summit, year)
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

    output_file = "{}{}.json".format(summit, year)

    if not results:
        print("未找到符合条件的论文。")
        # 仍然创建空的 JSON 文件
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump([], f, indent=2, ensure_ascii=False)
        print(f"已创建空的 {output_file}")
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
            "citation": ""                             # 留空，可手动补充
        }
        papers.append(record)

    # 写入 JSON 文件
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(papers, f, indent=2, ensure_ascii=False)

    print(f"成功保存 {len(papers)} 条记录到 {output_file}")
