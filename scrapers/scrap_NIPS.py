#!/usr/bin/env python3
"""
爬取 NeurIPS (Conference on Neural Information Processing Systems) 论文元数据，
并将结果保存为 JSON 文件，字段包括：
    _id, paper_url, paper_abstract, paper_authors, paper_name, paper_year, citation
"""

from bs4 import BeautifulSoup
from urllib.parse import urljoin

from scrapers.base_scraper import BaseScraper


class NIPSScraper(BaseScraper):
    """NeurIPS 论文元数据爬虫"""

    def __init__(self):
        super().__init__("https://papers.nips.cc/")

    def get_conference_papers(self, conference, year):
        """
        获取特定年份的 NeurIPS 论文列表
        :param year: 年份
        :return: 论文元数据列表
        """
        # NeurIPS 论文列表页面
        conference_url = f"{self.base_url}paper_files/paper/{year}"
        print(f"正在获取 NeurIPS {year} 的论文列表...")

        response = self._make_request(conference_url)
        if not response:
            return []

        # 随机延迟
        self._random_delay()

        # 解析HTML内容
        soup = BeautifulSoup(response.text, 'html.parser')

        papers = []
        # 查找论文链接 - 使用包含 paper title 的 a 标签
        paper_links = soup.find_all('a', {'title': 'paper title'})

        print(f"找到 {len(paper_links)} 篇论文")

        for i, link in enumerate(paper_links):
            try:
                paper_data = self._extract_paper_metadata(link, conference, year)
                if paper_data:
                    papers.append(paper_data)
                # 每处理10篇论文显示一次进度
                if (i + 1) % 10 == 0:
                    print(f"进度: {i+1}/{len(paper_links)}")
            except Exception as e:
                print(f"提取论文元数据时出错: {e}")
                continue

        print(f"完成！成功提取 {len(papers)} 篇论文")
        return papers

    def _extract_paper_metadata(self, title_elem, conference, year):
        """
        从论文链接元素中提取元数据
        :param title_elem: 包含 paper title 的 a 标签
        :param conference: 会议名称
        :param year: 年份
        :return: 论文元数据字典
        """
        # 获取论文标题
        paper_name = title_elem.get_text(strip=True)
        if not paper_name:
            return None

        # 获取论文详情页URL
        paper_url = urljoin(self.base_url, title_elem['href'])

        print(f"正在获取论文详情: {paper_name}")
        
        # 获取论文详情页内容
        abstract, authors = self._get_paper_details(paper_url)

        paper_data = {
            "_id": "",  # 留空
            "paper_url": paper_url,
            "paper_abstract": abstract,
            "paper_authors": authors,
            "paper_name": paper_name,
            "paper_year": str(year),
            "citation": "",  # 留空
            "conference": "NeurIPS"
        }

        # 随机延迟
        self._random_delay()

        return paper_data

    def _get_paper_details(self, paper_url):
        """
        从论文详情页获取摘要和作者信息
        :param paper_url: 论文详情页URL
        :return: (摘要, 作者) 元组
        """
        response = self._make_request(paper_url)
        if not response:
            return "", "Unknown"

        soup = BeautifulSoup(response.text, 'html.parser')

        # 从 meta 标签提取作者信息
        authors = []
        author_metas = soup.find_all('meta', {'name': 'citation_author'})
        for meta in author_metas:
            if 'content' in meta.attrs:
                authors.append(meta['content'])

        authors_str = ", ".join(authors) if authors else "Unknown"

        # 查找摘要内容 - 在 p.paper-abstract 标签中
        abstract = ""
        abstract_elem = soup.find('p', class_='paper-abstract')
        if abstract_elem:
            # 获取所有文本内容，包括嵌套的 p 标签
            abstract = abstract_elem.get_text(separator=' ', strip=True)
        else:
            # 备选方案：查找包含 Abstract 的 section
            abstract_section = soup.find('section', class_='paper-abstract-section')
            if abstract_section:
                abstract = abstract_section.get_text(strip=True)

        return abstract, authors_str
