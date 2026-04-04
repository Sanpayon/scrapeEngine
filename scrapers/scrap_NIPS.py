#!/usr/bin/env python3
"""
爬取 NeurIPS (Conference on Neural Information Processing Systems) 论文元数据，
并将结果保存为 JSON 文件，字段包括：
    _id, paper_url, paper_abstract, paper_authors, paper_name, paper_year, citation
"""

from bs4 import BeautifulSoup
from urllib.parse import urljoin
import logging

from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class NIPSScraper(BaseScraper):
    """NeurIPS 论文元数据爬虫"""

    def __init__(self):
        super().__init__("https://papers.nips.cc/")

    def get_conference_metadata(self, conference, year):
        """
        提取轻量级元数据（标题、作者、URL），不访问详情页，摘要为空
        """
        conference_url = f"{self.base_url}paper_files/paper/{year}"
        logger.info(f"正在获取 NeurIPS {year} 的轻量级元数据...")

        html = self._make_request(conference_url)
        if not html:
            return []

        soup = BeautifulSoup(html, 'html.parser')
        paper_links = soup.find_all('a', {'title': 'paper title'})
        logger.info(f"找到 {len(paper_links)} 篇论文")

        papers = []
        for link in paper_links:
            paper_name = link.get_text(strip=True)
            if not paper_name:
                continue

            paper_url = urljoin(self.base_url, link['href'])

            paper_data = {
                "_id": "",
                "paper_url": paper_url,
                "paper_abstract": "",
                "paper_authors": "",
                "paper_name": paper_name,
                "paper_year": str(year),
                "citation": "",
                "conference": "NeurIPS"
            }
            papers.append(paper_data)

        logger.info(f"完成！提取 {len(papers)} 篇 NeurIPS {year} 轻量级元数据")
        return papers

    def get_conference_papers(self, conference, year):
        """
        获取特定年份的 NeurIPS 论文列表
        :param year: 年份
        :return: 论文元数据列表
        """
        conference_url = f"{self.base_url}paper_files/paper/{year}"
        logger.info(f"正在获取 NeurIPS {year} 的论文列表...")

        response = self._make_request(conference_url)
        if not response:
            return []

        self._random_delay()

        soup = BeautifulSoup(response, 'html.parser')

        papers = []
        paper_links = soup.find_all('a', {'title': 'paper title'})

        logger.info(f"找到 {len(paper_links)} 篇论文")

        for i, link in enumerate(paper_links):
            try:
                paper_data = self._extract_paper_metadata(link, conference, year)
                if paper_data:
                    papers.append(paper_data)
                if (i + 1) % 10 == 0:
                    logger.info(f"进度: {i+1}/{len(paper_links)}")
            except Exception as e:
                logger.error(f"提取论文元数据时出错: {e}")
                continue

        logger.info(f"完成！成功提取 {len(papers)} 篇论文")
        return papers

    def _extract_paper_metadata(self, title_elem, conference, year):
        """
        从论文链接元素中提取元数据
        :param title_elem: 包含 paper title 的 a 标签
        :param conference: 会议名称
        :param year: 年份
        :return: 论文元数据字典
        """
        paper_name = title_elem.get_text(strip=True)
        if not paper_name:
            return None

        paper_url = urljoin(self.base_url, title_elem['href'])

        logger.info(f"正在获取论文详情: {paper_name}")
        
        abstract, authors = self._get_paper_details(paper_url)

        paper_data = {
            "_id": "",
            "paper_url": paper_url,
            "paper_abstract": abstract,
            "paper_authors": authors,
            "paper_name": paper_name,
            "paper_year": str(year),
            "citation": "",
            "conference": "NeurIPS"
        }

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

        soup = BeautifulSoup(response, 'html.parser')

        authors = []
        author_metas = soup.find_all('meta', {'name': 'citation_author'})
        for meta in author_metas:
            if 'content' in meta.attrs:
                authors.append(meta['content'])

        authors_str = ", ".join(authors) if authors else "Unknown"

        abstract = ""
        abstract_elem = soup.find('p', class_='paper-abstract')
        if abstract_elem:
            abstract = abstract_elem.get_text(separator=' ', strip=True)
        else:
            abstract_section = soup.find('section', class_='paper-abstract-section')
            if abstract_section:
                abstract = abstract_section.get_text(strip=True)

        return abstract, authors_str
