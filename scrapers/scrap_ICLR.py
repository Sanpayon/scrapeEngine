#!/usr/bin/env python3
"""
爬取 ICLR (International Conference on Learning Representations) 论文元数据，
并将结果保存为 JSON 文件，字段包括：
    _id, paper_url, paper_abstract, paper_authors, paper_name, paper_year, citation
"""

from bs4 import BeautifulSoup
from urllib.parse import urljoin
import logging

from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class ICLRScraper(BaseScraper):
    """ICLR 论文元数据爬虫"""

    def __init__(self):
        super().__init__("https://iclr.cc")

    def get_conference_papers(self, conference, year):
        """
        获取特定会议和年份的论文列表
        :param conference: 会议名称 (ICLR)
        :param year: 年份
        :return: 论文元数据列表
        """
        conference_url = f"{self.base_url}/virtual/{year}/papers.html"
        logger.info(f"正在获取 ICLR {year} 的论文列表...")

        html = self._make_request(conference_url)
        if not html:
            return []

        self._random_delay()

        soup = BeautifulSoup(html, 'html.parser')

        papers = []
        poster_links = soup.find_all('a', href=lambda x: x and f'/virtual/{year}/poster/' in x)

        poster_links = [l for l in poster_links if '/accounts/login' not in l.get('href', '')]

        logger.info(f"找到 {len(poster_links)} 篇论文")

        for i, link in enumerate(poster_links):
            try:
                paper_url = urljoin(self.base_url, link['href'])
                paper_name = link.get_text(strip=True)
                if not paper_name:
                    continue

                paper_data = self._get_paper_details(paper_url, paper_name, conference, year)
                if paper_data:
                    papers.append(paper_data)
                if (i + 1) % 10 == 0:
                    logger.info(f"进度: {i+1}/{len(poster_links)}")
            except Exception as e:
                logger.error(f"提取论文元数据时出错: {e}")
                continue

        logger.info(f"完成！成功提取 {len(papers)} 篇论文")
        return papers

    def get_conference_metadata(self, conference, year):
        """
        提取轻量级元数据（标题、作者、URL），不访问详情页，摘要为空
        """
        conference_url = f"{self.base_url}/virtual/{year}/papers.html"
        logger.info(f"正在获取 ICLR {year} 的轻量级元数据...")

        html = self._make_request(conference_url)
        if not html:
            return []

        soup = BeautifulSoup(html, 'html.parser')
        poster_links = soup.find_all('a', href=lambda x: x and f'/virtual/{year}/poster/' in x)
        poster_links = [l for l in poster_links if '/accounts/login' not in l.get('href', '')]
        logger.info(f"找到 {len(poster_links)} 篇论文")

        papers = []
        for link in poster_links:
            paper_url = urljoin(self.base_url, link['href'])
            paper_name = link.get_text(strip=True)
            if not paper_name:
                continue

            paper_data = {
                "_id": "",
                "paper_url": paper_url,
                "paper_abstract": "",
                "paper_authors": "",
                "paper_name": paper_name,
                "paper_year": str(year),
                "citation": "",
                "conference": "ICLR"
            }
            papers.append(paper_data)

        logger.info(f"完成！提取 {len(papers)} 篇轻量级元数据")
        return papers

    def _extract_paper_metadata(self, title_elem, conference, year):
        """
        从论文元素中提取元数据（ICLR 不使用此方法，保留接口兼容性）
        """
        return None

    def _get_paper_details(self, paper_url, paper_name, conference, year):
        """
        从论文详情页获取摘要、作者等信息
        :param paper_url: 论文详情页URL
        :param paper_name: 论文标题
        :param conference: 会议名称
        :param year: 年份
        :return: 论文元数据字典
        """
        html = self._make_request(paper_url)
        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')

        paper_authors = "Unknown"
        paper_abstract = ""

        body = soup.find('body')
        if body:
            text = body.get_text()
            title_idx = text.find(paper_name)
            if title_idx >= 0:
                after_title = text[title_idx + len(paper_name):]
                lines = after_title.strip().split('\n')
                for line in lines:
                    line = line.strip()
                    if line and len(line) > 2 and not line.startswith('[') and not line.startswith('20'):
                        if any(c.isalpha() for c in line) and len(line) < 500:
                            paper_authors = line.replace('\u00b7', ',').replace('·', ',')
                            break

        abstract_div = soup.find('div', class_='abstract-content')
        if abstract_div:
            paper_abstract = abstract_div.get_text(strip=True)

        paper_data = {
            "_id": "",
            "paper_url": paper_url,
            "paper_abstract": paper_abstract,
            "paper_authors": paper_authors,
            "paper_name": paper_name,
            "paper_year": str(year),
            "citation": "",
            "conference": "ICLR"
        }

        return paper_data
