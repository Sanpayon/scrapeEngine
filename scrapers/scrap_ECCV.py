#!/usr/bin/env python3
"""
爬取 ECCV (European Conference on Computer Vision) 论文元数据，
并将结果保存为 JSON 文件，字段包括：
    _id, paper_url, paper_abstract, paper_authors, paper_name, paper_year, citation
"""

from bs4 import BeautifulSoup
from urllib.parse import urljoin
import logging

from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class ECCVScraper(BaseScraper):
    """ECCV 论文元数据爬虫"""

    def __init__(self):
        super().__init__("https://www.ecva.net/")

    def get_conference_metadata(self, conference, year):
        """
        提取轻量级元数据（标题、作者、URL），不访问详情页，摘要为空
        """
        conference_url = f"{self.base_url}papers.php"
        logger.info(f"正在获取 ECCV {year} 的轻量级元数据...")

        html = self._make_request(conference_url)
        if not html:
            return []

        soup = BeautifulSoup(html, 'html.parser')
        paper_title_elements = soup.find_all('dt', class_='ptitle')
        logger.info(f"找到 {len(paper_title_elements)} 篇论文（所有年份）")

        papers = []
        for title_elem in paper_title_elements:
            paper_data = self._extract_lightweight_from_elem(title_elem, conference, year)
            if paper_data:
                papers.append(paper_data)

        logger.info(f"完成！提取 {len(papers)} 篇 ECCV {year} 轻量级元数据")
        return papers

    def _extract_lightweight_from_elem(self, title_elem, conference, year):
        """从 dt 元素中提取轻量级元数据（不访问详情页）"""
        title_link = title_elem.find('a', href=True)
        if not title_link:
            return None

        paper_url = title_link['href']
        if f'eccv_{year}' not in paper_url:
            return None

        paper_name = title_link.get_text(strip=True)
        paper_url = urljoin(self.base_url, paper_url)

        next_dd = title_elem.find_next_sibling('dd')
        if not next_dd:
            return None

        authors_text = next_dd.get_text(strip=True)
        paper_authors = authors_text if authors_text else "Unknown"

        return {
            "_id": "",
            "paper_url": paper_url,
            "paper_abstract": "",
            "paper_authors": paper_authors,
            "paper_name": paper_name,
            "paper_year": str(year),
            "citation": "",
            "conference": "ECCV"
        }

    def get_conference_papers(self, conference, year):
        """
        获取特定会议和年份的论文列表
        :param conference: 会议名称 (ECCV)
        :param year: 年份
        :return: 论文元数据列表
        """
        conference_url = f"{self.base_url}papers.php"
        logger.info(f"正在获取 ECCV {year} 的论文列表...")

        html = self._make_request(conference_url)
        if not html:
            return []

        self._random_delay()

        soup = BeautifulSoup(html, 'html.parser')

        papers = []
        paper_title_elements = soup.find_all('dt', class_='ptitle')

        logger.info(f"找到 {len(paper_title_elements)} 篇论文（所有年份）")

        for i, title_elem in enumerate(paper_title_elements):
            try:
                paper_data = self._extract_paper_metadata(title_elem, conference, year)
                if paper_data:
                    papers.append(paper_data)
                if (i + 1) % 100 == 0:
                    logger.info(f"进度: 已处理 {i+1}/{len(paper_title_elements)}，成功提取 {len(papers)} 篇")
            except Exception as e:
                logger.error(f"提取论文元数据时出错: {e}")
                continue

        logger.info(f"完成！成功提取 {len(papers)} 篇 ECCV {year} 论文")
        return papers

    def _extract_paper_metadata(self, title_elem, conference, year):
        """
        从论文元素中提取元数据
        :param title_elem: 论文标题的 BeautifulSoup dt 元素
        :param conference: 会议名称
        :param year: 年份
        :return: 论文元数据字典
        """
        title_link = title_elem.find('a', href=True)
        if not title_link:
            return None

        paper_url = title_link['href']
        if f'eccv_{year}' not in paper_url:
            return None

        paper_name = title_link.get_text(strip=True)
        paper_url = urljoin(self.base_url, paper_url)

        next_dd = title_elem.find_next_sibling('dd')
        if not next_dd:
            return None

        authors_text = next_dd.get_text(strip=True)
        paper_authors = authors_text if authors_text else "Unknown"

        logger.info(f"正在获取论文详情: {paper_name}")

        abstract = self._get_paper_details(paper_url)

        paper_data = {
            "_id": "",
            "paper_url": paper_url,
            "paper_abstract": abstract,
            "paper_authors": paper_authors,
            "paper_name": paper_name,
            "paper_year": str(year),
            "citation": "",
            "conference": "ECCV"
        }

        return paper_data

    def _get_paper_details(self, paper_url):
        """
        从论文详情页获取摘要
        :param paper_url: 论文详情页URL
        :return: 摘要文本
        """
        html = self._make_request(paper_url)
        if not html:
            return ""

        soup = BeautifulSoup(html, 'html.parser')

        body = soup.find('body')
        if not body:
            return ""

        text = body.get_text()

        abstract_idx = text.find('Abstract')
        if abstract_idx < 0:
            return ""

        after_abstract = text[abstract_idx + len('Abstract'):].strip()
        if after_abstract.startswith('"'):
            after_abstract = after_abstract[1:]

        end_markers = ['\nBibTeX', '\nBibtex', '\nbibtex', '\nDOI', 'Acknowledgements', 'Acknowledgment']
        end_idx = len(after_abstract)
        for marker in end_markers:
            idx = after_abstract.find(marker)
            if idx >= 0:
                end_idx = min(end_idx, idx)

        abstract = after_abstract[:end_idx].strip()
        if abstract.startswith('"'):
            abstract = abstract[1:]
        abstract = abstract.strip()

        return abstract
