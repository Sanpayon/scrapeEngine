#!/usr/bin/env python3
"""
爬取 CVF (Computer Vision Foundation) 开放获取论文元数据，
并将结果保存为 JSON 文件，字段包括：
    _id, paper_url, paper_abstract, paper_authors, paper_name, paper_year, citation
"""

from bs4 import BeautifulSoup
from urllib.parse import urljoin
import logging

from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class CVFScraper(BaseScraper):
    """CVF 论文元数据爬虫"""

    def __init__(self):
        super().__init__("https://openaccess.thecvf.com/")

    def get_conference_metadata(self, conference, year):
        """
        提取轻量级元数据（标题、作者、URL），不访问详情页，摘要为空
        """
        conference_url = f"{self.base_url}{conference}{year}?day=all"
        logger.info(f"正在获取 {conference} {year} 的轻量级元数据...")

        html = self._make_request(conference_url)
        if not html:
            return []

        soup = BeautifulSoup(html, 'html.parser')
        paper_title_elements = soup.find_all('dt', class_='ptitle')
        logger.info(f"找到 {len(paper_title_elements)} 篇论文")

        papers = []
        for title_elem in paper_title_elements:
            paper_data = self._extract_lightweight_from_elem(title_elem, conference, year)
            if paper_data:
                papers.append(paper_data)

        logger.info(f"完成！提取 {len(papers)} 篇 {conference} {year} 轻量级元数据")
        return papers

    def _extract_lightweight_from_elem(self, title_elem, conference, year):
        """从 dt 元素中提取轻量级元数据（不访问详情页）"""
        title_link = title_elem.find('a', href=True)
        if not title_link:
            return None

        paper_name = title_link.get_text(strip=True)
        paper_url = urljoin(self.base_url, title_link['href'])

        next_dd = title_elem.find_next_sibling('dd')
        if not next_dd:
            return None

        authors = []
        author_forms = next_dd.find_all('form', class_='authsearch')
        for form in author_forms:
            author_link = form.find('a')
            if author_link:
                authors.append(author_link.get_text(strip=True))

        paper_authors = ", ".join(authors) if authors else "Unknown"

        return {
            "_id": "",
            "paper_url": paper_url,
            "paper_abstract": "",
            "paper_authors": paper_authors,
            "paper_name": paper_name,
            "paper_year": str(year),
            "citation": "",
            "conference": conference
        }

    def get_conference_papers(self, conference, year):
        """
        获取特定会议和年份的论文列表
        :param conference: 会议名称 (如 CVPR, ICCV, WACV)
        :param year: 年份
        :return: 论文元数据列表
        """
        conference_url = f"{self.base_url}{conference}{year}?day=all"
        logger.info(f"正在获取 {conference} {year} 的论文列表...")

        response = self._make_request(conference_url)
        if not response:
            return []

        self._random_delay()

        soup = BeautifulSoup(response, 'html.parser')

        papers = []
        paper_title_elements = soup.find_all('dt', class_='ptitle')

        logger.info(f"找到 {len(paper_title_elements)} 篇论文")

        for i, title_elem in enumerate(paper_title_elements):
            try:
                paper_data = self._extract_paper_metadata(title_elem, conference, year)
                if paper_data:
                    papers.append(paper_data)
                if (i + 1) % 10 == 0:
                    logger.info(f"进度: {i+1}/{len(paper_title_elements)}")
            except Exception as e:
                logger.error(f"提取论文元数据时出错: {e}")
                continue

        logger.info(f"完成！成功提取 {len(papers)} 篇论文")
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

        paper_name = title_link.get_text(strip=True)
        paper_url = urljoin(self.base_url, title_link['href'])

        next_dd = title_elem.find_next_sibling('dd')
        if not next_dd:
            return None

        authors = []
        author_forms = next_dd.find_all('form', class_='authsearch')
        for form in author_forms:
            author_link = form.find('a')
            if author_link:
                authors.append(author_link.get_text(strip=True))

        paper_authors = ", ".join(authors) if authors else "Unknown"

        bibref_div = next_dd.find('div', class_='bibref')
        paper_abstract = ""
        if bibref_div:
            bibtex_text = bibref_div.get_text(strip=True)

        logger.info(f"正在获取论文详情: {paper_name}")
        paper_data = {
            "_id": "",
            "paper_url": paper_url,
            "paper_abstract": paper_abstract,
            "paper_authors": paper_authors,
            "paper_name": paper_name,
            "paper_year": str(year),
            "citation": "",
            "conference": conference
        }

        abstract = self._get_paper_details(paper_url)
        if abstract:
            paper_data["paper_abstract"] = abstract

        return paper_data

    def _get_paper_details(self, paper_url):
        """
        从论文详情页获取摘要
        :param paper_url: 论文详情页URL
        :return: 摘要文本
        """
        response = self._make_request(paper_url)
        if not response:
            return ""

        soup = BeautifulSoup(response, 'html.parser')

        divs = soup.find_all('div')
        for div in divs:
            text = div.get_text(strip=True)
            if 'Abstract' in text and len(text) > 200:
                parts = text.split('Abstract', 1)
                if len(parts) > 1:
                    abstract_content = parts[1].strip()
                    if len(abstract_content) > 100:
                        return abstract_content
                    else:
                        continue

        paragraphs = soup.find_all('p')
        for p in paragraphs:
            text = p.get_text(strip=True)
            if len(text) > 100:
                return text

        return ""
