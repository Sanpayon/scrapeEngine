#!/usr/bin/env python3
"""
爬取 ICML (International Conference on Machine Learning) 论文元数据，
并将结果保存为 JSON 文件，字段包括：
    _id, paper_url, paper_abstract, paper_authors, paper_name, paper_year, citation
"""

from bs4 import BeautifulSoup
from urllib.parse import urljoin

from scrapers.base_scraper import BaseScraper


class ICMLScraper(BaseScraper):
    """ICML 论文元数据爬虫"""

    YEAR_TO_VOLUME = {
        2025: "v267",
        2024: "v235",
        2023: "v202",
        2022: "v162",
        2021: "v139",
        2020: "v119",
        2019: "v97",
        2018: "v80",
        2017: "v70",
        2016: "v48",
        2015: "v37",
        2014: "v32",
        2013: "v28",
    }

    def __init__(self):
        super().__init__("https://proceedings.mlr.press/")

    def get_conference_papers(self, conference, year):
        """
        获取特定会议和年份的论文列表
        :param conference: 会议名称 (ICML)
        :param year: 年份
        :return: 论文元数据列表
        """
        volume = self.YEAR_TO_VOLUME.get(year)
        if not volume:
            print(f"不支持的年份: {year}，支持的年份: {sorted(self.YEAR_TO_VOLUME.keys())}")
            return []

        conference_url = f"{self.base_url}{volume}/"
        print(f"正在获取 ICML {year} ({volume}) 的论文列表...")

        html = self._make_request(conference_url)
        if not html:
            return []

        self._random_delay()

        soup = BeautifulSoup(html, 'html.parser')

        papers = []
        paper_divs = soup.find_all('div', class_='paper')

        print(f"找到 {len(paper_divs)} 篇论文")

        for i, paper_div in enumerate(paper_divs):
            try:
                paper_data = self._extract_paper_metadata(paper_div, conference, year)
                if paper_data:
                    papers.append(paper_data)
                if (i + 1) % 10 == 0:
                    print(f"进度: {i+1}/{len(paper_divs)}")
            except Exception as e:
                print(f"提取论文元数据时出错: {e}")
                continue

        print(f"完成！成功提取 {len(papers)} 篇论文")
        return papers

    def get_conference_metadata(self, conference, year):
        """
        提取轻量级元数据（标题、作者、URL），不访问详情页，摘要为空
        """
        volume = self.YEAR_TO_VOLUME.get(year)
        if not volume:
            return []

        conference_url = f"{self.base_url}{volume}/"
        print(f"正在获取 ICML {year} ({volume}) 的轻量级元数据...")

        html = self._make_request(conference_url)
        if not html:
            return []

        soup = BeautifulSoup(html, 'html.parser')
        paper_divs = soup.find_all('div', class_='paper')
        print(f"找到 {len(paper_divs)} 篇论文")

        papers = []
        for paper_div in paper_divs:
            paper_data = self._extract_lightweight_from_div(paper_div, conference, year)
            if paper_data:
                papers.append(paper_data)

        print(f"完成！提取 {len(papers)} 篇轻量级元数据")
        return papers

    def _extract_lightweight_from_div(self, paper_div, conference, year):
        """从 div 中提取轻量级元数据（不访问详情页）"""
        title_elem = paper_div.find('p', class_='title')
        if not title_elem:
            return None

        paper_name = title_elem.get_text(strip=True)
        if not paper_name:
            return None

        details_elem = paper_div.find('p', class_='details')
        authors = []
        if details_elem:
            authors_span = details_elem.find('span', class_='authors')
            if authors_span:
                author_texts = authors_span.get_text(strip=True).split(',')
                authors = [a.strip() for a in author_texts if a.strip()]

        paper_authors = ", ".join(authors) if authors else "Unknown"

        links_elem = paper_div.find('p', class_='links')
        paper_url = ""
        if links_elem:
            abs_link = links_elem.find('a', string='abs')
            if abs_link and abs_link.get('href'):
                paper_url = urljoin(self.base_url, abs_link['href'])

        if not paper_url:
            return None

        return {
            "_id": "",
            "paper_url": paper_url,
            "paper_abstract": "",
            "paper_authors": paper_authors,
            "paper_name": paper_name,
            "paper_year": str(year),
            "citation": "",
            "conference": "ICML"
        }

    def _extract_paper_metadata(self, paper_div, conference, year):
        """
        从论文元素中提取元数据
        :param paper_div: 包含论文信息的 div 元素
        :param conference: 会议名称
        :param year: 年份
        :return: 论文元数据字典
        """
        title_elem = paper_div.find('p', class_='title')
        if not title_elem:
            return None

        paper_name = title_elem.get_text(strip=True)
        if not paper_name:
            return None

        details_elem = paper_div.find('p', class_='details')
        authors = []
        if details_elem:
            authors_span = details_elem.find('span', class_='authors')
            if authors_span:
                author_texts = authors_span.get_text(strip=True).split(',')
                authors = [a.strip() for a in author_texts if a.strip()]

        paper_authors = ", ".join(authors) if authors else "Unknown"

        links_elem = paper_div.find('p', class_='links')
        paper_url = ""
        if links_elem:
            abs_link = links_elem.find('a', string='abs')
            if abs_link and abs_link.get('href'):
                paper_url = urljoin(self.base_url, abs_link['href'])

        if not paper_url:
            return None

        print(f"正在获取论文详情: {paper_name}")

        abstract = self._get_paper_details(paper_url)

        paper_data = {
            "_id": "",
            "paper_url": paper_url,
            "paper_abstract": abstract,
            "paper_authors": paper_authors,
            "paper_name": paper_name,
            "paper_year": str(year),
            "citation": "",
            "conference": "ICML"
        }

        self._random_delay()

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

        abstract = ""
        abstract_elem = soup.find('div', class_='abstract')
        if abstract_elem:
            abstract = abstract_elem.get_text(strip=True)
        else:
            abstract_elem = soup.find('p', class_='abstract')
            if abstract_elem:
                abstract = abstract_elem.get_text(strip=True)

        return abstract
