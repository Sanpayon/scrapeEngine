#!/usr/bin/env python3
"""
爬取 AAAI (Association for the Advancement of Artificial Intelligence) 论文元数据，
并将结果保存为 JSON 文件，字段包括：
    _id, paper_url, paper_abstract, paper_authors, paper_name, paper_year, citation
"""

import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import logging

from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class AAAIScraper(BaseScraper):
    """AAAI 论文元数据爬虫"""

    def __init__(self):
        super().__init__("https://ojs.aaai.org/index.php/AAAI/")

    def _get_issue_urls_for_year(self, year):
        """
        获取指定年份的所有 issue URL
        :param year: 年份
        :return: issue URL 列表
        """
        issue_urls = []
        seen_urls = set()
        page = 1

        while page <= 10:
            if page == 1:
                archive_url = f"{self.base_url}issue/archive"
            else:
                archive_url = f"{self.base_url}issue/archive/{page}"

            html = self._make_request(archive_url)
            if not html:
                break

            soup = BeautifulSoup(html, 'html.parser')
            issue_links = soup.find_all('a', href=lambda x: x and 'issue/view/' in str(x))

            page_has_any = False
            for link in issue_links:
                text = link.get_text(strip=True)
                href = link['href']
                if not text:
                    continue

                m = re.search(r'AAAI-(\d{2})', text)
                if m:
                    yr = int('20' + m.group(1))
                    page_has_any = True
                    if yr == year and href not in seen_urls:
                        seen_urls.add(href)
                        issue_urls.append(urljoin(self.base_url, href))

            if not page_has_any:
                break

            page += 1
            self._random_delay()

        logger.info(f"找到 {len(issue_urls)} 个 AAAI {year} issue")
        return issue_urls

    def get_conference_metadata(self, conference, year):
        """
        提取轻量级元数据（标题、作者、URL），不访问详情页，摘要为空
        """
        logger.info(f"正在获取 AAAI {year} 的轻量级元数据...")

        issue_urls = self._get_issue_urls_for_year(year)
        if not issue_urls:
            return []

        papers = []
        for issue_url in issue_urls:
            html = self._make_request(issue_url)
            if not html:
                continue

            soup = BeautifulSoup(html, 'html.parser')
            article_links = soup.find_all('a', href=lambda x: x and 'article/view/' in str(x))

            for link in article_links:
                href = link['href']
                if 'download' in href or 'supp' in href.lower():
                    continue

                path_parts = href.rstrip('/').split('/')
                if len(path_parts) != 8:
                    continue

                paper_url = urljoin(self.base_url, href)
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
                    "conference": "AAAI"
                }
                papers.append(paper_data)

        logger.info(f"完成！提取 {len(papers)} 篇 AAAI {year} 轻量级元数据")
        return papers

    def get_conference_papers(self, conference, year):
        """
        获取特定会议和年份的论文列表
        :param conference: 会议名称 (AAAI)
        :param year: 年份
        :return: 论文元数据列表
        """
        logger.info(f"正在获取 AAAI {year} 的论文列表...")

        issue_urls = self._get_issue_urls_for_year(year)
        if not issue_urls:
            logger.warning(f"未找到 AAAI {year} 的 issue")
            return []

        papers = []
        total_papers = 0

        for issue_idx, issue_url in enumerate(issue_urls):
            logger.info(f"正在获取 issue {issue_idx + 1}/{len(issue_urls)}: {issue_url}")

            html = self._make_request(issue_url)
            if not html:
                continue

            soup = BeautifulSoup(html, 'html.parser')
            article_links = soup.find_all('a', href=lambda x: x and 'article/view/' in str(x))

            for link in article_links:
                href = link['href']
                if 'download' in href or 'supp' in href.lower():
                    continue

                path_parts = href.rstrip('/').split('/')
                if len(path_parts) != 8:
                    continue

                paper_url = urljoin(self.base_url, href)
                paper_name = link.get_text(strip=True)
                if not paper_name:
                    continue

                total_papers += 1
                try:
                    paper_data = self._get_paper_details(paper_url, paper_name, year)
                    if paper_data:
                        papers.append(paper_data)
                    if total_papers % 10 == 0:
                        logger.info(f"进度: 已处理 {total_papers} 篇，成功提取 {len(papers)} 篇")
                except Exception as e:
                    logger.error(f"提取论文元数据时出错: {e}")
                    continue

            self._random_delay()

        logger.info(f"完成！共处理 {total_papers} 篇，成功提取 {len(papers)} 篇 AAAI {year} 论文")
        return papers

    def _extract_paper_metadata(self, title_elem, conference, year):
        """
        从论文元素中提取元数据（AAAI 不使用此方法，保留接口兼容性）
        """
        return None

    def _get_paper_details(self, paper_url, paper_name, year):
        """
        从论文详情页获取摘要、作者等信息
        :param paper_url: 论文详情页URL
        :param paper_name: 论文标题
        :param year: 年份
        :return: 论文元数据字典
        """
        html = self._make_request(paper_url)
        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')

        authors = []
        author_metas = soup.find_all('meta', {'name': 'citation_author'})
        for meta in author_metas:
            if 'content' in meta.attrs:
                authors.append(meta['content'])

        paper_authors = ", ".join(authors) if authors else "Unknown"

        abstract = ""
        body = soup.find('body')
        if body:
            text = body.get_text()
            abstract_idx = text.find('Abstract')
            if abstract_idx >= 0:
                after_abstract = text[abstract_idx + len('Abstract'):].strip()
                if after_abstract.startswith('"'):
                    after_abstract = after_abstract[1:]

                end_markers = ['\nBibTeX', '\nBibtex', '\nbibtex', '\nDOI', 'Acknowledgements', 'Acknowledgment', 'References']
                end_idx = len(after_abstract)
                for marker in end_markers:
                    idx = after_abstract.find(marker)
                    if idx >= 0:
                        end_idx = min(end_idx, idx)

                abstract = after_abstract[:end_idx].strip()
                if abstract.startswith('"'):
                    abstract = abstract[1:]
                abstract = abstract.strip()

        paper_data = {
            "_id": "",
            "paper_url": paper_url,
            "paper_abstract": abstract,
            "paper_authors": paper_authors,
            "paper_name": paper_name,
            "paper_year": str(year),
            "citation": "",
            "conference": "AAAI"
        }

        return paper_data
