#!/usr/bin/env python3
"""
爬取 CVF (Computer Vision Foundation) 开放获取论文元数据，
并将结果保存为 JSON 文件，字段包括：
    _id, paper_url, paper_abstract, paper_authors, paper_name, paper_year, citation
"""

from bs4 import BeautifulSoup
from urllib.parse import urljoin

from scrapers.base_scraper import BaseScraper

# TODO: 在初始阶段，不应当从链接爬取摘要。应当到数据库中匹配，如果该文件已在数据库中存在，则不纳入;
# TODO: 如果不在数据库中，才爬取链接获取摘要

class CVFScraper(BaseScraper):
    """CVF 论文元数据爬虫"""

    def __init__(self):
        super().__init__("https://openaccess.thecvf.com/")

    def get_conference_metadata(self, conference, year):
        """
        提取轻量级元数据（标题、作者、URL），不访问详情页，摘要为空
        """
        conference_url = f"{self.base_url}{conference}{year}?day=all"
        print(f"正在获取 {conference} {year} 的轻量级元数据...")

        html = self._make_request(conference_url)
        if not html:
            return []

        soup = BeautifulSoup(html, 'html.parser')
        paper_title_elements = soup.find_all('dt', class_='ptitle')
        print(f"找到 {len(paper_title_elements)} 篇论文")

        papers = []
        for title_elem in paper_title_elements:
            paper_data = self._extract_lightweight_from_elem(title_elem, conference, year)
            if paper_data:
                papers.append(paper_data)

        print(f"完成！提取 {len(papers)} 篇 {conference} {year} 轻量级元数据")
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
        # 使用 day=all 参数获取所有论文
        conference_url = f"{self.base_url}{conference}{year}?day=all"
        print(f"正在获取 {conference} {year} 的论文列表...")

        response = self._make_request(conference_url)
        if not response:
            return []

        # 随机延迟
        self._random_delay()

        # 解析HTML内容
        soup = BeautifulSoup(response.text, 'html.parser')

        papers = []
        # 查找论文条目 - 使用 dt.ptitle 作为论文标题元素
        paper_title_elements = soup.find_all('dt', class_='ptitle')

        print(f"找到 {len(paper_title_elements)} 篇论文")

        for i, title_elem in enumerate(paper_title_elements):
            try:
                paper_data = self._extract_paper_metadata(title_elem, conference, year)
                if paper_data:
                    papers.append(paper_data)
                # 每处理10篇论文显示一次进度
                if (i + 1) % 10 == 0:
                    print(f"进度: {i+1}/{len(paper_title_elements)}")
            except Exception as e:
                print(f"提取论文元数据时出错: {e}")
                continue

        print(f"完成！成功提取 {len(papers)} 篇论文")
        return papers

    def _extract_paper_metadata(self, title_elem, conference, year):
        """
        从论文元素中提取元数据
        :param title_elem: 论文标题的 BeautifulSoup dt 元素
        :param conference: 会议名称
        :param year: 年份
        :param soup: 整个页面的 BeautifulSoup 对象
        :return: 论文元数据字典
        """
        # 提取论文标题和URL
        # 在 dt.ptitle 中找到 a 标签
        title_link = title_elem.find('a', href=True)
        if not title_link:
            return None

        paper_name = title_link.get_text(strip=True)
        paper_url = urljoin(self.base_url, title_link['href'])

        # 获取下一个 dd 元素，包含作者信息
        next_dd = title_elem.find_next_sibling('dd')
        if not next_dd:
            return None

        # 提取作者信息 - 从多个 form 中提取
        authors = []
        author_forms = next_dd.find_all('form', class_='authsearch')
        for form in author_forms:
            author_link = form.find('a')
            if author_link:
                authors.append(author_link.get_text(strip=True))

        paper_authors = ", ".join(authors) if authors else "Unknown"

        # --------- 从这里开始，需要进行数据库改写 ------------

        # 查找包含摘要的 div (bibref)
        bibref_div = next_dd.find('div', class_='bibref')
        paper_abstract = ""
        if bibref_div:
            # 从 bibtex 中提取摘要信息（虽然这里主要是bibtex，但可能包含其他信息）
            bibtex_text = bibref_div.get_text(strip=True)
            # bibtex 通常不包含摘要，但我们保留这个字段以备后用

        # 获取论文详情页以提取摘要
        print(f"正在获取论文详情: {paper_name}")
        paper_data = {
            "_id": "",  # 留空
            "paper_url": paper_url,
            "paper_abstract": paper_abstract,
            "paper_authors": paper_authors,
            "paper_name": paper_name,
            "paper_year": str(year),
            "citation": "",  # 留空
            "conference": conference
        }

        # 获取论文详情页以提取摘要
        abstract = self._get_paper_details(paper_url)
        if abstract:
            paper_data["paper_abstract"] = abstract

        # ---------------- 到这里终止改写 ----------------

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

        soup = BeautifulSoup(response.text, 'html.parser')

        # CVF 页面的摘要通常在包含"Abstract"标题的 div 中
        # 查找所有 div 元素
        divs = soup.find_all('div')
        for div in divs:
            text = div.get_text(strip=True)
            # 查找包含"Abstract"且长度足够的内容
            if 'Abstract' in text and len(text) > 200:
                # 分割 Abstract 标题和内容
                parts = text.split('Abstract', 1)
                if len(parts) > 1:
                    abstract_content = parts[1].strip()
                    # 确保摘要内容有意义
                    if len(abstract_content) > 100:
                        return abstract_content
                    else:
                        continue

        # 备选方案：查找所有段落
        paragraphs = soup.find_all('p')
        for p in paragraphs:
            text = p.get_text(strip=True)
            if len(text) > 100:
                return text

        return ""
