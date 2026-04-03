from abc import ABC, abstractmethod

import requests
import json
import time
import random
from urllib.parse import urljoin

class BaseScraper(ABC):
    """通用请求爬虫"""

    def __init__(self, base_url):
        # 使用真实的浏览器 User-Agent
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0"
        ]

        # 完整的请求头
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "max-age=0",
        }

        # 会话管理
        self.session = requests.Session()
        self.session.headers.update(self.headers)

        # 基础URL
        self.base_url = base_url

        self.min_delay = 1.0
        self.max_delay = 3.0

    def _rotate_user_agent(self):
        """轮换 User-Agent"""
        user_agent = random.choice(self.user_agents)
        self.session.headers['User-Agent'] = user_agent

    def _random_delay(self):
        """随机延迟以避免被封禁"""
        delay = random.uniform(self.min_delay, self.max_delay)
        time.sleep(delay)

    def _make_request(self, url, max_retries=3):
        """发送HTTP请求并处理错误"""
        for attempt in range(max_retries):
            try:
                self._rotate_user_agent()
                response = self.session.get(url, timeout=10)
                response.raise_for_status()  # 检查HTTP错误
                return response.text
            except requests.exceptions.RequestException as e:
                print(f"请求错误: {e} (尝试 {attempt + 1}/{max_retries})")
                self._random_delay()
        print(f"请求失败: {url}")
        return None

    @abstractmethod
    def get_conference_papers(self, conference, year):
        """获取特定会议和年份的论文列表"""
        pass

    @abstractmethod
    def _extract_paper_metadata(self, title_elem, conference, year):
        """从论文元素中提取元数据"""
        pass

    @abstractmethod
    def _get_paper_details(self, paper_url):
        """获取论文详情页面的元数据"""
        pass

    
    def crawl_conference(self, conference, year):
        """爬取特定会议和年份的论文元数据"""
        all_papers = []
        papers = self.get_conference_papers(conference, year)
        all_papers.extend(papers)
        self._save_to_json(all_papers, f"{conference}_{year}_papers.json")
        self._save_to_dataset(all_papers)
        return all_papers

    def _save_to_dataset(self, data):
        """将数据保存到 SQLite 数据库"""
        try:
            from database import insert_papers
            inserted = insert_papers(data)
            print(f"数据库保存完成！新插入 {inserted} 篇论文（共 {len(data)} 篇）")
        except Exception as e:
            print(f"数据库保存失败: {e}")

    def _save_to_json(self, data, filename):
        """将数据保存为JSON文件"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        print(f"\n{'='*50}")
        print(f"爬取完成！共获取 {len(data)} 篇论文")
        print(f"所有数据已保存到 {filename}")
        print(f"{'='*50}")
