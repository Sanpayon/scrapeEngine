#!/usr/bin/env python3
"""
scrapEngine 调度器主入口。
后台常驻运行，按时间表自动执行：
  - 每天 18:00  获取 CCF-Deadlines 会议信息
  - 每天 08:00  爬取 arxiv 预印本
  - 每天 02:00  检查并爬取已到时间的会议论文
"""

import sys
import os
import logging
import random
import importlib
from datetime import datetime, timedelta
import time

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import (
    init_db, insert_or_update_conference, get_conferences,
    update_conference_status, update_conference_scrape_info,
    get_paper_titles, insert_papers, paper_exists_by_name,
    update_paper_abstract, get_stats
)
from ccf_parser import (
    fetch_all_target_conferences, fetch_conference_info,
    get_conference_scraper_name, get_conference_scraper_class,
    TARGET_CONFERENCES
)
from matcher import find_new_papers

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scheduler.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

jobstores = {
    "default": SQLAlchemyJobStore(url=f"sqlite:///{os.path.join(os.path.dirname(os.path.abspath(__file__)), 'jobs.db')}"),
}
executors = {
    "default": ThreadPoolExecutor(4),
}
scheduler = BlockingScheduler(
    jobstores=jobstores,
    executors=executors,
)

def _random_delay(min_minute=0, max_minute=30):
    """在指定范围内随机延迟，单位为分钟。"""
    delay = random.randint(min_minute, max_minute)
    logger.info(f"随机延迟 {delay} 分钟...")
    time.sleep(delay * 60)


def _get_scraper(conf_name: str):
    """根据会议名动态加载并实例化对应的 Scraper。"""
    scraper_file = get_conference_scraper_name(conf_name)
    if not scraper_file:
        logger.error(f"未找到 {conf_name} 对应的 scraper 文件")
        return None

    class_name = get_conference_scraper_class(conf_name)
    if not class_name:
        logger.error(f"未找到 {conf_name} 对应的 scraper 类")
        return None

    try:
        mod = importlib.import_module(f"scrapers.{scraper_file}")
        scraper_cls = getattr(mod, class_name)
        return scraper_cls()
    except Exception as e:
        logger.error(f"加载 scraper 失败 {conf_name}: {e}")
        return None


# ============================================================
# Job 1: 每天 18:00 获取 CCF-Deadlines 会议信息
# ============================================================
def job_check_ccf_deadlines(random_delay=True):
    """获取 CCF-Deadlines 会议信息并更新到数据库。"""
    if random_delay:
        _random_delay()
    
    logger.info("=== 开始检查 CCF-Deadlines ===")
    try:
        all_confs = fetch_all_target_conferences()
        logger.info(f"获取到 {len(all_confs)} 条会议记录")

        now = datetime.now().strftime("%Y-%m-%d")
        for conf in all_confs:
            # 判断会议状态
            start_date = conf.get("start_date", "")
            end_date = conf.get("end_date", "")

            if start_date:
                start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else start_dt

                if now > end_dt.strftime("%Y-%m-%d"):
                    conf["status"] = "completed"
                elif now >= start_dt.strftime("%Y-%m-%d"):
                    conf["status"] = "ongoing"
                else:
                    conf["status"] = "upcoming"
            else:
                conf["status"] = "upcoming"

            insert_or_update_conference(conf)
            logger.info(f"  更新: {conf['name']} {conf['year']} - {conf['status']}")

        logger.info("=== CCF-Deadlines 检查完成 ===")
    except Exception as e:
        logger.error(f"CCF-Deadlines 检查失败: {e}", exc_info=True)


# ============================================================
# Job 2: 每天 08:00 爬取 arxiv 预印本
# ============================================================
def job_scrape_arxiv_for_upcoming():
    """对 upcoming 会议爬取 arxiv 预印本。"""
    _random_delay()
    logger.info("=== 开始爬取 arxiv 预印本 ===")
    try:
        upcoming = get_conferences(status="upcoming")
        if not upcoming:
            logger.info("没有 upcoming 状态的会议")
            return

        from scrapers.scrap_arxiv import crawl_arxiv

        for conf in upcoming:
            name = conf["name"]
            year = conf["year"]
            logger.info(f"爬取 arxiv: {name} {year}")
            try:
                crawl_arxiv(max_results=500, conference=name, year=year)
            except Exception as e:
                logger.error(f"arxiv 爬取失败 {name} {year}: {e}")

        logger.info("=== arxiv 预印本爬取完成 ===")
    except Exception as e:
        logger.error(f"arxiv 预印本爬取失败: {e}", exc_info=True)


# ============================================================
# Job 3: 每天 02:00 检查并爬取会议论文
# ============================================================
def job_check_and_scrape_conferences():
    """检查哪些会议开始日期 + 10 天，触发爬取。"""
    _random_delay()
    logger.info("=== 开始检查会议爬取任务 ===")
    try:
        now = datetime.now()
        upcoming = get_conferences(status="upcoming")
        ongoing = get_conferences(status="ongoing")

        for conf in upcoming + ongoing:
            name = conf["name"]
            year = conf["year"]
            start_date = conf.get("start_date", "")

            if not start_date:
                continue

            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            scrape_date = start_dt + timedelta(days=10)

            if now >= scrape_date:
                logger.info(f"触发爬取: {name} {year} (开始日期: {start_date})")
                _scrape_conference_with_retry(name, year)
            else:
                days_left = (scrape_date - now).days
                logger.info(f"  跳过: {name} {year} (还需 {days_left} 天)")

        logger.info("=== 会议爬取检查完成 ===")
    except Exception as e:
        logger.error(f"会议爬取检查失败: {e}", exc_info=True)


def _scrape_conference_with_retry(conf_name: str, year: str, max_retries: int = 3, retry_count: int = 0):
    """爬取会议论文，失败则在 30-45 分钟后重试，最多 3 次。"""
    logger.info(f"开始爬取 {conf_name} {year} (尝试 {retry_count + 1}/{max_retries + 1})")

    scraper = _get_scraper(conf_name)
    if not scraper:
        return

    try:
        # 阶段1：提取轻量级元数据（标题、作者、URL，摘要为空）
        papers_meta = scraper.extract_lightweight_metadata(conf_name, int(year))
        if not papers_meta:
            logger.warning(f"{conf_name} {year} 未获取到任何元数据")
            update_conference_status(conf_name, year, "scraped")
            return

        logger.info(f"获取到 {len(papers_meta)} 篇元数据")

        # 阶段2：与数据库比对，筛选新论文
        db_titles = get_paper_titles(conf_name, year)
        new_titles = find_new_papers(
            [p["paper_name"] for p in papers_meta],
            db_titles,
        )
        logger.info(f"发现 {len(new_titles)}/{len(papers_meta)} 篇新论文")

        if not new_titles:
            logger.info(f"{conf_name} {year} 无新论文，跳过")
            update_conference_status(conf_name, year, "scraped")
            return

        # 阶段3：只对新论文填充摘要
        new_papers = [p for p in papers_meta if p["paper_name"] in new_titles]
        logger.info(f"开始为 {len(new_papers)} 篇新论文获取摘要...")
        filled_papers = scraper.fill_abstracts(new_papers)

        # 阶段4：批量插入数据库
        inserted = insert_papers(filled_papers)
        logger.info(f"数据库插入完成：新插入 {inserted} 篇")

        update_conference_status(conf_name, year, "scraped")
        update_conference_scrape_info(conf_name, year, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 0)
        logger.info(f"{conf_name} {year} 爬取完成")

    except Exception as e:
        logger.error(f"{conf_name} {year} 爬取失败: {e}", exc_info=True)

        next_retry = retry_count + 1
        if next_retry <= max_retries:
            delay_minutes = random.randint(30, 45)
            retry_time = datetime.now() + timedelta(minutes=delay_minutes)
            logger.info(f"安排重试 ({next_retry}/{max_retries}): {conf_name} {year} @ {retry_time} (延迟 {delay_minutes} 分钟)")

            scheduler.add_job(
                _scrape_conference_with_retry,
                "date",
                run_date=retry_time,
                args=[conf_name, year, max_retries, next_retry],
                id=f"retry_{conf_name}_{year}_{next_retry}",
                replace_existing=True,
            )
            update_conference_scrape_info(
                conf_name, year,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                next_retry,
            )
        else:
            logger.error(f"{conf_name} {year} 已达最大重试次数 ({max_retries})，放弃")


# ============================================================
# 调度器配置
# ============================================================
def setup_scheduler():
    """配置定时任务"""


    # 每天 18:00 检查 CCF-Deadlines
    scheduler.add_job(
        job_check_ccf_deadlines,
        "cron", hour=18, minute=0,
        id="ccf_check",
        replace_existing=True,
        max_instances=1,
    )

    # 每天 08:00 爬取 arxiv 预印本
    scheduler.add_job(
        job_scrape_arxiv_for_upcoming,
        "cron", hour=8, minute=0,
        id="arxiv_scrape",
        replace_existing=True,
        max_instances=1,
    )

    # 每天 02:00 检查并爬取会议论文
    scheduler.add_job(
        job_check_and_scrape_conferences,
        "cron", hour=2, minute=0,
        id="conference_check",
        replace_existing=True,
        max_instances=1,
    )


def print_status():
    """打印当前状态。"""
    stats = get_stats()
    logger.info(f"数据库统计: {stats}")

    confs = get_conferences()
    if confs:
        logger.info("会议状态:")
        for c in confs:
            logger.info(f"  {c['name']} {c['year']}: {c['status']} (start: {c['start_date']}, end: {c['end_date']})")


if __name__ == "__main__":
    init_db()
    setup_scheduler()

    logger.info("=" * 60)
    logger.info("scrapEngine 调度器启动")
    logger.info("=" * 60)

    # 首次运行时立即执行一次 CCF 检查
    job_check_ccf_deadlines(random_delay=False)
    print_status()

    logger.info("调度器开始运行...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("调度器已停止")
