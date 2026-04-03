#!/usr/bin/env python3
"""
解析 ccf-deadlines GitHub 仓库的 YAML 文件，
获取目标会议的元数据（deadline、start_date、end_date 等）。
"""

import yaml
import requests
import json
import os
from datetime import datetime
import re

GITHUB_RAW_URL = "https://raw.githubusercontent.com/ccfddl/ccf-deadlines/main"
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".ccf_cache")
CACHE_TTL = 3600  # 1 hour

TARGET_CONFERENCES = {
    "AAAI":    {"sub": "AI", "yaml": "conference/AI/aaai.yml"},
    "CVPR":    {"sub": "CG", "yaml": "conference/CG/cvpr.yml"},
    "ICCV":    {"sub": "CG", "yaml": "conference/CG/iccv.yml"},
    "ECCV":    {"sub": "CG", "yaml": "conference/CG/eccv.yml"},
    "ICLR":    {"sub": "AI", "yaml": "conference/AI/iclr.yml"},
    "ICML":    {"sub": "AI", "yaml": "conference/AI/icml.yml"},
    "NeurIPS": {"sub": "AI", "yaml": "conference/AI/neurips.yml"},
}


def _get_cache_path(yaml_path: str) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    safe_name = yaml_path.replace("/", "_")
    return os.path.join(CACHE_DIR, f"{safe_name}.json")


def _fetch_yaml(yaml_path: str) -> dict | None:
    """从 GitHub 获取 YAML 文件，带本地缓存。"""
    cache_path = _get_cache_path(yaml_path)

    # 尝试读取缓存
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r") as f:
                cached = json.load(f)
            if (datetime.now().timestamp() - cached.get("_cached_at", 0)) < CACHE_TTL:
                return cached.get("data")
        except Exception:
            pass

    # 从 GitHub 获取
    url = f"{GITHUB_RAW_URL}/{yaml_path}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = yaml.safe_load(resp.text)

        # 写入缓存
        with open(cache_path, "w") as f:
            json.dump({"data": data, "_cached_at": datetime.now().timestamp()}, f)

        return data
    except Exception as e:
        print(f"获取 YAML 失败 {yaml_path}: {e}")
        # 返回过期缓存
        if os.path.exists(cache_path):
            try:
                with open(cache_path, "r") as f:
                    return json.load(f).get("data")
            except Exception:
                pass
        return None


def _parse_date_str(date_str: str) -> str | None:
    """解析日期字符串，返回 ISO 格式。"""
    if not date_str or date_str.strip().upper() == "TBD":
        return None

    date_str = date_str.strip()

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    m = re.match(r"(\w+)\s+(\d{1,2})-(\d{1,2}),?\s+(\d{4})", date_str)
    if m:
        month_str, day_start, day_end, year = m.groups()
        try:
            end_dt = datetime.strptime(f"{month_str} {day_end} {year}", "%b %d %Y")
            return end_dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    m = re.match(r"(\w+)\s+(\d{1,2}),?\s+(\d{4})", date_str)
    if m:
        month_str, day, year = m.groups()
        try:
            dt = datetime.strptime(f"{month_str} {day} {year}", "%b %d %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    return None


def _get_latest_deadline(timeline: list) -> str | None:
    """从 timeline 中获取最晚的 deadline。"""
    if not timeline:
        return None
    deadlines = []
    for t in timeline:
        dl = t.get("deadline", "")
        if dl and dl.strip().upper() != "TBD":
            parsed = _parse_date_str(dl)
            if parsed:
                deadlines.append(parsed)
    return max(deadlines) if deadlines else None


def fetch_conference_info(conf_name: str) -> list[dict]:
    """
    获取指定会议的所有年份信息。
    返回: [{"name": "ICML", "year": "2025", "deadline": "...", "start_date": "...", "end_date": "..."}, ...]
    """
    if conf_name not in TARGET_CONFERENCES:
        print(f"不支持的会议: {conf_name}")
        return []

    conf_config = TARGET_CONFERENCES[conf_name]
    data = _fetch_yaml(conf_config["yaml"])
    if not data or not isinstance(data, list):
        return []

    results = []
    for entry in data:
        title = entry.get("title", "").upper()
        if title != conf_name.upper():
            continue

        confs = entry.get("confs", [])
        for c in confs:
            year = c.get("year")
            if not year:
                continue

            timeline = c.get("timeline", [])
            deadline = _get_latest_deadline(timeline)
            start_date = _parse_date_str(c.get("date", ""))

            date_str = c.get("date", "")
            end_date = None
            if date_str:
                m = re.match(r"(\w+)\s+(\d{1,2})-(\d{1,2}),?\s+(\d{4})", date_str)
                if m:
                    month_str, day_start, day_end, year_str = m.groups()
                    try:
                        end_dt = datetime.strptime(f"{month_str} {day_end} {year_str}", "%b %d %Y")
                        end_date = end_dt.strftime("%Y-%m-%d")
                    except ValueError:
                        end_date = start_date
                else:
                    end_date = start_date

            results.append({
                "name": conf_name,
                "year": str(year),
                "deadline": deadline or "",
                "start_date": start_date or "",
                "end_date": end_date or "",
            })

    return results


def fetch_all_target_conferences() -> list[dict]:
    """获取所有目标会议的信息。"""
    all_confs = []
    for conf_name in TARGET_CONFERENCES:
        infos = fetch_conference_info(conf_name)
        all_confs.extend(infos)
    return all_confs


def get_conference_scraper_name(conf_name: str) -> str | None:
    """将会议名映射到 scraper 文件名。"""
    mapping = {
        "AAAI": "scrap_AAAI",
        "CVPR": "scrap_cvf",
        "ICCV": "scrap_cvf",
        "ECCV": "scrap_ECCV",
        "ICLR": "scrap_ICLR",
        "ICML": "scrap_ICML",
        "NeurIPS": "scrap_NIPS",
    }
    return mapping.get(conf_name)


def get_conference_scraper_class(conf_name: str) -> str | None:
    """将会议名映射到 scraper 类名。"""
    mapping = {
        "AAAI": "AAAIScraper",
        "CVPR": "CVFScraper",
        "ICCV": "CVFScraper",
        "ECCV": "ECCVScraper",
        "ICLR": "ICLRScraper",
        "ICML": "ICMLScraper",
        "NeurIPS": "NIPSScraper",
    }
    return mapping.get(conf_name)
