
import yaml
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

CONFIG_PATH = Path("config.yaml")


def normalize_proxy_url(proxy_url: str | None) -> str | None:
    """规范化代理地址，允许用户省略 http:// 前缀。"""
    if not proxy_url:
        return None

    proxy_url = str(proxy_url).strip()
    if not proxy_url:
        return None
    if "://" not in proxy_url:
        proxy_url = f"http://{proxy_url}"
    return proxy_url


def get_proxy_url(config: dict) -> str | None:
    """获取全局代理地址，并兼容旧版 Telegram 节点中的配置。"""
    network_proxy = config.get("network", {}).get("proxy_url")
    legacy_proxy = config.get("notifier", {}).get("telegram", {}).get("proxy_url")
    return normalize_proxy_url(network_proxy or legacy_proxy)

def load_config(path: Path = CONFIG_PATH) -> dict:
    """加载配置文件"""
    if not path.exists():
        # Fallback to example if exists? No, just log error
        logger.error(f"配置文件未找到: {path}")
        return {}
    
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        logger.error(f"加载配置文件失败: {e}")
        return {}
