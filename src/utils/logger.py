# src/utils/logger.py
from loguru import logger
import sys
import os
from pathlib import Path
from datetime import datetime
from utils.config_loader import get_log_config

def setup_logger():
    """配置项目日志系统"""
    config = get_log_config()
    
    # 清除默认配置
    logger.remove()
    
    # 控制台日志配置
    logger.add(
        sys.stdout,
        level=config.get('console_level', 'INFO'),
        format=config.get('console_format', '<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{module}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>'),
        colorize=True
    )
    
    # 文件日志配置
    log_dir = Path(config.get('log_dir', 'data/logs'))
    log_dir.mkdir(parents=True, exist_ok=True)
    
    logger.add(
        log_dir / f"stock_data_{datetime.now().strftime('%Y%m%d')}.log",
        level=config.get('file_level', 'DEBUG'),
        format=config.get('file_format', '{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{function}:{line} - {message}'),
        rotation=config.get('rotation', '00:00'),  # 每天午夜轮转
        retention=config.get('retention', '30 days'),  # 保留30天
        compression=config.get('compression', 'zip'),
        enqueue=True  # 线程安全
    )
    
    # 错误日志单独记录
    logger.add(
        log_dir / "error.log",
        level="ERROR",
        format=config.get('error_format', '{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{function}:{line} - {message}'),
        rotation="100 MB",
        retention='90 days',
        compression='zip',
        enqueue=True
    )
    
    return logger

# 导出已配置的logger实例
log = setup_logger()