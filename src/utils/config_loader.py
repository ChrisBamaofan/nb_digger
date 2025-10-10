# src/utils/config_loader.py
import yaml
from pathlib import Path
from typing import Any, Dict
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

class ConfigLoader:
    """统一配置加载器"""
    
    _instance = None
    _configs = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigLoader, cls).__new__(cls)
            cls._instance._load_all_configs()
        return cls._instance
    
    def _load_all_configs(self):
        """加载所有配置文件"""
        config_dir = Path(__file__).parent.parent.parent / 'config'
        
        try:
            # 主配置
            self._configs['app'] = self._load_yaml(config_dir / 'config.yaml')
            
            # 数据库配置
            self._configs['db'] = self._load_yaml(config_dir / 'db_config.yaml')
            
            # 日志配置
            self._configs['log'] = self._load_yaml(config_dir / 'log_config.yaml')
            
            # 环境变量覆盖
            self._override_with_env()
            
        except Exception as e:
            print(f"加载配置文件失败: {repr(e)}") 
            raise
    
    def _load_yaml(self, file_path: Path) -> Dict[str, Any]:
        """加载YAML配置文件"""
        if not file_path.exists():
            print(f"not file exist {file_path}")
            return {}
            
        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    
    def _override_with_env(self):
        """用环境变量覆盖配置"""
        # 数据库配置覆盖
        if 'DB_HOST' in os.environ:
            self._configs['db']['mysql']['host'] = os.getenv('DB_HOST')
        if 'DB_PASSWORD' in os.environ:
            self._configs['db']['mysql']['password'] = os.getenv('DB_PASSWORD')
        
        # Tushare Token覆盖
        if 'TUSHARE_TOKEN' in os.environ:
            self._configs['app']['data_sources']['tushare']['api_key'] = os.getenv('TUSHARE_TOKEN')
    
    def get_config(self, section: str = 'app') -> Dict[str, Any]:
        """获取指定配置节"""
        return self._configs.get(section, {})
    
    def get_db_config(self) -> Dict[str, Any]:
        """获取数据库配置"""
        return self._configs.get('db', {}).get('mysql', {})
    
    def get_log_config(self) -> Dict[str, Any]:
        """获取日志配置"""
        return self._configs.get('log', {}).get('logging', {})

# 单例导出
def load_config(section: str = 'app') -> Dict[str, Any]:
    return ConfigLoader().get_config(section)

def load_db_config() -> Dict[str, Any]:
    return ConfigLoader().get_db_config()

def get_log_config() -> Dict[str, Any]:
    return ConfigLoader().get_log_config()

def get_tushare() -> Dict[str, Any]:
    config = ConfigLoader().get_config('app')
    return config['data_sources']['tushare']
