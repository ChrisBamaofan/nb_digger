from typing import List
from sqlalchemy import create_engine,update
from sqlalchemy.orm import sessionmaker
from .models import Base
from utils.config_loader import load_db_config
from loguru import logger
import pandas as pd
from sqlalchemy import desc  
from database.models import StockDaily,StockBasicInfo

class DBManager:
    def __init__(self):
        config = load_db_config()
        print("Loaded DB config:", config)  # 调试输出
        

        self.engine = create_engine(
            f"mysql+pymysql://{config['user']}:{config['password']}@"
            f"{config['host']}:{config['port']}/{config['database']}",
            pool_size=config['pool_size'],
            max_overflow=config['max_overflow']
        )
        self.Session = sessionmaker(bind=self.engine)
        
    # def init_db(self):
    #     """初始化数据库表"""
    #     Base.metadata.create_all(self.engine)
    #     logger.info("数据库表初始化完成")
    
    def save_data(self, data_df, model_class):
        """保存数据到数据库"""
        session = self.Session()
        try:
            records = [model_class(**row) for row in data_df.to_dict('records')]
            session.add_all(records)
            session.commit()
            logger.success(f"成功保存{len(records)}条数据")
        except Exception as e:
            session.rollback()
            logger.error(f"数据保存失败: {e}")
        finally:
            session.close()

    def save_daily_data(self, data_df: pd.DataFrame):
        """保存日线数据到stock_per_day_final表"""
        session = self.Session()
        try:
            # 转换NaN为默认值
            data_df = data_df.fillna({
                'change_price': 0,
                'change_percent': 0,
                'turnover_ratio': 0
            })
            
            # 转换为ORM对象
            records = [StockDaily(**row) for row in data_df.to_dict('records')]
            
            # 使用merge实现upsert操作
            for record in records:
                session.merge(record)
                
            session.commit()
            logger.success(f"成功保存/更新{len(records)}条日线数据")
        except Exception as e:
            session.rollback()
            logger.error(f"数据保存失败: {e}")
            raise
        finally:
            session.close()

    def get_stock_id_list(self)  -> List[StockBasicInfo] :
        """获取stock_per_day_final表中的所有数据"""
        session=self.Session()
        try:
            # 查询所有数据 todo 再多获取数据 002336
            stock_daily_list = session.query(StockBasicInfo.stock_id,StockBasicInfo.location).where(StockBasicInfo.is_retired==0).order_by(desc(StockBasicInfo.pid)).all()
            
            logger.success(stock_daily_list)
            return stock_daily_list
        except Exception as e:
            logger.error(f"获取日线数据列表失败: {e}")
            raise
        finally:
            session.close()

    def get_stock_per_day(self, stock_id: str, period: str,trade_date: str) -> StockDaily:
        session = self.Session()
        try:
            result = session.query(StockDaily).where(
                StockDaily.stock_id == stock_id,
                StockDaily.scope_type == period,
                StockDaily.trade_date == trade_date
            ).one()
            logger.info(f"成功获取股票{stock_id}数据")
            return result
        except Exception as e:
            logger.warning(f"未找到{stock_id}的{period}周期数据")
            raise
        finally:
            session.close()

    def update_delisted_status(self, delisted_stocks: List[str]) -> int:
        """
        批量更新退市状态
        :param delisted_stocks: 已退市的股票代码列表
        :return: 更新的记录数
        """
        if not delisted_stocks:
            print("没有需要更新的退市股票")
            return 0
            
        session = self.Session()
        try:
            # 使用SQLAlchemy Core的update语句进行批量更新
            stmt = (
                update(StockBasicInfo)
                .where(StockBasicInfo.stock_id.in_(delisted_stocks))
                .values(is_retired=1)
            )
            
            result = session.execute(stmt)
            session.commit()
            
            updated_count = result.rowcount
            print(f"成功更新 {updated_count} 条退市记录")
            return updated_count
            
        except Exception as e:
            session.rollback()
            print(f"更新退市状态时出错: {e}")
            return 0
        finally:
            session.close()