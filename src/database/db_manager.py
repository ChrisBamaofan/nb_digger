from typing import List
from sqlalchemy import create_engine,update
from sqlalchemy.orm import sessionmaker
from .models import Base
from utils.config_loader import load_db_config
from loguru import logger
import pandas as pd
from sqlalchemy import desc  
from database.models import StockDaily,StockBasicInfo
from sqlalchemy import text

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
            # 定义所有数值列的默认值
            numeric_columns_defaults = {
                'amount': 0,
                'volume': 0,
                'start_price': 0,
                'end_price': 0,
                'high_price': 0,
                'low_price': 0,
                'change_price': 0,
                'change_percent': 0,
                'turnover_ratio': 0
            }
            
            # 只填充数据框中实际存在的列
            existing_columns = {col: default for col, default in numeric_columns_defaults.items() 
                            if col in data_df.columns}
            
            # 转换NaN为默认值
            data_df = data_df.fillna(existing_columns)
            
            # 确保数据类型正确
            for col in existing_columns:
                if col in data_df.columns:
                    data_df[col] = pd.to_numeric(data_df[col], errors='coerce').fillna(0)
            
            # 转换为ORM对象
            records = [StockDaily(**row) for row in data_df.to_dict('records')]
            
            # 使用merge实现upsert操作
            for record in records:
                session.merge(record)
                
            session.commit()
            logger.success(f"成功保存/更新{len(records)}条交易数据")
        except Exception as e:
            session.rollback()
            logger.error(f"数据保存失败: {e}")
            raise
        finally:
            session.close()

    def get_stock_id_list(self,is_new:0)  -> List[StockBasicInfo] :
        session=self.Session()
        try:
            stock_daily_list = session.query(StockBasicInfo.stock_id,StockBasicInfo.location).where(StockBasicInfo.is_retired==0,StockBasicInfo.is_new == is_new).all()
            return stock_daily_list
        except Exception as e:
            logger.error(f"获取stock列表失败: {e}")
            raise
        finally:
            session.close()

    def get_new_stock_id_list(self)  -> List[StockBasicInfo] :
        session=self.Session()
        try:
            stock_daily_list = session.query(StockBasicInfo.stock_id,StockBasicInfo.location,StockBasicInfo.launch_date).where(StockBasicInfo.is_retired==0,StockBasicInfo.is_new == 1).all()
            return stock_daily_list
        except Exception as e:
            logger.error(f"获取stock列表失败: {e}")
            raise
        finally:
            session.close()
            
    def get_beijing_stock_list(self)  -> List[StockBasicInfo] :
        session = self.Session()
        try:
            stock_daily_list = session.query(StockBasicInfo.stock_id,StockBasicInfo.location).where(StockBasicInfo.is_retired==0,StockBasicInfo.location == 'china.beijing',StockBasicInfo.pid >= 6391).all()
            return stock_daily_list
        except Exception as e:
            logger.error(f"获取stock列表失败: {e}")
            raise
        finally:
            session.close()

    def get_stock_id_list_point(self) -> List[StockBasicInfo]:
        """获取需要处理的股票列表"""
        session = self.Session()
        try:
            sql = """
            SELECT sbi.* FROM stock_basic_info sbi
            WHERE sbi.is_retired = 0 
            AND sbi.stock_id NOT IN (
                SELECT spdf.stock_id FROM stock_per_day_final spdf
                WHERE spdf.scope_type = 'week' 
                AND spdf.trade_date = '2025-09-26 00:00:00'
            )
            order by sbi.stock_id asc
            """
            
            # 使用from_statement方法直接映射到模型
            stock_list = session.query(StockBasicInfo).from_statement(text(sql)).all()
            
            return stock_list
            
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            raise
        finally:
            session.close()

    def get_stock_basic_info(self,stock_id:str) -> StockBasicInfo:
        session=self.Session()
        try:
            stock_daily_list = session.query(StockBasicInfo.stock_id,StockBasicInfo.launch_date).where(StockBasicInfo.stock_id== stock_id).one()
            
            # logger.success(stock_daily_list)
            return stock_daily_list
        except Exception as e:
            logger.error(f"获取基本数据列表失败: {e}")
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
            return None
        finally:
            session.close()

    def get_stock_per_day_list(self, stock_id: str, period: str,trade_date: str) -> List[StockDaily]:
        session = self.Session()
        try:
            result = session.query(StockDaily).where(
                StockDaily.stock_id == stock_id,
                StockDaily.scope_type == period,
                StockDaily.trade_date <= trade_date
            ).order_by(desc(StockDaily.trade_date)).all()
            logger.info(f"成功获取股票{stock_id}的历史交易数据")
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

            # 'total_stock': float(df[df['item'] == '总股本']['value'].iloc[0]),
            # 'circulating_stock': float(df[df['item'] == '流通股']['value'].iloc[0]),
            # 'total_mark_value': float(df[df['item'] == '总市值']['value'].iloc[0]),
            # 'circulating_market_value': float(df[df['item'] == '流通市值']['value'].iloc[0]),
            # 'industry': df[df['item'] == '行业']['value'].iloc[0]

    def update_basic_info(self, basic_df: dict):
        """更新股票基本信息 - 只更新传入的字段"""
        session = self.Session()
        try:
            # 数据预处理
            stock_id = basic_df['stock_id']
            
            existing = session.query(StockBasicInfo).filter_by(stock_id=stock_id).first()
            if existing:
                # 只更新传入的字段
                update_fields = [
                    'circulating_market_value', 'total_market_value', 'circulating_stock',
                    'total_stock', 'industry', 'launch_date', 'fullname', 'enname',
                    'cnspell', 'market', 'is_hs', 'curr_type', 'delist_date',
                    'act_name', 'act_ent_type', 'is_new'
                ]
                
                for field in update_fields:
                    if field in basic_df and basic_df[field] is not None:
                        setattr(existing, field, basic_df[field])
                
                # 特殊处理 is_retired，如果没有传入则保持原值
                if 'is_retired' in basic_df and basic_df['is_retired'] is not None:
                    existing.is_retired = basic_df['is_retired']
                else:
                    existing.is_retired = 0  # 或者保持原值 existing.is_retired
                    
            else:
                session.add(StockBasicInfo(**basic_df))
                
            session.commit()
            logger.success(f"成功更新股票{stock_id}基本信息")
            
        except Exception as e:
            session.rollback()
            logger.error(f"股票基本信息更新失败: {e}")
            raise
        finally:
            session.close()

    def execute_sql(self,sql):
        session = self.Session()
        try:
            # 使用 text() 包装 SQL 语句
            rows = session.execute(text(sql))
            session.commit()
            return rows
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()


    # 批量更新
    def execute_bulk_price_adjustment(self, stock_id, adjustment_diff, adjustment_date):
        
        sql = text("""
        UPDATE stock_per_day_final 
        SET 
            start_price = start_price + :diff,
            end_price = end_price + :diff,
            high_price = high_price + :diff,
            low_price = low_price + :diff
        WHERE 
            stock_id = :stock_id 
            AND trade_date <= :adjust_date
            AND scope_type = 'week'
        """)
        
        params = {
            'diff': adjustment_diff,
            'stock_id': stock_id,
            'adjust_date': adjustment_date
        }
        
        try:
            with self.Session() as session:
                result = session.execute(
                    sql,
                    params
                )
                session.commit()
                affected_rows = result
                
                print(f"成功调整股票 {stock_id}: 差值={adjustment_diff}, "
                      f"截止日期={adjustment_date}, 影响行数={affected_rows}")
                return affected_rows
                
        except Exception as e:
            print(f"调整股票 {stock_id} 失败: {str(e)}")
            # 可以在这里添加更详细的错误处理逻辑
            raise

    @staticmethod
    def convert_to_db_format_tushare(stockId:str,tushare_data: pd.DataFrame, period: str) -> pd.DataFrame:
        """
        将Tushare数据转换为数据库表字段格式
        """
        if tushare_data.empty:
            return pd.DataFrame()
        
        # Tushare数据字段映射:cite[9]
        return pd.DataFrame({
            'stock_id': stockId,
            'trade_date': tushare_data['trade_date'],
            'amount': tushare_data['amount'],
            'volume': tushare_data['vol'],  # 注意字段名差异
            'start_price': tushare_data['open'],
            'end_price': tushare_data['close'],
            'high_price': tushare_data['high'],
            'low_price': tushare_data['low'],
            'change_price': tushare_data.get('change', 0),  # 涨跌额
            'change_percent': tushare_data.get('pct_chg', 0),  # 涨跌幅
            'turnover_ratio': tushare_data.get('turnover_rate', 0),  # 换手率
            'scope_type': period,
            'create_user': 'system'
        })