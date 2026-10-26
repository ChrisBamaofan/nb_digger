from typing import List, Optional, Tuple
from sqlalchemy import create_engine,update
from sqlalchemy.orm import sessionmaker
from .models import Base
from utils.config_loader import load_db_config
from loguru import logger
import pandas as pd
from sqlalchemy import desc  
from database.models import StockDaily,StockBasicInfo
from sqlalchemy import text
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation

class DBManager:
    @staticmethod
    def quantize_price(value) -> Optional[Decimal]:
        """将价格安全量化为两位 Decimal；NaN/None/无效值返回 None。"""
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        try:
            price = Decimal(str(value))
            if not price.is_finite():
                return None
            return price.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        except (InvalidOperation, ValueError, TypeError):
            return None

    @staticmethod
    def _price_series_to_float(series: pd.Series) -> pd.Series:
        """OHLC 序列统一量化为两位小数 float，无效值填 0。"""
        return series.apply(
            lambda v: float(p) if (p := DBManager.quantize_price(v)) is not None else 0.0
        )

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

    def get_stock_id_list_all(self)  -> List[StockBasicInfo] :
            session=self.Session()
            try:
                stock_daily_list = session.query(StockBasicInfo.stock_id,StockBasicInfo.location,StockBasicInfo.launch_date).where(StockBasicInfo.is_retired==0).all()
                return stock_daily_list
            except Exception as e:
                logger.error(f"获取stock列表失败: {e}")
                raise
            finally:
                session.close()
                
    def get_stock_id_list(self, trade_date: str = '2026-04-03 00:00:00')  -> List[StockBasicInfo] :
        """获取在stock_per_day_final中缺少指定trade_date数据的未退市股票"""
        session=self.Session()
        try:
            sql = text("""
            SELECT sbi.stock_id, sbi.location, sbi.launch_date FROM stock_basic_info sbi
            WHERE sbi.is_retired = 0
            ORDER BY sbi.stock_id ASC
            """)
            
            result = session.execute(sql, {'trade_date': trade_date})
            stock_daily_list = result.fetchall()
            return stock_daily_list # type: ignore
        except Exception as e:
            logger.error(f"获取stock列表失败: {e}")
            raise
        finally:
            session.close()

    def get_new_stock_id_list(self)  -> List[StockBasicInfo] :
        session=self.Session()
        try:
            stock_daily_list = session.query(StockBasicInfo.stock_id,StockBasicInfo.location,StockBasicInfo.launch_date).where(StockBasicInfo.is_retired==0,
                                                                                                                               StockBasicInfo.is_new == 1).all()
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
    def execute_bulk_price_adjustment(self, stock_id, adjustment_diff, adjustment_date,
                                       scope_types=None):
        """
        批量修正股价，默认修正 day/week/month（有 Tushare qfq 源的周期）。
        quarter/year 由聚合逻辑另行处理。
        
        :param scope_types: 要修正的 scope 列表，None 或省略 = ['day','week','month']
        """
        if scope_types is None:
            scope_types = ['day', 'week', 'month']
        
        scopes_placeholder = ','.join([f"'{s}'" for s in scope_types])
        
        sql = text(f"""
        UPDATE stock_per_day_final 
        SET 
            start_price = start_price + :diff,
            end_price = end_price + :diff,
            high_price = high_price + :diff,
            low_price = low_price + :diff
        WHERE 
            stock_id = :stock_id 
            AND trade_date <= :adjust_date
            AND scope_type IN ({scopes_placeholder})
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

    def delete_stock_data_by_date_range(self, stock_id: str, scope_type: str,
                                         start_date: str, end_date: str) -> int:
        """
        删除某股票某 scope 在指定日期范围内的所有数据。
        用于复权修正：删除脏数据后重新拉取插入。
        
        :param start_date: 开始日期 YYYYMMDD（包含）
        :param end_date:   结束日期 YYYYMMDD（包含）
        :return: 删除行数
        """
        sql = text("""
        DELETE FROM stock_per_day_final
        WHERE stock_id = :stock_id
          AND scope_type = :scope_type
          AND trade_date >= :start_date
          AND trade_date <= :end_date
        """)
        try:
            with self.Session() as session:
                result = session.execute(sql, {
                    'stock_id': stock_id,
                    'scope_type': scope_type,
                    'start_date': start_date,
                    'end_date': end_date,
                })
                session.commit()
                deleted = result.rowcount
                logger.info(f"删除 {stock_id} {scope_type} [{start_date}~{end_date}]: {deleted} 行")
                return deleted
        except Exception as e:
            logger.error(f"删除 {stock_id} {scope_type} 失败: {e}")
            raise

    def batch_insert_tushare_data(self, data_df: pd.DataFrame, scope_type: str,
                                   stock_id: str) -> int:
        """
        批量插入 Tushare qfq 数据（已通过 convert_to_db_format_tushare 转换格式），
        覆盖 stock_per_day_final 表。

        与 save_daily_data 区别：本方法使用 INSERT（不复用 session.merge），
        因为调用前已经 DELETE 了旧数据，不需要 upsert 行为，INSERT 更快。
        
        :param data_df:  已经转换好格式的 DataFrame（列名对齐 stock_per_day_final）
        :param scope_type: 'day'/'week'/'month' 等
        :param stock_id:  股票代码
        :return: 插入行数
        """
        if data_df.empty:
            return 0
        session = self.Session()
        try:
            data_df = data_df.copy()
            numeric_cols = ['amount', 'volume', 'start_price', 'end_price',
                            'high_price', 'low_price', 'change_price',
                            'change_percent', 'turnover_ratio']
            for col in numeric_cols:
                if col in data_df.columns:
                    data_df[col] = pd.to_numeric(data_df[col], errors='coerce').fillna(0)

            records = [StockDaily(**row) for row in data_df.to_dict('records')]
            session.add_all(records)
            session.commit()
            logger.info(f"批量插入 {stock_id} {scope_type}: {len(records)} 条")
            return len(records)
        except Exception as e:
            session.rollback()
            logger.error(f"批量插入 {stock_id} {scope_type} 失败: {e}")
            raise
        finally:
            session.close()

    def batch_upsert_tushare_data(self, data_df: pd.DataFrame, scope_type: str,
                                   stock_id: str) -> int:
        """
        批量 upsert（INSERT ON DUPLICATE KEY UPDATE），使用 MySQL 原生语法。
        依赖 stock_per_day_final 表的 UNIQUE KEY (stock_id, scope_type, trade_date)。
        无需先 DELETE，直接原地覆盖，性能远高于 DELETE + INSERT。

        :param data_df:   已通过 convert_to_db_format_tushare 转换好格式的 DataFrame
        :param scope_type: 'day'/'week'/'month' 等
        :param stock_id:  股票代码
        :return: 受影响行数
        """
        if data_df.empty:
            return 0

        data_df = data_df.copy()
        numeric_cols = ['amount', 'volume', 'start_price', 'end_price',
                        'high_price', 'low_price', 'change_price',
                        'change_percent', 'turnover_ratio']
        for col in numeric_cols:
            if col in data_df.columns:
                data_df[col] = pd.to_numeric(data_df[col], errors='coerce').fillna(0)

        # 构建 VALUES 列表
        value_rows = []
        for _, row in data_df.iterrows():
            trade_date = pd.to_datetime(row['trade_date']).strftime('%Y-%m-%d %H:%M:%S')
            value_rows.append(
                f"('{stock_id}', '{trade_date}', '{scope_type}', "
                f"{int(row['amount'])}, {int(row['volume'])}, "
                f"{float(row['start_price'])}, {float(row['end_price'])}, "
                f"{float(row['high_price'])}, {float(row['low_price'])}, "
                f"{float(row['change_price'])}, {float(row['change_percent'])}, "
                f"{float(row['turnover_ratio'])})"
            )

        sql = f"""
        INSERT INTO stock_per_day_final
            (stock_id, trade_date, scope_type,
             amount, volume, start_price, end_price, high_price, low_price,
             change_price, change_percent, turnover_ratio)
        VALUES {','.join(value_rows)}
        ON DUPLICATE KEY UPDATE
            amount = VALUES(amount),
            volume = VALUES(volume),
            start_price = VALUES(start_price),
            end_price = VALUES(end_price),
            high_price = VALUES(high_price),
            low_price = VALUES(low_price),
            change_price = VALUES(change_price),
            change_percent = VALUES(change_percent),
            turnover_ratio = VALUES(turnover_ratio)
        """
        try:
            with self.Session() as session:
                result = session.execute(text(sql))
                session.commit()
                affected = result.rowcount
                logger.info(f"UPSERT {stock_id} {scope_type}: {affected} 行")
                return affected
        except Exception as e:
            logger.error(f"UPSERT {stock_id} {scope_type} 失败: {e}")
            raise

    def get_daily_data_from_db(self, stock_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从 MySQL 读取某只股票的日线数据，用于聚合季度/年度"""
        session = self.Session()
        try:
            sql = text("""
                SELECT trade_date, start_price as open, end_price as close,
                       high_price as high, low_price as low,
                       volume as vol, amount, change_price as `change`,
                       change_percent as pct_chg, turnover_ratio as turnover_rate
                FROM stock_per_day_final
                WHERE stock_id = :stock_id AND scope_type = 'day'
                  AND trade_date >= :start_date AND trade_date <= :end_date
                ORDER BY trade_date ASC
            """)
            result = session.execute(sql, {
                'stock_id': stock_id,
                'start_date': start_date,
                'end_date': end_date,
            })
            rows = result.fetchall()
            if not rows:
                return pd.DataFrame()
            columns = ['trade_date', 'open', 'close', 'high', 'low',
                        'vol', 'amount', 'change', 'pct_chg', 'turnover_rate']
            return pd.DataFrame(rows, columns=columns)
        except Exception as e:
            logger.error(f"查询日线数据失败: stock={stock_id} err={e}")
            return pd.DataFrame()
        finally:
            session.close()

    def get_latest_trade_date_by_scope(self, stock_id: str, scope_type: str):
        """查询某只股票某个周期已入库的最新交易日期，用于增量拉取"""
        session = self.Session()
        try:
            sql = text("""
                SELECT MAX(trade_date) as max_date
                FROM stock_per_day_final
                WHERE stock_id = :stock_id AND scope_type = :scope_type
            """)
            result = session.execute(sql, {'stock_id': stock_id, 'scope_type': scope_type})
            row = result.fetchone()
            if row and row[0]:
                return row[0]
            return None
        except Exception as e:
            logger.error(f"查询最新交易日失败: stock={stock_id} scope={scope_type} err={e}")
            return None
        finally:
            session.close()

    @staticmethod
    def _tushare_change_pct_from_close(tushare_data: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
        """用本周(期)收盘相对上一周期收盘计算涨跌额、涨跌幅(%)，与日线 change/pct_chg 口径一致。"""
        close = pd.to_numeric(tushare_data['close'], errors='coerce')
        prev = close.shift(1)
        ch = close - prev
        pct = (ch / prev * 100).where(prev.notna() & (prev != 0), 0.0)
        return ch.fillna(0.0), pct.fillna(0.0)

    @staticmethod
    def _tushare_effective_change_pct(tushare_data: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
        """
        优先使用接口返回的 change / pct_chg；若缺列、全 NaN 或全为 0（周/月线 pro_bar 常见），则按收盘价推算。
        """
        close = pd.to_numeric(tushare_data['close'], errors='coerce')

        def _usable_series(name: str) -> Optional[pd.Series]:
            if name not in tushare_data.columns:
                return None
            s = pd.to_numeric(tushare_data[name], errors='coerce')
            if not s.notna().any():
                return None
            if (s.fillna(0).abs() < 1e-12).all():
                return None
            return s

        ch_api = _usable_series('change')
        pct_api = _usable_series('pct_chg')

        ch_fb, pct_fb = DBManager._tushare_change_pct_from_close(tushare_data)

        if ch_api is not None:
            change_price = ch_api.fillna(0.0)
        else:
            change_price = ch_fb

        if pct_api is not None:
            change_percent = pct_api.fillna(0.0)
        else:
            change_percent = pct_fb

        return change_price, change_percent

    @staticmethod
    def convert_to_db_format_tushare(stockId:str,tushare_data: pd.DataFrame, period: str) -> pd.DataFrame:
        """
        将Tushare数据转换为数据库表字段格式
        """
        if tushare_data.empty:
            return pd.DataFrame()

        change_price, change_percent = DBManager._tushare_effective_change_pct(tushare_data)

        tor = tushare_data['turnover_rate'] if 'turnover_rate' in tushare_data.columns else 0.0

        return pd.DataFrame({
            'stock_id': stockId,
            'trade_date': tushare_data['trade_date'],
            'amount': tushare_data['amount'],
            'volume': tushare_data['vol'],  # 注意字段名差异
            'start_price': DBManager._price_series_to_float(tushare_data['open']),
            'end_price': DBManager._price_series_to_float(tushare_data['close']),
            'high_price': DBManager._price_series_to_float(tushare_data['high']),
            'low_price': DBManager._price_series_to_float(tushare_data['low']),
            'change_price': change_price,
            'change_percent': change_percent,
            'turnover_ratio': tor,
            'scope_type': period,
            'create_user': 'system'
        })