from sqlalchemy import Column, Integer, String, TIMESTAMP, BigInteger, Numeric,Boolean
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class StockDaily(Base):
    """对应stock_per_day_final表的ORM模型"""
    __tablename__ = 'stock_per_day_final'
    
    pid = Column(Integer, primary_key=True, autoincrement=True, comment='主键')
    stock_id = Column(String(30), nullable=False, default='NULL999999', comment='股票编号，SH000001')
    trade_date = Column(TIMESTAMP, nullable=False, server_default='CURRENT_TIMESTAMP', comment='交易日期')
    amount = Column(BigInteger, nullable=False, default=0, comment='成交额,10亿元')
    volume = Column(BigInteger, nullable=False, default=0, comment='成交量,14万手')
    start_price = Column(Numeric(15, 6), nullable=False, default=0.0, comment='开盘价')
    end_price = Column(Numeric(15, 6), nullable=False, default=0.0, comment='收盘价')
    high_price = Column(Numeric(15, 6), nullable=False, default=0.0, comment='最高价')
    low_price = Column(Numeric(15, 6), nullable=False, default=0.0, comment='最低价')
    change_price = Column(Numeric(15, 6), nullable=False, default=0.0, comment='涨跌额')
    change_percent = Column(Numeric(15, 6), nullable=False, default=0.0, comment='涨跌幅度')
    turnover_ratio = Column(Numeric(15, 6), nullable=False, default=0.0, comment='换手率')
    create_time = Column(TIMESTAMP, nullable=False, server_default='CURRENT_TIMESTAMP', comment='创建时间')
    create_user = Column(String(100), nullable=False, default='system', comment='创建人')
    scope_type = Column(String(10), nullable=False, default='day', comment='周期类型')
    
    __table_args__ = (
        {'comment': '股票每日交易详情表'},
        # 索引在SQLAlchemy中也可以通过__table_args__定义
    )

class StockBasicInfo(Base):
    """对应stock_basic_info表的ORM模型"""
    __tablename__ = 'stock_basic_info'
    
    pid = Column(Integer, primary_key=True, autoincrement=True, comment='主键')
    stock_name = Column(String(30), nullable=False, default='0', comment='股票名,中文或英文')
    stock_id = Column(String(30), nullable=False, default='0', comment='股票编号，SH000001')
    location = Column(String(100), nullable=False, default='china.shanghai', comment='上市交易所，例如，上交所')
    launch_date = Column(TIMESTAMP, nullable=False, server_default='CURRENT_TIMESTAMP', comment='上市日期')
    create_time = Column(TIMESTAMP, nullable=False, server_default='CURRENT_TIMESTAMP', comment='创建时间')
    create_user = Column(String(100), nullable=False, default='system', comment='创建人')
    circulating_market_value = Column(Numeric(16, 2), nullable=False, default=0.0, comment='流通市值')
    total_market_value = Column(Numeric(17, 6), nullable=False, default=0.0, comment='总市值')
    circulating_stock = Column(Numeric(16, 2), nullable=False, default=0.0, comment='流通股')
    total_stock = Column(Numeric(16, 2), nullable=False, default=0.0, comment='总股本')
    industry = Column(String(100), nullable=True, comment='行业')
    is_retired = Column(Boolean,nullable=False, default=0.0, comment='是否退市')

    
    __table_args__ = (
        {'comment': '单个股票简介表'},
        # 索引定义
        # SQLAlchemy 不支持直接在 __table_args__ 中定义索引，需使用 __table_args__ 的其他方式或单独定义
    )
 
    # # 如果你需要定义索引，可以使用如下方式
    # __mapper_args__ = {
    #     'primary_key': [pid],
    #     'indexes': [
    #         # 定义索引
    #         ('stock_id',),  # 对应 UNIQUE KEY `stockIdIdx` (`stock_id`)
    #         ('create_time',),  # 对应 KEY `idx_createtime` (`create_time`)
    #         ('stock_id',),  # 对应 KEY `idx_stock` (`stock_id`)
    #     ]
    # }