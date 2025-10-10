import pandas as pd
from finance_report.balance_sheet import BalanceSheet
from finance_report.cash_flow_statement import CashFlowStatement

def map_tushare_to_xueqiu(tushare_data):
    """将Tushare利润表数据映射到雪球表结构"""
    
    # 字段映射字典：雪球字段 -> Tushare字段
    mapping = {
        # 基础信息字段
        'ts': 'end_date',                      # 报告期作为时间戳
        'report_name': 'report_type',          # 需要根据report_type生成报告名称
        'ctime': 'ann_date',                   # 公告日期作为创建时间
        
        # 核心利润指标
        'net_profit': 'n_income',                          # 净利润
        'net_profit_atsopc': 'n_income_attr_p',           # 归属于母公司股东的净利润  111765299.46
        'total_revenue': 'total_revenue',                  # 营业总收入
        'op': 'operate_profit',                           # 营业利润
        'income_from_chg_in_fv': 'fv_value_chg_gain',     # 公允价值变动收益
        'invest_incomes_from_rr': 'ass_invest_income',    # 对联营企业和合营企业的投资收益
        'invest_income': 'invest_income',                 # 投资收益
        'exchg_gain': 'forex_gain',                       # 汇兑收益
        'operating_taxes_and_surcharge': 'biz_tax_surchg', # 营业税金及附加
        'asset_impairment_loss': 'assets_impair_loss',    # 资产减值损失
        'non_operating_income': 'non_oper_income',        # 营业外收入
        'non_operating_payout': 'non_oper_exp',           # 营业外支出
        'profit_total_amt': 'total_profit',               # 利润总额
        'minority_gal': 'minority_gain',                  # 少数股东损益     3695819.76
        'basic_eps': 'basic_eps',                         # 基本每股收益
        'dlt_earnings_per_share': 'diluted_eps',          # 稀释每股收益
        
        # 综合收益相关
        'othr_compre_income_atoopc': 'compr_inc_attr_p',      # 归属于母公司股东的其他综合收益
        'othr_compre_income_atms': 'compr_inc_attr_m_s',      # 归属于少数股东的其他综合收益
        'total_compre_income': 't_compr_income',              # 综合收益总额
        'total_compre_income_atsopc': 'compr_inc_attr_p',     # 归属于母公司股东的综合收益总额
        'total_compre_income_atms': 'compr_inc_attr_m_s',     # 归属于少数股东的综合收益总额
        'othr_compre_income': 'oth_compr_income',             # 其他综合收益
        
        # 成本费用相关
        'net_profit_after_nrgal_atsolc': 'net_after_nr_lp_correct',  # 扣除非经常性损益后的净利润
        'income_tax_expenses': 'income_tax',                 # 所得税费用
        'credit_impairment_loss': 'credit_impa_loss',        # 信用减值损失
        'revenue': 'revenue',                                # 营业收入
        'operating_costs': 'total_cogs',                     # 营业总成本
        'operating_cost': 'oper_cost',                       # 营业成本
        'sales_fee': 'sell_exp',                             # 销售费用
        'manage_fee': 'admin_exp',                           # 管理费用
        'financing_expenses': 'fin_exp',                     # 财务费用
        'rad_cost': 'rd_exp',                                # 研发费用
        'finance_cost_interest_fee': 'fin_exp_int_exp',      # 利息费用
        'finance_cost_interest_income': 'fin_exp_int_inc',   # 利息收入
        
        # 其他收益
        'asset_disposal_income': 'asset_disp_income',        # 资产处置收益
        'other_income': 'oth_income',                        # 其他收益
        
        # 持续经营
        'continous_operating_np': 'continued_net_profit',    # 持续经营净利润
        
        # 新增的金融行业字段
        'int_income': 'int_income',                          # 利息收入
        'prem_earned': 'prem_earned',                        # 已赚保费
        'comm_income': 'comm_income',                        # 手续费及佣金收入
        'n_commis_income': 'n_commis_income',                # 手续费及佣金净收入
        'prem_income': 'prem_income',                        # 保险业务收入
        'n_sec_tb_income': 'n_sec_tb_income',                # 代理买卖证券业务净收入
        'n_sec_uw_income': 'n_sec_uw_income',                # 证券承销业务净收入
        'ebit': 'ebit',                                      # 息税前利润
        'ebitda': 'ebitda',                                  # 息税折旧摊销前利润
        'comp_type': 'comp_type',                            # 公司类型
    }
    
    xueqiu_data = {}
    
    # 基础字段映射
    for xueqiu_field, tushare_field in mapping.items():
        if tushare_field in tushare_data and pd.notna(tushare_data[tushare_field]):
            xueqiu_data[xueqiu_field] = tushare_data[tushare_field]
        else:
            xueqiu_data[xueqiu_field] = None
    
    # 特殊处理字段
    # 1. 报告名称生成
    xueqiu_data['report_name'] = generate_report_name(
        tushare_data.get('end_date'), 
        tushare_data.get('report_type')
    )
    
    # 2. 时间戳处理
    if 'end_date' in tushare_data and tushare_data['end_date']:
        xueqiu_data['ts'] = pd.to_datetime(tushare_data['end_date'])
    
    if 'ann_date' in tushare_data and tushare_data['ann_date']:
        xueqiu_data['ctime'] = int(pd.to_datetime(tushare_data['ann_date']).timestamp() * 1000)
    
    # 3. 公司ID
    xueqiu_data['company_id'] = tushare_data.get('ts_code', '')
    
    # 4. 创建时间（使用当前时间）
    xueqiu_data['create_time'] = pd.Timestamp.now()
    
    # 5. 设置默认值字段
    xueqiu_data.update({
        'noncurrent_assets_dispose_gain': None,
        'noncurrent_asset_disposal_loss': None,
        'net_profit_bi': None,
    })
    
    return xueqiu_data


def generate_report_name(end_date, report_type):
    """根据报告期和报告类型生成报告名称"""
    if not end_date:
        return "未知报告"
    
    # 提取年份和季度
    date_obj = pd.to_datetime(end_date)
    year = date_obj.year
    month = date_obj.month
    
    # 根据月份判断报告期
    if month == 3:
        period = "一季报"
    elif month == 6:
        period = "中报"
    elif month == 9:
        period = "三季报"
    elif month == 12:
        period = "年报"
    else:
        period = f"{month}月报"
    
    return f"{year}{period}"


def map_tushare_to_xueqiu_balance(tushare_data):
    """将Tushare资产负债表 数据映射到雪球表结构"""
    bs = BalanceSheet()
    mapping = bs.field_mapping
    
    xueqiu_data = {}
    
    # 基础字段映射
    for tushare_field, xueqiu_field in mapping.items():
        if tushare_field in tushare_data and pd.notna(tushare_data[tushare_field]):
            xueqiu_data[xueqiu_field] = tushare_data[tushare_field]
        else:
            xueqiu_data[xueqiu_field] = None
    
    # 特殊处理字段
    # 1. 报告名称生成
    xueqiu_data['report_name'] = generate_report_name(
        tushare_data.get('end_date'), 
        tushare_data.get('report_type')
    )
    
    # 2. 时间戳处理
    if 'end_date' in tushare_data and tushare_data['end_date']:
        xueqiu_data['ts'] = pd.to_datetime(tushare_data['end_date'])
    
    if 'ann_date' in tushare_data and tushare_data['ann_date']:
        xueqiu_data['ctime'] = int(pd.to_datetime(tushare_data['ann_date']).timestamp() * 1000)
    
    # 3. 公司ID
    xueqiu_data['company_id'] = tushare_data.get('ts_code', '')
    
    # 4. 创建时间（使用当前时间）
    xueqiu_data['create_time'] = pd.Timestamp.now()
    
    # 3. 计算资产负债率
    total_assets = tushare_data.get('total_assets')
    total_liab = tushare_data.get('total_liab')
    if total_assets and total_liab and total_assets > 0:
        xueqiu_data['asset_liab_ratio'] = total_liab / total_assets
    else:
        xueqiu_data['asset_liab_ratio'] = None
    
    return xueqiu_data


def map_tushare_to_xueqiu_cashflowstatment(tushare_data):
    """将Tushare资产负债表 数据映射到雪球表结构"""
    cfs = CashFlowStatement()
    mapping = cfs.field_mapping
    
    xueqiu_data = {}
    
    # 基础字段映射
    for tushare_field, xueqiu_field in mapping.items():
        if tushare_field in tushare_data and pd.notna(tushare_data[tushare_field]):
            xueqiu_data[xueqiu_field] = tushare_data[tushare_field]
        else:
            xueqiu_data[xueqiu_field] = None
    
    # 特殊处理字段
    # 1. 报告名称生成
    xueqiu_data['report_name'] = generate_report_name(
        tushare_data.get('end_date'), 
        tushare_data.get('report_type')
    )
    
    # 2. 时间戳处理
    if 'end_date' in tushare_data and tushare_data['end_date']:
        xueqiu_data['ts'] = pd.to_datetime(tushare_data['end_date'])
    
    if 'ann_date' in tushare_data and tushare_data['ann_date']:
        xueqiu_data['ctime'] = int(pd.to_datetime(tushare_data['ann_date']).timestamp() * 1000)
    
    # 3. 公司ID
    xueqiu_data['company_id'] = tushare_data.get('ts_code', '')
    
    # 4. 创建时间（使用当前时间）
    xueqiu_data['create_time'] = pd.Timestamp.now()
    
    
    return xueqiu_data