
class BalanceSheet:
    def __init__(self):
        self.field_mapping = {
        # 基础信息字段
        'end_date': 'ts',                    # 报告期作为时间戳
        'report_type': 'report_name',        # 报告类型
        'ann_date': 'ctime',                 # 公告日期
        
        # 资产类字段
        'total_assets': 'total_assets',                          # 资产总计
        'total_liab': 'total_liab',                              # 负债合计
        'total_hldr_eqy_inc_min_int': 'total_holders_equity',    # 股东权益合计
        'total_liab_hldr_eqy': 'total_liab_and_holders_equity',  # 负债及股东权益总计
        
        # 流动资产
        'money_cap': 'currency_funds',                           # 货币资金
        'trad_asset': 'tradable_fnncl_assets',                   # 交易性金融资产
        'notes_receiv': 'bills_receivable',                      # 应收票据
        'accounts_receiv': 'account_receivable',                 # 应收账款
        'prepayment': 'pre_payment',                             # 预付款项
        'div_receiv': 'dividend_receivable',                     # 应收股利
        'int_receiv': 'interest_receivable',                     # 应收利息
        'oth_receiv': 'othr_receivables',                        # 其他应收款
        'inventories': 'inventory',                              # 存货
        'nca_within_1y': 'nca_due_within_one_year',              # 一年内到期的非流动资产
        'oth_cur_assets': 'othr_current_assets',                 # 其他流动资产
        'total_cur_assets': 'total_current_assets',              # 流动资产合计
        
        # 非流动资产
        'fa_avail_for_sale': 'saleable_finacial_assets',         # 可供出售金融资产
        'htm_invest': 'held_to_maturity_invest',                 # 持有至到期投资
        'lt_eqt_invest': 'lt_equity_invest',                     # 长期股权投资
        'invest_real_estate': 'invest_property',                 # 投资性房地产
        'fix_assets': 'fixed_asset',                             # 固定资产
        'cip': 'construction_in_process',                        # 在建工程
        'intan_assets': 'intangible_assets',                     # 无形资产
        'goodwill': 'goodwill',                                  # 商誉
        'lt_amor_exp': 'lt_deferred_expense',                    # 长期待摊费用
        'defer_tax_assets': 'dt_assets',                         # 递延所得税资产
        'oth_nca': 'othr_noncurrent_assets',                     # 其他非流动资产
        'total_nca': 'total_noncurrent_assets',                  # 非流动资产合计
        
        # 流动负债
        'st_borr': 'st_loan',                                    # 短期借款
        'notes_payable': 'bill_payable',                         # 应付票据
        'acct_payable': 'accounts_payable',                      # 应付账款
        'adv_receipts': 'pre_receivable',                        # 预收款项
        'payroll_payable': 'payroll_payable',                    # 应付职工薪酬
        'taxes_payable': 'tax_payable',                          # 应交税费
        'int_payable': 'interest_payable',                       # 应付利息
        'div_payable': 'dividend_payable',                       # 应付股利
        'oth_payable': 'othr_payables',                          # 其他应付款
        'non_cur_liab_due_1y': 'noncurrent_liab_due_in1y',       # 一年内到期的非流动负债
        'oth_cur_liab': 'othr_current_liab',                     # 其他流动负债
        'total_cur_liab': 'total_current_liab',                  # 流动负债合计
        
        # 非流动负债
        'lt_borr': 'lt_loan',                                    # 长期借款
        'bond_payable': 'bond_payable',                          # 应付债券
        'lt_payable': 'lt_payable',                              # 长期应付款
        'estimated_liab': 'estimated_liab',                      # 预计负债
        'defer_tax_liab': 'dt_liab',                             # 递延所得税负债
        'oth_ncl': 'othr_non_current_liab',                      # 其他非流动负债
        'total_ncl': 'total_noncurrent_liab',                    # 非流动负债合计
        
        # 股东权益
        'total_share': 'shares',                                 # 期末总股本
        'cap_rese': 'capital_reserve',                           # 资本公积金
        'surplus_rese': 'earned_surplus',                        # 盈余公积金
        'undistr_porfit': 'undstrbtd_profit',                    # 未分配利润
        'special_rese': 'special_reserve',                       # 专项储备
        'minority_int': 'minority_equity',                       # 少数股东权益
        'ordin_risk_reser': 'general_risk_provision',            # 一般风险准备
        'forex_differ': 'frgn_currency_convert_diff',            # 外币报表折算差额
        'treasury_share': 'treasury_stock',                      # 减:库存股
        'oth_comp_income': 'othr_compre_income',                 # 其他综合收益
        'oth_eqt_tools': 'othr_equity_instruments',              # 其他权益工具
        
        # 新增字段
        'deriv_liab': 'derivative_fnncl_liab',                   # 衍生金融负债
        'r_and_d': 'dev_expenditure',                            # 研发支出
        'accounts_receiv_bill': 'ar_and_br',                     # 应收票据及应收账款
        'accounts_pay': 'bp_and_ap',                             # 应付票据及应付账款
        'contract_assets': 'contractual_assets',                 # 合同资产
        'contract_liab': 'contract_liabilities',                 # 合同负债
        'hfs_assets': 'to_sale_asset',                           # 持有待售的资产
        'oth_eq_invest': 'other_eq_ins_invest',                  # 其他权益工具投资
        'oth_illiq_fin_assets': 'other_illiquid_fnncl_assets',   # 其他非流动金融资产
        'fix_assets_total': 'fixed_asset_sum',                   # 固定资产(合计)
        'fixed_assets_disp': 'fixed_assets_disposal',            # 固定资产清理
        'cip_total': 'construction_in_process_sum',              # 在建工程(合计)
        'const_materials': 'project_goods_and_material',         # 工程物资
        'produc_bio_assets': 'productive_biological_assets',     # 生产性生物资产
        'oil_and_gas_assets': 'oil_and_gas_asset',               # 油气资产
        'hfs_sales': 'to_sale_debt',                             # 持有待售的负债
        'long_pay_total': 'lt_payable_sum',                      # 长期应付款(合计)
        'oth_eq_ppbond': 'perpetual_bond',                       # 其他权益工具:永续债
    }
    
    