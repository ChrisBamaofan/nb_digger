
class CashFlowStatement:
    def __init__(self):
        self.field_mapping = {
            # 基础信息字段
            'end_date': 'ts',                    # 报告期作为时间戳
            'report_type': 'report_name',        # 报告类型
            'ann_date': 'ctime',                 # 公告日期
            
            # 经营活动现金流
            'n_cashflow_act': 'ncf_from_oa',                          # 经营活动产生的现金流量净额
            'c_fr_sale_sg': 'cash_received_of_sales_service',         # 销售商品、提供劳务收到的现金
            'recp_tax_rends': 'refund_of_tax_and_levies',             # 收到的税费返还
            'c_fr_oth_operate_a': 'cash_received_of_othr_oa',         # 收到其他与经营活动有关的现金
            'c_inf_fr_operate_a': 'sub_total_of_ci_from_oa',          # 经营活动现金流入小计
            'c_paid_to_for_empl': 'cash_paid_to_employee_etc',        # 支付给职工以及为职工支付的现金
            'c_paid_for_taxes': 'payments_of_all_taxes',              # 支付的各项税费
            'c_paid_goods_s': 'goods_buy_and_service_cash_pay',       # 购买商品、接受劳务支付的现金
            'oth_cash_pay_oper_act': 'othrcash_paid_relating_to_oa',  # 支付其他与经营活动有关的现金
            'st_cash_out_act': 'sub_total_of_cos_from_oa',            # 经营活动现金流出小计
            
            # 投资活动现金流
            'n_cashflow_inv_act': 'ncf_from_ia',                      # 投资活动产生的现金流量净额
            'c_disp_withdrwl_invest': 'cash_received_of_dspsl_invest', # 收回投资收到的现金
            'c_recp_return_invest': 'invest_income_cash_received',    # 取得投资收益收到的现金
            'n_recp_disp_fiolta': 'net_cash_of_disposal_assets',      # 处置固定资产、无形资产和其他长期资产收回的现金净额
            'n_recp_disp_sobu': 'net_cash_of_disposal_branch',        # 处置子公司及其他营业单位收到的现金净额
            'oth_recp_ral_inv_act': 'cash_received_of_othr_ia',       # 收到其他与投资活动有关的现金
            'stot_inflows_inv_act': 'sub_total_of_ci_from_ia',        # 投资活动现金流入小计
            'c_pay_acq_const_fiolta': 'cash_paid_for_assets',         # 购建固定资产、无形资产和其他长期资产支付的现金
            'c_paid_invest': 'invest_paid_cash',                      # 投资支付的现金
            'oth_pay_ral_inv_act': 'othrcash_paid_relating_to_ia',    # 支付其他与投资活动有关的现金
            'stot_out_inv_act': 'sub_total_of_cos_from_ia',           # 投资活动现金流出小计
            
            # 筹资活动现金流
            'n_cash_flows_fnc_act': 'ncf_from_fa',                    # 筹资活动产生的现金流量净额
            'c_recp_borrow': 'cash_received_of_borrowing',            # 取得借款收到的现金
            'c_recp_cap_contrib': 'cash_received_of_absorb_invest',   # 吸收投资收到的现金
            'proc_issue_bonds': 'cash_received_from_bond_issue',      # 发行债券收到的现金
            'oth_cash_recp_ral_fnc_act': 'cash_received_of_othr_fa',  # 收到其他与筹资活动有关的现金
            'stot_cash_in_fnc_act': 'sub_total_of_ci_from_fa',        # 筹资活动现金流入小计
            'c_prepay_amt_borr': 'cash_pay_for_debt',                 # 偿还债务支付的现金
            'c_pay_dist_dpcp_int_exp': 'cash_paid_of_distribution',   # 分配股利、利润或偿付利息支付的现金
            'incl_dvd_profit_paid_sc_ms': 'branch_paid_to_minority_holder', # 子公司支付给少数股东的股利、利润
            'oth_cashpay_ral_fnc_act': 'othrcash_paid_relating_to_fa', # 支付其他与筹资活动有关的现金
            'stot_cashout_fnc_act': 'sub_total_of_cos_from_fa',       # 筹资活动现金流出小计
            
            # 现金及现金等价物
            'eff_fx_flu_cash': 'effect_of_exchange_chg_on_cce',       # 汇率变动对现金的影响
            'n_incr_cash_cash_equ': 'net_increase_in_cce',            # 现金及现金等价物净增加额
            'c_cash_equ_beg_period': 'initial_balance_of_cce',        # 期初现金及现金等价物余额
            'c_cash_equ_end_period': 'final_balance_of_cce',          # 期末现金及现金等价物余额
            
            # 其他字段
            'n_disp_subs_oth_biz': 'net_cash_amt_from_branch',        # 取得子公司及其他营业单位支付的现金净额
        }
    
    