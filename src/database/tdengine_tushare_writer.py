from database.tdengine_connector import tdengine
import pandas as pd
from datetime import datetime
from finance_report.finance_report_constant import FinanceReportConstant

class TDEngineTushareWriter:
    
    @staticmethod
    def insert_income_statement_tushare(tushare_data, stock_id: str):
        
        for _, row in tushare_data.iterrows():
            # 处理NaN值为NULL
            row = row.where(pd.notna(row), None)
            
            # 转换时间戳
            # 使用报告期末日期作为时间戳，格式化为YYYY-MM-DD HH:MM:SS.MS
            report_date = row['end_date']
            report_date = datetime.strptime(report_date, '%Y%m%d').strftime('%Y-%m-%d')
            utc_ts = tdengine._convert_to_utc2(report_date)
            
            # 构建插入SQL
            sql = f"""
                INSERT INTO is_tsh_{stock_id}
                VALUES (
                    '{utc_ts}',                                 
                    '{stock_id}',
                    '{row['ann_date'] or ''}', 
                    '{row['f_ann_date'] or ''}', 
                    '{row['end_date'] or ''}',
                    '{row['report_type'] or ''}', 
                    '{row['comp_type'] or ''}', 
                    '{row['end_type'] or ''}', 
                    {tdengine._format_sql_value(row['basic_eps'])},                    
                    {tdengine._format_sql_value(row['diluted_eps'])},                  
                    {tdengine._format_sql_value(row['total_revenue'])},                
                    {tdengine._format_sql_value(row['revenue'])},                      
                    {tdengine._format_sql_value(row['int_income'])},                   
                    {tdengine._format_sql_value(row['prem_earned'])},                  
                    {tdengine._format_sql_value(row['comm_income'])},                  
                    {tdengine._format_sql_value(row['n_commis_income'])},              
                    {tdengine._format_sql_value(row['n_oth_income'])},                 
                    {tdengine._format_sql_value(row['n_oth_b_income'])},               
                    {tdengine._format_sql_value(row['prem_income'])},                  
                    {tdengine._format_sql_value(row['out_prem'])},                     
                    {tdengine._format_sql_value(row['une_prem_reser'])},               
                    {tdengine._format_sql_value(row['reins_income'])},                 
                    {tdengine._format_sql_value(row['n_sec_tb_income'])},              
                    {tdengine._format_sql_value(row['n_sec_uw_income'])},              
                    {tdengine._format_sql_value(row['n_asset_mg_income'])},            
                    {tdengine._format_sql_value(row['oth_b_income'])},                 
                    {tdengine._format_sql_value(row['fv_value_chg_gain'])},            
                    {tdengine._format_sql_value(row['invest_income'])},                
                    {tdengine._format_sql_value(row['ass_invest_income'])},            
                    {tdengine._format_sql_value(row['forex_gain'])},                   
                    {tdengine._format_sql_value(row['total_cogs'])},                   
                    {tdengine._format_sql_value(row['oper_cost'])},                    
                    {tdengine._format_sql_value(row['int_exp'])},                      
                    {tdengine._format_sql_value(row['comm_exp'])},                     
                    {tdengine._format_sql_value(row['biz_tax_surchg'])},               
                    {tdengine._format_sql_value(row['sell_exp'])},                     
                    {tdengine._format_sql_value(row['admin_exp'])},                    
                    {tdengine._format_sql_value(row['fin_exp'])},                      
                    {tdengine._format_sql_value(row['assets_impair_loss'])},           
                    {tdengine._format_sql_value(row['prem_refund'])},                  
                    {tdengine._format_sql_value(row['compens_payout'])},               
                    {tdengine._format_sql_value(row['reser_insur_liab'])},             
                    {tdengine._format_sql_value(row['div_payt'])},                     
                    {tdengine._format_sql_value(row['reins_exp'])},                    
                    {tdengine._format_sql_value(row['oper_exp'])},                     
                    {tdengine._format_sql_value(row['compens_payout_refu'])},          
                    {tdengine._format_sql_value(row['insur_reser_refu'])},             
                    {tdengine._format_sql_value(row['reins_cost_refund'])},            
                    {tdengine._format_sql_value(row['other_bus_cost'])},               
                    {tdengine._format_sql_value(row['operate_profit'])},               
                    {tdengine._format_sql_value(row['non_oper_income'])},              
                    {tdengine._format_sql_value(row['non_oper_exp'])},                 
                    {tdengine._format_sql_value(row['nca_disploss'])},                 
                    {tdengine._format_sql_value(row['total_profit'])},                 
                    {tdengine._format_sql_value(row['income_tax'])},                   
                    {tdengine._format_sql_value(row['n_income'])},                     
                    {tdengine._format_sql_value(row['n_income_attr_p'])},              
                    {tdengine._format_sql_value(row['minority_gain'])},                
                    {tdengine._format_sql_value(row['oth_compr_income'])},             
                    {tdengine._format_sql_value(row['t_compr_income'])},               
                    {tdengine._format_sql_value(row['compr_inc_attr_p'])},             
                    {tdengine._format_sql_value(row['compr_inc_attr_m_s'])},           
                    {tdengine._format_sql_value(row['ebit'])},                         
                    {tdengine._format_sql_value(row['ebitda'])},                       
                    {tdengine._format_sql_value(row['insurance_exp'])},                
                    {tdengine._format_sql_value(row['undist_profit'])},                
                    {tdengine._format_sql_value(row['distable_profit'])},              
                    {tdengine._format_sql_value(row['rd_exp'])},                       
                    {tdengine._format_sql_value(row['fin_exp_int_exp'])},              
                    {tdengine._format_sql_value(row['fin_exp_int_inc'])},              
                    {tdengine._format_sql_value(row['transfer_surplus_rese'])},        
                    {tdengine._format_sql_value(row['transfer_housing_imprest'])},     
                    {tdengine._format_sql_value(row['transfer_oth'])},                 
                    {tdengine._format_sql_value(row['adj_lossgain'])},                 
                    {tdengine._format_sql_value(row['withdra_legal_surplus'])},        
                    {tdengine._format_sql_value(row['withdra_legal_pubfund'])},        
                    {tdengine._format_sql_value(row['withdra_biz_devfund'])},          
                    {tdengine._format_sql_value(row['withdra_rese_fund'])},            
                    {tdengine._format_sql_value(row['withdra_oth_ersu'])},             
                    {tdengine._format_sql_value(row['workers_welfare'])},              
                    {tdengine._format_sql_value(row['distr_profit_shrhder'])},         
                    {tdengine._format_sql_value(row['prfshare_payable_dvd'])},         
                    {tdengine._format_sql_value(row['comshare_payable_dvd'])},         
                    {tdengine._format_sql_value(row['capit_comstock_div'])},           
                    {tdengine._format_sql_value(row['net_after_nr_lp_correct'])},      
                    {tdengine._format_sql_value(row['credit_impa_loss'])},             
                    {tdengine._format_sql_value(row['net_expo_hedging_benefits'])},    
                    {tdengine._format_sql_value(row['oth_impair_loss_assets'])},       
                    {tdengine._format_sql_value(row['total_opcost'])},                 
                    {tdengine._format_sql_value(row['amodcost_fin_assets'])},          
                    {tdengine._format_sql_value(row['oth_income'])},                   
                    {tdengine._format_sql_value(row['asset_disp_income'])},            
                    {tdengine._format_sql_value(row['continued_net_profit'])},         
                    {tdengine._format_sql_value(row['end_net_profit'])},               
                    '{row['update_flag'] or ''}',                 
                    NOW(),                                        
                    NOW()                                         
                )
            """
            tdengine.execute(sql)
            # print(sql)
    @staticmethod
    def insert_balance_sheet_tushare(tushare_data, stock_id: str):
        
        for _, row in tushare_data.iterrows():
            # 处理NaN值为NULL
            row = row.where(pd.notna(row), None)
            
            # 转换时间戳
            # 使用报告期末日期作为时间戳，格式化为YYYY-MM-DD HH:MM:SS.MS
            report_date = row['end_date']
            report_date = datetime.strptime(report_date, '%Y%m%d').strftime('%Y-%m-%d')
            utc_ts = tdengine._convert_to_utc2(report_date)
            
            sql = f"""
                INSERT INTO bs_tsh_{stock_id}
                VALUES (
                    '{utc_ts}',                                
                    '{stock_id}',                              
                    '{row['ann_date'] or ''}',                 
                    '{row['f_ann_date'] or ''}',               
                    '{row['end_date'] or ''}',                 
                    '{row['report_type'] or ''}',              
                    '{row['comp_type'] or ''}',                
                    '{row['end_type'] or ''}',                 
                    {tdengine._format_sql_value(row['total_share'])},                    
                    {tdengine._format_sql_value(row['cap_rese'])},                       
                    {tdengine._format_sql_value(row['undistr_porfit'])},                 
                    {tdengine._format_sql_value(row['surplus_rese'])},                   
                    {tdengine._format_sql_value(row['special_rese'])},                   
                    {tdengine._format_sql_value(row['money_cap'])},                      
                    {tdengine._format_sql_value(row['trad_asset'])},                     
                    {tdengine._format_sql_value(row['notes_receiv'])},                   
                    {tdengine._format_sql_value(row['accounts_receiv'])},                
                    {tdengine._format_sql_value(row['oth_receiv'])},                     
                    {tdengine._format_sql_value(row['prepayment'])},                     
                    {tdengine._format_sql_value(row['div_receiv'])},                     
                    {tdengine._format_sql_value(row['int_receiv'])},                     
                    {tdengine._format_sql_value(row['inventories'])},                    
                    {tdengine._format_sql_value(row['amor_exp'])},                       
                    {tdengine._format_sql_value(row['nca_within_1y'])},                  
                    {tdengine._format_sql_value(row['sett_rsrv'])},                      
                    {tdengine._format_sql_value(row['loanto_oth_bank_fi'])},             
                    {tdengine._format_sql_value(row['premium_receiv'])},                 
                    {tdengine._format_sql_value(row['reinsur_receiv'])},                 
                    {tdengine._format_sql_value(row['reinsur_res_receiv'])},             
                    {tdengine._format_sql_value(row['pur_resale_fa'])},                  
                    {tdengine._format_sql_value(row['oth_cur_assets'])},                 
                    {tdengine._format_sql_value(row['total_cur_assets'])},               
                    {tdengine._format_sql_value(row['fa_avail_for_sale'])},              
                    {tdengine._format_sql_value(row['htm_invest'])},                     
                    {tdengine._format_sql_value(row['lt_eqt_invest'])},                  
                    {tdengine._format_sql_value(row['invest_real_estate'])},             
                    {tdengine._format_sql_value(row['time_deposits'])},                  
                    {tdengine._format_sql_value(row['oth_assets'])},                     
                    {tdengine._format_sql_value(row['lt_rec'])},                         
                    {tdengine._format_sql_value(row['fix_assets'])},                     
                    {tdengine._format_sql_value(row['cip'])},                            
                    {tdengine._format_sql_value(row['const_materials'])},                
                    {tdengine._format_sql_value(row['fixed_assets_disp'])},              
                    {tdengine._format_sql_value(row['produc_bio_assets'])},              
                    {tdengine._format_sql_value(row['oil_and_gas_assets'])},             
                    {tdengine._format_sql_value(row['intan_assets'])},                   
                    {tdengine._format_sql_value(row['r_and_d'])},                        
                    {tdengine._format_sql_value(row['goodwill'])},                       
                    {tdengine._format_sql_value(row['lt_amor_exp'])},                    
                    {tdengine._format_sql_value(row['defer_tax_assets'])},               
                    {tdengine._format_sql_value(row['decr_in_disbur'])},                 
                    {tdengine._format_sql_value(row['oth_nca'])},                        
                    {tdengine._format_sql_value(row['total_nca'])},                      
                    {tdengine._format_sql_value(row['cash_reser_cb'])},                  
                    {tdengine._format_sql_value(row['depos_in_oth_bfi'])},               
                    {tdengine._format_sql_value(row['prec_metals'])},                    
                    {tdengine._format_sql_value(row['deriv_assets'])},                   
                    {tdengine._format_sql_value(row['rr_reins_une_prem'])},              
                    {tdengine._format_sql_value(row['rr_reins_outstd_cla'])},            
                    {tdengine._format_sql_value(row['rr_reins_lins_liab'])},             
                    {tdengine._format_sql_value(row['rr_reins_lthins_liab'])},           
                    {tdengine._format_sql_value(row['refund_depos'])},                   
                    {tdengine._format_sql_value(row['ph_pledge_loans'])},                
                    {tdengine._format_sql_value(row['refund_cap_depos'])},               
                    {tdengine._format_sql_value(row['indep_acct_assets'])},              
                    {tdengine._format_sql_value(row['client_depos'])},                   
                    {tdengine._format_sql_value(row['client_prov'])},                    
                    {tdengine._format_sql_value(row['transac_seat_fee'])},               
                    {tdengine._format_sql_value(row['invest_as_receiv'])},               
                    {tdengine._format_sql_value(row['total_assets'])},                   
                    {tdengine._format_sql_value(row['lt_borr'])},                        
                    {tdengine._format_sql_value(row['st_borr'])},                        
                    {tdengine._format_sql_value(row['cb_borr'])},                        
                    {tdengine._format_sql_value(row['depos_ib_deposits'])},              
                    {tdengine._format_sql_value(row['loan_oth_bank'])},                  
                    {tdengine._format_sql_value(row['trading_fl'])},                     
                    {tdengine._format_sql_value(row['notes_payable'])},                  
                    {tdengine._format_sql_value(row['acct_payable'])},                   
                    {tdengine._format_sql_value(row['adv_receipts'])},                   
                    {tdengine._format_sql_value(row['sold_for_repur_fa'])},              
                    {tdengine._format_sql_value(row['comm_payable'])},                   
                    {tdengine._format_sql_value(row['payroll_payable'])},                
                    {tdengine._format_sql_value(row['taxes_payable'])},                  
                    {tdengine._format_sql_value(row['int_payable'])},                    
                    {tdengine._format_sql_value(row['div_payable'])},                    
                    {tdengine._format_sql_value(row['oth_payable'])},                    
                    {tdengine._format_sql_value(row['acc_exp'])},                        
                    {tdengine._format_sql_value(row['deferred_inc'])},                   
                    {tdengine._format_sql_value(row['st_bonds_payable'])},               
                    {tdengine._format_sql_value(row['payable_to_reinsurer'])},           
                    {tdengine._format_sql_value(row['rsrv_insur_cont'])},                
                    {tdengine._format_sql_value(row['acting_trading_sec'])},             
                    {tdengine._format_sql_value(row['acting_uw_sec'])},                  
                    {tdengine._format_sql_value(row['non_cur_liab_due_1y'])},            
                    {tdengine._format_sql_value(row['oth_cur_liab'])},                   
                    {tdengine._format_sql_value(row['total_cur_liab'])},                 
                    {tdengine._format_sql_value(row['bond_payable'])},                   
                    {tdengine._format_sql_value(row['lt_payable'])},                     
                    {tdengine._format_sql_value(row['specific_payables'])},              
                    {tdengine._format_sql_value(row['estimated_liab'])},                 
                    {tdengine._format_sql_value(row['defer_tax_liab'])},                 
                    {tdengine._format_sql_value(row['defer_inc_non_cur_liab'])},         
                    {tdengine._format_sql_value(row['oth_ncl'])},                        
                    {tdengine._format_sql_value(row['total_ncl'])},                      
                    {tdengine._format_sql_value(row['depos_oth_bfi'])},                  
                    {tdengine._format_sql_value(row['deriv_liab'])},                     
                    {tdengine._format_sql_value(row['depos'])},                          
                    {tdengine._format_sql_value(row['agency_bus_liab'])},                
                    {tdengine._format_sql_value(row['oth_liab'])},                       
                    {tdengine._format_sql_value(row['prem_receiv_adva'])},               
                    {tdengine._format_sql_value(row['depos_received'])},                 
                    {tdengine._format_sql_value(row['ph_invest'])},                      
                    {tdengine._format_sql_value(row['reser_une_prem'])},                 
                    {tdengine._format_sql_value(row['reser_outstd_claims'])},            
                    {tdengine._format_sql_value(row['reser_lins_liab'])},                
                    {tdengine._format_sql_value(row['reser_lthins_liab'])},              
                    {tdengine._format_sql_value(row['indept_acc_liab'])},                
                    {tdengine._format_sql_value(row['pledge_borr'])},                    
                    {tdengine._format_sql_value(row['indem_payable'])},                  
                    {tdengine._format_sql_value(row['policy_div_payable'])},             
                    {tdengine._format_sql_value(row['total_liab'])},                     
                    {tdengine._format_sql_value(row['treasury_share'])},                 
                    {tdengine._format_sql_value(row['ordin_risk_reser'])},               
                    {tdengine._format_sql_value(row['forex_differ'])},                   
                    {tdengine._format_sql_value(row['invest_loss_unconf'])},             
                    {tdengine._format_sql_value(row['minority_int'])},                   
                    {tdengine._format_sql_value(row['total_hldr_eqy_exc_min_int'])},    
                    {tdengine._format_sql_value(row['total_hldr_eqy_inc_min_int'])},   
                    {tdengine._format_sql_value(row['total_liab_hldr_eqy'])},            
                    {tdengine._format_sql_value(row['lt_payroll_payable'])},             
                    {tdengine._format_sql_value(row['oth_comp_income'])},                
                    {tdengine._format_sql_value(row['oth_eqt_tools'])},                  
                    {tdengine._format_sql_value(row['oth_eqt_tools_p_shr'])},            
                    {tdengine._format_sql_value(row['lending_funds'])},                  
                    {tdengine._format_sql_value(row['acc_receivable'])},                 
                    {tdengine._format_sql_value(row['st_fin_payable'])},                 
                    {tdengine._format_sql_value(row['payables'])},                       
                    {tdengine._format_sql_value(row['hfs_assets'])},                     
                    {tdengine._format_sql_value(row['hfs_sales'])},                      
                    {tdengine._format_sql_value(row['cost_fin_assets'])},                
                    {tdengine._format_sql_value(row['fair_value_fin_assets'])},          
                    {tdengine._format_sql_value(row['cip_total'])},                      
                    {tdengine._format_sql_value(row['oth_pay_total'])},                  
                    {tdengine._format_sql_value(row['long_pay_total'])},                 
                    {tdengine._format_sql_value(row['debt_invest'])},                    
                    {tdengine._format_sql_value(row['oth_debt_invest'])},                
                    {tdengine._format_sql_value(row['oth_eq_invest'])},                  
                    {tdengine._format_sql_value(row['oth_illiq_fin_assets'])},           
                    {tdengine._format_sql_value(row['oth_eq_ppbond'])},                  
                    {tdengine._format_sql_value(row['receiv_financing'])},               
                    {tdengine._format_sql_value(row['use_right_assets'])},               
                    {tdengine._format_sql_value(row['lease_liab'])},                     
                    {tdengine._format_sql_value(row['contract_assets'])},                
                    {tdengine._format_sql_value(row['contract_liab'])},                  
                    {tdengine._format_sql_value(row['accounts_receiv_bill'])},           
                    {tdengine._format_sql_value(row['accounts_pay'])},                   
                    {tdengine._format_sql_value(row['oth_rcv_total'])},                  
                    {tdengine._format_sql_value(row['fix_assets_total'])},  
                    '{row['update_flag'] or ''}',                
                    NOW(),                                       
                    NOW()                                        
                )
                """
            tdengine.execute(sql)
            # print(sql)
    @staticmethod
    def insert_balance_sheet_tushare(tushare_data, stock_id: str):
        
        # 构建字段名列表（排除时间戳字段）
        field_names =  FinanceReportConstant.bs_numeric_fields 
        
        for _, row in tushare_data.iterrows():
            # 处理NaN值为NULL
            row = row.where(pd.notna(row), None)
            
            # 转换时间戳
            report_date = row['end_date']
            report_date = datetime.strptime(report_date, '%Y%m%d').strftime('%Y-%m-%d')
            utc_ts = tdengine._convert_to_utc2(report_date)
            
            # 构建值列表
            values = []
            # 添加时间戳和股票ID '{row['ann_date'] or ''}',
            values.append(f"'{utc_ts}'")
            values.append(f"'{stock_id}'")
            values.append(f"'{row['ann_date'] or ''}  '")
            values.append(f"'{row['f_ann_date']  or ''} '")
            values.append(f"'{row['end_date']  or ''} '")
            values.append(f"'{row['comp_type']  or ''} '")
            values.append(f"'{row['report_type']  or ''} '")
            values.append(f"'{row['end_type']  or ''}'")
            
            # 添加所有资产负债表字段
            for field in field_names:
                values.append(tdengine._format_sql_value(row[field]))
            
            # 添加更新时间标记
            values.append(f"'{row['update_flag'] or ''}'")
            
            # 添加创建和更新时间
            values.append("NOW()")
            values.append("NOW()")
            
            # 构建SQL
            sql = f"""
                INSERT INTO bs_tsh_{stock_id}
                VALUES ({', '.join(values)})
                """
            
            tdengine.execute(sql)
            
    @staticmethod
    def insert_cash_flow_statement_tushare(tushare_data, stock_id: str):
        
        # 构建字段名列表（排除时间戳字段）
        field_names =  FinanceReportConstant.cfs_numeric_fields 
        
        for _, row in tushare_data.iterrows():
            # 处理NaN值为NULL
            row = row.where(pd.notna(row), None)
            
            # 转换时间戳
            report_date = row['end_date']
            report_date = datetime.strptime(report_date, '%Y%m%d').strftime('%Y-%m-%d')
            utc_ts = tdengine._convert_to_utc2(report_date)
            
            # 构建值列表
            values = []
            # 添加时间戳和股票ID '{row['ann_date'] or ''}',
            values.append(f"'{utc_ts}'")
            values.append(f"'{stock_id}'")
            values.append(f"'{row['ann_date'] or ''}  '")
            values.append(f"'{row['f_ann_date']  or ''} '")
            values.append(f"'{row['end_date']  or ''} '")
            values.append(f"'{row['comp_type']  or ''} '")
            values.append(f"'{row['report_type']  or ''} '")
            values.append(f"'{row['end_type']  or ''}'")
            
            # 添加所有资产负债表字段
            for field in field_names:
                values.append(tdengine._format_sql_value(row[field]))
            
            # 添加更新时间标记
            values.append(f"'{row['update_flag'] or ''}'")
            
            # 添加创建和更新时间
            values.append("NOW()")
            values.append("NOW()")
            
            # 构建SQL
            sql = f"""
                INSERT INTO cfs_tsh_{stock_id}
                VALUES ({', '.join(values)})
                """
            
            tdengine.execute(sql)