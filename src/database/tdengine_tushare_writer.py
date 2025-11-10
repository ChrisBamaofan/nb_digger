from database.tdengine_connector import tdengine
import pandas as pd
from datetime import datetime

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
            print(sql)
