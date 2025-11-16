-- 创建利润表超级表
CREATE STABLE IF NOT EXISTS income_statement_tushare (
    ts TIMESTAMP,
    ts_code VARCHAR(50),                                      -- TS股票代码
    ann_date VARCHAR(50),                                     -- 公告日期
    f_ann_date VARCHAR(50),                                   -- 实际公告日期
    end_date VARCHAR(50),                                     -- 报告期
    report_type VARCHAR(50),                                  -- 报表类型
    comp_type VARCHAR(50),                                    -- 公司类型
    end_type VARCHAR(50),                                     -- 报告期类型

    -- 每股收益
    basic_eps DOUBLE,                                         -- 基本每股收益
    diluted_eps DOUBLE,                                       -- 稀释每股收益

    -- 收入相关
    total_revenue DOUBLE,                                     -- 营业总收入
    revenue DOUBLE,                                           -- 营业收入
    int_income DOUBLE,                                        -- 利息收入
    prem_earned DOUBLE,                                       -- 已赚保费
    comm_income DOUBLE,                                       -- 手续费及佣金收入
    n_commis_income DOUBLE,                                   -- 手续费及佣金净收入
    n_oth_income DOUBLE,                                      -- 其他经营净收益
    n_oth_b_income DOUBLE,                                   -- 加:其他业务净收益
    prem_income DOUBLE,                                       -- 保险业务收入
    out_prem DOUBLE,                                          -- 减:分出保费
    une_prem_reser DOUBLE,                                    -- 提取未到期责任准备金
    reins_income DOUBLE,                                      -- 其中:分保费收入
    n_sec_tb_income DOUBLE,                                   -- 代理买卖证券业务净收入
    n_sec_uw_income DOUBLE,                                   -- 证券承销业务净收入
    n_asset_mg_income DOUBLE,                                 -- 受托客户资产管理业务净收入
    oth_b_income DOUBLE,                                      -- 其他业务收入

    -- 收益变动
    fv_value_chg_gain DOUBLE,                                 -- 加:公允价值变动净收益
    invest_income DOUBLE,                                     -- 加:投资净收益
    ass_invest_income DOUBLE,                                 -- 其中:对联营企业和合营企业的投资收益
    forex_gain DOUBLE,                                        -- 加:汇兑净收益

    -- 成本费用
    total_cogs DOUBLE,                                        -- 营业总成本
    oper_cost DOUBLE,                                         -- 减:营业成本
    int_exp DOUBLE,                                           -- 减:利息支出
    comm_exp DOUBLE,                                          -- 减:手续费及佣金支出
    biz_tax_surchg DOUBLE,                                    -- 减:营业税金及附加
    sell_exp DOUBLE,                                          -- 减:销售费用
    admin_exp DOUBLE,                                         -- 减:管理费用
    fin_exp DOUBLE,                                           -- 减:财务费用
    assets_impair_loss DOUBLE,                                -- 减:资产减值损失
    prem_refund DOUBLE,                                       -- 退保金
    compens_payout DOUBLE,                                    -- 赔付总支出
    reser_insur_liab DOUBLE,                                  -- 提取保险责任准备金
    div_payt DOUBLE,                                          -- 保户红利支出
    reins_exp DOUBLE,                                         -- 分保费用
    oper_exp DOUBLE,                                          -- 营业支出
    compens_payout_refu DOUBLE,                               -- 减:摊回赔付支出
    insur_reser_refu DOUBLE,                                  -- 减:摊回保险责任准备金
    reins_cost_refund DOUBLE,                                 -- 减:摊回分保费用
    other_bus_cost DOUBLE,                                    -- 其他业务成本

    -- 利润计算
    operate_profit DOUBLE,                                    -- 营业利润
    non_oper_income DOUBLE,                                   -- 加:营业外收入
    non_oper_exp DOUBLE,                                      -- 减:营业外支出
    nca_disploss DOUBLE,                                      -- 其中:减:非流动资产处置净损失
    total_profit DOUBLE,                                      -- 利润总额
    income_tax DOUBLE,                                        -- 所得税费用
    n_income DOUBLE,                                          -- 净利润(含少数股东损益)
    n_income_attr_p DOUBLE,                                   -- 净利润(不含少数股东损益)
    minority_gain DOUBLE,                                     -- 少数股东损益

    -- 综合收益
    oth_compr_income DOUBLE,                                  -- 其他综合收益
    t_compr_income DOUBLE,                                    -- 综合收益总额
    compr_inc_attr_p DOUBLE,                                  -- 归属于母公司(或股东)的综合收益总额
    compr_inc_attr_m_s DOUBLE,                                -- 归属于少数股东的综合收益总额

    -- 财务指标
    ebit DOUBLE,                                              -- 息税前利润
    ebitda DOUBLE,                                            -- 息税折旧摊销前利润
    insurance_exp DOUBLE,                                     -- 保险业务支出
    undist_profit DOUBLE,                                     -- 年初未分配利润
    distable_profit DOUBLE,                                   -- 可分配利润
    rd_exp DOUBLE,                                            -- 研发费用
    fin_exp_int_exp DOUBLE,                                   -- 财务费用:利息费用
    fin_exp_int_inc DOUBLE,                                   -- 财务费用:利息收入

    -- 利润分配
    transfer_surplus_rese DOUBLE,                             -- 盈余公积转入
    transfer_housing_imprest DOUBLE,                          -- 住房周转金转入
    transfer_oth DOUBLE,                                      -- 其他转入
    adj_lossgain DOUBLE,                                      -- 调整以前年度损益
    withdra_legal_surplus DOUBLE,                             -- 提取法定盈余公积
    withdra_legal_pubfund DOUBLE,                             -- 提取法定公益金
    withdra_biz_devfund DOUBLE,                               -- 提取企业发展基金
    withdra_rese_fund DOUBLE,                                 -- 提取储备基金
    withdra_oth_ersu DOUBLE,                                  -- 提取任意盈余公积金
    workers_welfare DOUBLE,                                   -- 职工奖金福利
    distr_profit_shrhder DOUBLE,                              -- 可供股东分配的利润
    prfshare_payable_dvd DOUBLE,                              -- 应付优先股股利
    comshare_payable_dvd DOUBLE,                              -- 应付普通股股利
    capit_comstock_div DOUBLE,                                -- 转作股本的普通股股利

    -- 新增字段
    net_after_nr_lp_correct DOUBLE,                           -- 扣除非经常性损益后的净利润（更正前）
    credit_impa_loss DOUBLE,                                  -- 信用减值损失
    net_expo_hedging_benefits DOUBLE,                         -- 净敞口套期收益
    oth_impair_loss_assets DOUBLE,                            -- 其他资产减值损失
    total_opcost DOUBLE,                                      -- 营业总成本（二）
    amodcost_fin_assets DOUBLE,                               -- 以摊余成本计量的金融资产终止确认收益
    oth_income DOUBLE,                                        -- 其他收益
    asset_disp_income DOUBLE,                                 -- 资产处置收益
    continued_net_profit DOUBLE,                              -- 持续经营净利润
    end_net_profit DOUBLE,                                    -- 终止经营净利润

    update_flag VARCHAR(50),                                  -- 更新标识
    created_time TIMESTAMP,                                   -- 数据创建时间
    updated_time TIMESTAMP                                    -- 数据更新时间
) TAGS (
    location VARCHAR(64),                                     -- 地区标签
    company_id VARCHAR(20),                                   -- 公司ID
    currency VARCHAR(10)                                      -- 货币单位
);



-- 创建资产负债表超级表
CREATE STABLE IF NOT EXISTS balance_sheet_tushare (
    ts TIMESTAMP,                                            -- 时间戳（TDengine要求）
    ts_code VARCHAR(50),                                     -- TS股票代码
    ann_date VARCHAR(50),                                    -- 公告日期
    f_ann_date VARCHAR(50),                                  -- 实际公告日期
    end_date VARCHAR(50),                                    -- 报告期
    report_type VARCHAR(50),                                 -- 报表类型
    comp_type VARCHAR(50),                                   -- 公司类型
    end_type VARCHAR(50),                                    -- 报告期类型
    
    -- 股东权益相关
    total_share DOUBLE,                                      -- 期末总股本
    cap_rese DOUBLE,                                         -- 资本公积金
    undistr_porfit DOUBLE,                                   -- 未分配利润
    surplus_rese DOUBLE,                                     -- 盈余公积金
    special_rese DOUBLE,                                     -- 专项储备
    money_cap DOUBLE,                                        -- 货币资金
    trad_asset DOUBLE,                                       -- 交易性金融资产
    notes_receiv DOUBLE,                                     -- 应收票据
    accounts_receiv DOUBLE,                                  -- 应收账款
    oth_receiv DOUBLE,                                       -- 其他应收款
    prepayment DOUBLE,                                       -- 预付款项
    div_receiv DOUBLE,                                       -- 应收股利
    int_receiv DOUBLE,                                       -- 应收利息
    inventories DOUBLE,                                      -- 存货
    amor_exp DOUBLE,                                         -- 待摊费用
    nca_within_1y DOUBLE,                                    -- 一年内到期的非流动资产
    sett_rsrv DOUBLE,                                        -- 结算备付金
    loanto_oth_bank_fi DOUBLE,                               -- 拆出资金
    premium_receiv DOUBLE,                                   -- 应收保费
    reinsur_receiv DOUBLE,                                   -- 应收分保账款
    reinsur_res_receiv DOUBLE,                               -- 应收分保合同准备金
    pur_resale_fa DOUBLE,                                    -- 买入返售金融资产
    oth_cur_assets DOUBLE,                                   -- 其他流动资产
    total_cur_assets DOUBLE,                                 -- 流动资产合计
    fa_avail_for_sale DOUBLE,                                -- 可供出售金融资产
    htm_invest DOUBLE,                                       -- 持有至到期投资
    lt_eqt_invest DOUBLE,                                    -- 长期股权投资
    invest_real_estate DOUBLE,                               -- 投资性房地产
    time_deposits DOUBLE,                                    -- 定期存款
    oth_assets DOUBLE,                                       -- 其他资产
    lt_rec DOUBLE,                                           -- 长期应收款
    fix_assets DOUBLE,                                       -- 固定资产
    cip DOUBLE,                                              -- 在建工程
    const_materials DOUBLE,                                  -- 工程物资
    fixed_assets_disp DOUBLE,                                -- 固定资产清理
    produc_bio_assets DOUBLE,                                -- 生产性生物资产
    oil_and_gas_assets DOUBLE,                               -- 油气资产
    intan_assets DOUBLE,                                     -- 无形资产
    r_and_d DOUBLE,                                          -- 研发支出
    goodwill DOUBLE,                                         -- 商誉
    lt_amor_exp DOUBLE,                                      -- 长期待摊费用
    defer_tax_assets DOUBLE,                                 -- 递延所得税资产
    decr_in_disbur DOUBLE,                                   -- 发放贷款及垫款
    oth_nca DOUBLE,                                          -- 其他非流动资产
    total_nca DOUBLE,                                        -- 非流动资产合计
    cash_reser_cb DOUBLE,                                    -- 现金及存放中央银行款项
    depos_in_oth_bfi DOUBLE,                                 -- 存放同业和其它金融机构款项
    prec_metals DOUBLE,                                      -- 贵金属
    deriv_assets DOUBLE,                                     -- 衍生金融资产
    rr_reins_une_prem DOUBLE,                                -- 应收分保未到期责任准备金
    rr_reins_outstd_cla DOUBLE,                              -- 应收分保未决赔款准备金
    rr_reins_lins_liab DOUBLE,                               -- 应收分保寿险责任准备金
    rr_reins_lthins_liab DOUBLE,                             -- 应收分保长期健康险责任准备金
    refund_depos DOUBLE,                                     -- 存出保证金
    ph_pledge_loans DOUBLE,                                  -- 保户质押贷款
    refund_cap_depos DOUBLE,                                 -- 存出资本保证金
    indep_acct_assets DOUBLE,                                -- 独立账户资产
    client_depos DOUBLE,                                     -- 其中：客户资金存款
    client_prov DOUBLE,                                      -- 其中：客户备付金
    transac_seat_fee DOUBLE,                                 -- 其中:交易席位费
    invest_as_receiv DOUBLE,                                 -- 应收款项类投资
    total_assets DOUBLE,                                     -- 资产总计
    lt_borr DOUBLE,                                          -- 长期借款
    st_borr DOUBLE,                                          -- 短期借款
    cb_borr DOUBLE,                                          -- 向中央银行借款
    depos_ib_deposits DOUBLE,                                -- 吸收存款及同业存放
    loan_oth_bank DOUBLE,                                    -- 拆入资金
    trading_fl DOUBLE,                                       -- 交易性金融负债
    notes_payable DOUBLE,                                    -- 应付票据
    acct_payable DOUBLE,                                     -- 应付账款
    adv_receipts DOUBLE,                                     -- 预收款项
    sold_for_repur_fa DOUBLE,                                -- 卖出回购金融资产款
    comm_payable DOUBLE,                                     -- 应付手续费及佣金
    payroll_payable DOUBLE,                                  -- 应付职工薪酬
    taxes_payable DOUBLE,                                    -- 应交税费
    int_payable DOUBLE,                                      -- 应付利息
    div_payable DOUBLE,                                      -- 应付股利
    oth_payable DOUBLE,                                      -- 其他应付款
    acc_exp DOUBLE,                                          -- 预提费用
    deferred_inc DOUBLE,                                     -- 递延收益
    st_bonds_payable DOUBLE,                                 -- 应付短期债券
    payable_to_reinsurer DOUBLE,                             -- 应付分保账款
    rsrv_insur_cont DOUBLE,                                  -- 保险合同准备金
    acting_trading_sec DOUBLE,                               -- 代理买卖证券款
    acting_uw_sec DOUBLE,                                    -- 代理承销证券款
    non_cur_liab_due_1y DOUBLE,                              -- 一年内到期的非流动负债
    oth_cur_liab DOUBLE,                                     -- 其他流动负债
    total_cur_liab DOUBLE,                                   -- 流动负债合计
    bond_payable DOUBLE,                                     -- 应付债券
    lt_payable DOUBLE,                                       -- 长期应付款
    specific_payables DOUBLE,                                -- 专项应付款
    estimated_liab DOUBLE,                                   -- 预计负债
    defer_tax_liab DOUBLE,                                   -- 递延所得税负债
    defer_inc_non_cur_liab DOUBLE,                           -- 递延收益-非流动负债
    oth_ncl DOUBLE,                                          -- 其他非流动负债
    total_ncl DOUBLE,                                        -- 非流动负债合计
    depos_oth_bfi DOUBLE,                                    -- 同业和其它金融机构存放款项
    deriv_liab DOUBLE,                                       -- 衍生金融负债
    depos DOUBLE,                                            -- 吸收存款
    agency_bus_liab DOUBLE,                                  -- 代理业务负债
    oth_liab DOUBLE,                                         -- 其他负债
    prem_receiv_adva DOUBLE,                                 -- 预收保费
    depos_received DOUBLE,                                   -- 存入保证金
    ph_invest DOUBLE,                                        -- 保户储金及投资款
    reser_une_prem DOUBLE,                                   -- 未到期责任准备金
    reser_outstd_claims DOUBLE,                              -- 未决赔款准备金
    reser_lins_liab DOUBLE,                                  -- 寿险责任准备金
    reser_lthins_liab DOUBLE,                                -- 长期健康险责任准备金
    indept_acc_liab DOUBLE,                                  -- 独立账户负债
    pledge_borr DOUBLE,                                      -- 其中:质押借款
    indem_payable DOUBLE,                                    -- 应付赔付款
    policy_div_payable DOUBLE,                               -- 应付保单红利
    total_liab DOUBLE,                                       -- 负债合计
    treasury_share DOUBLE,                                   -- 减:库存股
    ordin_risk_reser DOUBLE,                                 -- 一般风险准备
    forex_differ DOUBLE,                                     -- 外币报表折算差额
    invest_loss_unconf DOUBLE,                               -- 未确认的投资损失
    minority_int DOUBLE,                                     -- 少数股东权益
    total_hldr_eqy_exc_min_int DOUBLE,                       -- 股东权益合计(不含少数股东权益)
    total_hldr_eqy_inc_min_int DOUBLE,                       -- 股东权益合计(含少数股东权益)
    total_liab_hldr_eqy DOUBLE,                              -- 负债及股东权益总计
    lt_payroll_payable DOUBLE,                               -- 长期应付职工薪酬
    oth_comp_income DOUBLE,                                  -- 其他综合收益
    oth_eqt_tools DOUBLE,                                    -- 其他权益工具
    oth_eqt_tools_p_shr DOUBLE,                              -- 其他权益工具(优先股)
    lending_funds DOUBLE,                                    -- 融出资金
    acc_receivable DOUBLE,                                   -- 应收款项
    st_fin_payable DOUBLE,                                   -- 应付短期融资款
    payables DOUBLE,                                         -- 应付款项
    hfs_assets DOUBLE,                                       -- 持有待售的资产
    hfs_sales DOUBLE,                                        -- 持有待售的负债
    cost_fin_assets DOUBLE,                                  -- 以摊余成本计量的金融资产
    fair_value_fin_assets DOUBLE,                            -- 以公允价值计量且其变动计入其他综合收益的金融资产
    cip_total DOUBLE,                                        -- 在建工程(合计)(元)
    oth_pay_total DOUBLE,                                    -- 其他应付款(合计)(元)
    long_pay_total DOUBLE,                                   -- 长期应付款(合计)(元)
    debt_invest DOUBLE,                                      -- 债权投资(元)
    oth_debt_invest DOUBLE,                                  -- 其他债权投资(元)
    oth_eq_invest DOUBLE,                                    -- 其他权益工具投资(元)
    oth_illiq_fin_assets DOUBLE,                             -- 其他非流动金融资产(元)
    oth_eq_ppbond DOUBLE,                                    -- 其他权益工具:永续债(元)
    receiv_financing DOUBLE,                                 -- 应收款项融资
    use_right_assets DOUBLE,                                 -- 使用权资产
    lease_liab DOUBLE,                                       -- 租赁负债
    contract_assets DOUBLE,                                  -- 合同资产
    contract_liab DOUBLE,                                    -- 合同负债
    accounts_receiv_bill DOUBLE,                             -- 应收票据及应收账款
    accounts_pay DOUBLE,                                     -- 应付票据及应付账款
    oth_rcv_total DOUBLE,                                    -- 其他应收款(合计)（元）
    fix_assets_total DOUBLE,                                 -- 固定资产(合计)(元)
    update_flag VARCHAR(50),                                 -- 更新标识
    created_time TIMESTAMP,                                  -- 数据创建时间
    updated_time TIMESTAMP                                   -- 数据更新时间
) TAGS (
    location VARCHAR(64),                                    -- 地区标签
    company_id VARCHAR(20),                                  -- 公司ID
    currency VARCHAR(10)                                     -- 货币单位
);


-- 创建现金流量表超级表
CREATE STABLE IF NOT EXISTS cash_flow_tushare (

    ts TIMESTAMP,
    -- 基础信息字段
    ts_code VARCHAR(50),                                      -- TS股票代码
    ann_date VARCHAR(50),                                     -- 公告日期
    f_ann_date VARCHAR(50),                                   -- 实际公告日期
    end_date VARCHAR(50),                                     -- 报告期
    comp_type VARCHAR(50),                                    -- 公司类型
    report_type VARCHAR(50),                                  -- 报表类型
    end_type VARCHAR(50),                                     -- 报告期类型

    -- 净利润和财务费用
    net_profit DOUBLE,                                        -- 净利润
    finan_exp DOUBLE,                                         -- 财务费用

    -- 经营活动现金流入
    c_fr_sale_sg DOUBLE,                                      -- 销售商品、提供劳务收到的现金
    recp_tax_rends DOUBLE,                                    -- 收到的税费返还
    n_depos_incr_fi DOUBLE,                                   -- 客户存款和同业存放款项净增加额
    n_incr_loans_cb DOUBLE,                                   -- 向中央银行借款净增加额
    n_inc_borr_oth_fi DOUBLE,                                 -- 向其他金融机构拆入资金净增加额
    prem_fr_orig_contr DOUBLE,                                -- 收到原保险合同保费取得的现金
    n_incr_insured_dep DOUBLE,                                -- 保户储金净增加额
    n_reinsur_prem DOUBLE,                                    -- 收到再保业务现金净额
    n_incr_disp_tfa DOUBLE,                                   -- 处置交易性金融资产净增加额
    ifc_cash_incr DOUBLE,                                     -- 收取利息和手续费净增加额
    n_incr_disp_faas DOUBLE,                                  -- 处置可供出售金融资产净增加额
    n_incr_loans_oth_bank DOUBLE,                             -- 拆入资金净增加额
    n_cap_incr_repur DOUBLE,                                  -- 回购业务资金净增加额
    c_fr_oth_operate_a DOUBLE,                                -- 收到其他与经营活动有关的现金
    c_inf_fr_operate_a DOUBLE,                                -- 经营活动现金流入小计

    -- 经营活动现金流出
    c_paid_goods_s DOUBLE,                                    -- 购买商品、接受劳务支付的现金
    c_paid_to_for_empl DOUBLE,                                -- 支付给职工以及为职工支付的现金
    c_paid_for_taxes DOUBLE,                                  -- 支付的各项税费
    n_incr_clt_loan_adv DOUBLE,                               -- 客户贷款及垫款净增加额
    n_incr_dep_cbob DOUBLE,                                   -- 存放央行和同业款项净增加额
    c_pay_claims_orig_inco DOUBLE,                            -- 支付原保险合同赔付款项的现金
    pay_handling_chrg DOUBLE,                                 -- 支付手续费的现金
    pay_comm_insur_plcy DOUBLE,                               -- 支付保单红利的现金
    oth_cash_pay_oper_act DOUBLE,                             -- 支付其他与经营活动有关的现金
    st_cash_out_act DOUBLE,                                   -- 经营活动现金流出小计
    n_cashflow_act DOUBLE,                                    -- 经营活动产生的现金流量净额

    -- 投资活动现金流入
    oth_recp_ral_inv_act DOUBLE,                              -- 收到其他与投资活动有关的现金
    c_disp_withdrwl_invest DOUBLE,                            -- 收回投资收到的现金
    c_recp_return_invest DOUBLE,                              -- 取得投资收益收到的现金
    n_recp_disp_fiolta DOUBLE,                                -- 处置固定资产、无形资产和其他长期资产收回的现金净额
    n_recp_disp_sobu DOUBLE,                                  -- 处置子公司及其他营业单位收到的现金净额
    stot_inflows_inv_act DOUBLE,                              -- 投资活动现金流入小计

    -- 投资活动现金流出
    c_pay_acq_const_fiolta DOUBLE,                            -- 购建固定资产、无形资产和其他长期资产支付的现金
    c_paid_invest DOUBLE,                                     -- 投资支付的现金
    n_disp_subs_oth_biz DOUBLE,                               -- 取得子公司及其他营业单位支付的现金净额
    oth_pay_ral_inv_act DOUBLE,                               -- 支付其他与投资活动有关的现金
    n_incr_pledge_loan DOUBLE,                                -- 质押贷款净增加额
    stot_out_inv_act DOUBLE,                                  -- 投资活动现金流出小计
    n_cashflow_inv_act DOUBLE,                                -- 投资活动产生的现金流量净额

    -- 筹资活动现金流入
    c_recp_borrow DOUBLE,                                     -- 取得借款收到的现金
    proc_issue_bonds DOUBLE,                                  -- 发行债券收到的现金
    oth_cash_recp_ral_fnc_act DOUBLE,                         -- 收到其他与筹资活动有关的现金
    stot_cash_in_fnc_act DOUBLE,                              -- 筹资活动现金流入小计
    free_cashflow DOUBLE,                                     -- 企业自由现金流量

    -- 筹资活动现金流出
    c_prepay_amt_borr DOUBLE,                                 -- 偿还债务支付的现金
    c_pay_dist_dpcp_int_exp DOUBLE,                           -- 分配股利、利润或偿付利息支付的现金
    incl_dvd_profit_paid_sc_ms DOUBLE,                        -- 其中:子公司支付给少数股东的股利、利润
    oth_cashpay_ral_fnc_act DOUBLE,                           -- 支付其他与筹资活动有关的现金
    stot_cashout_fnc_act DOUBLE,                              -- 筹资活动现金流出小计
    n_cash_flows_fnc_act DOUBLE,                              -- 筹资活动产生的现金流量净额

    -- 汇率变动和现金净增加
    eff_fx_flu_cash DOUBLE,                                   -- 汇率变动对现金的影响
    n_incr_cash_cash_equ DOUBLE,                              -- 现金及现金等价物净增加额
    c_cash_equ_beg_period DOUBLE,                             -- 期初现金及现金等价物余额
    c_cash_equ_end_period DOUBLE,                             -- 期末现金及现金等价物余额

    -- 其他筹资活动
    c_recp_cap_contrib DOUBLE,                                -- 吸收投资收到的现金
    incl_cash_rec_saims DOUBLE,                               -- 其中:子公司吸收少数股东投资收到的现金

    -- 间接法调整项目
    uncon_invest_loss DOUBLE,                                 -- 未确认投资损失
    prov_depr_assets DOUBLE,                                  -- 加:资产减值准备
    depr_fa_coga_dpba DOUBLE,                                 -- 固定资产折旧、油气资产折耗、生产性生物资产折旧
    amort_intang_assets DOUBLE,                               -- 无形资产摊销
    lt_amort_deferred_exp DOUBLE,                             -- 长期待摊费用摊销
    decr_deferred_exp DOUBLE,                                 -- 待摊费用减少
    incr_acc_exp DOUBLE,                                      -- 预提费用增加
    loss_disp_fiolta DOUBLE,                                  -- 处置固定、无形资产和其他长期资产的损失
    loss_scr_fa DOUBLE,                                       -- 固定资产报废损失
    loss_fv_chg DOUBLE,                                       -- 公允价值变动损失
    invest_loss DOUBLE,                                       -- 投资损失
    decr_def_inc_tax_assets DOUBLE,                           -- 递延所得税资产减少
    incr_def_inc_tax_liab DOUBLE,                             -- 递延所得税负债增加
    decr_inventories DOUBLE,                                  -- 存货的减少
    decr_oper_payable DOUBLE,                                 -- 经营性应收项目的减少
    incr_oper_payable DOUBLE,                                 -- 经营性应付项目的增加
    others DOUBLE,                                            -- 其他
    im_net_cashflow_oper_act DOUBLE,                          -- 经营活动产生的现金流量净额(间接法)

    -- 特殊项目
    conv_debt_into_cap DOUBLE,                                -- 债务转为资本
    conv_copbonds_due_within_1y DOUBLE,                       -- 一年内到期的可转换公司债券
    fa_fnc_leases DOUBLE,                                     -- 融资租入固定资产
    im_n_incr_cash_equ DOUBLE,                                -- 现金及现金等价物净增加额(间接法)
    net_dism_capital_add DOUBLE,                              -- 拆出资金净增加额
    net_cash_rece_sec DOUBLE,                                 -- 代理买卖证券收到的现金净额(元)

    -- 新增字段
    credit_impa_loss DOUBLE,                                  -- 信用减值损失
    use_right_asset_dep DOUBLE,                               -- 使用权资产折旧
    oth_loss_asset DOUBLE,                                    -- 其他资产减值损失
    end_bal_cash DOUBLE,                                      -- 现金的期末余额
    beg_bal_cash DOUBLE,                                      -- 减:现金的期初余额
    end_bal_cash_equ DOUBLE,                                  -- 加:现金等价物的期末余额
    beg_bal_cash_equ DOUBLE,                                  -- 减:现金等价物的期初余额

    update_flag VARCHAR(50),                                  -- 更新标志
    created_time TIMESTAMP,                                   -- 数据创建时间
    updated_time TIMESTAMP                                    -- 数据更新时间
) TAGS (
    location VARCHAR(64),                                     -- 地区标签
    company_id VARCHAR(20),                                   -- 公司ID
    currency VARCHAR(10)                                      -- 货币单位
);


-- 创建利润表同比增长超级表
CREATE STABLE IF NOT EXISTS income_statement_yoy_tushare (
    ts TIMESTAMP,
    ts_code VARCHAR(50),                                      -- TS股票代码
    ann_date VARCHAR(50),                                     -- 公告日期
    end_date VARCHAR(50),                                     -- 报告期
    report_type VARCHAR(50),                                  -- 报表类型
    
    -- 每股收益同比
    basic_eps_abs_yoy DOUBLE,                                 -- 基本每股收益同比绝对值
    basic_eps_pct_yoy DOUBLE,                                 -- 基本每股收益同比百分比
    diluted_eps_abs_yoy DOUBLE,                               -- 稀释每股收益同比绝对值
    diluted_eps_pct_yoy DOUBLE,                               -- 稀释每股收益同比百分比

    -- 收入相关同比
    total_revenue_abs_yoy DOUBLE,                             -- 营业总收入同比绝对值
    total_revenue_pct_yoy DOUBLE,                             -- 营业总收入同比百分比
    revenue_abs_yoy DOUBLE,                                   -- 营业收入同比绝对值
    revenue_pct_yoy DOUBLE,                                   -- 营业收入同比百分比
    int_income_abs_yoy DOUBLE,                                -- 利息收入同比绝对值
    int_income_pct_yoy DOUBLE,                                -- 利息收入同比百分比
    prem_earned_abs_yoy DOUBLE,                               -- 已赚保费同比绝对值
    prem_earned_pct_yoy DOUBLE,                               -- 已赚保费同比百分比
    comm_income_abs_yoy DOUBLE,                               -- 手续费及佣金收入同比绝对值
    comm_income_pct_yoy DOUBLE,                               -- 手续费及佣金收入同比百分比
    n_commis_income_abs_yoy DOUBLE,                           -- 手续费及佣金净收入同比绝对值
    n_commis_income_pct_yoy DOUBLE,                           -- 手续费及佣金净收入同比百分比
    n_oth_income_abs_yoy DOUBLE,                              -- 其他经营净收益同比绝对值
    n_oth_income_pct_yoy DOUBLE,                              -- 其他经营净收益同比百分比
    n_oth_b_income_abs_yoy DOUBLE,                            -- 其他业务净收益同比绝对值
    n_oth_b_income_pct_yoy DOUBLE,                            -- 其他业务净收益同比百分比

    -- 收益变动同比
    fv_value_chg_gain_abs_yoy DOUBLE,                         -- 公允价值变动净收益同比绝对值
    fv_value_chg_gain_pct_yoy DOUBLE,                         -- 公允价值变动净收益同比百分比
    invest_income_abs_yoy DOUBLE,                             -- 投资净收益同比绝对值
    invest_income_pct_yoy DOUBLE,                             -- 投资净收益同比百分比
    ass_invest_income_abs_yoy DOUBLE,                         -- 对联营企业和合营企业的投资收益同比绝对值
    ass_invest_income_pct_yoy DOUBLE,                         -- 对联营企业和合营企业的投资收益同比百分比
    forex_gain_abs_yoy DOUBLE,                                -- 汇兑净收益同比绝对值
    forex_gain_pct_yoy DOUBLE,                                -- 汇兑净收益同比百分比

    -- 成本费用同比
    total_cogs_abs_yoy DOUBLE,                                -- 营业总成本同比绝对值
    total_cogs_pct_yoy DOUBLE,                                -- 营业总成本同比百分比
    oper_cost_abs_yoy DOUBLE,                                 -- 营业成本同比绝对值
    oper_cost_pct_yoy DOUBLE,                                 -- 营业成本同比百分比
    int_exp_abs_yoy DOUBLE,                                   -- 利息支出同比绝对值
    int_exp_pct_yoy DOUBLE,                                   -- 利息支出同比百分比
    comm_exp_abs_yoy DOUBLE,                                  -- 手续费及佣金支出同比绝对值
    comm_exp_pct_yoy DOUBLE,                                  -- 手续费及佣金支出同比百分比
    biz_tax_surchg_abs_yoy DOUBLE,                            -- 营业税金及附加同比绝对值
    biz_tax_surchg_pct_yoy DOUBLE,                            -- 营业税金及附加同比百分比
    sell_exp_abs_yoy DOUBLE,                                  -- 销售费用同比绝对值
    sell_exp_pct_yoy DOUBLE,                                  -- 销售费用同比百分比
    admin_exp_abs_yoy DOUBLE,                                 -- 管理费用同比绝对值
    admin_exp_pct_yoy DOUBLE,                                 -- 管理费用同比百分比
    fin_exp_abs_yoy DOUBLE,                                   -- 财务费用同比绝对值
    fin_exp_pct_yoy DOUBLE,                                   -- 财务费用同比百分比
    assets_impair_loss_abs_yoy DOUBLE,                        -- 资产减值损失同比绝对值
    assets_impair_loss_pct_yoy DOUBLE,                        -- 资产减值损失同比百分比

    -- 利润计算同比
    operate_profit_abs_yoy DOUBLE,                            -- 营业利润同比绝对值
    operate_profit_pct_yoy DOUBLE,                            -- 营业利润同比百分比
    total_profit_abs_yoy DOUBLE,                              -- 利润总额同比绝对值
    total_profit_pct_yoy DOUBLE,                              -- 利润总额同比百分比
    n_income_abs_yoy DOUBLE,                                  -- 净利润(含少数股东损益)同比绝对值
    n_income_pct_yoy DOUBLE,                                  -- 净利润(含少数股东损益)同比百分比
    n_income_attr_p_abs_yoy DOUBLE,                           -- 净利润(不含少数股东损益)同比绝对值
    n_income_attr_p_pct_yoy DOUBLE,                           -- 净利润(不含少数股东损益)同比百分比

    -- 关键指标当前值（便于分析）
    current_total_revenue DOUBLE,                             -- 当前期营业总收入
    current_revenue DOUBLE,                                   -- 当前期营业收入
    current_n_income DOUBLE,                                  -- 当前期净利润
    current_n_income_attr_p DOUBLE,                           -- 当前期归属母公司净利润

    created_time TIMESTAMP,                                   -- 数据创建时间
    updated_time TIMESTAMP                                    -- 数据更新时间
) TAGS (
    location VARCHAR(64),                                     -- 地区标签
    company_id VARCHAR(20),                                   -- 公司ID
    currency VARCHAR(10)                                      -- 货币单位
);


CREATE STABLE IF NOT EXISTS balance_sheet_yoy_tushare (
    ts TIMESTAMP,
    ts_code VARCHAR(50),                                      
    ann_date VARCHAR(50),                                     
    end_date VARCHAR(50),                                     
    report_type VARCHAR(50),                                  
    
    -- 股东权益相关同比变化
    total_share_abs_yoy DOUBLE,
    total_share_pct_yoy DOUBLE,
    cap_rese_abs_yoy DOUBLE,
    cap_rese_pct_yoy DOUBLE,
    undistr_porfit_abs_yoy DOUBLE,
    undistr_porfit_pct_yoy DOUBLE,
    surplus_rese_abs_yoy DOUBLE,
    surplus_rese_pct_yoy DOUBLE,
    special_rese_abs_yoy DOUBLE,
    special_rese_pct_yoy DOUBLE,
    
    -- 流动资产同比变化
    money_cap_abs_yoy DOUBLE,
    money_cap_pct_yoy DOUBLE,
    trad_asset_abs_yoy DOUBLE,
    trad_asset_pct_yoy DOUBLE,
    notes_receiv_abs_yoy DOUBLE,
    notes_receiv_pct_yoy DOUBLE,
    accounts_receiv_abs_yoy DOUBLE,
    accounts_receiv_pct_yoy DOUBLE,
    oth_receiv_abs_yoy DOUBLE,
    oth_receiv_pct_yoy DOUBLE,
    prepayment_abs_yoy DOUBLE,
    prepayment_pct_yoy DOUBLE,
    div_receiv_abs_yoy DOUBLE,
    div_receiv_pct_yoy DOUBLE,
    int_receiv_abs_yoy DOUBLE,
    int_receiv_pct_yoy DOUBLE,
    inventories_abs_yoy DOUBLE,
    inventories_pct_yoy DOUBLE,
    amor_exp_abs_yoy DOUBLE,
    amor_exp_pct_yoy DOUBLE,
    nca_within_1y_abs_yoy DOUBLE,
    nca_within_1y_pct_yoy DOUBLE,
    sett_rsrv_abs_yoy DOUBLE,
    sett_rsrv_pct_yoy DOUBLE,
    loanto_oth_bank_fi_abs_yoy DOUBLE,
    loanto_oth_bank_fi_pct_yoy DOUBLE,
    premium_receiv_abs_yoy DOUBLE,
    premium_receiv_pct_yoy DOUBLE,
    reinsur_receiv_abs_yoy DOUBLE,
    reinsur_receiv_pct_yoy DOUBLE,
    reinsur_res_receiv_abs_yoy DOUBLE,
    reinsur_res_receiv_pct_yoy DOUBLE,
    pur_resale_fa_abs_yoy DOUBLE,
    pur_resale_fa_pct_yoy DOUBLE,
    oth_cur_assets_abs_yoy DOUBLE,
    oth_cur_assets_pct_yoy DOUBLE,
    total_cur_assets_abs_yoy DOUBLE,
    total_cur_assets_pct_yoy DOUBLE,
    
    -- 非流动资产同比变化
    fa_avail_for_sale_abs_yoy DOUBLE,
    fa_avail_for_sale_pct_yoy DOUBLE,
    htm_invest_abs_yoy DOUBLE,
    htm_invest_pct_yoy DOUBLE,
    lt_eqt_invest_abs_yoy DOUBLE,
    lt_eqt_invest_pct_yoy DOUBLE,
    invest_real_estate_abs_yoy DOUBLE,
    invest_real_estate_pct_yoy DOUBLE,
    time_deposits_abs_yoy DOUBLE,
    time_deposits_pct_yoy DOUBLE,
    oth_assets_abs_yoy DOUBLE,
    oth_assets_pct_yoy DOUBLE,
    lt_rec_abs_yoy DOUBLE,
    lt_rec_pct_yoy DOUBLE,
    fix_assets_abs_yoy DOUBLE,
    fix_assets_pct_yoy DOUBLE,
    cip_abs_yoy DOUBLE,
    cip_pct_yoy DOUBLE,
    const_materials_abs_yoy DOUBLE,
    const_materials_pct_yoy DOUBLE,
    fixed_assets_disp_abs_yoy DOUBLE,
    fixed_assets_disp_pct_yoy DOUBLE,
    produc_bio_assets_abs_yoy DOUBLE,
    produc_bio_assets_pct_yoy DOUBLE,
    oil_and_gas_assets_abs_yoy DOUBLE,
    oil_and_gas_assets_pct_yoy DOUBLE,
    intan_assets_abs_yoy DOUBLE,
    intan_assets_pct_yoy DOUBLE,
    r_and_d_abs_yoy DOUBLE,
    r_and_d_pct_yoy DOUBLE,
    goodwill_abs_yoy DOUBLE,
    goodwill_pct_yoy DOUBLE,
    lt_amor_exp_abs_yoy DOUBLE,
    lt_amor_exp_pct_yoy DOUBLE,
    defer_tax_assets_abs_yoy DOUBLE,
    defer_tax_assets_pct_yoy DOUBLE,
    decr_in_disbur_abs_yoy DOUBLE,
    decr_in_disbur_pct_yoy DOUBLE,
    oth_nca_abs_yoy DOUBLE,
    oth_nca_pct_yoy DOUBLE,
    total_nca_abs_yoy DOUBLE,
    total_nca_pct_yoy DOUBLE,
    
    -- 金融行业特殊资产同比变化
    cash_reser_cb_abs_yoy DOUBLE,
    cash_reser_cb_pct_yoy DOUBLE,
    depos_in_oth_bfi_abs_yoy DOUBLE,
    depos_in_oth_bfi_pct_yoy DOUBLE,
    prec_metals_abs_yoy DOUBLE,
    prec_metals_pct_yoy DOUBLE,
    deriv_assets_abs_yoy DOUBLE,
    deriv_assets_pct_yoy DOUBLE,
    rr_reins_une_prem_abs_yoy DOUBLE,
    rr_reins_une_prem_pct_yoy DOUBLE,
    rr_reins_outstd_cla_abs_yoy DOUBLE,
    rr_reins_outstd_cla_pct_yoy DOUBLE,
    rr_reins_lins_liab_abs_yoy DOUBLE,
    rr_reins_lins_liab_pct_yoy DOUBLE,
    rr_reins_lthins_liab_abs_yoy DOUBLE,
    rr_reins_lthins_liab_pct_yoy DOUBLE,
    refund_depos_abs_yoy DOUBLE,
    refund_depos_pct_yoy DOUBLE,
    ph_pledge_loans_abs_yoy DOUBLE,
    ph_pledge_loans_pct_yoy DOUBLE,
    refund_cap_depos_abs_yoy DOUBLE,
    refund_cap_depos_pct_yoy DOUBLE,
    indep_acct_assets_abs_yoy DOUBLE,
    indep_acct_assets_pct_yoy DOUBLE,
    client_depos_abs_yoy DOUBLE,
    client_depos_pct_yoy DOUBLE,
    client_prov_abs_yoy DOUBLE,
    client_prov_pct_yoy DOUBLE,
    transac_seat_fee_abs_yoy DOUBLE,
    transac_seat_fee_pct_yoy DOUBLE,
    invest_as_receiv_abs_yoy DOUBLE,
    invest_as_receiv_pct_yoy DOUBLE,
    total_assets_abs_yoy DOUBLE,
    total_assets_pct_yoy DOUBLE,
    
    -- 流动负债同比变化
    lt_borr_abs_yoy DOUBLE,
    lt_borr_pct_yoy DOUBLE,
    st_borr_abs_yoy DOUBLE,
    st_borr_pct_yoy DOUBLE,
    cb_borr_abs_yoy DOUBLE,
    cb_borr_pct_yoy DOUBLE,
    depos_ib_deposits_abs_yoy DOUBLE,
    depos_ib_deposits_pct_yoy DOUBLE,
    loan_oth_bank_abs_yoy DOUBLE,
    loan_oth_bank_pct_yoy DOUBLE,
    trading_fl_abs_yoy DOUBLE,
    trading_fl_pct_yoy DOUBLE,
    notes_payable_abs_yoy DOUBLE,
    notes_payable_pct_yoy DOUBLE,
    acct_payable_abs_yoy DOUBLE,
    acct_payable_pct_yoy DOUBLE,
    adv_receipts_abs_yoy DOUBLE,
    adv_receipts_pct_yoy DOUBLE,
    sold_for_repur_fa_abs_yoy DOUBLE,
    sold_for_repur_fa_pct_yoy DOUBLE,
    comm_payable_abs_yoy DOUBLE,
    comm_payable_pct_yoy DOUBLE,
    payroll_payable_abs_yoy DOUBLE,
    payroll_payable_pct_yoy DOUBLE,
    taxes_payable_abs_yoy DOUBLE,
    taxes_payable_pct_yoy DOUBLE,
    int_payable_abs_yoy DOUBLE,
    int_payable_pct_yoy DOUBLE,
    div_payable_abs_yoy DOUBLE,
    div_payable_pct_yoy DOUBLE,
    oth_payable_abs_yoy DOUBLE,
    oth_payable_pct_yoy DOUBLE,
    acc_exp_abs_yoy DOUBLE,
    acc_exp_pct_yoy DOUBLE,
    deferred_inc_abs_yoy DOUBLE,
    deferred_inc_pct_yoy DOUBLE,
    st_bonds_payable_abs_yoy DOUBLE,
    st_bonds_payable_pct_yoy DOUBLE,
    payable_to_reinsurer_abs_yoy DOUBLE,
    payable_to_reinsurer_pct_yoy DOUBLE,
    rsrv_insur_cont_abs_yoy DOUBLE,
    rsrv_insur_cont_pct_yoy DOUBLE,
    acting_trading_sec_abs_yoy DOUBLE,
    acting_trading_sec_pct_yoy DOUBLE,
    acting_uw_sec_abs_yoy DOUBLE,
    acting_uw_sec_pct_yoy DOUBLE,
    non_cur_liab_due_1y_abs_yoy DOUBLE,
    non_cur_liab_due_1y_pct_yoy DOUBLE,
    oth_cur_liab_abs_yoy DOUBLE,
    oth_cur_liab_pct_yoy DOUBLE,
    total_cur_liab_abs_yoy DOUBLE,
    total_cur_liab_pct_yoy DOUBLE,
    
    -- 非流动负债同比变化
    bond_payable_abs_yoy DOUBLE,
    bond_payable_pct_yoy DOUBLE,
    lt_payable_abs_yoy DOUBLE,
    lt_payable_pct_yoy DOUBLE,
    specific_payables_abs_yoy DOUBLE,
    specific_payables_pct_yoy DOUBLE,
    estimated_liab_abs_yoy DOUBLE,
    estimated_liab_pct_yoy DOUBLE,
    defer_tax_liab_abs_yoy DOUBLE,
    defer_tax_liab_pct_yoy DOUBLE,
    defer_inc_non_cur_liab_abs_yoy DOUBLE,
    defer_inc_non_cur_liab_pct_yoy DOUBLE,
    oth_ncl_abs_yoy DOUBLE,
    oth_ncl_pct_yoy DOUBLE,
    total_ncl_abs_yoy DOUBLE,
    total_ncl_pct_yoy DOUBLE,
    
    -- 金融行业特殊负债同比变化
    depos_oth_bfi_abs_yoy DOUBLE,
    depos_oth_bfi_pct_yoy DOUBLE,
    deriv_liab_abs_yoy DOUBLE,
    deriv_liab_pct_yoy DOUBLE,
    depos_abs_yoy DOUBLE,
    depos_pct_yoy DOUBLE,
    agency_bus_liab_abs_yoy DOUBLE,
    agency_bus_liab_pct_yoy DOUBLE,
    oth_liab_abs_yoy DOUBLE,
    oth_liab_pct_yoy DOUBLE,
    prem_receiv_adva_abs_yoy DOUBLE,
    prem_receiv_adva_pct_yoy DOUBLE,
    depos_received_abs_yoy DOUBLE,
    depos_received_pct_yoy DOUBLE,
    ph_invest_abs_yoy DOUBLE,
    ph_invest_pct_yoy DOUBLE,
    reser_une_prem_abs_yoy DOUBLE,
    reser_une_prem_pct_yoy DOUBLE,
    reser_outstd_claims_abs_yoy DOUBLE,
    reser_outstd_claims_pct_yoy DOUBLE,
    reser_lins_liab_abs_yoy DOUBLE,
    reser_lins_liab_pct_yoy DOUBLE,
    reser_lthins_liab_abs_yoy DOUBLE,
    reser_lthins_liab_pct_yoy DOUBLE,
    indept_acc_liab_abs_yoy DOUBLE,
    indept_acc_liab_pct_yoy DOUBLE,
    pledge_borr_abs_yoy DOUBLE,
    pledge_borr_pct_yoy DOUBLE,
    indem_payable_abs_yoy DOUBLE,
    indem_payable_pct_yoy DOUBLE,
    policy_div_payable_abs_yoy DOUBLE,
    policy_div_payable_pct_yoy DOUBLE,
    total_liab_abs_yoy DOUBLE,
    total_liab_pct_yoy DOUBLE,
    
    -- 其他权益项目同比变化
    treasury_share_abs_yoy DOUBLE,
    treasury_share_pct_yoy DOUBLE,
    ordin_risk_reser_abs_yoy DOUBLE,
    ordin_risk_reser_pct_yoy DOUBLE,
    forex_differ_abs_yoy DOUBLE,
    forex_differ_pct_yoy DOUBLE,
    invest_loss_unconf_abs_yoy DOUBLE,
    invest_loss_unconf_pct_yoy DOUBLE,
    minority_int_abs_yoy DOUBLE,
    minority_int_pct_yoy DOUBLE,
    total_hldr_eqy_exc_min_int_abs_yoy DOUBLE,
    total_hldr_eqy_exc_min_int_pct_yoy DOUBLE,
    total_hldr_eqy_inc_min_int_abs_yoy DOUBLE,
    total_hldr_eqy_inc_min_int_pct_yoy DOUBLE,
    total_liab_hldr_eqy_abs_yoy DOUBLE,
    total_liab_hldr_eqy_pct_yoy DOUBLE,
    
    -- 其他字段同比变化
    lt_payroll_payable_abs_yoy DOUBLE,
    lt_payroll_payable_pct_yoy DOUBLE,
    oth_comp_income_abs_yoy DOUBLE,
    oth_comp_income_pct_yoy DOUBLE,
    oth_eqt_tools_abs_yoy DOUBLE,
    oth_eqt_tools_pct_yoy DOUBLE,
    oth_eqt_tools_p_shr_abs_yoy DOUBLE,
    oth_eqt_tools_p_shr_pct_yoy DOUBLE,
    lending_funds_abs_yoy DOUBLE,
    lending_funds_pct_yoy DOUBLE,
    acc_receivable_abs_yoy DOUBLE,
    acc_receivable_pct_yoy DOUBLE,
    st_fin_payable_abs_yoy DOUBLE,
    st_fin_payable_pct_yoy DOUBLE,
    payables_abs_yoy DOUBLE,
    payables_pct_yoy DOUBLE,
    hfs_assets_abs_yoy DOUBLE,
    hfs_assets_pct_yoy DOUBLE,
    hfs_sales_abs_yoy DOUBLE,
    hfs_sales_pct_yoy DOUBLE,
    cost_fin_assets_abs_yoy DOUBLE,
    cost_fin_assets_pct_yoy DOUBLE,
    fair_value_fin_assets_abs_yoy DOUBLE,
    fair_value_fin_assets_pct_yoy DOUBLE,
    cip_total_abs_yoy DOUBLE,
    cip_total_pct_yoy DOUBLE,
    oth_pay_total_abs_yoy DOUBLE,
    oth_pay_total_pct_yoy DOUBLE,
    long_pay_total_abs_yoy DOUBLE,
    long_pay_total_pct_yoy DOUBLE,
    debt_invest_abs_yoy DOUBLE,
    debt_invest_pct_yoy DOUBLE,
    oth_debt_invest_abs_yoy DOUBLE,
    oth_debt_invest_pct_yoy DOUBLE,
    oth_eq_invest_abs_yoy DOUBLE,
    oth_eq_invest_pct_yoy DOUBLE,
    oth_illiq_fin_assets_abs_yoy DOUBLE,
    oth_illiq_fin_assets_pct_yoy DOUBLE,
    oth_eq_ppbond_abs_yoy DOUBLE,
    oth_eq_ppbond_pct_yoy DOUBLE,
    receiv_financing_abs_yoy DOUBLE,
    receiv_financing_pct_yoy DOUBLE,
    use_right_assets_abs_yoy DOUBLE,
    use_right_assets_pct_yoy DOUBLE,
    lease_liab_abs_yoy DOUBLE,
    lease_liab_pct_yoy DOUBLE,
    contract_assets_abs_yoy DOUBLE,
    contract_assets_pct_yoy DOUBLE,
    contract_liab_abs_yoy DOUBLE,
    contract_liab_pct_yoy DOUBLE,
    accounts_receiv_bill_abs_yoy DOUBLE,
    accounts_receiv_bill_pct_yoy DOUBLE,
    accounts_pay_abs_yoy DOUBLE,
    accounts_pay_pct_yoy DOUBLE,
    oth_rcv_total_abs_yoy DOUBLE,
    oth_rcv_total_pct_yoy DOUBLE,
    fix_assets_total_abs_yoy DOUBLE,
    fix_assets_total_pct_yoy DOUBLE,

    created_time TIMESTAMP,                                   
    updated_time TIMESTAMP                                    
) TAGS (
    location VARCHAR(64),                                     
    company_id VARCHAR(20),                                   
    currency VARCHAR(10)                                      
);