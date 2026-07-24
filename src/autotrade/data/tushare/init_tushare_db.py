# 此文档用于创造数据库和表
import pymysql
from autotrade.coreutils.config import DatabaseInfo

# 获取数据库的信息，包括用户名、密码等
conn = pymysql.connect(host=DatabaseInfo.host, port=DatabaseInfo.port, user=DatabaseInfo.user, passwd=DatabaseInfo.password)
cursor = conn.cursor()

# todo
# 其他表还没有设置主键

def create_option_data():
    cursor.execute("CREATE DATABASE IF NOT EXISTS option_data")  # 创建基础数据库
    cursor.execute("use option_data ")
    # 期权合约信息
    cursor.execute("""CREATE TABLE IF NOT EXISTS option_basic (
            ts_code VARCHAR(20) NOT NULL,
            exchange VARCHAR(20),
            name TEXT,
            per_unit DECIMAL(17,6),
            opt_code TEXT,
            opt_type VARCHAR(20),
            call_put VARCHAR(20),
            exercise_type VARCHAR(20),
            exercise_price DECIMAL(17,6),
            s_month VARCHAR(20),
            maturity_date DATE,
            list_price DECIMAL(16,6),
            list_date DATE,
            delist_date DATE,
            last_edate DATE,
            last_ddate DATE,
            quote_unit VARCHAR(20),
            min_price_chg VARCHAR(20),
            PRIMARY KEY (ts_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    		""")

    # 期权日线行情
    cursor.execute("""CREATE TABLE IF NOT EXISTS option_daily (
                ts_code VARCHAR(20) NOT NULL,
                trade_date DATE NOT NULL,
                exchange VARCHAR(20),
                pre_settle DECIMAL(16,6),
                pre_close DECIMAL(16,6),
                open DECIMAL(16,6),
                high DECIMAL(16,6),
                low DECIMAL(16,6),
                close DECIMAL(16,6),
                settle DECIMAL(16,6),
                vol DECIMAL(17,6),
                amount DECIMAL(17,6),
                oi DECIMAL(17,6),
                PRIMARY KEY (ts_code, trade_date)
            )
            ENGINE=InnoDB
            PARTITION BY RANGE COLUMNS(trade_date) (
                PARTITION p1990 VALUES LESS THAN ('1991-01-01'),
                PARTITION p2000 VALUES LESS THAN ('2001-01-01'),
                PARTITION p2010 VALUES LESS THAN ('2011-01-01'),
                PARTITION p2020 VALUES LESS THAN ('2021-01-01'),
                PARTITION p2030 VALUES LESS THAN ('2031-01-01')
            );

            """)

    conn.commit()

def create_etf_data():
    cursor.execute("CREATE DATABASE IF NOT EXISTS etf_data")
    cursor.execute("USE etf_data")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS etf_basic (
            ts_code VARCHAR(20) NOT NULL,
            csname VARCHAR(100),
            extname VARCHAR(100),
            cname VARCHAR(200),
            index_code VARCHAR(20),
            index_name VARCHAR(200),
            setup_date DATE,
            list_date DATE,
            list_status CHAR(1),
            exchange VARCHAR(10),
            mgr_name VARCHAR(100),
            custod_name VARCHAR(100),
            mgt_fee DECIMAL(6,4),
            etf_type VARCHAR(20),
            PRIMARY KEY (ts_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fund_daily (
            ts_code VARCHAR(20) NOT NULL,
            trade_date DATE NOT NULL,
            open DECIMAL(10,4),
            high DECIMAL(10,4),
            low DECIMAL(10,4),
            close DECIMAL(10,4),
            pre_close DECIMAL(10,4),
            `change` DECIMAL(10,4),
            pct_chg DECIMAL(7,4),
            vol DECIMAL(20,4),
            amount DECIMAL(20,4),
            PRIMARY KEY (ts_code, trade_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fund_adj (
            ts_code VARCHAR(20) NOT NULL,
            trade_date DATE NOT NULL,
            adj_factor DECIMAL(20,8),
            PRIMARY KEY (ts_code, trade_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)
# 用于创造股票基础数据库和里面的表
def create_stock_basic_data_database():
    # 创建股票基础数据数据库，库包含股票财务数据、股票列表、交易日历、公司基本信息等数据
    cursor.execute("CREATE DATABASE IF NOT EXISTS stock_basic_data")  # 创建基础数据库
    cursor.execute("use stock_basic_data ")

    # 创建stock_list表
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS stock_list
                        (ts_code VARCHAR(15),
                        symbol INT,
                        `name` VARCHAR(20),
                        `area`  VARCHAR(10),
                        industry VARCHAR(20),
                        fullname  TEXT,
                        enname TEXT,	
                        cnspell VARCHAR(15),
                        market VARCHAR(5),
                        `exchange` VARCHAR(10),
                        curr_type VARCHAR(5),
                        list_status VARCHAR(5),
                        list_date DATE,
                        delist_date DATE,
                        is_hs VARCHAR(5)
                        );
            """
    )

    # 创建trade_calendar表
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS trade_calendar
                            (exchange VARCHAR(5),
                            cal_date DATE,
                            is_open int,
                            pretrade_date DATE
                            );
                """
    )

    # 用于创建company_basic_info表
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS company_basic_info
        		(ts_code VARCHAR(20),exchange VARCHAR(20),chairman TEXT,manager TEXT,secretary TEXT,
        		reg_capital DECIMAL(20,6),setup_date DATE,province VARCHAR(20),city VARCHAR(20),
        		website TEXT,email TEXT,employees DECIMAL(18,6)
        		);
        		""")

    # 用于创建ipo_info表
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS ipo_info
                            (ts_code VARCHAR(20),	
                            sub_code INT,
                            `name`  VARCHAR(20),
                            ipo_date DATE,
                            issue_date DATE,
                            amount INT,
                            market_amount DECIMAL,
                            price DECIMAL,
                            pe DECIMAL,	
                            limit_amount DECIMAL,
                            funds DECIMAL,
                            ballot DECIMAL
                            );
                """
    )

    # 用于创建other_basic_info表
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS other_basic_info
                            (trade_date	DATE,
                            ts_code	VARCHAR(20),
                            name VARCHAR(20),
                            industry VARCHAR(10),
                            area VARCHAR(5),
                            pe	DECIMAL,
                            float_share DECIMAL,
                            total_share	DECIMAL,
                            total_assets DECIMAL,
                            liquid_assets DECIMAL,
                            fixed_assets DECIMAL,
                            reserved DECIMAL,
                            reserved_pershare DECIMAL,
                            eps DECIMAL,
                            bvps DECIMAL,
                            pb DECIMAL,
                            undp DECIMAL,
                            per_undp DECIMAL,
                            rev_yoy DECIMAL,
                            profit_yoy DECIMAL,
                            gpr	DECIMAL,
                            npr	DECIMAL,
                            holder_num INT
                            );
                """
    )

    # 上市公司管理层
    cursor.execute("""CREATE TABLE IF NOT EXISTS stk_managers
    		(ts_code VARCHAR(20),ann_date DATE,name TEXT,gender VARCHAR(20),lev VARCHAR(20),title VARCHAR(50),
    		edu VARCHAR(20),national VARCHAR(20),birthday VARCHAR(20),begin_date TEXT,end_date TEXT,resume TEXT
    		);
    		""")

    # 管理层薪酬和持股
    cursor.execute("""CREATE TABLE IF NOT EXISTS stk_rewards
    		(ts_code VARCHAR(20),ann_date DATE,end_date DATE,name TEXT,title TEXT,reward DECIMAL(20,6),
    		hold_vol DECIMAL(18,6)
    		);
    		""")

    # 用于创建利润表
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS profit_table
                            (ts_code VARCHAR(20),ann_date DATE,f_ann_date DATE,end_date DATE,report_type INT,
                            comp_type	INT,end_type INT,basic_eps DECIMAL(20,4),diluted_eps DECIMAL(20,4),
                            total_revenue DECIMAL(20,4),revenue DECIMAL(20,4),int_income DECIMAL(20,4),
                            prem_earned	DECIMAL(20,4),comm_income DECIMAL(20,4),n_commis_income	DECIMAL(20,4),
                            n_oth_income DECIMAL(20,4),n_oth_b_income DECIMAL(20,4),prem_income	DECIMAL(20,4),
                            out_prem DECIMAL(20,4),une_prem_reser DECIMAL(20,4),reins_income DECIMAL(20,4),
                            n_sec_tb_income DECIMAL(20,4),n_sec_uw_income DECIMAL(20,4),n_asset_mg_income DECIMAL(20,4),
                            oth_b_income DECIMAL(20,4),fv_value_chg_gain DECIMAL(20,4),invest_income DECIMAL(20,4),
                            ass_invest_income DECIMAL(20,4),forex_gain DECIMAL(20,4),total_cogs DECIMAL(20,4),
                            oper_cost DECIMAL(20,4),int_exp DECIMAL(20,4),comm_exp DECIMAL(20,4),
                            biz_tax_surchg DECIMAL(20,4),sell_exp DECIMAL(20,4),admin_exp DECIMAL(20,4),
                            fin_exp DECIMAL(20,4),assets_impair_loss DECIMAL(20,4),prem_refund DECIMAL(20,4),
                            compens_payout DECIMAL(20,4),reser_insur_liab DECIMAL(20,4),div_payt DECIMAL(20,4),
                            reins_exp DECIMAL(20,4),oper_exp DECIMAL(20,4),compens_payout_refu DECIMAL(20,4),
                            insur_reser_refu DECIMAL(20,4),reins_cost_refund DECIMAL(20,4),other_bus_cost DECIMAL(20,4),
                            operate_profit DECIMAL(20,4),non_oper_income DECIMAL(20,4),non_oper_exp DECIMAL(20,4),
                            nca_disploss DECIMAL(20,4),total_profit DECIMAL(20,4),income_tax DECIMAL(20,4),
                            n_income DECIMAL(20,4),n_income_attr_p DECIMAL(20,4),minority_gain DECIMAL(20,4),
                            oth_compr_income DECIMAL(20,4),t_compr_income DECIMAL(20,4),compr_inc_attr_p DECIMAL(20,4),
                            compr_inc_attr_m_s DECIMAL(20,4),ebit DECIMAL(20,4),ebitda DECIMAL(20,4),
                            insurance_exp DECIMAL(20,4),undist_profit DECIMAL(20,4),distable_profit DECIMAL(20,4),
                            rd_exp DECIMAL(20,4),fin_exp_int_exp DECIMAL(20,4),fin_exp_int_inc DECIMAL(20,4),
                            transfer_surplus_rese DECIMAL(20,4),transfer_housing_imprest DECIMAL(20,4),
                            transfer_oth DECIMAL(20,4),adj_lossgain DECIMAL(20,4),
                            withdra_legal_surplus DECIMAL(20,4),withdra_legal_pubfund DECIMAL(20,4),
                            withdra_biz_devfund DECIMAL(20,4),withdra_rese_fund DECIMAL(20,4),
                            withdra_oth_ersu DECIMAL(20,4),workers_welfare DECIMAL(20,4),
                            distr_profit_shrhder DECIMAL(20,4),prfshare_payable_dvd DECIMAL(20,4),
                            comshare_payable_dvd DECIMAL(20,4),capit_comstock_div DECIMAL(20,4),
                            net_after_nr_lp_correct DECIMAL(20,4),oth_income DECIMAL(20,4),
                            asset_disp_income DECIMAL(20,4),continued_net_profit DECIMAL(20,4),
                            end_net_profit DECIMAL(20,4),credit_impa_loss DECIMAL(20,4),
                            net_expo_hedging_benefits DECIMAL(20,4),oth_impair_loss_assets DECIMAL(20,4),
                            total_opcost DECIMAL(20,4),amodcost_fin_assets DECIMAL(20,4),update_flag DECIMAL(20,4)
                            );
                """
    )

    # 用于创建资产负债表
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS balancesheet
                            (ts_code VARCHAR(20),ann_date DATE,f_ann_date DATE,end_date DATE,report_type INT,
                            comp_type INT,
                            end_type INT,total_share DECIMAL(20,4),cap_rese DECIMAL(20,4),undistr_porfit DECIMAL(20,4),
                            surplus_rese DECIMAL(20,4),special_rese DECIMAL(20,4),money_cap DECIMAL(20,4),
                            trad_asset DECIMAL(20,4),notes_receiv DECIMAL(20,4),accounts_receiv DECIMAL(20,4),
                            oth_receiv DECIMAL(20,4),prepayment DECIMAL(20,4),div_receiv DECIMAL(20,4),
                            int_receiv DECIMAL(20,4),
                            inventories DECIMAL(20,4),amor_exp DECIMAL(20,4),nca_within_1y DECIMAL(20,4),
                            sett_rsrv DECIMAL(20,4),
                            loanto_oth_bank_fi DECIMAL(20,4),premium_receiv DECIMAL(20,4),reinsur_receiv DECIMAL(20,4),
                            reinsur_res_receiv DECIMAL(20,4),pur_resale_fa DECIMAL(20,4),oth_cur_assets DECIMAL(20,4),
                            total_cur_assets DECIMAL(20,4),fa_avail_for_sale DECIMAL(20,4),htm_invest DECIMAL(20,4),
                            lt_eqt_invest DECIMAL(20,4),invest_real_estate DECIMAL(20,4),time_deposits DECIMAL(20,4),
                            oth_assets DECIMAL(20,4),lt_rec DECIMAL(20,4),fix_assets DECIMAL(20,4),cip DECIMAL(20,4),
                            const_materials DECIMAL(20,4),fixed_assets_disp DECIMAL(20,4),
                            produc_bio_assets DECIMAL(20,4),
                            oil_and_gas_assets DECIMAL(20,4),intan_assets DECIMAL(20,4),r_and_d DECIMAL(20,4),
                            goodwill DECIMAL(20,4),lt_amor_exp DECIMAL(20,4),defer_tax_assets DECIMAL(20,4),
                            decr_in_disbur DECIMAL(20,4),oth_nca DECIMAL(20,4),total_nca DECIMAL(20,4),
                            cash_reser_cb DECIMAL(20,4),
                            depos_in_oth_bfi DECIMAL(20,4),prec_metals DECIMAL(20,4),deriv_assets DECIMAL(20,4),
                            rr_reins_une_prem DECIMAL(20,4),rr_reins_outstd_cla DECIMAL(20,4),
                            rr_reins_lins_liab DECIMAL(20,4),
                            rr_reins_lthins_liab DECIMAL(20,4),refund_depos DECIMAL(20,4),ph_pledge_loans DECIMAL(20,4),
                            refund_cap_depos DECIMAL(20,4),indep_acct_assets DECIMAL(20,4),client_depos DECIMAL(20,4),
                            client_prov DECIMAL(20,4),transac_seat_fee DECIMAL(20,4),invest_as_receiv DECIMAL(20,4),
                            total_assets DECIMAL(20,4),lt_borr DECIMAL(20,4),st_borr DECIMAL(20,4),cb_borr DECIMAL(20,4),
                            depos_ib_deposits DECIMAL(20,4),loan_oth_bank DECIMAL(20,4),trading_fl DECIMAL(20,4),
                            notes_payable DECIMAL(20,4),acct_payable DECIMAL(20,4),adv_receipts DECIMAL(20,4),
                            sold_for_repur_fa DECIMAL(20,4),comm_payable DECIMAL(20,4),payroll_payable DECIMAL(20,4),
                            taxes_payable DECIMAL(20,4),int_payable DECIMAL(20,4),div_payable DECIMAL(20,4),
                            oth_payable DECIMAL(20,4),acc_exp DECIMAL(20,4),deferred_inc DECIMAL(20,4),
                            st_bonds_payable DECIMAL(20,4),payable_to_reinsurer DECIMAL(20,4),
                            rsrv_insur_cont DECIMAL(20,4),
                            acting_trading_sec DECIMAL(20,4),acting_uw_sec DECIMAL(20,4),
                            non_cur_liab_due_1y DECIMAL(20,4),
                            oth_cur_liab DECIMAL(20,4),total_cur_liab DECIMAL(20,4),bond_payable DECIMAL(20,4),
                            lt_payable DECIMAL(20,4),specific_payables DECIMAL(20,4),estimated_liab DECIMAL(20,4),
                            defer_tax_liab DECIMAL(20,4),defer_inc_non_cur_liab DECIMAL(20,4),oth_ncl DECIMAL(20,4),
                            total_ncl DECIMAL(20,4),depos_oth_bfi DECIMAL(20,4),
                            deriv_liab DECIMAL(20,4),depos DECIMAL(20,4),
                            agency_bus_liab DECIMAL(20,4),oth_liab DECIMAL(20,4),prem_receiv_adva DECIMAL(20,4),
                            depos_received DECIMAL(20,4),ph_invest DECIMAL(20,4),reser_une_prem DECIMAL(20,4),
                            reser_outstd_claims DECIMAL(20,4),reser_lins_liab DECIMAL(20,4),
                            reser_lthins_liab DECIMAL(20,4),
                            indept_acc_liab DECIMAL(20,4),pledge_borr DECIMAL(20,4),indem_payable DECIMAL(20,4),
                            policy_div_payable DECIMAL(20,4),total_liab DECIMAL(20,4),treasury_share DECIMAL(20,4),
                            ordin_risk_reser DECIMAL(20,4),forex_differ DECIMAL(20,4),invest_loss_unconf DECIMAL(20,4),
                            minority_int DECIMAL(20,4),total_hldr_eqy_exc_min_int DECIMAL(20,4),
                            total_hldr_eqy_inc_min_int DECIMAL(20,4),total_liab_hldr_eqy DECIMAL(20,4),
                            lt_payroll_payable DECIMAL(20,4),oth_comp_income DECIMAL(20,4),oth_eqt_tools DECIMAL(20,4),
                            oth_eqt_tools_p_shr DECIMAL(20,4),lending_funds DECIMAL(20,4),acc_receivable DECIMAL(20,4),
                            st_fin_payable DECIMAL(20,4),payables DECIMAL(20,4),
                            hfs_assets DECIMAL(20,4),hfs_sales DECIMAL(20,4),
                            cost_fin_assets DECIMAL(20,4),
                            fair_value_fin_assets DECIMAL(20,4),contract_assets DECIMAL(20,4),
                            contract_liab DECIMAL(20,4),accounts_receiv_bill DECIMAL(20,4),accounts_pay DECIMAL(20,4),
                            oth_rcv_total DECIMAL(20,4),fix_assets_total DECIMAL(20,4),cip_total DECIMAL(20,4),
                            oth_pay_total DECIMAL(20,4),long_pay_total DECIMAL(20,4),debt_invest DECIMAL(20,4),
                            oth_debt_invest DECIMAL(20,4),update_flag DECIMAL(20,4),oth_eq_invest DECIMAL(20,4),
                            oth_illiq_fin_assets DECIMAL(20,4),oth_eq_ppbond DECIMAL(20,4),
                            receiv_financing DECIMAL(20,4),
                            use_right_assets DECIMAL(20,4),lease_liab DECIMAL(20,4)
                            );
                """
    )

    # 用于创建现金流量表
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS cashflow
                            (ts_code VARCHAR(20),ann_date DATE,f_ann_date DATE,end_date DATE,comp_type INT,
                            report_type INT,end_type INT,
                            net_profit DECIMAL(20,4),finan_exp DECIMAL(20,4),c_fr_sale_sg DECIMAL(20,4),
                            recp_tax_rends DECIMAL(20,4),n_depos_incr_fi DECIMAL(20,4),n_incr_loans_cb DECIMAL(20,4),
                            n_inc_borr_oth_fi DECIMAL(20,4),prem_fr_orig_contr DECIMAL(20,4),
                            n_incr_insured_dep DECIMAL(20,4),
                            n_reinsur_prem DECIMAL(20,4),n_incr_disp_tfa DECIMAL(20,4),ifc_cash_incr DECIMAL(20,4),
                            n_incr_disp_faas DECIMAL(20,4),n_incr_loans_oth_bank DECIMAL(20,4),
                            n_cap_incr_repur DECIMAL(20,4),c_fr_oth_operate_a DECIMAL(20,4),
                            c_inf_fr_operate_a DECIMAL(20,4),c_paid_goods_s DECIMAL(20,4),
                            c_paid_to_for_empl DECIMAL(20,4),c_paid_for_taxes DECIMAL(20,4),
                            n_incr_clt_loan_adv DECIMAL(20,4),n_incr_dep_cbob DECIMAL(20,4),
                            c_pay_claims_orig_inco DECIMAL(20,4),pay_handling_chrg DECIMAL(20,4),
                            pay_comm_insur_plcy DECIMAL(20,4),oth_cash_pay_oper_act DECIMAL(20,4),
                            st_cash_out_act DECIMAL(20,4),n_cashflow_act DECIMAL(20,4),
                            oth_recp_ral_inv_act DECIMAL(20,4),c_disp_withdrwl_invest DECIMAL(20,4),
                            c_recp_return_invest DECIMAL(20,4),n_recp_disp_fiolta DECIMAL(20,4),
                            n_recp_disp_sobu DECIMAL(20,4),stot_inflows_inv_act DECIMAL(20,4),
                            c_pay_acq_const_fiolta DECIMAL(20,4),c_paid_invest DECIMAL(20,4),
                            n_disp_subs_oth_biz DECIMAL(20,4),oth_pay_ral_inv_act DECIMAL(20,4),
                            n_incr_pledge_loan DECIMAL(20,4),stot_out_inv_act DECIMAL(20,4),
                            n_cashflow_inv_act DECIMAL(20,4),c_recp_borrow DECIMAL(20,4),proc_issue_bonds DECIMAL(20,4),
                            oth_cash_recp_ral_fnc_act DECIMAL(20,4),stot_cash_in_fnc_act DECIMAL(20,4),
                            free_cashflow DECIMAL(20,4),c_prepay_amt_borr DECIMAL(20,4),
                            c_pay_dist_dpcp_int_exp DECIMAL(20,4),incl_dvd_profit_paid_sc_ms DECIMAL(20,4),
                            oth_cashpay_ral_fnc_act DECIMAL(20,4),stot_cashout_fnc_act DECIMAL(20,4),
                            n_cash_flows_fnc_act DECIMAL(20,4),eff_fx_flu_cash DECIMAL(20,4),
                            n_incr_cash_cash_equ DECIMAL(20,4),c_cash_equ_beg_period DECIMAL(20,4),
                            c_cash_equ_end_period DECIMAL(20,4),c_recp_cap_contrib DECIMAL(20,4),
                            incl_cash_rec_saims DECIMAL(20,4),uncon_invest_loss DECIMAL(20,4),
                            prov_depr_assets DECIMAL(20,4),depr_fa_coga_dpba DECIMAL(20,4),
                            amort_intang_assets DECIMAL(20,4),lt_amort_deferred_exp DECIMAL(20,4),
                            decr_deferred_exp DECIMAL(20,4),incr_acc_exp DECIMAL(20,4),
                            loss_disp_fiolta DECIMAL(20,4),loss_scr_fa DECIMAL(20,4),loss_fv_chg DECIMAL(20,4),
                            invest_loss DECIMAL(20,4),decr_def_inc_tax_assets DECIMAL(20,4),
                            incr_def_inc_tax_liab DECIMAL(20,4),decr_inventories DECIMAL(20,4),
                            decr_oper_payable DECIMAL(20,4),incr_oper_payable DECIMAL(20,4),
                            others DECIMAL(20,4),im_net_cashflow_oper_act DECIMAL(20,4),
                            conv_debt_into_cap DECIMAL(20,4),conv_copbonds_due_within_1y DECIMAL(20,4),
                            fa_fnc_leases DECIMAL(20,4),im_n_incr_cash_equ DECIMAL(20,4),
                            net_dism_capital_add DECIMAL(20,4),net_cash_rece_sec DECIMAL(20,4),
                            credit_impa_loss DECIMAL(20,4),use_right_asset_dep DECIMAL(20,4),
                            oth_loss_asset DECIMAL(20,4),end_bal_cash DECIMAL(20,4),beg_bal_cash DECIMAL(20,4),
                            end_bal_cash_equ DECIMAL(20,4),beg_bal_cash_equ DECIMAL(20,4),update_flag INT
                            );
                """
    )

    # 用于创建业绩预告表
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS performance_forecast
                            (ts_code VARCHAR(20),ann_date DATE,end_date DATE,type VARCHAR(5),p_change_min DECIMAL(15,3),
                            p_change_max DECIMAL(15,3),net_profit_min DECIMAL(15,3),net_profit_max DECIMAL(15,3),
                            last_parent_net DECIMAL(15,3),notice_times INT,first_ann_date DATE,summary TEXT,
                            change_reason TEXT
                            );
                """
    )

    # 用于创建业绩快报表
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS performance_forecast_express
                            (ts_code VARCHAR(20),ann_date DATE,end_date DATE,revenue DECIMAL(20,4),
                            operate_profit DECIMAL(20,4),
                            total_profit DECIMAL(20,4),n_income DECIMAL(20,4),total_assets DECIMAL(20,4),
                            total_hldr_eqy_exc_min_int DECIMAL(20,4),diluted_eps DECIMAL(20,4),diluted_roe DECIMAL(20,4),
                            yoy_net_profit DECIMAL(20,4),bps DECIMAL(20,4),perf_summary DECIMAL(20,4),
                            yoy_sales DECIMAL(20,4),yoy_op DECIMAL(20,4),yoy_tp DECIMAL(20,4),yoy_dedu_np DECIMAL(20,4),
                            yoy_eps DECIMAL(20,4),yoy_roe DECIMAL(20,4),growth_assets DECIMAL(20,4),
                            yoy_equity DECIMAL(20,4),
                            growth_bps DECIMAL(20,4),or_last_year DECIMAL(20,4),op_last_year DECIMAL(20,4),
                            tp_last_year DECIMAL(20,4),np_last_year DECIMAL(20,4),eps_last_year DECIMAL(20,4),
                            open_net_assets DECIMAL(20,4),open_bps DECIMAL(20,4),is_audit INT,remark TEXT
                            );
                """
    )

    # 用于创建财务指标表
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS fin_indicator
                            (ts_code VARCHAR(20),ann_date DATE,end_date DATE,eps DECIMAL(22,6),dt_eps DECIMAL(22,6),
                            total_revenue_ps DECIMAL(22,6),revenue_ps DECIMAL(22,6),capital_rese_ps DECIMAL(22,6),
                            surplus_rese_ps DECIMAL(22,6),undist_profit_ps DECIMAL(22,6),extra_item DECIMAL(22,6),
                            profit_dedt DECIMAL(22,6),gross_margin DECIMAL(22,6),current_ratio DECIMAL(22,6),
                            quick_ratio DECIMAL(22,6),cash_ratio DECIMAL(22,6),ar_turn DECIMAL(22,6),
                            ca_turn DECIMAL(22,6),
                            fa_turn DECIMAL(22,6),assets_turn DECIMAL(22,6),op_income DECIMAL(22,6),ebit DECIMAL(22,6),
                            ebitda DECIMAL(22,6),fcff DECIMAL(22,6),fcfe DECIMAL(22,6),current_exint DECIMAL(22,6),
                            noncurrent_exint DECIMAL(22,6),interestdebt DECIMAL(22,6),netdebt DECIMAL(22,6),
                            tangible_asset DECIMAL(22,6),working_capital DECIMAL(22,6),networking_capital DECIMAL(22,6),
                            invest_capital DECIMAL(22,6),retained_earnings DECIMAL(22,6),diluted2_eps DECIMAL(22,6),
                            bps DECIMAL(22,6),ocfps DECIMAL(22,6),retainedps DECIMAL(22,6),cfps DECIMAL(22,6),
                            ebit_ps DECIMAL(22,6),fcff_ps DECIMAL(22,6),fcfe_ps DECIMAL(22,6),
                            netprofit_margin DECIMAL(22,6),
                            grossprofit_margin DECIMAL(22,6),cogs_of_sales DECIMAL(22,6),expense_of_sales DECIMAL(22,6),
                            profit_to_gr DECIMAL(22,6),saleexp_to_gr DECIMAL(22,6),adminexp_of_gr DECIMAL(22,6),
                            finaexp_of_gr DECIMAL(22,6),impai_ttm DECIMAL(22,6),gc_of_gr DECIMAL(22,6),
                            op_of_gr DECIMAL(22,6)
                            ,ebit_of_gr DECIMAL(22,6),roe DECIMAL(22,6),roe_waa DECIMAL(22,6),roe_dt DECIMAL(22,6),
                            roa DECIMAL(22,6),npta DECIMAL(22,6),roic DECIMAL(22,6),roe_yearly DECIMAL(22,6),
                            roa2_yearly DECIMAL(22,6),debt_to_assets DECIMAL(22,6),assets_to_eqt DECIMAL(22,6),
                            dp_assets_to_eqt DECIMAL(22,6),ca_to_assets DECIMAL(22,6),nca_to_assets DECIMAL(22,6),
                            tbassets_to_totalassets DECIMAL(22,6),int_to_talcap DECIMAL(22,6),
                            eqt_to_talcapital DECIMAL(22,6),
                            currentdebt_to_debt DECIMAL(22,6),longdeb_to_debt DECIMAL(22,6),
                            ocf_to_shortdebt DECIMAL(22,6),
                            debt_to_eqt DECIMAL(22,6),eqt_to_debt DECIMAL(22,6),eqt_to_interestdebt DECIMAL(22,6),
                            tangibleasset_to_debt DECIMAL(22,6),tangasset_to_intdebt DECIMAL(22,6),
                            tangibleasset_to_netdebt DECIMAL(22,6),ocf_to_debt DECIMAL(22,6),turn_days DECIMAL(22,6),
                            roa_yearly DECIMAL(22,6),roa_dp DECIMAL(22,6),fixed_assets DECIMAL(22,6),
                            profit_to_op DECIMAL(22,6),q_saleexp_to_gr DECIMAL(22,6),q_gc_to_gr DECIMAL(22,6),
                            q_roe DECIMAL(22,6),q_dt_roe DECIMAL(22,6),q_npta DECIMAL(22,6),q_ocf_to_sales DECIMAL(22,6),
                            basic_eps_yoy DECIMAL(22,6),dt_eps_yoy DECIMAL(22,6),cfps_yoy DECIMAL(22,6),
                            op_yoy DECIMAL(22,6),
                            ebt_yoy DECIMAL(22,6),netprofit_yoy DECIMAL(22,6),dt_netprofit_yoy DECIMAL(22,6),
                            ocf_yoy DECIMAL(22,6),roe_yoy DECIMAL(22,6),bps_yoy DECIMAL(22,6),assets_yoy DECIMAL(22,6),
                            eqt_yoy DECIMAL(22,6),tr_yoy DECIMAL(22,6),or_yoy DECIMAL(22,6),q_sales_yoy DECIMAL(22,6),
                            q_op_qoq DECIMAL(22,6),equity_yoy DECIMAL(22,6),invturn_days DECIMAL(22,6),
                            arturn_days DECIMAL(22,6),inv_turn DECIMAL(22,6),valuechange_income DECIMAL(22,6),
                            interst_income DECIMAL(22,6),daa DECIMAL(22,6),roe_avg DECIMAL(22,6),
                            opincome_of_ebt DECIMAL(22,6),
                            investincome_of_ebt DECIMAL(22,6),n_op_profit_of_ebt DECIMAL(22,6),tax_to_ebt DECIMAL(22,6),
                            dtprofit_to_profit DECIMAL(22,6),salescash_to_or DECIMAL(22,6),ocf_to_or DECIMAL(22,6),
                            ocf_to_opincome DECIMAL(22,6),capitalized_to_da DECIMAL(22,6),
                            ocf_to_interestdebt DECIMAL(22,6),ocf_to_netdebt DECIMAL(22,6),
                            ebit_to_interest DECIMAL(22,6),
                            longdebt_to_workingcapital DECIMAL(22,6),ebitda_to_debt DECIMAL(22,6),
                            profit_prefin_exp DECIMAL(22,6),non_op_profit DECIMAL(22,6),op_to_ebt DECIMAL(22,6),
                            nop_to_ebt DECIMAL(22,6),ocf_to_profit DECIMAL(22,6),cash_to_liqdebt DECIMAL(22,6),
                            cash_to_liqdebt_withinterest DECIMAL(22,6),op_to_liqdebt DECIMAL(22,6),
                            op_to_debt DECIMAL(22,6),
                            roic_yearly DECIMAL(22,6),total_fa_trun DECIMAL(22,6),q_opincome DECIMAL(22,6),
                            q_investincome DECIMAL(22,6),q_dtprofit DECIMAL(22,6),q_eps DECIMAL(22,6),
                            q_netprofit_margin DECIMAL(22,6),q_gsprofit_margin DECIMAL(22,6),
                            q_exp_to_sales DECIMAL(22,6),
                            q_profit_to_gr DECIMAL(22,6),q_adminexp_to_gr DECIMAL(22,6),q_finaexp_to_gr DECIMAL(22,6),
                            q_impair_to_gr_ttm DECIMAL(22,6),q_op_to_gr DECIMAL(22,6),q_opincome_to_ebt DECIMAL(22,6),
                            q_investincome_to_ebt DECIMAL(22,6),q_dtprofit_to_profit DECIMAL(22,6),
                            q_salescash_to_or DECIMAL(22,6),q_ocf_to_or DECIMAL(22,6),q_gr_yoy DECIMAL(22,6),
                            q_gr_qoq DECIMAL(22,6),q_sales_qoq DECIMAL(22,6),q_op_yoy DECIMAL(22,6),
                            q_profit_yoy DECIMAL(22,6),q_profit_qoq DECIMAL(22,6),q_netprofit_yoy DECIMAL(22,6),
                            q_netprofit_qoq DECIMAL(22,6),rd_exp DECIMAL(22,6)
                            );
                """
    )

    # 用于创建主营业务成分表
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS main_business_composition
                            (ts_code VARCHAR(20),end_date DATE,bz_item TEXT,bz_sales DECIMAL(22,4),
                            bz_profit DECIMAL(22,4),bz_cost DECIMAL(22,4),curr_type VARCHAR(10),bz_code VARCHAR(5)
                            );
                """
    )

    # 用于创建财务披露时间表
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS disclosure_date
                            (ts_code VARCHAR(20),ann_date DATE,end_date DATE,pre_date DATE,actual_date DATE
                            );
                """
    )

    # 用于创建公司分红数据表
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS dividend
                            (ts_code VARCHAR(20),end_date DATE,ann_date DATE,div_proc VARCHAR(30),stk_div DECIMAL(15,6),
                            stk_bo_rate DECIMAL(15,6),stk_co_rate DECIMAL(15,6),cash_div DECIMAL(15,6),
                            cash_div_tax DECIMAL(15,6),record_date DATE,ex_date DATE,pay_date DATE,div_listdate DATE,
                            imp_ann_date DATE
                            );
                """
    )

    # 用于创建日行情
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS daily_price
                            (ts_code VARCHAR(20),trade_date DATE,open DECIMAL(10,4),high DECIMAL(10,4),low DECIMAL(10,4),
                            close DECIMAL(10,4),pre_close DECIMAL(10,4),`change` DECIMAL(10,7),pct_chg DECIMAL(12,7),
                            vol DECIMAL(18,4),amount DECIMAL(18,4)
                            )PARTITION BY RANGE COLUMNS(trade_date) (
								PARTITION p1990 VALUES LESS THAN ('19910101'),
								PARTITION p1991 VALUES LESS THAN ('19920101'),
								PARTITION p1992 VALUES LESS THAN ('19930101'),
								PARTITION p1993 VALUES LESS THAN ('19940101'),
								PARTITION p1994 VALUES LESS THAN ('19950101'),
								PARTITION p1995 VALUES LESS THAN ('19960101'),
								PARTITION p1996 VALUES LESS THAN ('19970101'),
								PARTITION p1997 VALUES LESS THAN ('19980101'),
								PARTITION p1998 VALUES LESS THAN ('19990101'),
								PARTITION p1999 VALUES LESS THAN ('20000101'),
								PARTITION p2000 VALUES LESS THAN ('20010101'),
								PARTITION p2001 VALUES LESS THAN ('20020101'),
								PARTITION p2002 VALUES LESS THAN ('20030101'),
								PARTITION p2003 VALUES LESS THAN ('20040101'),
								PARTITION p2004 VALUES LESS THAN ('20050101'),
								PARTITION p2005 VALUES LESS THAN ('20060101'),
								PARTITION p2006 VALUES LESS THAN ('20070101'),
								PARTITION p2007 VALUES LESS THAN ('20080101'),
								PARTITION p2008 VALUES LESS THAN ('20090101'),
								PARTITION p2009 VALUES LESS THAN ('20100101'),
								PARTITION p2010 VALUES LESS THAN ('20110101'),
								PARTITION p2011 VALUES LESS THAN ('20120101'),
								PARTITION p2012 VALUES LESS THAN ('20130101'),
								PARTITION p2013 VALUES LESS THAN ('20140101'),
								PARTITION p2014 VALUES LESS THAN ('20150101'),
								PARTITION p2015 VALUES LESS THAN ('20160101'),
								PARTITION p2016 VALUES LESS THAN ('20170101'),
								PARTITION p2017 VALUES LESS THAN ('20180101'),
								PARTITION p2018 VALUES LESS THAN ('20190101'),
								PARTITION p2019 VALUES LESS THAN ('20200101'),
								PARTITION p2020 VALUES LESS THAN ('20210101'),
								PARTITION p2021 VALUES LESS THAN ('20220101'),
								PARTITION p2022 VALUES LESS THAN ('20230101'),
								PARTITION p2023 VALUES LESS THAN ('20240101'),
								PARTITION p2024 VALUES LESS THAN ('20250101')
								);
                """
    )

    # 用于创建周行情
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS weekly_price
                            (ts_code VARCHAR(20),trade_date DATE,open DECIMAL(10,4),high DECIMAL(10,4),low DECIMAL(10,4),
                            close DECIMAL(10,4),pre_close DECIMAL(10,4),`change` DECIMAL(10,7),pct_chg DECIMAL(10,7),
                            vol DECIMAL(14,4),amount DECIMAL(18,4)
                            );
                """
    )

    # 用于创建月行情
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS monthly_price
                            (ts_code VARCHAR(20),trade_date DATE,open DECIMAL(10,4),high DECIMAL(10,4),low DECIMAL(10,4),
                            close DECIMAL(10,4),pre_close DECIMAL(10,4),`change` DECIMAL(10,7),pct_chg DECIMAL(10,7),
                            vol DECIMAL(18,4),amount DECIMAL(18,4)
                            );
                """
    )

    # 用于创建复权因子表
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS adj_factor
                            (ts_code VARCHAR(20),trade_date DATE,adj_factor DECIMAL(16,8)
                            )PARTITION BY RANGE COLUMNS(trade_date) (
								PARTITION p1990 VALUES LESS THAN ('19910101'),
								PARTITION p1991 VALUES LESS THAN ('19920101'),
								PARTITION p1992 VALUES LESS THAN ('19930101'),
								PARTITION p1993 VALUES LESS THAN ('19940101'),
								PARTITION p1994 VALUES LESS THAN ('19950101'),
								PARTITION p1995 VALUES LESS THAN ('19960101'),
								PARTITION p1996 VALUES LESS THAN ('19970101'),
								PARTITION p1997 VALUES LESS THAN ('19980101'),
								PARTITION p1998 VALUES LESS THAN ('19990101'),
								PARTITION p1999 VALUES LESS THAN ('20000101'),
								PARTITION p2000 VALUES LESS THAN ('20010101'),
								PARTITION p2001 VALUES LESS THAN ('20020101'),
								PARTITION p2002 VALUES LESS THAN ('20030101'),
								PARTITION p2003 VALUES LESS THAN ('20040101'),
								PARTITION p2004 VALUES LESS THAN ('20050101'),
								PARTITION p2005 VALUES LESS THAN ('20060101'),
								PARTITION p2006 VALUES LESS THAN ('20070101'),
								PARTITION p2007 VALUES LESS THAN ('20080101'),
								PARTITION p2008 VALUES LESS THAN ('20090101'),
								PARTITION p2009 VALUES LESS THAN ('20100101'),
								PARTITION p2010 VALUES LESS THAN ('20110101'),
								PARTITION p2011 VALUES LESS THAN ('20120101'),
								PARTITION p2012 VALUES LESS THAN ('20130101'),
								PARTITION p2013 VALUES LESS THAN ('20140101'),
								PARTITION p2014 VALUES LESS THAN ('20150101'),
								PARTITION p2015 VALUES LESS THAN ('20160101'),
								PARTITION p2016 VALUES LESS THAN ('20170101'),
								PARTITION p2017 VALUES LESS THAN ('20180101'),
								PARTITION p2018 VALUES LESS THAN ('20190101'),
								PARTITION p2019 VALUES LESS THAN ('20200101'),
								PARTITION p2020 VALUES LESS THAN ('20210101'),
								PARTITION p2021 VALUES LESS THAN ('20220101'),
								PARTITION p2022 VALUES LESS THAN ('20230101'),
								PARTITION p2023 VALUES LESS THAN ('20240101'),
								PARTITION p2024 VALUES LESS THAN ('20250101')
								);
                """
    )

    # 用于创建每日停复牌信息
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS daily_suspend
                            (ts_code VARCHAR(20),trade_date DATE,suspend_timing TEXT ,suspend_type VARCHAR(3)
                            );
                """
    )

    # 用于创建每日指标
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS daily_basic
                            (ts_code VARCHAR(20),trade_date DATE,close DECIMAL(24,6),turnover_rate DECIMAL(24,6),
                            turnover_rate_f DECIMAL(24,6),volume_ratio DECIMAL(24,6),
                            pe DECIMAL(24,6),pe_ttm DECIMAL(24,6),pb DECIMAL(24,6),ps DECIMAL(24,6),
                            ps_ttm DECIMAL(24,6),dv_ratio DECIMAL(24,6),dv_ttm DECIMAL(24,6),total_share DECIMAL(24,6),
                            float_share DECIMAL(24,6),free_share DECIMAL(24,6),total_mv DECIMAL(24,6),
                            circ_mv DECIMAL(24,6),limit_status DECIMAL(24,6)
                            )PARTITION BY RANGE COLUMNS(trade_date) (
								PARTITION p1990 VALUES LESS THAN ('19910101'),
								PARTITION p1991 VALUES LESS THAN ('19920101'),
								PARTITION p1992 VALUES LESS THAN ('19930101'),
								PARTITION p1993 VALUES LESS THAN ('19940101'),
								PARTITION p1994 VALUES LESS THAN ('19950101'),
								PARTITION p1995 VALUES LESS THAN ('19960101'),
								PARTITION p1996 VALUES LESS THAN ('19970101'),
								PARTITION p1997 VALUES LESS THAN ('19980101'),
								PARTITION p1998 VALUES LESS THAN ('19990101'),
								PARTITION p1999 VALUES LESS THAN ('20000101'),
								PARTITION p2000 VALUES LESS THAN ('20010101'),
								PARTITION p2001 VALUES LESS THAN ('20020101'),
								PARTITION p2002 VALUES LESS THAN ('20030101'),
								PARTITION p2003 VALUES LESS THAN ('20040101'),
								PARTITION p2004 VALUES LESS THAN ('20050101'),
								PARTITION p2005 VALUES LESS THAN ('20060101'),
								PARTITION p2006 VALUES LESS THAN ('20070101'),
								PARTITION p2007 VALUES LESS THAN ('20080101'),
								PARTITION p2008 VALUES LESS THAN ('20090101'),
								PARTITION p2009 VALUES LESS THAN ('20100101'),
								PARTITION p2010 VALUES LESS THAN ('20110101'),
								PARTITION p2011 VALUES LESS THAN ('20120101'),
								PARTITION p2012 VALUES LESS THAN ('20130101'),
								PARTITION p2013 VALUES LESS THAN ('20140101'),
								PARTITION p2014 VALUES LESS THAN ('20150101'),
								PARTITION p2015 VALUES LESS THAN ('20160101'),
								PARTITION p2016 VALUES LESS THAN ('20170101'),
								PARTITION p2017 VALUES LESS THAN ('20180101'),
								PARTITION p2018 VALUES LESS THAN ('20190101'),
								PARTITION p2019 VALUES LESS THAN ('20200101'),
								PARTITION p2020 VALUES LESS THAN ('20210101'),
								PARTITION p2021 VALUES LESS THAN ('20220101'),
								PARTITION p2022 VALUES LESS THAN ('20230101'),
								PARTITION p2023 VALUES LESS THAN ('20240101'),
								PARTITION p2024 VALUES LESS THAN ('20250101')
								);
                """
    )

    # 个股资金流向
    cursor.execute("""CREATE TABLE IF NOT EXISTS moneyflow
    		(ts_code VARCHAR(20),trade_date DATE,buy_sm_vol INT,buy_sm_amount DECIMAL(18,6),sell_sm_vol INT,
    		sell_sm_amount DECIMAL(18,6),buy_md_vol INT,buy_md_amount DECIMAL(18,6),sell_md_vol INT,
    		sell_md_amount DECIMAL(18,6),buy_lg_vol INT,buy_lg_amount DECIMAL(18,6),sell_lg_vol INT,
    		sell_lg_amount DECIMAL(18,6),buy_elg_vol INT,buy_elg_amount DECIMAL(18,6),sell_elg_vol INT,
    		sell_elg_amount DECIMAL(18,6),net_mf_vol INT,net_mf_amount DECIMAL(18,6)
    		)PARTITION BY RANGE COLUMNS(trade_date) (
				PARTITION p1990 VALUES LESS THAN ('19910101'),
				PARTITION p1991 VALUES LESS THAN ('19920101'),
				PARTITION p1992 VALUES LESS THAN ('19930101'),
				PARTITION p1993 VALUES LESS THAN ('19940101'),
				PARTITION p1994 VALUES LESS THAN ('19950101'),
				PARTITION p1995 VALUES LESS THAN ('19960101'),
				PARTITION p1996 VALUES LESS THAN ('19970101'),
				PARTITION p1997 VALUES LESS THAN ('19980101'),
				PARTITION p1998 VALUES LESS THAN ('19990101'),
				PARTITION p1999 VALUES LESS THAN ('20000101'),
				PARTITION p2000 VALUES LESS THAN ('20010101'),
				PARTITION p2001 VALUES LESS THAN ('20020101'),
				PARTITION p2002 VALUES LESS THAN ('20030101'),
				PARTITION p2003 VALUES LESS THAN ('20040101'),
				PARTITION p2004 VALUES LESS THAN ('20050101'),
				PARTITION p2005 VALUES LESS THAN ('20060101'),
				PARTITION p2006 VALUES LESS THAN ('20070101'),
				PARTITION p2007 VALUES LESS THAN ('20080101'),
				PARTITION p2008 VALUES LESS THAN ('20090101'),
				PARTITION p2009 VALUES LESS THAN ('20100101'),
				PARTITION p2010 VALUES LESS THAN ('20110101'),
				PARTITION p2011 VALUES LESS THAN ('20120101'),
				PARTITION p2012 VALUES LESS THAN ('20130101'),
				PARTITION p2013 VALUES LESS THAN ('20140101'),
				PARTITION p2014 VALUES LESS THAN ('20150101'),
				PARTITION p2015 VALUES LESS THAN ('20160101'),
				PARTITION p2016 VALUES LESS THAN ('20170101'),
				PARTITION p2017 VALUES LESS THAN ('20180101'),
				PARTITION p2018 VALUES LESS THAN ('20190101'),
				PARTITION p2019 VALUES LESS THAN ('20200101'),
				PARTITION p2020 VALUES LESS THAN ('20210101'),
				PARTITION p2021 VALUES LESS THAN ('20220101'),
				PARTITION p2022 VALUES LESS THAN ('20230101'),
				PARTITION p2023 VALUES LESS THAN ('20240101'),
				PARTITION p2024 VALUES LESS THAN ('20250101')
				);
    		""")

    # 用于创建沪深港通资金流向
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS moneyflow_hsgt
                            (trade_date DATE,ggt_ss INT,ggt_sz INT,hgt INT,sgt INT,north_money INT,south_money INT
                            );
                """
    )

    # 用于创建港股通十大成交股
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS hsgt_top10
                            (trade_date DATE,ts_code VARCHAR(20),name VARCHAR(30),close DECIMAL(12,6),
                            `change` DECIMAL(9,6),`rank` INT,market_type INT,`amount` DECIMAL(24,6),
                            net_amount DECIMAL(24,6),buy DECIMAL(24,6),
                            sell DECIMAL(24,6)
                            );
                """
    )

    # 用于创建沪深股通十大成交股
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS ggt_top10
                            (trade_date DATE,ts_code VARCHAR(20),name VARCHAR(30),close DECIMAL(12,6),
                            p_change DECIMAL(24,6),`rank` INT,market_type INT,`amount` DECIMAL(24,6),
                            net_amount DECIMAL(24,6),sh_amount DECIMAL(24,6),sh_net_amount DECIMAL(24,6),
                            sh_buy DECIMAL(24,6),sh_sell DECIMAL(24,6),sz_amount DECIMAL(24,6),
                            sz_net_amount DECIMAL(24,6),sz_buy DECIMAL(24,6),sz_sell DECIMAL(24,6)
                            );
                """
    )

    # 用于创建港股通每月成交统计
    cursor.execute("""CREATE TABLE IF NOT EXISTS ggt_daily
    		(trade_date DATE,buy_amount DECIMAL(15,6),buy_volume DECIMAL(14,6),sell_amount DECIMAL(15,6),
    		sell_volume DECIMAL(14,6)
    		);
    		""")

    # 用于创建港股通每月成交统计
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS ggt_monthly
                            (month INT(20),day_buy_amt DECIMAL(24,6),day_buy_vol DECIMAL(24,6),
                            day_sell_amt DECIMAL(24,6),day_sell_vol DECIMAL(24,6),total_buy_amt DECIMAL(24,6),
                            total_buy_vol DECIMAL(24,6),total_sell_amt DECIMAL(24,6),total_sell_vol DECIMAL(24,6)
                            );
                """
    )
    # 用于创建融资融券交易汇总
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS capitalize_summary
                            (trade_date DATE,exchange_id VARCHAR(10),rzye DECIMAL(24,6),rzmre DECIMAL(24,6),
                            rzche DECIMAL(24,6),rqye DECIMAL(24,6),rqmcl DECIMAL(24,6),rzrqye DECIMAL(24,6),
                            rqyl DECIMAL(24,6)
                            );
                """
    )

    # 用于创建融资融券交易明细
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS capitalize_detail
                            (trade_date DATE,ts_code VARCHAR(20),rzye DECIMAL(24,6),rzmre DECIMAL(24,6),
                            rzche DECIMAL(24,6),rqye DECIMAL(24,6),rqmcl DECIMAL(24,6),rzrqye DECIMAL(24,6),
                            rqyl DECIMAL(24,6)
                            )PARTITION BY RANGE COLUMNS(trade_date) (
								PARTITION p1990 VALUES LESS THAN ('19910101'),
								PARTITION p1991 VALUES LESS THAN ('19920101'),
								PARTITION p1992 VALUES LESS THAN ('19930101'),
								PARTITION p1993 VALUES LESS THAN ('19940101'),
								PARTITION p1994 VALUES LESS THAN ('19950101'),
								PARTITION p1995 VALUES LESS THAN ('19960101'),
								PARTITION p1996 VALUES LESS THAN ('19970101'),
								PARTITION p1997 VALUES LESS THAN ('19980101'),
								PARTITION p1998 VALUES LESS THAN ('19990101'),
								PARTITION p1999 VALUES LESS THAN ('20000101'),
								PARTITION p2000 VALUES LESS THAN ('20010101'),
								PARTITION p2001 VALUES LESS THAN ('20020101'),
								PARTITION p2002 VALUES LESS THAN ('20030101'),
								PARTITION p2003 VALUES LESS THAN ('20040101'),
								PARTITION p2004 VALUES LESS THAN ('20050101'),
								PARTITION p2005 VALUES LESS THAN ('20060101'),
								PARTITION p2006 VALUES LESS THAN ('20070101'),
								PARTITION p2007 VALUES LESS THAN ('20080101'),
								PARTITION p2008 VALUES LESS THAN ('20090101'),
								PARTITION p2009 VALUES LESS THAN ('20100101'),
								PARTITION p2010 VALUES LESS THAN ('20110101'),
								PARTITION p2011 VALUES LESS THAN ('20120101'),
								PARTITION p2012 VALUES LESS THAN ('20130101'),
								PARTITION p2013 VALUES LESS THAN ('20140101'),
								PARTITION p2014 VALUES LESS THAN ('20150101'),
								PARTITION p2015 VALUES LESS THAN ('20160101'),
								PARTITION p2016 VALUES LESS THAN ('20170101'),
								PARTITION p2017 VALUES LESS THAN ('20180101'),
								PARTITION p2018 VALUES LESS THAN ('20190101'),
								PARTITION p2019 VALUES LESS THAN ('20200101'),
								PARTITION p2020 VALUES LESS THAN ('20210101'),
								PARTITION p2021 VALUES LESS THAN ('20220101'),
								PARTITION p2022 VALUES LESS THAN ('20230101'),
								PARTITION p2023 VALUES LESS THAN ('20240101'),
								PARTITION p2024 VALUES LESS THAN ('20250101')
								);
                """
    )

    # 用于创建融资融券交易标的
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS capitalize_target
                            (ts_code VARCHAR(20),mg_type VARCHAR(5),is_new VARCHAR(5),in_date DATE,out_date DATE,ann_date DATE
                            );
                """
    )

    # 用于创建前十大股东
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS top10_holders
                            (ts_code VARCHAR(20),ann_date DATE,end_date DATE,holder_name TEXT,
                            hold_amount DECIMAL(24,6),hold_ratio DECIMAL(10,6)
                            );
                """
    )

    # 用于创建前十大流通股东
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS top10_floatholders
                            (ts_code VARCHAR(20),ann_date DATE,end_date DATE,holder_name TEXT,hold_amount DECIMAL(24,6)
                            );
                """
    )

    # 用于创建龙虎榜每日明细
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS top_list
    		(trade_date DATE,ts_code VARCHAR(20),name VARCHAR(20),close DECIMAL(14,6),pct_change DECIMAL(14,6),
    		turnover_rate DECIMAL(14,6),amount DECIMAL(21,6),l_sell DECIMAL(21,6),l_buy DECIMAL(21,6),
    		l_amount DECIMAL(21,6),net_amount DECIMAL(20,6),net_rate DECIMAL(14,6),amount_rate DECIMAL(15,6),
    		float_values DECIMAL(23,6),reason TEXT
    		);
    		"""
    )

    # 用于创建龙虎榜机构明细
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS top_inst
    		(trade_date DATE,ts_code VARCHAR(20),exalter TEXT,buy DECIMAL(21,6),buy_rate DECIMAL(14,6),
    		sell DECIMAL(20,6),sell_rate DECIMAL(14,6),net_buy DECIMAL(20,6),side VARCHAR(20),reason TEXT
    		);
    		"""
    )

    # 股权质押统计数据
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS pledge_stat
    		(ts_code VARCHAR(20),end_date DATE,pledge_count INT,unrest_pledge DECIMAL(18,6),rest_pledge DECIMAL(18,6),
    		total_share DECIMAL(20,6),pledge_ratio DECIMAL(14,6)
    		);
    		"""
    )

    # 股权质押明细
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS pledge_detail
    		(ts_code VARCHAR(20),ann_date DATE,holder_name TEXT,pledge_amount DECIMAL(16,6),start_date INT,end_date INT,
    		is_release VARCHAR(20),release_date DATE,pledgor TEXT,holding_amount DECIMAL(16,6),
    		pledged_amount DECIMAL(16,6),p_total_ratio DECIMAL(13,6),h_total_ratio DECIMAL(14,6),
    		is_buyback VARCHAR(20)
    		);
    		"""
    )

    # 股票回购
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS repurchase
    		(ts_code VARCHAR(20),ann_date DATE,end_date DATE,proc VARCHAR(20),exp_date DATE,vol DECIMAL(20,6),
    		amount DECIMAL(22,6),high_limit DECIMAL(14,6),low_limit DECIMAL(14,6)
    		);
    		"""
    )

    # 概念股分类
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS concept
    		(code VARCHAR(20),name TEXT,src VARCHAR(5)
    		);
    		"""
    )

    # 概念股列表
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS concept_detail
    		(id VARCHAR(20),concept_name VARCHAR(20),ts_code VARCHAR(20),name VARCHAR(20)
    		);
    		"""
    )

    # 限售股解禁
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS share_float
    		(ts_code VARCHAR(20),ann_date DATE,float_date DATE,float_share DECIMAL(20,6),float_ratio DECIMAL(14,6),
    		holder_name TEXT,share_type VARCHAR(20)
    		)PARTITION BY RANGE COLUMNS(float_date) (
				PARTITION p1990 VALUES LESS THAN ('19910101'),
				PARTITION p1991 VALUES LESS THAN ('19920101'),
				PARTITION p1992 VALUES LESS THAN ('19930101'),
				PARTITION p1993 VALUES LESS THAN ('19940101'),
				PARTITION p1994 VALUES LESS THAN ('19950101'),
				PARTITION p1995 VALUES LESS THAN ('19960101'),
				PARTITION p1996 VALUES LESS THAN ('19970101'),
				PARTITION p1997 VALUES LESS THAN ('19980101'),
				PARTITION p1998 VALUES LESS THAN ('19990101'),
				PARTITION p1999 VALUES LESS THAN ('20000101'),
				PARTITION p2000 VALUES LESS THAN ('20010101'),
				PARTITION p2001 VALUES LESS THAN ('20020101'),
				PARTITION p2002 VALUES LESS THAN ('20030101'),
				PARTITION p2003 VALUES LESS THAN ('20040101'),
				PARTITION p2004 VALUES LESS THAN ('20050101'),
				PARTITION p2005 VALUES LESS THAN ('20060101'),
				PARTITION p2006 VALUES LESS THAN ('20070101'),
				PARTITION p2007 VALUES LESS THAN ('20080101'),
				PARTITION p2008 VALUES LESS THAN ('20090101'),
				PARTITION p2009 VALUES LESS THAN ('20100101'),
				PARTITION p2010 VALUES LESS THAN ('20110101'),
				PARTITION p2011 VALUES LESS THAN ('20120101'),
				PARTITION p2012 VALUES LESS THAN ('20130101'),
				PARTITION p2013 VALUES LESS THAN ('20140101'),
				PARTITION p2014 VALUES LESS THAN ('20150101'),
				PARTITION p2015 VALUES LESS THAN ('20160101'),
				PARTITION p2016 VALUES LESS THAN ('20170101'),
				PARTITION p2017 VALUES LESS THAN ('20180101'),
				PARTITION p2018 VALUES LESS THAN ('20190101'),
				PARTITION p2019 VALUES LESS THAN ('20200101'),
				PARTITION p2020 VALUES LESS THAN ('20210101'),
				PARTITION p2021 VALUES LESS THAN ('20220101'),
				PARTITION p2022 VALUES LESS THAN ('20230101'),
				PARTITION p2023 VALUES LESS THAN ('20240101'),
				PARTITION p2024 VALUES LESS THAN ('20250101')
				);
    		"""
    )

    # 大宗交易
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS block_trade
    		(ts_code VARCHAR(20),trade_date DATE,price DECIMAL(14,6),vol DECIMAL(17,6),amount DECIMAL(18,6),buyer TEXT,
    		seller TEXT
    		);
    		"""
    )

    # 股票账户开户数据
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS stk_account_old
    		(date VARCHAR(13),new_sh INT,new_sz INT,active_sh DECIMAL(16,6),active_sz DECIMAL(16,6),
    		total_sh DECIMAL(16,6),total_sz DECIMAL(16,6),trade_sh DECIMAL(16,6),trade_sz DECIMAL(16,6)
    		);
    		"""
    )

    # 股东人数
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS stk_holdernumber
    		(ts_code VARCHAR(20),ann_date DATE,end_date DATE,holder_num INT
    		);
    		"""
    )

    # 股东增减持
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS stk_holdertrade
    		(ts_code VARCHAR(20),ann_date DATE,holder_name TEXT,holder_type VARCHAR(20),in_de VARCHAR(20),
    		change_vol DECIMAL(20,6),change_ratio DECIMAL(13,6),after_share DECIMAL(20,6),
    		after_ratio DECIMAL(14,6),avg_price DECIMAL(14,6),total_share DECIMAL(21,6),begin_date DATE,close_date DATE
    		);
    		"""
    )

    conn.commit()


def create_futures_data():
    cursor.execute("CREATE DATABASE IF NOT EXISTS futures_data")  # 创建基础数据库
    cursor.execute("use futures_data ")

    # 期货合约信息表
    cursor.execute("""        CREATE TABLE IF NOT EXISTS fut_basic (
            ts_code VARCHAR(30) NOT NULL,
            symbol VARCHAR(20),
            exchange VARCHAR(10),
            name VARCHAR(100),
            fut_code VARCHAR(20),
            multiplier DECIMAL(20,4),
            trade_unit VARCHAR(20),
            per_unit DECIMAL(20,4),
            quote_unit VARCHAR(20),
            quote_unit_desc VARCHAR (100),
            d_mode_desc VARCHAR(100),
            list_date DATE,
            delist_date DATE,
            d_month VARCHAR(10),
            last_ddate DATE,
            trade_time_desc TEXT,
            PRIMARY KEY (ts_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    		""")

    # 交易日历
    cursor.execute("""CREATE TABLE IF NOT EXISTS future_trade_date
    		(exchange VARCHAR(20),cal_date DATE,is_open INT,pretrade_date DATE
    		);
    		""")

    # 期货日线行情
    cursor.execute("""CREATE TABLE IF NOT EXISTS fut_daily (
            ts_code VARCHAR(30) NOT NULL COMMENT 'TS合约代码',
            trade_date DATE NOT NULL COMMENT '交易日期',
        
            pre_close   DECIMAL(16,6) COMMENT '昨收盘价',
            pre_settle  DECIMAL(16,6) COMMENT '昨结算价',
            open        DECIMAL(16,6) COMMENT '开盘价',
            high        DECIMAL(16,6) COMMENT '最高价',
            low         DECIMAL(16,6) COMMENT '最低价',
            close       DECIMAL(16,6) COMMENT '收盘价',
            settle      DECIMAL(16,6) COMMENT '结算价',
        
            change1     DECIMAL(16,6) COMMENT '涨跌1 收盘价-昨结算价',
            change2     DECIMAL(16,6) COMMENT '涨跌2 结算价-昨结算价',
        
            vol         DECIMAL(20,4) COMMENT '成交量(手)',
            amount      DECIMAL(20,4) COMMENT '成交金额(万元)',
            oi          DECIMAL(20,4) COMMENT '持仓量(手)',
            oi_chg      DECIMAL(20,4) COMMENT '持仓量变化',
        
            delv_settle DECIMAL(16,6) COMMENT '交割结算价',
        
            -- 主键：分区键在前，保证顺序写
            PRIMARY KEY (trade_date, ts_code),
        
            -- 常用查询索引：按合约查历史
            KEY idx_ts_code_date (ts_code, trade_date)
        )
        ENGINE=InnoDB
        ROW_FORMAT=DYNAMIC
        DEFAULT CHARSET=utf8mb4
        PARTITION BY RANGE COLUMNS (trade_date)
        (
            -- 历史异常兜底
            PARTITION p_lt_2000 VALUES LESS THAN ('2000-01-01'),
        
            -- 五年一分（主写入区）
            PARTITION p2000_2004 VALUES LESS THAN ('2005-01-01'),
            PARTITION p2005_2009 VALUES LESS THAN ('2010-01-01'),
            PARTITION p2010_2014 VALUES LESS THAN ('2015-01-01'),
            PARTITION p2015_2019 VALUES LESS THAN ('2020-01-01'),
            PARTITION p2020_2024 VALUES LESS THAN ('2025-01-01'),
            PARTITION p2025_2029 VALUES LESS THAN ('2030-01-01'),
        
            -- 未来兜底
            PARTITION p_ge_2030 VALUES LESS THAN (MAXVALUE)
        );
    		""")

    # 每日成交持仓排名 ！！要以trade_date和exchange获取
    cursor.execute("""CREATE TABLE IF NOT EXISTS future_holding
    		(trade_date DATE,symbol VARCHAR(20),broker VARCHAR(20),vol DECIMAL(19,6),vol_chg DECIMAL(17,6),
    		long_hld DECIMAL(19,6),long_chg DECIMAL(17,6),short_hld DECIMAL(19,6),short_chg DECIMAL(17,6)
    		)PARTITION BY RANGE COLUMNS(trade_date) (
				PARTITION p1990 VALUES LESS THAN ('19910101'),
				PARTITION p1991 VALUES LESS THAN ('19920101'),
				PARTITION p1992 VALUES LESS THAN ('19930101'),
				PARTITION p1993 VALUES LESS THAN ('19940101'),
				PARTITION p1994 VALUES LESS THAN ('19950101'),
				PARTITION p1995 VALUES LESS THAN ('19960101'),
				PARTITION p1996 VALUES LESS THAN ('19970101'),
				PARTITION p1997 VALUES LESS THAN ('19980101'),
				PARTITION p1998 VALUES LESS THAN ('19990101'),
				PARTITION p1999 VALUES LESS THAN ('20000101'),
				PARTITION p2000 VALUES LESS THAN ('20010101'),
				PARTITION p2001 VALUES LESS THAN ('20020101'),
				PARTITION p2002 VALUES LESS THAN ('20030101'),
				PARTITION p2003 VALUES LESS THAN ('20040101'),
				PARTITION p2004 VALUES LESS THAN ('20050101'),
				PARTITION p2005 VALUES LESS THAN ('20060101'),
				PARTITION p2006 VALUES LESS THAN ('20070101'),
				PARTITION p2007 VALUES LESS THAN ('20080101'),
				PARTITION p2008 VALUES LESS THAN ('20090101'),
				PARTITION p2009 VALUES LESS THAN ('20100101'),
				PARTITION p2010 VALUES LESS THAN ('20110101'),
				PARTITION p2011 VALUES LESS THAN ('20120101'),
				PARTITION p2012 VALUES LESS THAN ('20130101'),
				PARTITION p2013 VALUES LESS THAN ('20140101'),
				PARTITION p2014 VALUES LESS THAN ('20150101'),
				PARTITION p2015 VALUES LESS THAN ('20160101'),
				PARTITION p2016 VALUES LESS THAN ('20170101'),
				PARTITION p2017 VALUES LESS THAN ('20180101'),
				PARTITION p2018 VALUES LESS THAN ('20190101'),
				PARTITION p2019 VALUES LESS THAN ('20200101'),
				PARTITION p2020 VALUES LESS THAN ('20210101'),
				PARTITION p2021 VALUES LESS THAN ('20220101'),
				PARTITION p2022 VALUES LESS THAN ('20230101'),
				PARTITION p2023 VALUES LESS THAN ('20240101'),
				PARTITION p2024 VALUES LESS THAN ('20250101')
				);

    		""")

    # 仓单日报
    cursor.execute("""CREATE TABLE IF NOT EXISTS future_wsr
    		(trade_date DATE,symbol VARCHAR(20),fut_name VARCHAR(20),warehouse VARCHAR(20),wh_id VARCHAR(20),
    		pre_vol DECIMAL(18,6),vol INT,vol_chg INT,area VARCHAR(20),year DECIMAL(16,6),grade VARCHAR(20),
    		is_ct VARCHAR(20),unit VARCHAR(20),exchange VARCHAR(20)
    		);
    		""")

    # 结算参数  以trade_date获取
    cursor.execute("""CREATE TABLE IF NOT EXISTS future_settle
    		(ts_code VARCHAR(20),trade_date DATE,settle DECIMAL(17,6),trading_fee_rate DECIMAL(13,6),
    		trading_fee DECIMAL(14,6),delivery_fee DECIMAL(13,6),b_hedging_margin_rate DECIMAL(13,6),
    		s_hedging_margin_rate DECIMAL(13,6),long_margin_rate DECIMAL(13,6),short_margin_rate DECIMAL(13,6)
    		);
    		""")

    # 南华期货指数日线行情  ！！！以ts_code获取'CU.NH'
    cursor.execute("""CREATE TABLE IF NOT EXISTS future_index_daily
    		(ts_code VARCHAR(20),trade_date DATE,close DECIMAL(16,6),open DECIMAL(16,6),high DECIMAL(16,6),
    		low DECIMAL(16,6),pre_close DECIMAL(16,6),`change` DECIMAL(15,6),pct_chg DECIMAL(13,6),vol DECIMAL(19,6),
    		`amount` DECIMAL(18,6)
    		);
    		""")

    # 期货主力与连续合约 以trade_date获取
    cursor.execute("""CREATE TABLE IF NOT EXISTS future_mapping
    		(ts_code VARCHAR(20),trade_date DATE,mapping_ts_code VARCHAR(20)
    		);
    		""")

    conn.commit()

# 指数数据
def create_index_data():
    cursor.execute("CREATE DATABASE IF NOT EXISTS index_data")  # 创建基础数据库
    cursor.execute("use index_data ")
    cursor.execute("""CREATE TABLE IF NOT EXISTS index_basic (
            ts_code VARCHAR(20) NOT NULL COMMENT 'TS代码',
            name VARCHAR(100) COMMENT '简称',
            fullname TEXT COMMENT '指数全称',
            market VARCHAR(20) COMMENT '市场',
            publisher VARCHAR(100) COMMENT '发布方',
            index_type VARCHAR(50) COMMENT '指数风格',
            category VARCHAR(50) COMMENT '指数类别',
            base_date DATE COMMENT '基期',
            base_point DECIMAL(16,6) COMMENT '基点',
            list_date DATE COMMENT '发布日期',
            exp_date DATE COMMENT '终止日期',
            weight_rule VARCHAR(100) COMMENT '加权方式',
            `desc` TEXT COMMENT '描述',
            PRIMARY KEY (ts_code),
            KEY idx_market (market),
            KEY idx_category (category),
            KEY idx_list_date (list_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    		""")
    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS index_daily
                   (
                       ts_code
                       VARCHAR
                   (
                       20
                   ) NOT NULL,
                       trade_date DATE NOT NULL,
                       close DECIMAL
                   (
                       16,
                       6
                   ), open DECIMAL
                   (
                       16,
                       6
                   ), high DECIMAL
                   (
                       16,
                       6
                   ), low DECIMAL
                   (
                       16,
                       6
                   ),
                       pre_close DECIMAL
                   (
                       16,
                       6
                   ), `change` DECIMAL
                   (
                       15,
                       6
                   ), pct_chg DECIMAL
                   (
                       13,
                       6
                   ),
                       vol DECIMAL
                   (
                       21,
                       6
                   ), amount DECIMAL
                   (
                       21,
                       6
                   ),
                       PRIMARY KEY
                   (
                       ts_code,
                       trade_date
                   ),
                       KEY idx_trade_date
                   (
                       trade_date
                   )
                       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                   """)


# 基金数据
def create_funds_data():
    cursor.execute("CREATE DATABASE IF NOT EXISTS funds_data")  # 创建基础数据库
    cursor.execute("use funds_data ")
    cursor.execute("""CREATE TABLE IF NOT EXISTS fund_basic
    		(ts_code VARCHAR(20),trade_date DATE,open DECIMAL(17,6),close DECIMAL(17,6),high DECIMAL(17,6),
    		low DECIMAL(17,6),pre_close DECIMAL(17,6),`change` DECIMAL(16,6),pct_chg DECIMAL(13,6),swing DECIMAL(13,6),
    		vol DECIMAL(20,6)
    		);
    		""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS fund_company
    		(name TEXT,shortname TEXT,province VARCHAR(20),city VARCHAR(20),address TEXT,phone TEXT,office TEXT,
    		website TEXT,chairman TEXT,manager TEXT,reg_capital DECIMAL(20,6),setup_date DATE,end_date DATE,
    		employees DECIMAL(18,6),main_business TEXT,org_code VARCHAR(20),credit_code TEXT
    		);
    		""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS fund_manager
    		(ts_code VARCHAR(20),ann_date DATE,name VARCHAR(20),gender VARCHAR(20),birth_year VARCHAR(20),edu VARCHAR(20),nationality VARCHAR(20),begin_date DATE,end_date DATE,resume TEXT
    		);
    		""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS fund_share
    		(ts_code VARCHAR(20),trade_date DATE,fd_share DECIMAL(20,6),fund_type VARCHAR(20),market VARCHAR(20)
    		);
    		""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS fund_nav
    		(ts_code VARCHAR(20),ann_date DATE,nav_date DATE,unit_nav DECIMAL(15,6),accum_nav DECIMAL(14,6),accum_div DECIMAL(17,6),net_asset DECIMAL(22,6),total_netasset DECIMAL(22,6),adj_nav DECIMAL(17,6),update_flag INT(2)
    		);
    		""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS fund_div
    		(ts_code VARCHAR(20),ann_date DATE,imp_anndate DATE,base_date DATE,div_proc VARCHAR(20),record_date DATE,ex_date DATE,pay_date DATE,earpay_date DATE,net_ex_date DATE,div_cash DECIMAL(13,6),base_unit DECIMAL(18,6),ear_distr DECIMAL(21,6),ear_amount DECIMAL(21,6),account_date DATE,base_year VARCHAR(20)
    		);
    		""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS fund_portfolio
    		(ts_code VARCHAR(20),ann_date DATE,end_date DATE,symbol VARCHAR(20),mkv DECIMAL(20,6),amount DECIMAL(20,6),stk_mkv_ratio DECIMAL(14,6),stk_float_ratio DECIMAL(13,6)
    		);
    		""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS fund_daily
    		(ts_code VARCHAR(20),trade_date DATE,pre_close DECIMAL(15,6),open DECIMAL(15,6),high DECIMAL(15,6),low DECIMAL(15,6),close DECIMAL(15,6),`change` DECIMAL(13,6),pct_chg DECIMAL(13,6),vol DECIMAL(20,6),amount DECIMAL(20,6)
    		);
    		""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS fund_daily
    		(ts_code VARCHAR(20),trade_date DATE,pre_close DECIMAL(15,6),open DECIMAL(15,6),high DECIMAL(15,6),low DECIMAL(15,6),close DECIMAL(15,6),`change` DECIMAL(13,6),pct_chg DECIMAL(13,6),vol DECIMAL(20,6),amount DECIMAL(20,6)
    		);
    		""")


def create_exchange_data():
    cursor.execute("CREATE DATABASE IF NOT EXISTS exchange_data")  # 创建基础数据库
    cursor.execute("use exchange_data")
    cursor.execute("""CREATE TABLE IF NOT EXISTS fx_obasic
    		(ts_code VARCHAR(20),name TEXT,classify VARCHAR(20),exchange VARCHAR(20),min_unit DECIMAL(15,6),max_unit DECIMAL(18,6),pip DECIMAL(13,6),pip_cost DECIMAL(14,6),traget_spread DECIMAL(14,6),min_stop_distance DECIMAL(14,6),trading_hours TEXT,break_time TEXT
    		);
    		""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS fx_daily
    		(ts_code VARCHAR(20),trade_date DATE,bid_open DECIMAL(17,6),bid_close DECIMAL(17,6),bid_high DECIMAL(17,6),bid_low DECIMAL(17,6),ask_open DECIMAL(17,6),ask_close DECIMAL(17,6),ask_high DECIMAL(17,6),ask_low DECIMAL(17,6),tick_qty INT
    		);
    		""")


# 期货分钟行情
def create_future_minute():
    cursor.execute("CREATE DATABASE IF NOT EXISTS future_minute")  # 创建基础数据库
    cursor.execute("use future_minute ")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS future_minutes
                (ts_code VARCHAR(20),trade_time TIMESTAMP,open DECIMAL(10,3),close DECIMAL(10,3),high DECIMAL(10,3),
                low DECIMAL(10,3),vol DECIMAL(10,3),amount DECIMAL(18,4),oi DECIMAL(18,4)
                )
                partition by range (unix_timestamp(trade_time))
            (
            PARTITION p1001 VALUES LESS THAN (UNIX_TIMESTAMP('2010-02-01')),
            PARTITION p1002 VALUES LESS THAN (UNIX_TIMESTAMP('2010-03-01')),
            PARTITION p1003 VALUES LESS THAN (UNIX_TIMESTAMP('2010-04-01')),
            PARTITION p1004 VALUES LESS THAN (UNIX_TIMESTAMP('2010-05-01')),
            PARTITION p1005 VALUES LESS THAN (UNIX_TIMESTAMP('2010-06-01')),
            PARTITION p1006 VALUES LESS THAN (UNIX_TIMESTAMP('2010-07-01')),
            PARTITION p1007 VALUES LESS THAN (UNIX_TIMESTAMP('2010-08-01')),
            PARTITION p1008 VALUES LESS THAN (UNIX_TIMESTAMP('2010-09-01')),
            PARTITION p1009 VALUES LESS THAN (UNIX_TIMESTAMP('2010-10-01')),
            PARTITION p1010 VALUES LESS THAN (UNIX_TIMESTAMP('2010-11-01')),
            PARTITION p1011 VALUES LESS THAN (UNIX_TIMESTAMP('2010-12-01')),
            PARTITION p1012 VALUES LESS THAN (UNIX_TIMESTAMP('2011-01-01')),
            PARTITION p1101 VALUES LESS THAN (UNIX_TIMESTAMP('2011-02-01')),
            PARTITION p1102 VALUES LESS THAN (UNIX_TIMESTAMP('2011-03-01')),
            PARTITION p1103 VALUES LESS THAN (UNIX_TIMESTAMP('2011-04-01')),
            PARTITION p1104 VALUES LESS THAN (UNIX_TIMESTAMP('2011-05-01')),
            PARTITION p1105 VALUES LESS THAN (UNIX_TIMESTAMP('2011-06-01')),
            PARTITION p1106 VALUES LESS THAN (UNIX_TIMESTAMP('2011-07-01')),
            PARTITION p1107 VALUES LESS THAN (UNIX_TIMESTAMP('2011-08-01')),
            PARTITION p1108 VALUES LESS THAN (UNIX_TIMESTAMP('2011-09-01')),
            PARTITION p1109 VALUES LESS THAN (UNIX_TIMESTAMP('2011-10-01')),
            PARTITION p1110 VALUES LESS THAN (UNIX_TIMESTAMP('2011-11-01')),
            PARTITION p1111 VALUES LESS THAN (UNIX_TIMESTAMP('2011-12-01')),
            PARTITION p1112 VALUES LESS THAN (UNIX_TIMESTAMP('2012-01-01')),
            PARTITION p1201 VALUES LESS THAN (UNIX_TIMESTAMP('2012-02-01')),
            PARTITION p1202 VALUES LESS THAN (UNIX_TIMESTAMP('2012-03-01')),
            PARTITION p1203 VALUES LESS THAN (UNIX_TIMESTAMP('2012-04-01')),
            PARTITION p1204 VALUES LESS THAN (UNIX_TIMESTAMP('2012-05-01')),
            PARTITION p1205 VALUES LESS THAN (UNIX_TIMESTAMP('2012-06-01')),
            PARTITION p1206 VALUES LESS THAN (UNIX_TIMESTAMP('2012-07-01')),
            PARTITION p1207 VALUES LESS THAN (UNIX_TIMESTAMP('2012-08-01')),
            PARTITION p1208 VALUES LESS THAN (UNIX_TIMESTAMP('2012-09-01')),
            PARTITION p1209 VALUES LESS THAN (UNIX_TIMESTAMP('2012-10-01')),
            PARTITION p1210 VALUES LESS THAN (UNIX_TIMESTAMP('2012-11-01')),
            PARTITION p1211 VALUES LESS THAN (UNIX_TIMESTAMP('2012-12-01')),
            PARTITION p1212 VALUES LESS THAN (UNIX_TIMESTAMP('2013-01-01')),
            PARTITION p1301 VALUES LESS THAN (UNIX_TIMESTAMP('2013-02-01')),
            PARTITION p1302 VALUES LESS THAN (UNIX_TIMESTAMP('2013-03-01')),
            PARTITION p1303 VALUES LESS THAN (UNIX_TIMESTAMP('2013-04-01')),
            PARTITION p1304 VALUES LESS THAN (UNIX_TIMESTAMP('2013-05-01')),
            PARTITION p1305 VALUES LESS THAN (UNIX_TIMESTAMP('2013-06-01')),
            PARTITION p1306 VALUES LESS THAN (UNIX_TIMESTAMP('2013-07-01')),
            PARTITION p1307 VALUES LESS THAN (UNIX_TIMESTAMP('2013-08-01')),
            PARTITION p1308 VALUES LESS THAN (UNIX_TIMESTAMP('2013-09-01')),
            PARTITION p1309 VALUES LESS THAN (UNIX_TIMESTAMP('2013-10-01')),
            PARTITION p1310 VALUES LESS THAN (UNIX_TIMESTAMP('2013-11-01')),
            PARTITION p1311 VALUES LESS THAN (UNIX_TIMESTAMP('2013-12-01')),
            PARTITION p1312 VALUES LESS THAN (UNIX_TIMESTAMP('2014-01-01')),
            PARTITION p1401 VALUES LESS THAN (UNIX_TIMESTAMP('2014-02-01')),
            PARTITION p1402 VALUES LESS THAN (UNIX_TIMESTAMP('2014-03-01')),
            PARTITION p1403 VALUES LESS THAN (UNIX_TIMESTAMP('2014-04-01')),
            PARTITION p1404 VALUES LESS THAN (UNIX_TIMESTAMP('2014-05-01')),
            PARTITION p1405 VALUES LESS THAN (UNIX_TIMESTAMP('2014-06-01')),
            PARTITION p1406 VALUES LESS THAN (UNIX_TIMESTAMP('2014-07-01')),
            PARTITION p1407 VALUES LESS THAN (UNIX_TIMESTAMP('2014-08-01')),
            PARTITION p1408 VALUES LESS THAN (UNIX_TIMESTAMP('2014-09-01')),
            PARTITION p1409 VALUES LESS THAN (UNIX_TIMESTAMP('2014-10-01')),
            PARTITION p1410 VALUES LESS THAN (UNIX_TIMESTAMP('2014-11-01')),
            PARTITION p1411 VALUES LESS THAN (UNIX_TIMESTAMP('2014-12-01')),
            PARTITION p1412 VALUES LESS THAN (UNIX_TIMESTAMP('2015-01-01')),
            PARTITION p1501 VALUES LESS THAN (UNIX_TIMESTAMP('2015-02-01')),
            PARTITION p1502 VALUES LESS THAN (UNIX_TIMESTAMP('2015-03-01')),
            PARTITION p1503 VALUES LESS THAN (UNIX_TIMESTAMP('2015-04-01')),
            PARTITION p1504 VALUES LESS THAN (UNIX_TIMESTAMP('2015-05-01')),
            PARTITION p1505 VALUES LESS THAN (UNIX_TIMESTAMP('2015-06-01')),
            PARTITION p1506 VALUES LESS THAN (UNIX_TIMESTAMP('2015-07-01')),
            PARTITION p1507 VALUES LESS THAN (UNIX_TIMESTAMP('2015-08-01')),
            PARTITION p1508 VALUES LESS THAN (UNIX_TIMESTAMP('2015-09-01')),
            PARTITION p1509 VALUES LESS THAN (UNIX_TIMESTAMP('2015-10-01')),
            PARTITION p1510 VALUES LESS THAN (UNIX_TIMESTAMP('2015-11-01')),
            PARTITION p1511 VALUES LESS THAN (UNIX_TIMESTAMP('2015-12-01')),
            PARTITION p1512 VALUES LESS THAN (UNIX_TIMESTAMP('2016-01-01')),
            PARTITION p1601 VALUES LESS THAN (UNIX_TIMESTAMP('2016-02-01')),
            PARTITION p1602 VALUES LESS THAN (UNIX_TIMESTAMP('2016-03-01')),
            PARTITION p1603 VALUES LESS THAN (UNIX_TIMESTAMP('2016-04-01')),
            PARTITION p1604 VALUES LESS THAN (UNIX_TIMESTAMP('2016-05-01')),
            PARTITION p1605 VALUES LESS THAN (UNIX_TIMESTAMP('2016-06-01')),
            PARTITION p1606 VALUES LESS THAN (UNIX_TIMESTAMP('2016-07-01')),
            PARTITION p1607 VALUES LESS THAN (UNIX_TIMESTAMP('2016-08-01')),
            PARTITION p1608 VALUES LESS THAN (UNIX_TIMESTAMP('2016-09-01')),
            PARTITION p1609 VALUES LESS THAN (UNIX_TIMESTAMP('2016-10-01')),
            PARTITION p1610 VALUES LESS THAN (UNIX_TIMESTAMP('2016-11-01')),
            PARTITION p1611 VALUES LESS THAN (UNIX_TIMESTAMP('2016-12-01')),
            PARTITION p1612 VALUES LESS THAN (UNIX_TIMESTAMP('2017-01-01')),
            PARTITION p1701 VALUES LESS THAN (UNIX_TIMESTAMP('2017-02-01')),
            PARTITION p1702 VALUES LESS THAN (UNIX_TIMESTAMP('2017-03-01')),
            PARTITION p1703 VALUES LESS THAN (UNIX_TIMESTAMP('2017-04-01')),
            PARTITION p1704 VALUES LESS THAN (UNIX_TIMESTAMP('2017-05-01')),
            PARTITION p1705 VALUES LESS THAN (UNIX_TIMESTAMP('2017-06-01')),
            PARTITION p1706 VALUES LESS THAN (UNIX_TIMESTAMP('2017-07-01')),
            PARTITION p1707 VALUES LESS THAN (UNIX_TIMESTAMP('2017-08-01')),
            PARTITION p1708 VALUES LESS THAN (UNIX_TIMESTAMP('2017-09-01')),
            PARTITION p1709 VALUES LESS THAN (UNIX_TIMESTAMP('2017-10-01')),
            PARTITION p1710 VALUES LESS THAN (UNIX_TIMESTAMP('2017-11-01')),
            PARTITION p1711 VALUES LESS THAN (UNIX_TIMESTAMP('2017-12-01')),
            PARTITION p1712 VALUES LESS THAN (UNIX_TIMESTAMP('2018-01-01')),
            PARTITION p1801 VALUES LESS THAN (UNIX_TIMESTAMP('2018-02-01')),
            PARTITION p1802 VALUES LESS THAN (UNIX_TIMESTAMP('2018-03-01')),
            PARTITION p1803 VALUES LESS THAN (UNIX_TIMESTAMP('2018-04-01')),
            PARTITION p1804 VALUES LESS THAN (UNIX_TIMESTAMP('2018-05-01')),
            PARTITION p1805 VALUES LESS THAN (UNIX_TIMESTAMP('2018-06-01')),
            PARTITION p1806 VALUES LESS THAN (UNIX_TIMESTAMP('2018-07-01')),
            PARTITION p1807 VALUES LESS THAN (UNIX_TIMESTAMP('2018-08-01')),
            PARTITION p1808 VALUES LESS THAN (UNIX_TIMESTAMP('2018-09-01')),
            PARTITION p1809 VALUES LESS THAN (UNIX_TIMESTAMP('2018-10-01')),
            PARTITION p1810 VALUES LESS THAN (UNIX_TIMESTAMP('2018-11-01')),
            PARTITION p1811 VALUES LESS THAN (UNIX_TIMESTAMP('2018-12-01')),
            PARTITION p1812 VALUES LESS THAN (UNIX_TIMESTAMP('2019-01-01')),
            PARTITION p1901 VALUES LESS THAN (UNIX_TIMESTAMP('2019-02-01')),
            PARTITION p1902 VALUES LESS THAN (UNIX_TIMESTAMP('2019-03-01')),
            PARTITION p1903 VALUES LESS THAN (UNIX_TIMESTAMP('2019-04-01')),
            PARTITION p1904 VALUES LESS THAN (UNIX_TIMESTAMP('2019-05-01')),
            PARTITION p1905 VALUES LESS THAN (UNIX_TIMESTAMP('2019-06-01')),
            PARTITION p1906 VALUES LESS THAN (UNIX_TIMESTAMP('2019-07-01')),
            PARTITION p1907 VALUES LESS THAN (UNIX_TIMESTAMP('2019-08-01')),
            PARTITION p1908 VALUES LESS THAN (UNIX_TIMESTAMP('2019-09-01')),
            PARTITION p1909 VALUES LESS THAN (UNIX_TIMESTAMP('2019-10-01')),
            PARTITION p1910 VALUES LESS THAN (UNIX_TIMESTAMP('2019-11-01')),
            PARTITION p1911 VALUES LESS THAN (UNIX_TIMESTAMP('2019-12-01')),
            PARTITION p1912 VALUES LESS THAN (UNIX_TIMESTAMP('2020-01-01')),
            PARTITION p2001 VALUES LESS THAN (UNIX_TIMESTAMP('2020-02-01')),
            PARTITION p2002 VALUES LESS THAN (UNIX_TIMESTAMP('2020-03-01')),
            PARTITION p2003 VALUES LESS THAN (UNIX_TIMESTAMP('2020-04-01')),
            PARTITION p2004 VALUES LESS THAN (UNIX_TIMESTAMP('2020-05-01')),
            PARTITION p2005 VALUES LESS THAN (UNIX_TIMESTAMP('2020-06-01')),
            PARTITION p2006 VALUES LESS THAN (UNIX_TIMESTAMP('2020-07-01')),
            PARTITION p2007 VALUES LESS THAN (UNIX_TIMESTAMP('2020-08-01')),
            PARTITION p2008 VALUES LESS THAN (UNIX_TIMESTAMP('2020-09-01')),
            PARTITION p2009 VALUES LESS THAN (UNIX_TIMESTAMP('2020-10-01')),
            PARTITION p2010 VALUES LESS THAN (UNIX_TIMESTAMP('2020-11-01')),
            PARTITION p2011 VALUES LESS THAN (UNIX_TIMESTAMP('2020-12-01')),
            PARTITION p2012 VALUES LESS THAN (UNIX_TIMESTAMP('2021-01-01')),
            PARTITION p2101 VALUES LESS THAN (UNIX_TIMESTAMP('2021-02-01')),
            PARTITION p2102 VALUES LESS THAN (UNIX_TIMESTAMP('2021-03-01')),
            PARTITION p2103 VALUES LESS THAN (UNIX_TIMESTAMP('2021-04-01')),
            PARTITION p2104 VALUES LESS THAN (UNIX_TIMESTAMP('2021-05-01')),
            PARTITION p2105 VALUES LESS THAN (UNIX_TIMESTAMP('2021-06-01')),
            PARTITION p2106 VALUES LESS THAN (UNIX_TIMESTAMP('2021-07-01')),
            PARTITION p2107 VALUES LESS THAN (UNIX_TIMESTAMP('2021-08-01')),
            PARTITION p2108 VALUES LESS THAN (UNIX_TIMESTAMP('2021-09-01')),
            PARTITION p2109 VALUES LESS THAN (UNIX_TIMESTAMP('2021-10-01')),
            PARTITION p2110 VALUES LESS THAN (UNIX_TIMESTAMP('2021-11-01')),
            PARTITION p2111 VALUES LESS THAN (UNIX_TIMESTAMP('2021-12-01')),
            PARTITION p2112 VALUES LESS THAN (UNIX_TIMESTAMP('2022-01-01')),
            PARTITION p2201 VALUES LESS THAN (UNIX_TIMESTAMP('2022-02-01')),
            PARTITION p2202 VALUES LESS THAN (UNIX_TIMESTAMP('2022-03-01')),
            PARTITION p2203 VALUES LESS THAN (UNIX_TIMESTAMP('2022-04-01')),
            PARTITION p2204 VALUES LESS THAN (UNIX_TIMESTAMP('2022-05-01')),
            PARTITION p2205 VALUES LESS THAN (UNIX_TIMESTAMP('2022-06-01')),
            PARTITION p2206 VALUES LESS THAN (UNIX_TIMESTAMP('2022-07-01')),
            PARTITION p2207 VALUES LESS THAN (UNIX_TIMESTAMP('2022-08-01')),
            PARTITION p2208 VALUES LESS THAN (UNIX_TIMESTAMP('2022-09-01')),
            PARTITION p2209 VALUES LESS THAN (UNIX_TIMESTAMP('2022-10-01')),
            PARTITION p2210 VALUES LESS THAN (UNIX_TIMESTAMP('2022-11-01')),
            PARTITION p2211 VALUES LESS THAN (UNIX_TIMESTAMP('2022-12-01')),
            PARTITION p2212 VALUES LESS THAN (UNIX_TIMESTAMP('2023-01-01')),
            PARTITION p2301 VALUES LESS THAN (UNIX_TIMESTAMP('2023-02-01')),
            PARTITION p2302 VALUES LESS THAN (UNIX_TIMESTAMP('2023-03-01')),
            PARTITION p2303 VALUES LESS THAN (UNIX_TIMESTAMP('2023-04-01')),
            PARTITION p2304 VALUES LESS THAN (UNIX_TIMESTAMP('2023-05-01')),
            PARTITION p2305 VALUES LESS THAN (UNIX_TIMESTAMP('2023-06-01')),
            PARTITION p2306 VALUES LESS THAN (UNIX_TIMESTAMP('2023-07-01')),
            PARTITION p2307 VALUES LESS THAN (UNIX_TIMESTAMP('2023-08-01')),
            PARTITION p2308 VALUES LESS THAN (UNIX_TIMESTAMP('2023-09-01')),
            PARTITION p2309 VALUES LESS THAN (UNIX_TIMESTAMP('2023-10-01')),
            PARTITION p2310 VALUES LESS THAN (UNIX_TIMESTAMP('2023-11-01')),
            PARTITION p2311 VALUES LESS THAN (UNIX_TIMESTAMP('2023-12-01')),
            PARTITION p2312 VALUES LESS THAN (UNIX_TIMESTAMP('2024-01-01'))
            );
            """)

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS mhi
    		(ts_code VARCHAR(20),trade_date TIMESTAMP,open DECIMAL(17,6),
    		high DECIMAL(17,6),low DECIMAL(17,6),close DECIMAL(17,6),vol DECIMAL(19,6)
    		)
            """
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS mhi_5m
    		(ts_code VARCHAR(20),trade_date TIMESTAMP,open DECIMAL(17,6),
    		high DECIMAL(17,6),low DECIMAL(17,6),close DECIMAL(17,6),vol DECIMAL(19,6)
    		)
            """
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS mhi_10m
    		(ts_code VARCHAR(20),trade_date TIMESTAMP,open DECIMAL(17,6),
    		high DECIMAL(17,6),low DECIMAL(17,6),close DECIMAL(17,6),vol DECIMAL(19,6)
    		)
            """
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS mhi_15m
    		(ts_code VARCHAR(20),trade_date TIMESTAMP,open DECIMAL(17,6),
    		high DECIMAL(17,6),low DECIMAL(17,6),close DECIMAL(17,6),vol DECIMAL(19,6)
    		)
            """
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS mhi_30m
    		(ts_code VARCHAR(20),trade_date TIMESTAMP,open DECIMAL(17,6),
    		high DECIMAL(17,6),low DECIMAL(17,6),close DECIMAL(17,6),vol DECIMAL(19,6)
    		)
            """
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS mhi_1h
    		(ts_code VARCHAR(20),trade_date TIMESTAMP,open DECIMAL(17,6),
    		high DECIMAL(17,6),low DECIMAL(17,6),close DECIMAL(17,6),vol DECIMAL(19,6)
    		)
            """
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS mhi_2h
    		(ts_code VARCHAR(20),trade_date TIMESTAMP,open DECIMAL(17,6),
    		high DECIMAL(17,6),low DECIMAL(17,6),close DECIMAL(17,6),vol DECIMAL(19,6)
    		)
            """
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS mhi_3h
    		(ts_code VARCHAR(20),trade_date TIMESTAMP,open DECIMAL(17,6),
    		high DECIMAL(17,6),low DECIMAL(17,6),close DECIMAL(17,6),vol DECIMAL(19,6)
    		)
            """
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS mhi_4h
    		(ts_code VARCHAR(20),trade_date TIMESTAMP,open DECIMAL(17,6),
    		high DECIMAL(17,6),low DECIMAL(17,6),close DECIMAL(17,6),vol DECIMAL(19,6)
    		)
            """
    )

    conn.commit()

def create_cme():
    cursor.execute("CREATE DATABASE IF NOT EXISTS cme")  # 创建基础数据库
    cursor.execute("use cme ")

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS mnq_1m
    		(ts_code VARCHAR(20),trade_date TIMESTAMP,open DECIMAL(17,6),
    		high DECIMAL(17,6),low DECIMAL(17,6),close DECIMAL(17,6),vol DECIMAL(19,6)
    		)
            """
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS mnq_5m
    		(ts_code VARCHAR(20),trade_date TIMESTAMP,open DECIMAL(17,6),
    		high DECIMAL(17,6),low DECIMAL(17,6),close DECIMAL(17,6),vol DECIMAL(19,6)
    		)
            """
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS mnq_10m
    		(ts_code VARCHAR(20),trade_date TIMESTAMP,open DECIMAL(17,6),
    		high DECIMAL(17,6),low DECIMAL(17,6),close DECIMAL(17,6),vol DECIMAL(19,6)
    		)
            """
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS mnq_15m
    		(ts_code VARCHAR(20),trade_date TIMESTAMP,open DECIMAL(17,6),
    		high DECIMAL(17,6),low DECIMAL(17,6),close DECIMAL(17,6),vol DECIMAL(19,6)
    		)
            """
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS mnq_30m
    		(ts_code VARCHAR(20),trade_date TIMESTAMP,open DECIMAL(17,6),
    		high DECIMAL(17,6),low DECIMAL(17,6),close DECIMAL(17,6),vol DECIMAL(19,6)
    		)
            """
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS mnq_1h
    		(ts_code VARCHAR(20),trade_date TIMESTAMP,open DECIMAL(17,6),
    		high DECIMAL(17,6),low DECIMAL(17,6),close DECIMAL(17,6),vol DECIMAL(19,6)
    		)
            """
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS mnq_2h
    		(ts_code VARCHAR(20),trade_date TIMESTAMP,open DECIMAL(17,6),
    		high DECIMAL(17,6),low DECIMAL(17,6),close DECIMAL(17,6),vol DECIMAL(19,6)
    		)
            """
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS mnq_3h
    		(ts_code VARCHAR(20),trade_date TIMESTAMP,open DECIMAL(17,6),
    		high DECIMAL(17,6),low DECIMAL(17,6),close DECIMAL(17,6),vol DECIMAL(19,6)
    		)
            """
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS mnq_4h
    		(ts_code VARCHAR(20),trade_date TIMESTAMP,open DECIMAL(17,6),
    		high DECIMAL(17,6),low DECIMAL(17,6),close DECIMAL(17,6),vol DECIMAL(19,6)
    		)
            """
    )

    conn.commit()

# 创建日志数据库
def create_log_info():
    cursor.execute("CREATE DATABASE IF NOT EXISTS log_info")  # 创建基础数据库
    cursor.execute("use log_info ")
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS lost_data
                            (table_name TEXT,argument1 TEXT,argument2 TEXT,argument3 TEXT,reason TEXT,
                            occur_time TIMESTAMP,database_name VARCHAR(30)
                            );
                """
    )

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS lost_table
                            (table_name TEXT,start_date DATE,end_date DATE,reason TEXT,
                            occur_time TIMESTAMP,database_name VARCHAR(30)
                            );
                """
    )

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS tosql_record
                            (table_name TEXT,start_date DATE,end_date DATE,
                            occur_time TIMESTAMP,database_name VARCHAR(30)
                            );
                """
    )

    conn.commit()


if __name__ == '__main__':
    create_stock_basic_data_database()
    create_etf_data()
    create_log_info()
    create_futures_data()
    create_option_data()
    create_index_data()
    create_funds_data()
    create_exchange_data()
    create_future_minute()
    create_cme()
    '''
    cursor.execute("drop database log_info ")
    cursor.execute("drop database  stock_basic_data")
    cursor.execute("drop database  futures_data")
    cursor.execute("drop database  option_data")
    cursor.execute("drop database  funds_data")
    cursor.execute("drop database  index_data")
    cursor.execute("drop database  exchange_data")

    cursor.execute("drop database  future_minute")

    '''
