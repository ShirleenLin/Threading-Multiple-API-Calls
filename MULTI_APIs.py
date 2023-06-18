import tushare as ts
import time, datetime, sqlite3
from tqdm import tqdm
import pandas as pd
import concurrent.futures

def downLoadData(pro):

    start_date = "2019-04-19"
    end_date="2023-06-15"

    conn = sqlite3.connect("/Volumes/movable/Financial0615.sqlite")
    cursor = conn.cursor()

    #Financial Table
    codelist = pro.stock_basic()

    def income(i):
        income = pro.income(ts_code=codelist["ts_code"][i], start_date=start_date, end_date=end_date, fields='ts_code,ann_date,basic_eps, total_profit')
        return income

    def cashflow(i):
        cashflow = pro.cashflow(ts_code=codelist["ts_code"][i], start_date=start_date, end_date=end_date, fields='ts_code,ann_date,net_profit, c_inf_fr_operate_a, n_cashflow_inv_act, free_cashflow, n_cash_flows_fnc_act, im_net_cashflow_oper_act')
        return cashflow

    def fina_indicator(i):
        fina_indicator = pro.fina_indicator(ts_code=codelist["ts_code"][i], start_date=start_date, end_date=end_date, fields='ts_code,ann_date,total_revenue_ps, revenue_ps, capital_rese_ps, surplus_rese_ps, gross_margin, current_ratio, quick_ratio, cash_ratio, fcff, fcfe, working_capital, networking_capital, invest_capital, retained_earnings, bps, ocfps, retainedps, cfps, ebit_ps, fcff_ps, fcfe_ps, netprofit_margin, grossprofit_margin')
        return fina_indicator

    def balancesheet(i):
        balancesheet = pro.balancesheet(ts_code=codelist["ts_code"][i], start_date=start_date, end_date=end_date, fields='ts_code,ann_date,cap_rese,goodwill, fix_assets_total')
        return balancesheet

    for i in tqdm(range(len(codelist["ts_code"]))):
        print(i,codelist["ts_code"][i])
        with concurrent.futures.ThreadPoolExecutor() as executor:
            tasks = [
                executor.submit(income, i),
                executor.submit(cashflow, i),
                executor.submit(fina_indicator, i),
                executor.submit(balancesheet, i )    ]
        # Wait for all the results
            results = [task.result() for task in concurrent.futures.as_completed(tasks)]

        #Merge dataframes from different APIs
        df_new = pd.merge(pd.merge(pd.merge(results[0],results[1],on=['ts_code','ann_date']),results[2],on=['ts_code','ann_date']),results[3],on=['ts_code','ann_date'])
        df_new.to_sql(name='Financial in loop', con=conn,if_exists="append",index=False) #This is the insurance
    sql = "SELECT * FROM [Financial in loop]"
    df_new = pd.read_sql(sql,conn)
    df_new.drop_duplicates(inplace=True)
    df_new.to_sql(name='Financial', con=conn,if_exists="replace",index=False)
    #codelist.to_sql(name='Share_index', con=conn,index=False) #--> Use this line if you want a Share_index Table
    print("Successfully saved financial data into SQL..")
    conn.commit()
    conn.close()

#You need to get your own Tushare token from https://tushare.pro/document/1?doc_id=39
ts.set_token('')
pro = ts.pro_api()
downLoadData(pro)



