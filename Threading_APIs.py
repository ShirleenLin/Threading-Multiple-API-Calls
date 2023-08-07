import tushare as ts
import time, datetime, sqlite3
from tqdm import tqdm
import pandas as pd
import concurrent.futures
from functools import reduce

def downLoadData(pro):

    start_date = "2014-01-01"
    end_date="2023-08-06"

    conn = sqlite3.connect("/home/ec2-user/Financial0806.sqlite")
    cursor = conn.cursor()

    codelist = pro.stock_basic()

    def income(i):
        return pro.income(ts_code=codelist["ts_code"][i], start_date=start_date, end_date=end_date, fields='ts_code,ann_date,basic_eps,revenue')

    def cashflow(i):
        return pro.cashflow(ts_code=codelist["ts_code"][i], start_date=start_date, end_date=end_date, fields='ts_code,ann_date,net_profit,  free_cashflow')

    def fina_indicator(i):
        return pro.fina_indicator(ts_code=codelist["ts_code"][i], start_date=start_date, end_date=end_date, fields='ts_code,ann_date,debt_to_eqt, roe, revenue_ps, capital_rese_ps, gross_margin, current_ratio,  fcff,  working_capital, networking_capital, retained_earnings, bps,  retainedps, cfps, ebit_ps, fcff_ps, netprofit_margin')

    def balancesheet(i):
        return pro.balancesheet(ts_code=codelist["ts_code"][i], start_date=start_date, end_date=end_date, fields='ts_code,ann_date,cap_rese')

    for i in tqdm(range(len(codelist["ts_code"]))):
        print(i,codelist["ts_code"][i])
        with concurrent.futures.ThreadPoolExecutor() as executor:
            tasks = [executor.submit(income, i),
                executor.submit(cashflow, i),
                executor.submit(fina_indicator, i),
                executor.submit(balancesheet, i ) ]
            results = [task.result() for task in concurrent.futures.as_completed(tasks)]
        list(map(lambda df: print("Processing DataFrame:", df.shape), results))
        results = list(map(lambda df: df.drop_duplicates(subset=['ts_code', 'ann_date'], keep='last').dropna(subset=['ts_code', 'ann_date']), results)) #If rows with the same primary key are not unique, it indicates a data quality issue
        merged_df=pd.concat(list(map(lambda df:df.set_index(['ts_code','ann_date']),results)),axis=1,join='outer').reset_index()
        #REDUCE OPTION: merged_df = reduce(lambda x, y: x.merge(y, on=['ts_code','ann_date']), results)
        merged_df.to_sql(name='Financial in loop', con=conn,if_exists="append",index=False)
    print("Successfully saved financial data into SQL..")
    conn.commit()
    conn.close()
ts.set_token('') #You need to get your own Tushare token from https://tushare.pro/document/1?doc_id=39
pro = ts.pro_api()
downLoadData(pro)
