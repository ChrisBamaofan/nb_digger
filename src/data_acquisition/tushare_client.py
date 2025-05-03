import akshare as ak

# 获取单只股票的周线数据
df_weekly = ak.stock_zh_a_weekly(symbol="sh600000")  # sh表示沪市， sz表示深市
print(df_weekly)