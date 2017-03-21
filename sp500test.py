import pandas as pd
import pandas_datareader.data as web
import datetime as dt
import numpy as np



start = dt.datetime(2017, 2, 1)
end= dt.datetime(2017, 3, 13)
symbol = 'SPY'
df = web.DataReader(symbol, 'yahoo', start, end)

count = 0
df.sort_index(inplace=True, ascending=False)
df.index = df.index.date
change_count = -1
gap_count=1

#Calculate Change (AdjClose - Previous Day Adj Close), Gap(Open - Previous Day Adj Close), Gap%(Gap/Previous Day Adj Close)
for row in range(len(df)):
	df['Change'] = df['Adj Close'] - df['Adj Close'].shift(-1)
	if gap_count < len(df):
		df['Gap'] = df['Open'] - df['Adj Close'].shift(-1)
		df['Gap%'] = df['Gap']/df['Adj Close'].shift(-1)
		gap_count=gap_count + 1
		df['Gap%'] = pd.Series(['{0:.2f}%'.format(val*100) for val in df['Gap%']], index = df.index)
		#open = (df['Open'])
		#close = (df['Close'])
		#gap = (df['Gap'])
		#gap_prct = (df['Gap%'])
		
	else: break
for index,row in df.iterrows():
	date_id = index
	open = round(float(row['Open']),2)
	high = round(float(row['High']),2)
	low = round(float(row['Low']),2)
	close = round(float(row['Adj Close']),2)
	gap = round(float(row['Gap']),2)
	gap_prct = row['Gap%']
	print(date_id,open,high,low,close, gap, gap_prct)


	
