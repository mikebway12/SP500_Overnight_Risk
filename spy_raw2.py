import pandas as pd
import pandas_datareader.data as web
import datetime as dt
import numpy as np
import sqlite3

#Upload symbols from SP500 into dfsym
cnx = sqlite3.connect('SP500.sqlite')
dfsym= pd.read_sql_query("SELECT Symbol FROM SP500", cnx)
cnx.close()

start = dt.datetime(2012, 1, 1)
end= dt.datetime(2017, 3, 13)
gap_count = 1
lista = list()

#Range in dfsym['Symbol'][x] is arbitrary-505 total stocks in S&P, SPY is in DB as well
for symbol in dfsym['Symbol'][0:200]:
	lista.append(symbol)
print(lista)

#listb = ['BRK-B','BF-B'] - Yahoo doesn't analyze stocks with '.'. Need to convertto '-'. BRK.B to BRK-B and BF.B to BF-B

for symbol in lista:
	df = web.DataReader(symbol, 'yahoo', start, end)
	df.sort_index(inplace=True, ascending=False)
	df.index = df.index.date
	gap_count = 1
	#Calculate Change (AdjClose - Previous Day Adj Close), Gap(Open - Previous Day Adj Close), Gap%(Gap/Previous Day Adj Close)
	
	for row in range(len(df)):
		df['Change'] = df['Close'] - df['Close'].shift(-1)
		if gap_count < len(df):
			df['Gap'] = df['Open'] - df['Close'].shift(-1)
			df['Gap%'] = df['Gap']/df['Close'].shift(-1)
			gap_count=gap_count + 1
			df['Gap%'] = pd.Series(['{0:.2f}%'.format(val*100) for val in df['Gap%']], index = df.index)
		
		else: break
		
	#Create New Final_raw_data SQLite DB
	conn = sqlite3.connect('FINAL_raw_data.sqlite')
	cur = conn.cursor()

	cur.execute('''CREATE TABLE IF NOT EXISTS SP500_Raw
		(Row_number INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
		Date_id TEXT NOT NULL,
		Symbol TEXT, Open INTEGER, High INTEGER, Low INTEGER, Close INTEGER, Gap INTEGER, Gap_prct INTEGER)''')
		
	#Storing Data into variables and inserting into SQLite

	for index,row in df.iterrows():
		date_id = index
		open = round(float(row['Open']),2)
		high = round(float(row['High']),2)
		low = round(float(row['Low']),2)
		close = round(float(row['Close']),2)
		try:
			gap = round(float(row['Gap']),2)
			gap_prct = row['Gap%']
		except:
			continue
		cur.execute('INSERT INTO SP500_Raw (Date_id, Symbol, Open, High, Low, Close, Gap, Gap_prct) VALUES (?,?,?,?,?,?,?,?)',(date_id, symbol, open,high,low,close,gap,gap_prct))
		print(date_id,symbol,open,high,low,close, gap, gap_prct)
		#cur.execute('DELETE FROM SP500_Raw WHERE Symbol > 0')
	conn.commit()
	cur.close()
	conn.close()