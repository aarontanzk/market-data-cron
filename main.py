from alpha_vantage.timeseries import TimeSeries
import sqlalchemy
from StaticData import SYMBOLS_LIST, batchSymbolString
#DB Config
TABLE_NAME_US_EQUITIES = "<DB_TABLE_NAME>"
#SYMBOL CONFIG

engine = sqlalchemy.create_engine("mssql+pyodbc://<USERNAME>:<PWD>@<DB_SERVER>/<DB_NAME>?driver=SQL+Server")
connection = engine.connect()

#ALPHAVANTAGE CONFIG
ts = TimeSeries(key='<ALPHAVANTAGE_API_KEY>',output_format='pandas', indexing_type='date')


# main
getBatch()
#pushToDb()
#readFromDb()
print("end")

def pushToDb():
    df, meta_data = ts.get_intraday(SYMBOL_NAME, outputsize='full')
    df['symbol']=SYMBOL_NAME
    df.rename(columns=
                    { 
                        '1. open':'open',
                        '2. high':'high',
                        '3. low':'low',
                        '4. close':'close',
                        '5. volume':'volume'
                    }, 
                    inplace=True)
    print('appending...')
    df.to_sql(TABLE_NAME, engine, if_exists='append')

def getBatch():
    df, meta_data = ts.get_batch_stock_quotes(batchSymbolString())
    df.rename(columns=
                    { 
                        '1. symbol':'symbol',
                        '2. price':'price',
                        '3. volume':'volume',
                        '4. timestamp':'timestamp',
                    }, 
                    inplace=True)

    print('appending...')
    df.to_sql(TABLE_NAME_US_EQUITIES, engine, if_exists='append')

    
def readFromDb():
    print('reading...')
    metadata = sqlalchemy.MetaData()
    dbTable = sqlalchemy.Table(TABLE_NAME, metadata, autoload=True, autoload_with=engine)

    query = sqlalchemy.select([dbTable])
    ResultProxy = connection.execute(query)
    ResultSet = ResultProxy.fetchall()