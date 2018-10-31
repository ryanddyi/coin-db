import MySQLdb
import datetime
import pandas as pd 
import numpy as np
import util
import configparser
from cmc import coinmarketcap

# manage a cryptocurrency OHLCV database in coindb.core. sync data from coinmarketcap.com
# db_config specifies the mysql connection
# symbol_list specifies the symbol list that we are interested in. DatabaseManagement will not sync data for symbols outside symbol_list.


class DatabaseManagement:

    def __init__(self, db_config, name_list):

        self.database = db_config['database']
        self.name_list = name_list
        self.connection = MySQLdb.connect(host=db_config['host'],
                                          user=db_config['user'],
                                          password=db_config['password'])

    def sync_cmc_data(self):
        
        name_to_symbol = util.name_to_symbol()

        for coin in self.name_list:
            print(coin) 
            symbol = name_to_symbol[coin]
            cursor = self.connection.cursor()            
            
            # get the last time it 
            sql = "SELECT MAX(date) FROM %s.core WHERE asset = '%s'" % (self.database, symbol)
            cursor.execute(sql)

            temp = cursor.fetchall()
            start_date = temp[0][0]
            if start_date == None:
                start_date = datetime.date(2013, 4, 28)

            end_date = datetime.date.today()
            timedelata = end_date - start_date

            if timedelata.days > 1:
                df_crypto = coinmarketcap.getDataFor(coin, start_date, end_date)
                OHLCV = ['Open', 'High', 'Low', 'Close', 'Volume']
                for variable in OHLCV:
                    series = df_crypto[coin][variable]
                    for index in series.index:
                        sql = "replace into %s.core VALUES('%s', '%s', '%s', '%f')" % \
                            (self.database, str(index)[0:10], symbol, variable, series[index])
                        cursor.execute(sql)
                    self.connection.commit()
            else:
                print("Data up-to-date!")
            
            cursor.close()


    def get_data_frame(self, symbol):

        sql = "SELECT date, variable, value FROM %s.core WHERE asset='%s'" % (self.database, symbol)
        df = pd.read_sql(sql, con=self.connection)
        return df.pivot(index='date', columns='variable')['value']


if __name__ == '__main__':

    config = configparser.ConfigParser()
    config.read('config.ini')
    db_config = config['mysql']

    #crypto_list = util.get_full_list()
    crypto_list = util.get_top_k_list(50)

    db = DatabaseManagement(db_config, crypto_list)
    db.sync_cmc_data()


