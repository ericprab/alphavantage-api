# save this as app.py
from flask import Flask
import pymysql

app = Flask(__name__)
db = init_db()
db_name = 'alphavantage_uat'

class AlphavantageMySQL:
    api_url = 'https://www.alphavantage.co/query'

    def get_alphavantage(params_dict: dict()) -> dict():
        params = '&'.join([f'{key}={value}' for key, value in params_dict.items()])
        print(params)

        import requests

        url = f'{api_url}?{params}'
        r = requests.get(url)
        data = r.json()
        return data

    def init_db() -> pymysql.connections.Connection:
        # Open database connection
        # Connect to the database
        db = pymysql.connect(
            host='alphavantage.ckhcoakuhojq.ap-southeast-1.rds.amazonaws.com',
             user='admin',
             password='adminadmin',
             database='alphavantage_uat',
             cursorclass=pymysql.cursors.DictCursor
        )
        return db

    def insert_data(df, db_name, table_name, db):
        # prepare a cursor object using cursor() method
        cursor = db.cursor()

        field_len = len(df.columns.values)
        cols = ','.join(list(df.columns.values))

        for i, row in df.iterrows():
            print(i)
            data_row = ','.join([row[list(df.columns.values)[field_i]] for field_i in range(field_len)])

            sql = f"""
                INSERT IGNORE INTO {db_name}.{table_name} ({cols})
                VALUES ({data_row});
            """
            print(sql.format(db_name, table_name,cols,data_row))

            cursor.execute(sql)
        


@app.route("/", methods=['POST', 'GET'])
def hello():
    return "Hello, World!"



@app.route("/api/dailyeod", methods=['POST', 'GET'])
def daily_eod():
#     try:
#         import pandas as pd

#         ## daily ticker/currency (/api/dailyeod)
#         params_dict = {
#             'function': 'FX_DAILY',
#             'from_symbol': 'EUR',
#             'to_symbol': 'USD',
#             'apikey': 'demo'
#         }
#         data = get_alphavantage(params_dict)
#         # data

#         # df = pd.read_json(fname).reset_index()
#         df = pd.DataFrame(data).reset_index()

#         df = pd.concat([
#             df[df.columns.tolist()[-1]].dropna().apply(pd.Series),
#             df.drop(df.columns.tolist()[-1], axis=1)
#         ], axis=1)

#         df = df[
#             df[df.columns.tolist()[-1]].isnull() # take out last column with null value (Meta Data) - logic will change if json structure changes
#         ].drop(
#             df.columns.tolist()[-1], axis=1 # drop empty column (Meta Data)
#         )
#         df.rename(columns={'index': 'date'}, inplace=True)
#         df.columns = [x.replace('.','_').replace(' ','').lower() for x in df.columns]
#         df['date'] = df['date'].apply(lambda x: f"'{x}'")

#         df = df.reset_index(drop=True)
#         # df

#         table_name = '_'.join(list(params_dict.values())[:-1]).lower()
#         print(table_name)

#         insert_data(df, db_name, table_name, db)
#         db.commit()
#     except Exception as e:
#         print(f"Error: {e}")
        
    return "Hello, dailyeod!"



@app.route("/api/monthlycpi", methods=['POST', 'GET'])
def monthly_cpi():
#     try:
#         import pandas as pd

#         ## monthly CPI (/api/monthlycpi)
#         params_dict = {
#             'function': 'CPI',
#             'interval': 'monthly',
#             'apikey': 'demo'
#         }
#         data = get_alphavantage(params_dict)
#         # data

#         # df = pd.read_json(fname)
#         df = pd.DataFrame(data)

#         df = pd.concat([
#             df[df.columns.tolist()[-1]].apply(pd.Series),
#             df.drop(df.columns.tolist()[-1], axis=1)
#         ], axis=1)
#         df = df[['date','value']]
#         df['date'] = df['date'].apply(lambda x: f"'{x}'")

#         df = df.reset_index(drop=True)
#         # df

#         table_name = '_'.join(list(params_dict.values())[:-1]).lower()
#         print(table_name)

#         insert_data(df, db_name, table_name, db)
#         db.commit()
#     except Exception as e:
#         print(f"Error: {e}")

    return "Hello, monthlycpi!"



if __name__ == "__main__":
    app.run(debug=True)
    
    
