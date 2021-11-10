# save this as app.py
from flask import Flask, request
import pymysql
import os
import yaml

app = Flask(__name__)

db_name = 'alphavantage'
api_url = 'https://www.alphavantage.co/query'
config_path = '/home/ec2-user/data/config.yml'

class AlphavantageMySQL:
    def get_alphavantage(params_dict: dict()) -> dict():
        params = '&'.join([f'{key}={value}' for key, value in params_dict.items()])
        print(params)

        import requests

        url = f'{api_url}?{params}'
        r = requests.get(url)
        data = r.json()
        return data

    def init_db() -> pymysql.connections.Connection:
        # Read Credentials
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        # Connect to the database
        db = pymysql.connect(
            host=config['MYSQL']['HOST'],
            user=config['MYSQL']['USER'],
            password=config['MYSQL']['PASSWORD'],
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



@app.route("/test/post", methods=['POST', 'GET'])
def test_post():
    try:
        if request.method == 'POST':
            return f"Hello, test! data:{request.form}. data:{request.form['content']}"
    except Exception as e:
        return f"Error: {e}"
    
    return f"Hello, test!"


@app.route("/db/list", methods=['POST', 'GET'])
def db_list():
    try:
        vals = list()
        db = AlphavantageMySQL.init_db()
        cursor = db.cursor()
        
        if request.method == 'POST':
            if request.form['type'] == "db":
                cursor.execute("SHOW DATABASES;")
            elif request.form['type'] == "table":
                cursor.execute(f"SHOW TABLES FROM {request.form['db_name']};")
            elif request.form['type'] == "field":
                cursor.execute(f"DESCRIBE {request.form['db_name']}.{request.form['table_name']};")

            for i in cursor:
                vals.append(list(i.values())[0])
    except Exception as e:
        return f"Error: {e}"
    
    db.close()
    return f"Hello, list of data: {vals}"



@app.route("/api/dailyeod", methods=['POST', 'GET'])
def daily_eod():
    try:
        import pandas as pd
        db = AlphavantageMySQL.init_db()
        
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        ## daily ticker/currency (/api/dailyeod)
        params_dict = {
            'function': 'FX_DAILY',
            'from_symbol': 'EUR',
            'to_symbol': 'USD',
            'apikey': config['ALPHAVANTAGE']['KEY']
        }
        data = AlphavantageMySQL.get_alphavantage(params_dict)

        df = pd.DataFrame(data).reset_index()
        df = pd.concat([
            df[df.columns.tolist()[-1]].dropna().apply(pd.Series),
            df.drop(df.columns.tolist()[-1], axis=1)
        ], axis=1)
        df = df[
            df[df.columns.tolist()[-1]].isnull() # take out last column with null value (Meta Data) - logic will change if json structure changes
        ].drop(
            df.columns.tolist()[-1], axis=1 # drop empty column (Meta Data)
        )
        df.rename(columns={'index': 'date'}, inplace=True)
        df.columns = [x.replace('.','_').replace(' ','').lower() for x in df.columns]
        df['date'] = df['date'].apply(lambda x: f"'{x}'")
        df = df.reset_index(drop=True)

        table_name = '_'.join(list(params_dict.values())[:-1]).lower()
        print(table_name)

        AlphavantageMySQL.insert_data(df, db_name, table_name, db)
        db.commit()
    except Exception as e:
        return f"Error: {e}"
        
    db.close()
    return f"Hello, dailyeod! {db_name}.{table_name} data is updated."



@app.route("/api/monthlycpi", methods=['POST', 'GET'])
def monthly_cpi():
    try:
        import pandas as pd
        db = AlphavantageMySQL.init_db()

        ## monthly CPI (/api/monthlycpi)
        params_dict = {
            'function': 'CPI',
            'interval': 'monthly',
        }
        data = AlphavantageMySQL.get_alphavantage(params_dict)

        df = pd.DataFrame(data)
        df = pd.concat([
            df[df.columns.tolist()[-1]].apply(pd.Series),
            df.drop(df.columns.tolist()[-1], axis=1)
        ], axis=1)
        df = df[['date','value']]
        df['date'] = df['date'].apply(lambda x: f"'{x}'")
        df = df.reset_index(drop=True)

        table_name = '_'.join(list(params_dict.values())[:-1]).lower()
        print(table_name)

        AlphavantageMySQL.insert_data(df, db_name, table_name, db)
        db.commit()
    except Exception as e:
        print(f"Error: {e}")

    db.close()
    return f"Hello, monthlycpi! {db_name}.{table_name} data is updated."



if __name__ == "__main__":
    app.run(debug=True)
    
    
