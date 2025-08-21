import pandas as pd
import requests
import psycopg2
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# DAG default args
default_args = {
    'owner': 'kagan',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'btc_pipeline_modular',
    default_args=default_args,
    description='Fetch BTCUSDT OHLCV, calculate indicators, insert into NeonDB',
    schedule='*/5 * * * *',  # 5 dakikada bir
    catchup=False
)

# ------------------- TASK 1: Fetch Data -------------------
def fetch_ohlcv(**context):
    url = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=5m&limit=100"
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Binance API Error: {response.status_code} - {response.text}")
    
    data = response.json()
    df = pd.DataFrame(data, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base', 'taker_buy_quote', 'ignore'
    ])
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)

    print(f"Fetched {len(df)} rows from Binance")
    # XCom'a gönder (json formatında)
    context['ti'].xcom_push(key='ohlcv_data', value=df.to_json(orient="records", date_format="iso"))

# ------------------- TASK 2: Calculate Indicators -------------------
def calculate_indicators(**context):
    ohlcv_json = context['ti'].xcom_pull(key='ohlcv_data', task_ids='fetch_task')
    df = pd.DataFrame(json.loads(ohlcv_json))

    # SMA, EMA, RSI hesapla
    df['sma'] = df['close'].rolling(window=14).mean()
    df['ema'] = df['close'].ewm(span=14, adjust=False).mean()
    df['rsi'] = (df['close'].diff().clip(lower=0).rolling(14).mean() /
                 df['close'].diff().abs().rolling(14).mean()) * 100

    df_to_insert = df[['open_time', 'open', 'high', 'low', 'close', 'volume', 'sma', 'ema', 'rsi']]
    print("Calculated indicators, sample:")
    print(df_to_insert.head())

    context['ti'].xcom_push(key='indicators_data', value=df_to_insert.to_json(orient="records", date_format="iso"))

# ------------------- TASK 3: Insert to NeonDB -------------------
def insert_to_db(**context):
    indicators_json = context['ti'].xcom_pull(key='indicators_data', task_ids='calc_task')
    df = pd.DataFrame(json.loads(indicators_json))

    print("Connecting to NeonDB...")
    conn = psycopg2.connect(
        host="ep-nameless-surf-aecj97d7-pooler.c-2.us-east-2.aws.neon.tech",
        dbname="neondb",
        user="neondb_owner",
        password="npg_gqbu0E7WskxX",
        port="5432",
        sslmode="require"
    )
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO btc_usdt_technical (open_time, open, high, low, close, volume, sma, ema, rsi)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (open_time) DO UPDATE 
            SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                close=EXCLUDED.close, volume=EXCLUDED.volume,
                sma=EXCLUDED.sma, ema=EXCLUDED.ema, rsi=EXCLUDED.rsi
        """, (
            pd.to_datetime(row['open_time']).to_pydatetime(),
            row['open'], row['high'], row['low'], row['close'],
            row['volume'],
            row['sma'] if pd.notna(row['sma']) else None,
            row['ema'] if pd.notna(row['ema']) else None,
            row['rsi'] if pd.notna(row['rsi']) else None
        ))
        print(f"Inserted/updated row for {row['open_time']}")

    conn.commit()
    cur.close()
    conn.close()
    print("Finished inserting rows.")

# ------------------- Task Definitions -------------------
fetch_task = PythonOperator(
    task_id='fetch_task',
    python_callable=fetch_ohlcv,
    provide_context=True,
    dag=dag
)

calc_task = PythonOperator(
    task_id='calc_task',
    python_callable=calculate_indicators,
    provide_context=True,
    dag=dag
)

insert_task = PythonOperator(
    task_id='insert_task',
    python_callable=insert_to_db,
    provide_context=True,
    dag=dag
)

# Task pipeline
fetch_task >> calc_task >> insert_task
