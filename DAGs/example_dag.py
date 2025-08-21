from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values

def fetch_ohlcv(ti):
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": "BTCUSDT",
        "interval": "5m",
        "limit": 500
    }
    response = requests.get(url, params=params)
    data = response.json()

    df = pd.DataFrame(data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
    ])

    df["open_time"] = pd.to_datetime(df["open_time"], unit='ms')
    df["open"] = df["open"].astype(float)
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    df["close"] = df["close"].astype(float)
    df["volume"] = df["volume"].astype(float)

    ti.xcom_push(key='ohlcv_df', value=df.to_json())

def calculate_indicators(ti):
    df_json = ti.xcom_pull(key='ohlcv_df', task_ids='fetch_ohlcv')
    df = pd.read_json(df_json)

    df['sma'] = df['close'].rolling(window=14).mean()
    df['ema'] = df['close'].ewm(span=14, adjust=False).mean()

    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    rs = avg_gain / avg_loss
    df['rsi'] = 100 - (100 / (1 + rs))

    ti.xcom_push(key='indicators_df', value=df.to_json())

def insert_to_postgres(ti):
    df_json = ti.xcom_pull(key='indicators_df', task_ids='calculate_indicators')
    df = pd.read_json(df_json)

    conn = psycopg2.connect(
        host="ep-late-bread-aer69fpl-pooler.c-2.us-east-2.aws.neon.tech",
        port=5432,
        dbname="neondb",
        user="neondb_owner",
        password="npg_be5kPz1AitFR",
        sslmode="require"
    )
    cursor = conn.cursor()

    records = []
    for _, row in df.iterrows():
        records.append((
            row['open_time'],
            row['open'],
            row['high'],
            row['low'],
            row['close'],
            row['volume'],
            row['sma'],
            row['ema'],
            row['rsi'],
        ))

    sql = """
        INSERT INTO btc_usdt_technical
        (open_time, open, high, low, close, volume, sma, ema, rsi)
        VALUES %s
        ON CONFLICT (open_time) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            sma = EXCLUDED.sma,
            ema = EXCLUDED.ema,
            rsi = EXCLUDED.rsi
    """
    execute_values(cursor, sql, records)
    conn.commit()
    cursor.close()
    conn.close()

default_args = {
    'owner': 'beste',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='btc_technical_indicators',
    schedule='*/5 * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_ohlcv',
        python_callable=fetch_ohlcv,
    )

    calc_task = PythonOperator(
        task_id='calculate_indicators',
        python_callable=calculate_indicators,
    )

    insert_task = PythonOperator(
        task_id='insert_to_postgres',
        python_callable=insert_to_postgres,
    )

    fetch_task >> calc_task >> insert_task
