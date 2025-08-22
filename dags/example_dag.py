from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values

default_args = {
    'owner': 'kagan',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# =============== LOGIC FUNCTIONS ===============

def fetch_ohlcv_logic():
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": "BTCUSDT",
        "interval": "5m",
        "limit": 500
    }
    response = requests.get(url, params=params)
    data = response.json()

    print(f"[fetch_ohlcv] Binance returned {len(data)} rows")

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

    print("[fetch_ohlcv] Status:", response.status_code)
    print("[fetch_ohlcv] Shape:", df.shape)
    print("[fetch_ohlcv] Head:\n", df.head(3))

    return df.to_json()  # return JSON instead of ti.xcom_push


def calculate_indicators_logic(df_json):
    df = pd.read_json(df_json)

    print(f"[calculate_indicators] Shape: {df.shape}")

    df['sma'] = df['close'].rolling(window=14).mean()
    df['ema'] = df['close'].ewm(span=14, adjust=False).mean()

    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    rs = avg_gain / avg_loss
    df['rsi'] = 100 - (100 / (1 + rs))

    print("[calculate_indicators] Head:\n", df[['close','sma','ema','rsi']].head(3))

    return df.to_json()  # return JSON instead of ti.xcom_push


def insert_to_postgres_logic(df_json):
    df = pd.read_json(df_json)
    print(f"[insert_to_postgres] Rows to insert: {len(df)}")

    conn = psycopg2.connect(
        host="ep-nameless-surf-aecj97d7-pooler.c-2.us-east-2.aws.neon.tech",
        port=5432,
        dbname="neondb",
        user="neondb_owner",
        password="npg_gqbu0E7WskxX",
        sslmode="require"
    )
    cursor = conn.cursor()

    records = [
        (
            row['open_time'],
            row['open'],
            row['high'],
            row['low'],
            row['close'],
            row['volume'],
            row['sma'],
            row['ema'],
            row['rsi'],
        )
        for _, row in df.iterrows()
    ]

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

    print(f"[insert_to_postgres] {len(records)} rows inserted ✅")

# =============== DAG DEFINITION ===============

@dag(
    dag_id="btc_technical_indicators",
    schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
)
def btc_pipeline():

    @task()
    def fetch_ohlcv():
        return fetch_ohlcv_logic()

    @task()
    def calculate_indicators(df_json):
        return calculate_indicators_logic(df_json)

    @task()
    def insert_to_postgres(df_json):
        insert_to_postgres_logic(df_json)

    # dependencies via function calls
    raw_data = fetch_ohlcv()
    indicators = calculate_indicators(raw_data)
    insert_to_postgres(indicators)

dag = btc_pipeline()
