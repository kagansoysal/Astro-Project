from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# --- Default Args ---
default_args = {
    'owner': 'kagan',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# --- DAG Tanımı ---
dag = DAG(
    'btc_technical_indicators',
    default_args=default_args,
    description='Fetch BTCUSDT OHLCV and calculate SMA, EMA, RSI',
    schedule='*/5 * * * *',  # her 5 dakikada bir
    catchup=False
)

# --- 1. Fetch Task ---
def fetch_ohlcv(ti):
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": "BTCUSDT",
        "interval": "5m",
        "limit": 500
    }
    response = requests.get(url, params=params)
    
    if response.status_code != 200:
        raise Exception(f"Binance API Error: {response.status_code} - {response.text}")

    data = response.json()
    print(f"Fetched {len(data)} rows from Binance")

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

    # XCom’a gönder
    ti.xcom_push(key='ohlcv_df', value=df.to_json())

# --- 2. Indicator Calculation Task ---
def calculate_indicators(ti):
    df_json = ti.xcom_pull(key='ohlcv_df', task_ids='fetch_ohlcv')
    df = pd.read_json(df_json)

    # SMA & EMA
    df['sma'] = df['close'].rolling(window=14).mean()
    df['ema'] = df['close'].ewm(span=14, adjust=False).mean()

    # RSI
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    rs = avg_gain / avg_loss
    df['rsi'] = 100 - (100 / (1 + rs))

    print("Indicators calculated. Sample:")
    print(df[['open_time','close','sma','ema','rsi']].tail(5))

    ti.xcom_push(key='indicators_df', value=df.to_json())

# --- 3. Insert to DB Task ---
def insert_to_postgres(ti):
    df_json = ti.xcom_pull(key='indicators_df', task_ids='calculate_indicators')
    df = pd.read_json(df_json)

    print("Connecting to NeonDB...")
    conn = psycopg2.connect(
        host="ep-nameless-surf-aecj97d7-pooler.c-2.us-east-2.aws.neon.tech",
        port=5432,
        dbname="neondb",
        user="neondb_owner",
        password="npg_gqbu0E7WskxX",
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
    print("Finished inserting rows into NeonDB.")

# --- Task Definitions ---
fetch_task = PythonOperator(
    task_id='fetch_ohlcv',
    python_callable=fetch_ohlcv,
    dag=dag
)

calc_task = PythonOperator(
    task_id='calculate_indicators',
    python_callable=calculate_indicators,
    dag=dag
)

insert_task = PythonOperator(
    task_id='insert_to_postgres',
    python_callable=insert_to_postgres,
    dag=dag
)

# --- Dependency Chain ---
fetch_task >> calc_task >> insert_task
