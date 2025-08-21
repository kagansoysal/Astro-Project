import pandas as pd
import requests
import psycopg2
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
    'btc_technical_indicators',
    default_args=default_args,
    description='Fetch BTCUSDT OHLCV and calculate SMA, EMA, RSI',
    schedule='*/5 * * * *',  # her 5 dakikada bir
    catchup=False
)

# --- 1. Fetch Task ---
def fetch_ohlcv(**kwargs):
    url = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=5m&limit=100"
    response = requests.get(url)
    
    if response.status_code != 200:
        raise Exception(f"Binance API Error: {response.status_code} - {response.text}")
    
    data = response.json()
    print(f"Fetched {len(data)} rows from Binance")

    df = pd.DataFrame(data, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base', 'taker_buy_quote', 'ignore'
    ])

    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)

    # XCom’a gönder
    kwargs['ti'].xcom_push(key='ohlcv_data', value=df.to_json(orient='records', date_format='iso'))


# --- 2. Calculate Indicators Task ---
def calculate_indicators(**kwargs):
    ti = kwargs['ti']
    ohlcv_json = ti.xcom_pull(key='ohlcv_data', task_ids='fetch_task')
    df = pd.DataFrame.from_records(eval(ohlcv_json))

    # SMA, EMA, RSI hesapla
    df['sma'] = df['close'].rolling(window=14).mean()
    df['ema'] = df['close'].ewm(span=14, adjust=False).mean()
    df['rsi'] = pd.Series(
        (df['close'].diff(1).apply(lambda x: max(x,0))).rolling(14).mean() /
        (df['close'].diff(1).abs().rolling(14).mean()) * 100
    )

    print("Indicators calculated. Sample:")
    print(df[['open_time','close','sma','ema','rsi']].head())

    # Tekrar XCom’a gönder
    ti.xcom_push(key='indicator_data', value=df.to_json(orient='records', date_format='iso'))


# --- 3. Insert to DB Task ---
def insert_to_db(**kwargs):
    ti = kwargs['ti']
    data_json = ti.xcom_pull(key='indicator_data', task_ids='calculate_task')
    df = pd.DataFrame.from_records(eval(data_json))

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
        row_to_insert = (
            pd.to_datetime(row['open_time']).to_pydatetime(),
            row['open'], row['high'], row['low'], row['close'],
            row['volume'],
            row['sma'] if pd.notna(row['sma']) else None,
            row['ema'] if pd.notna(row['ema']) else None,
            row['rsi'] if pd.notna(row['rsi']) else None
        )
        print(f"Inserting row for {row['open_time']}")
        cur.execute("""
            INSERT INTO btc_usdt_technical (open_time, open, high, low, close, volume, sma, ema, rsi)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (open_time) DO UPDATE 
            SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                close=EXCLUDED.close, volume=EXCLUDED.volume,
                sma=EXCLUDED.sma, ema=EXCLUDED.ema, rsi=EXCLUDED.rsi
        """, row_to_insert)

    conn.commit()
    cur.close()
    conn.close()
    print("Finished inserting rows into NeonDB.")


# --- Task Definitions ---
fetch_task = PythonOperator(
    task_id='fetch_task',
    python_callable=fetch_ohlcv,
    dag=dag
)

calculate_task = PythonOperator(
    task_id='calculate_task',
    python_callable=calculate_indicators,
    dag=dag
)

insert_task = PythonOperator(
    task_id='insert_task',
    python_callable=insert_to_db,
    dag=dag
)

# --- Task Dependency Chain ---
fetch_task >> calculate_task >> insert_task
