from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import requests
import psycopg2
import ta  # technical analysis kütüphanesi
from datetime import datetime

default_args = {
    'owner': 'kagan',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),  # şu an UTC zamanı
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'btc_technical_indicators',
    default_args=default_args,
    description='Fetch BTCUSDT OHLCV and calculate SMA, EMA, RSI',
    schedule_interval='*/5 * * * *',
    catchup=False  # geçmişi doldurma
)

def fetch_ohlcv():
    url = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=5m&limit=100"
    response = requests.get(url)
    data = response.json()
    
    df = pd.DataFrame(data, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base', 'taker_buy_quote', 'ignore'
    ])
    
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
    
    # Teknik göstergeler
    df['sma'] = df['close'].rolling(window=14).mean()
    df['ema'] = df['close'].ewm(span=14, adjust=False).mean()
    df['rsi'] = ta.momentum.RSIIndicator(df['close'], window=14).rsi()
    
    df_to_insert = df[['open_time', 'open', 'high', 'low', 'close', 'volume', 'sma', 'ema', 'rsi']]
    
    conn = psycopg2.connect(
        host="ep-nameless-surf-aecj97d7-pooler.c-2.us-east-2.aws.neon.tech",
        dbname="neondb",
        user="neondb_owner",
        password="npg_gqbu0E7WskxX",
        port="5432"
    )
    
    cur = conn.cursor()
    for _, row in df_to_insert.iterrows():
        cur.execute("""
            INSERT INTO btc_usdt_technical (open_time, open, high, low, close, volume, sma, ema, rsi)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (open_time) DO UPDATE 
            SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                close=EXCLUDED.close, volume=EXCLUDED.volume,
                sma=EXCLUDED.sma, ema=EXCLUDED.ema, rsi=EXCLUDED.rsi
        """, (
            row['open_time'].to_pydatetime(),
            row['open'], row['high'], row['low'], row['close'],
            row['volume'], row['sma'], row['ema'], row['rsi']
        ))


    
    conn.commit()
    cur.close()
    conn.close()

task_fetch = PythonOperator(
    task_id='fetch_btc_data',
    python_callable=fetch_ohlcv,
    dag=dag
)
