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
    schedule='*/5 * * * *',  # 5 dakikada bir
    catchup=False
)

def fetch_ohlcv():
    url = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=5m&limit=100"
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"Binance API Error: {response.status_code} - {response.text}")
        return
    
    data = response.json()
    print(f"Fetched {len(data)} rows from Binance")
    
    df = pd.DataFrame(data, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base', 'taker_buy_quote', 'ignore'
    ])
    
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
    
    # Teknik göstergeler (SMA, EMA, RSI)
    df['sma'] = df['close'].rolling(window=14).mean()
    df['ema'] = df['close'].ewm(span=14, adjust=False).mean()
    df['rsi'] = pd.Series((df['close'].diff(1).apply(lambda x: max(x,0))).rolling(14).mean() /
                          (df['close'].diff(1).abs().rolling(14).mean()) * 100)  # basit RSI
    
    df_to_insert = df[['open_time', 'open', 'high', 'low', 'close', 'volume', 'sma', 'ema', 'rsi']]
    print(df_to_insert.head())  # İlk 5 satırı logla

    # PostgreSQL bağlantısı
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
    
    for _, row in df_to_insert.iterrows():
        row_to_insert = (
            row['open_time'].to_pydatetime(),
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
    print("Finished inserting rows.")

# Task tanımı
task_fetch = PythonOperator(
    task_id='fetch_btc_data',
    python_callable=fetch_ohlcv,
    dag=dag
)
