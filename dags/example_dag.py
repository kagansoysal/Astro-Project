from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


def fetch_ohlcv(ti):
    """
    Yahoo Finance'ten BTC-USD OHLCV verisi çek
    """
    ticker = yf.Ticker("BTC-USD")
    df = ticker.history(interval="5m", period="2d")  # son 2 gün, 5 dakikalık barlar.

    df = df.reset_index()
    df = df.rename(columns={
        "Datetime": "open_time",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume"
    })

    # sadece lazım olan kolonlar
    df = df[["open_time", "open", "high", "low", "close", "volume"]]

    ti.xcom_push(key="ohlcv_df", value=df.to_json())


def calculate_indicators(ti):
    """
    SMA, EMA, RSI hesapla
    """
    df_json = ti.xcom_pull(key="ohlcv_df", task_ids="fetch_ohlcv")
    df = pd.read_json(df_json)

    # SMA & EMA
    df["sma"] = df["close"].rolling(window=14).mean()
    df["ema"] = df["close"].ewm(span=14, adjust=False).mean()

    # RSI
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    rs = avg_gain / avg_loss
    df["rsi"] = 100 - (100 / (1 + rs))

    ti.xcom_push(key="indicators_df", value=df.to_json())


def insert_to_postgres(ti):
    """
    PostgreSQL'e yaz
    """
    df_json = ti.xcom_pull(key="indicators_df", task_ids="calculate_indicators")
    df = pd.read_json(df_json)

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
            row["open_time"],
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["volume"],
            row["sma"],
            row["ema"],
            row["rsi"],
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
    "owner": "kagan",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="btc_technical_indicators_yf",
    schedule="*/5 * * * *",  # her 5 dakikada bir
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_ohlcv",
        python_callable=fetch_ohlcv,
    )

    calc_task = PythonOperator(
        task_id="calculate_indicators",
        python_callable=calculate_indicators,
    )

    insert_task = PythonOperator(
        task_id="insert_to_postgres",
        python_callable=insert_to_postgres,
    )

    fetch_task >> calc_task >> insert_task
