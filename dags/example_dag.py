from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import numpy as np
import psycopg2
import io
import base64

# --- 1. Veri çekme ---
def get_binance_data(ti):
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": "BTCUSDT", "interval": "5m", "limit": 500}
    resp = requests.get(url, params=params)

    if resp.status_code == 200:
        raw = resp.json()
        print("[FETCH] Binance verisi alındı.")
        df = pd.DataFrame(raw, columns=[
            "open_time","open","high","low","close","volume",
            "close_time","quote_asset_volume","trades",
            "taker_buy_base","taker_buy_quote","ignore"
        ])
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
        float_cols = ["open","high","low","close","volume"]
        df[float_cols] = df[float_cols].astype(float)

    else:
        print(f"[WARN] Binance API hata verdi ({resp.status_code}). CoinGecko'ya geçiliyor...")
        cg_url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
        cg_params = {"vs_currency": "usd", "days": "2", "interval": "5m"}
        cg_resp = requests.get(cg_url, params=cg_params)
        if cg_resp.status_code != 200:
            raise Exception(f"CoinGecko da hata verdi: {cg_resp.status_code}")
        data = cg_resp.json()

        # CoinGecko sadece fiyat + hacim verir, OHLC yok
        prices = pd.DataFrame(data["prices"], columns=["ts", "price"])
        vols = pd.DataFrame(data["total_volumes"], columns=["ts", "volume"])
        df = prices.merge(vols, on="ts")
        df["open_time"] = pd.to_datetime(df["ts"], unit="ms")
        df["close"] = df["price"]
        df["open"] = df["close"]
        df["high"] = df["close"]
        df["low"] = df["close"]
        df.drop(columns=["ts","price"], inplace=True)
        print("[FETCH] CoinGecko verisi alındı (OHLC dummy).")

    # XCom’a gönder
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    ti.xcom_push(key="ohlcv_data", value=base64.b64encode(buf.getvalue()).decode())


# --- 2. İndikatör hesaplama ---
def enrich_with_indicators(ti):
    raw_bytes = base64.b64decode(ti.xcom_pull(key="ohlcv_data", task_ids="get_binance_data"))
    df = pd.read_parquet(io.BytesIO(raw_bytes))

    # SMA / EMA
    df["sma14"] = df["close"].rolling(14).mean()
    df["ema14"] = df["close"].ewm(span=14, adjust=False).mean()

    # RSI
    delta = df["close"].diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    roll_up = pd.Series(gain).rolling(14).mean()
    roll_down = pd.Series(loss).rolling(14).mean()
    rs = roll_up / roll_down
    df["rsi14"] = 100 - (100 / (1 + rs))

    print(f"[CALC] İndikatörler hesaplandı. Son satır:\n{df.tail(1)}")

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    ti.xcom_push(key="indicators", value=base64.b64encode(buf.getvalue()).decode())

# --- 3. DB insert ---
def load_to_neondb(ti):
    raw_bytes = base64.b64decode(ti.xcom_pull(key="indicators", task_ids="enrich_with_indicators"))
    df = pd.read_parquet(io.BytesIO(raw_bytes))

    conn = psycopg2.connect(
        host="ep-nameless-surf-aecj97d7-pooler.c-2.us-east-2.aws.neon.tech",
        dbname="neondb",
        user="neondb_owner",
        password="npg_gqbu0E7WskxX",
        port=5432,
        sslmode="require"
    )
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO btc_usdt_technical
        (open_time, open, high, low, close, volume, sma, ema, rsi)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (open_time) DO UPDATE SET
            open=EXCLUDED.open,
            high=EXCLUDED.high,
            low=EXCLUDED.low,
            close=EXCLUDED.close,
            volume=EXCLUDED.volume,
            sma=EXCLUDED.sma,
            ema=EXCLUDED.ema,
            rsi=EXCLUDED.rsi
    """

    rows = df[["open_time","open","high","low","close","volume","sma14","ema14","rsi14"]].values.tolist()
    cur.executemany(insert_sql, rows)
    conn.commit()
    cur.close()
    conn.close()
    print(f"[DB] {len(rows)} satır insert/update edildi.")

# --- DAG ---
default_args = {
    "owner": "kagan",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="btc_indicators_custom",
    start_date=datetime(2025,1,1),
    schedule="*/5 * * * *",
    catchup=False,
    default_args=default_args,
    description="BTCUSDT OHLCV -> SMA/EMA/RSI -> NeonDB pipeline"
) as dag:

    t1 = PythonOperator(
        task_id="get_binance_data",
        python_callable=get_binance_data
    )

    t2 = PythonOperator(
        task_id="enrich_with_indicators",
        python_callable=enrich_with_indicators
    )

    t3 = PythonOperator(
        task_id="load_to_neondb",
        python_callable=load_to_neondb
    )

    t1 >> t2 >> t3
