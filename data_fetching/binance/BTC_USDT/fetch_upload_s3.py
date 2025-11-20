from binance.client import Client
import pandas as pd
import boto3
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

def get_binance_data(symbol, start_ts=1628899200000, end_ts=1757462400000):
    client = Client()
    interval = Client.KLINE_INTERVAL_1MINUTE
    start_str = pd.to_datetime(start_ts, unit="ms", utc=True).strftime("%d %b %Y %H:%M:%S")   # Convert ms timestamps → Binance expects UTC string
    end_str = pd.to_datetime(end_ts, unit="ms", utc=True).strftime("%d %b %Y %H:%M:%S")

    klines = client.get_historical_klines(
        symbol=symbol,
        interval=interval,
        start_str=start_str,
        end_str=end_str
    )
  
    cols = [
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_volume', 'trades',
        'taker_buy_base', 'taker_buy_quote', 'ignore'
    ]# Binance API response columns

    df = pd.DataFrame(klines, columns=cols)
    df['open_time'] = pd.to_datetime(df['open_time'].astype(int), unit='ms', utc=True)     # Convert timestamps
    df['close_time'] = pd.to_datetime(df['close_time'].astype(int), unit='ms', utc=True)

    numeric_cols = ['open', 'high', 'low', 'close', 'volume',
                    'quote_volume', 'taker_buy_base', 'taker_buy_quote']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)

    return df

def upload_to_s3(local_path, bucket_name, s3_path=None): 
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESSS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESSS_KEY"),
        region_name='us-east-1'
    ) 

    local_path = Path(local_path)
    
    if local_path.is_file(): # Upload single file
        s3_key = s3_path or local_path.name
        s3.upload_file(str(local_path), bucket_name, s3_key)
        print(f"Uploaded: {local_path} -> {s3_key}")
    
    elif local_path.is_dir(): # Upload directory recursively
        for file_path in local_path.rglob('*'):
            if file_path.is_file():
                relative_path = file_path.relative_to(local_path)
                s3_key = str(Path(s3_path) / relative_path) if s3_path else str(relative_path)
                s3.upload_file(str(file_path), bucket_name, s3_key)
                print(f"Uploaded: {file_path} -> {s3_key}")

if __name__ == "__main__":
    from_timestamp = 1753711140000
    to_timestamp = 1753711260000
    symbol = "BTCUSDT"
    bucket_name = "binance-data-collection-tmliu628"
    path_to_store = rf"C:\Users\super\OneDrive - Durham College\skills_lab\Snowflake\snowflake_s3_data_analysis_w_mcp\data_fetching\binance\result\{symbol}_price_from_{from_timestamp}_to_{to_timestamp}.csv"
    btc_data = get_binance_data(symbol, from_timestamp, to_timestamp)
    btc_data.to_csv(rf"{path_to_store}")
    upload_to_s3(path_to_store, bucket_name, "")
