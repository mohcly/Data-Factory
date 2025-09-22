import os
from datetime import datetime, timedelta
import pandas as pd
from binance.client import Client
from dotenv import load_dotenv
import logging
from tqdm.auto import tqdm
import sys
from hurry.filesize import size, alternative

# ===================== CONFIG SECTION =====================
CONFIG = {
    # Trading pair to download
    'SYMBOL': 'ETHUSDT',
    
    # Date range
    'START_DATE': '2020-01-01',  # Format: YYYY-MM-DD
    'END_DATE': None,  # None for current date, or specify date in YYYY-MM-DD format
    
    # Timeframes to download - uncomment the ones you need
    'TIMEFRAMES': {
        '30m': Client.KLINE_INTERVAL_30MINUTE,
        # '1m': Client.KLINE_INTERVAL_1MINUTE,
        # '5m': Client.KLINE_INTERVAL_5MINUTE,
        # '15m': Client.KLINE_INTERVAL_15MINUTE,
        # '1h': Client.KLINE_INTERVAL_1HOUR,
        # '4h': Client.KLINE_INTERVAL_4HOUR,
        # '1d': Client.KLINE_INTERVAL_1DAY,
    },
    
    # Download settings
    'CHUNK_SIZE': 30,  # Number of days per request (lower this if getting timeout errors)
    'OUTPUT_DIR': 'data',  # Directory where data will be saved
    
    # Logging settings
    'LOG_LEVEL': logging.INFO,
}

# Set up logging
logging.basicConfig(
    level=CONFIG['LOG_LEVEL'], 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize Binance client
# Note: For historical data, we don't need API keys
client = Client()

class DownloadProgressBar:
    def __init__(self, timeframe, total_days):
        self.pbar = tqdm(
            total=total_days,
            desc=f"Downloading {timeframe}",
            unit='days',
            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} days [{elapsed}<{remaining}, {rate_fmt}]'
        )
        self.current_size = 0
        
    def update(self, days_chunk):
        self.pbar.update(days_chunk)
        
    def close(self):
        self.pbar.close()

def get_historical_klines(symbol, interval, start_date, end_date=None):
    """
    Download historical klines (candlestick data) for a given symbol and interval
    """
    try:
        # Convert dates to datetime objects
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.now() if end_date is None else datetime.strptime(end_date, "%Y-%m-%d")
        
        # Calculate total days for progress bar
        total_days = (end_dt - start_dt).days
        progress_bar = DownloadProgressBar(interval, total_days)
        
        # Initialize empty list for all klines
        all_klines = []
        current_start = start_dt
        
        # Download data in chunks
        while current_start < end_dt:
            current_end = min(current_start + timedelta(days=CONFIG['CHUNK_SIZE']), end_dt)
            
            # Convert to milliseconds timestamp
            start_ts = int(current_start.timestamp() * 1000)
            end_ts = int(current_end.timestamp() * 1000)
            
            # Download chunk
            chunk = client.get_historical_klines(
                symbol=symbol,
                interval=interval,
                start_str=start_ts,
                end_str=end_ts
            )
            
            all_klines.extend(chunk)
            
            # Update progress
            days_processed = min(CONFIG['CHUNK_SIZE'], (current_end - current_start).days)
            progress_bar.update(days_processed)
            
            # Move to next chunk
            current_start = current_end
        
        progress_bar.close()
        
        if not all_klines:
            logger.error(f"No data downloaded for {symbol} {interval}")
            return None
            
        # Convert to DataFrame
        df = pd.DataFrame(all_klines, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
        ])
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # Set timestamp as index
        df.set_index('timestamp', inplace=True)
        
        # Convert relevant columns to float
        float_columns = ['open', 'high', 'low', 'close', 'volume', 'quote_asset_volume',
                        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
        df[float_columns] = df[float_columns].astype(float)
        
        return df
        
    except Exception as e:
        logger.error(f"Error downloading data for {symbol} {interval}: {str(e)}")
        return None

def save_data(df, symbol, timeframe):
    """
    Save the DataFrame to a CSV file with progress bar
    """
    # Create output directory if it doesn't exist
    os.makedirs(CONFIG['OUTPUT_DIR'], exist_ok=True)
    
    # Create symbol subdirectory
    symbol_dir = os.path.join(CONFIG['OUTPUT_DIR'], symbol.lower())
    os.makedirs(symbol_dir, exist_ok=True)
    
    # Save file
    filename = f"{symbol.lower()}_{timeframe}.csv"
    filepath = os.path.join(symbol_dir, filename)
    
    # Get DataFrame size in memory
    df_size = sys.getsizeof(df)
    
    # Create progress bar for saving
    with tqdm(total=100, desc=f"Saving {filename}", 
             bar_format='{l_bar}{bar}| {n_fmt}% [{elapsed}<{remaining}] {postfix}') as pbar:
        df.to_csv(filepath)
        pbar.set_postfix_str(f"Size: {size(os.path.getsize(filepath), system=alternative)}")
        pbar.update(100)
    
    logger.info(f"Saved {filepath} (Size: {size(os.path.getsize(filepath), system=alternative)})")

def main():
    # Configuration
    symbol = CONFIG['SYMBOL']
    start_date = CONFIG['START_DATE']
    end_date = CONFIG['END_DATE']
    
    logger.info(f"Starting download for {symbol} across multiple timeframes")
    
    # Create overall progress bar for timeframes
    with tqdm(total=len(CONFIG['TIMEFRAMES']), desc="Overall Progress", 
             unit='timeframe', position=0, leave=True) as pbar:
        
        # Download data for each timeframe
        for timeframe_label, timeframe in CONFIG['TIMEFRAMES'].items():
            logger.info(f"Processing {timeframe_label} data...")
            
            df = get_historical_klines(symbol, timeframe, start_date, end_date)
            
            if df is not None and not df.empty:
                save_data(df, symbol, timeframe_label)
            else:
                logger.error(f"Failed to download data for {timeframe_label}")
            
            pbar.update(1)
    
    logger.info("Download complete!")

if __name__ == "__main__":
    main() 