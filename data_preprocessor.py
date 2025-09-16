#!/usr/bin/env python3
"""
Data Preprocessor for Crypto Trading Datasets

This module provides comprehensive preprocessing for cryptocurrency datasets,
ensuring consistency across BTC and ETH data with standardized column names,
file naming conventions, and data quality validation.

Standards enforced:
- All column names must be lowercase
- File naming convention: {SYMBOL}_{TIMEFRAME}_data.csv
- Consistent OHLCV column structure
- Data quality validation
- Automatic corrections for deviations

Author: Data Factory Team
Created: 2025-01-09
"""

import pandas as pd
import numpy as np
import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataPreprocessor:
    """
    Comprehensive data preprocessor for cryptocurrency datasets.

    Ensures consistent formatting, naming conventions, and data quality
    across all trading datasets.
    """

    # Standard column names (all lowercase)
    STANDARD_COLUMNS = {
        'timestamp': ['timestamp', 'open_time', 'time', 'date'],
        'open': ['open', 'o'],
        'high': ['high', 'h'],
        'low': ['low', 'l'],
        'close': ['close', 'c'],
        'volume': ['volume', 'vol', 'v', 'quote_asset_volume']
    }

    REQUIRED_COLUMNS = ['timestamp', 'open', 'high', 'low', 'close', 'volume']

    def __init__(self, base_dir: str = "data"):
        """
        Initialize the data preprocessor.

        Args:
            base_dir: Base directory for data operations
        """
        self.base_dir = Path(base_dir)
        self.raw_dir = self.base_dir / "raw"
        self.processed_dir = self.base_dir / "processed"

        # Create directories if they don't exist
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"DataPreprocessor initialized with base directory: {self.base_dir}")

    def standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names to lowercase and map variations to standard names.

        Args:
            df: Input DataFrame with potentially non-standard column names

        Returns:
            DataFrame with standardized column names
        """
        df = df.copy()

        # Convert all column names to lowercase
        df.columns = df.columns.str.lower().str.strip()

        # Map variations to standard names
        column_mapping = {}

        for standard_col, variations in self.STANDARD_COLUMNS.items():
            for variation in variations:
                if variation in df.columns:
                    if standard_col not in column_mapping.values():  # Avoid duplicate mappings
                        column_mapping[variation] = standard_col
                        break

        # Apply mapping
        df = df.rename(columns=column_mapping)

        # Verify all required columns are present
        missing_columns = set(self.REQUIRED_COLUMNS) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # Reorder columns to standard order
        df = df[self.REQUIRED_COLUMNS]

        logger.info(f"Column names standardized: {list(df.columns)}")
        return df

    def validate_data_quality(self, df: pd.DataFrame) -> Dict[str, bool]:
        """
        Validate data quality and return validation results.

        Args:
            df: DataFrame to validate

        Returns:
            Dictionary of validation results
        """
        validation_results = {
            'has_required_columns': False,
            'columns_are_lowercase': False,
            'no_missing_values': False,
            'no_infinite_values': False,
            'valid_timestamps': False,
            'positive_prices': False,
            'positive_volume': False
        }

        # Check required columns
        required_cols_present = all(col in df.columns for col in self.REQUIRED_COLUMNS)
        validation_results['has_required_columns'] = required_cols_present

        # Check lowercase columns
        all_lowercase = all(col.islower() for col in df.columns)
        validation_results['columns_are_lowercase'] = all_lowercase

        # Check for missing values
        has_no_missing = not df.isnull().any().any()
        validation_results['no_missing_values'] = has_no_missing

        # Check for infinite values
        has_no_infinite = not np.isinf(df.select_dtypes(include=[np.number])).any().any()
        validation_results['no_infinite_values'] = has_no_infinite

        # Check timestamp validity
        try:
            pd.to_datetime(df['timestamp'])
            validation_results['valid_timestamps'] = True
        except:
            validation_results['valid_timestamps'] = False

        # Check positive prices and volume
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            if col in df.columns:
                if col == 'volume':
                    validation_results['positive_volume'] = (df[col] >= 0).all()
                else:
                    validation_results['positive_prices'] = (df[col] > 0).all()

        return validation_results

    def preprocess_raw_data(self, symbol: str, timeframe: str = "1H",
                           input_path: Optional[str] = None) -> pd.DataFrame:
        """
        Preprocess raw cryptocurrency data to ensure consistency.

        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT', 'ETHUSDT')
            timeframe: Timeframe string (e.g., '1H', '4H', '1D')
            input_path: Optional custom input path, otherwise auto-detect

        Returns:
            Preprocessed DataFrame
        """
        logger.info(f"Starting preprocessing for {symbol} {timeframe}")

        # Auto-detect input path if not provided
        if input_path is None:
            if symbol.upper().startswith('BTC'):
                input_path = self.base_dir / "btcusdt" / f"{symbol.lower()}_{timeframe.lower()}_binance.csv"
            elif symbol.upper().startswith('ETH'):
                input_path = self.base_dir / "ethusdt" / f"{symbol.lower()}_{timeframe.lower()}_binance.csv"
            else:
                raise ValueError(f"Unsupported symbol: {symbol}")

        if not input_path.exists():
            # Try alternative naming
            alt_path = self.base_dir / symbol.lower() / f"{symbol.upper()}_{timeframe.upper()}_Binance.csv"
            if alt_path.exists():
                input_path = alt_path
            else:
                raise FileNotFoundError(f"Could not find data file for {symbol} {timeframe}")

        logger.info(f"Loading data from: {input_path}")

        # Read data
        df = pd.read_csv(input_path)
        logger.info(f"Loaded {len(df)} rows with columns: {list(df.columns)}")

        # Standardize column names
        df = self.standardize_column_names(df)

        # Convert timestamp to datetime if needed
        if 'timestamp' in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                logger.info("Converted timestamp to datetime")

        # Sort by timestamp
        df = df.sort_values('timestamp').reset_index(drop=True)

        # Validate data quality
        validation = self.validate_data_quality(df)
        failed_validations = [k for k, v in validation.items() if not v]

        if failed_validations:
            logger.warning(f"Data quality issues detected: {failed_validations}")
            # Auto-fix common issues
            df = self.auto_fix_data_quality(df, validation)
        else:
            logger.info("All data quality checks passed")

        return df

    def auto_fix_data_quality(self, df: pd.DataFrame, validation_results: Dict[str, bool]) -> pd.DataFrame:
        """
        Automatically fix common data quality issues.

        Args:
            df: DataFrame with quality issues
            validation_results: Results from quality validation

        Returns:
            Fixed DataFrame
        """
        df = df.copy()

        # Fix missing values (forward fill, then backward fill)
        if not validation_results['no_missing_values']:
            logger.info("Fixing missing values with forward/backward fill")
            df = df.fillna(method='ffill').fillna(method='bfill')

        # Fix infinite values
        if not validation_results['no_infinite_values']:
            logger.info("Replacing infinite values with NaN, then filling")
            df = df.replace([np.inf, -np.inf], np.nan)
            df = df.fillna(method='ffill').fillna(method='bfill')

        # Fix negative prices (replace with absolute values)
        if not validation_results.get('positive_prices', True):
            logger.info("Fixing negative prices with absolute values")
            for col in ['open', 'high', 'low', 'close']:
                if col in df.columns:
                    df[col] = df[col].abs()

        # Fix negative volume
        if not validation_results.get('positive_volume', True):
            logger.info("Fixing negative volume with absolute values")
            df['volume'] = df['volume'].abs()

        return df

    def save_processed_data(self, df: pd.DataFrame, symbol: str, timeframe: str = "1H",
                           output_format: str = "both") -> Dict[str, str]:
        """
        Save processed data to standardized locations.

        Args:
            df: Processed DataFrame
            symbol: Trading symbol
            timeframe: Timeframe string
            output_format: 'raw', 'processed', or 'both'

        Returns:
            Dictionary of saved file paths
        """
        saved_files = {}

        # Generate standardized filename
        filename = f"{symbol.upper()}_{timeframe.upper()}_data.csv"

        if output_format in ['raw', 'both']:
            raw_path = self.raw_dir / filename
            df.to_csv(raw_path, index=False)
            saved_files['raw'] = str(raw_path)
            logger.info(f"Saved raw data to: {raw_path}")

        if output_format in ['processed', 'both']:
            processed_path = self.processed_dir / filename
            df.to_csv(processed_path, index=False)
            saved_files['processed'] = str(processed_path)
            logger.info(f"Saved processed data to: {processed_path}")

        return saved_files

    def process_symbol(self, symbol: str, timeframe: str = "1H",
                      input_path: Optional[str] = None) -> Dict[str, str]:
        """
        Complete processing pipeline for a single symbol.

        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT', 'ETHUSDT')
            timeframe: Timeframe string
            input_path: Optional custom input path

        Returns:
            Dictionary with processing results and file paths
        """
        try:
            logger.info(f"Starting complete processing for {symbol} {timeframe}")

            # Preprocess raw data
            df = self.preprocess_raw_data(symbol, timeframe, input_path)

            # Save processed data
            saved_files = self.save_processed_data(df, symbol, timeframe)

            # Final validation
            final_validation = self.validate_data_quality(df)

            result = {
                'symbol': symbol,
                'timeframe': timeframe,
                'rows_processed': len(df),
                'columns': list(df.columns),
                'date_range': {
                    'start': df['timestamp'].min().isoformat(),
                    'end': df['timestamp'].max().isoformat()
                },
                'saved_files': saved_files,
                'validation_passed': all(final_validation.values()),
                'validation_details': final_validation
            }

            logger.info(f"Successfully processed {symbol} {timeframe}: {len(df)} rows")
            return result

        except Exception as e:
            logger.error(f"Failed to process {symbol} {timeframe}: {str(e)}")
            raise

def main():
    """
    Main function for command-line usage.
    """
    import argparse

    parser = argparse.ArgumentParser(description="Preprocess cryptocurrency trading data")
    parser.add_argument("--symbol", required=True, help="Trading symbol (e.g., BTCUSDT, ETHUSDT)")
    parser.add_argument("--timeframe", default="1H", help="Timeframe (e.g., 1H, 4H, 1D)")
    parser.add_argument("--input", help="Custom input file path")
    parser.add_argument("--output-dir", default="data", help="Output directory")

    args = parser.parse_args()

    # Initialize preprocessor
    preprocessor = DataPreprocessor(args.output_dir)

    # Process the symbol
    result = preprocessor.process_symbol(args.symbol, args.timeframe, args.input)

    # Print results
    print("\n" + "="*60)
    print(f"PROCESSING COMPLETE: {args.symbol} {args.timeframe}")
    print("="*60)
    print(f"Rows processed: {result['rows_processed']}")
    print(f"Date range: {result['date_range']['start']} to {result['date_range']['end']}")
    print(f"Columns: {', '.join(result['columns'])}")
    print(f"Validation passed: {result['validation_passed']}")
    print("\nSaved files:")
    for file_type, path in result['saved_files'].items():
        print(f"  {file_type.upper()}: {path}")

    if not result['validation_passed']:
        print("\n⚠️  Validation issues detected:")
        for check, passed in result['validation_details'].items():
            if not passed:
                print(f"  ❌ {check}")

if __name__ == "__main__":
    main()

