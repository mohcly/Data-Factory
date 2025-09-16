#!/usr/bin/env python3
"""
Batch Dataset Processor for Crypto Trading Data

This script processes both BTC and ETH datasets to ensure they meet
established standards for the time series forecasting pipeline.

Standards enforced:
- Column names converted to lowercase
- File naming convention: {SYMBOL}_{TIMEFRAME}_data.csv
- Data quality validation and auto-correction
- Consistent OHLCV structure

Usage:
    python process_datasets.py                    # Process both BTC and ETH
    python process_datasets.py --symbol BTC      # Process only BTC
    python process_datasets.py --symbol ETH      # Process only ETH
    python process_datasets.py --validate-only   # Only validate, don't process

Author: Data Factory Team
Created: 2025-01-09
"""

import sys
import os
import argparse
from pathlib import Path
import logging
from typing import Dict

# Add the data_preprocessor module to path
sys.path.append(str(Path(__file__).parent))

try:
    from data_preprocessor import DataPreprocessor
except ImportError as e:
    print(f"❌ Error: Could not import data_preprocessor: {e}")
    print("Please ensure data_preprocessor.py is in the same directory")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def process_single_symbol(preprocessor: DataPreprocessor, symbol: str, validate_only: bool = False) -> bool:
    """
    Process a single symbol (BTC or ETH).

    Args:
        preprocessor: DataPreprocessor instance
        symbol: Symbol to process ('BTC' or 'ETH')
        validate_only: If True, only validate without processing

    Returns:
        True if processing successful, False otherwise
    """
    try:
        logger.info(f"\n{'='*60}")
        logger.info(f"PROCESSING {symbol.upper()} DATASETS")
        logger.info(f"{'='*60}")

        # Process the symbol
        result = preprocessor.process_symbol(symbol, "1H")

        if result['validation_passed']:
            logger.info(f"✅ {symbol} processing completed successfully")
            logger.info(f"   - Rows processed: {result['rows_processed']}")
            logger.info(f"   - Date range: {result['date_range']['start']} to {result['date_range']['end']}")
            logger.info(f"   - Files saved: {len(result['saved_files'])}")
            for file_type, path in result['saved_files'].items():
                logger.info(f"     * {file_type.upper()}: {path}")
        else:
            logger.warning(f"⚠️  {symbol} processing completed with validation issues")
            failed_checks = [k for k, v in result['validation_details'].items() if not v]
            logger.warning(f"   - Failed checks: {', '.join(failed_checks)}")

        return result['validation_passed']

    except Exception as e:
        logger.error(f"❌ Failed to process {symbol}: {e}")
        return False

def validate_existing_datasets(base_dir: str = "data") -> Dict[str, Dict]:
    """
    Validate existing datasets without processing them.

    Args:
        base_dir: Base directory containing the data

    Returns:
        Dictionary with validation results for each dataset
    """
    logger.info(f"\n{'='*60}")
    logger.info("VALIDATING EXISTING DATASETS")
    logger.info(f"{'='*60}")

    preprocessor = DataPreprocessor(base_dir)
    validation_results = {}

    # Check for existing datasets
    raw_dir = Path(base_dir) / "raw"

    if not raw_dir.exists():
        logger.warning(f"Raw data directory not found: {raw_dir}")
        return validation_results

    dataset_files = list(raw_dir.glob("*_data.csv"))
    logger.info(f"Found {len(dataset_files)} dataset files to validate")

    for file_path in dataset_files:
        try:
            logger.info(f"\n🔍 Validating: {file_path.name}")

            # Load and validate the dataset
            df = preprocessor.preprocess_raw_data(
                file_path.stem.split('_')[0],  # Extract symbol from filename
                "1H",
                file_path
            )

            # Run standards validation
            validation = preprocessor.validate_data_quality(df)

            validation_results[file_path.name] = {
                'file_path': str(file_path),
                'rows': len(df),
                'columns': list(df.columns),
                'validation_passed': all(validation.values()),
                'validation_details': validation
            }

            # Report results
            if all(validation.values()):
                logger.info(f"✅ {file_path.name}: All checks passed")
            else:
                failed_checks = [k for k, v in validation.items() if not v]
                logger.warning(f"⚠️  {file_path.name}: {len(failed_checks)} issues found")
                for check in failed_checks:
                    logger.warning(f"   - {check}: FAILED")

        except Exception as e:
            logger.error(f"❌ Failed to validate {file_path.name}: {e}")
            validation_results[file_path.name] = {
                'file_path': str(file_path),
                'error': str(e)
            }

    return validation_results

def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(description="Process cryptocurrency trading datasets")
    parser.add_argument("--symbol", choices=['BTC', 'ETH'],
                       help="Process only specified symbol (default: both)")
    parser.add_argument("--base-dir", default="data",
                       help="Base directory for data operations")
    parser.add_argument("--validate-only", action="store_true",
                       help="Only validate existing datasets, don't process")
    parser.add_argument("--force", action="store_true",
                       help="Force reprocessing even if files exist")

    args = parser.parse_args()

    # Initialize preprocessor
    try:
        preprocessor = DataPreprocessor(args.base_dir)
        logger.info(f"Initialized DataPreprocessor with base directory: {args.base_dir}")
    except Exception as e:
        logger.error(f"Failed to initialize DataPreprocessor: {e}")
        sys.exit(1)

    success_count = 0
    total_count = 0

    if args.validate_only:
        # Only validate existing datasets
        validation_results = validate_existing_datasets(args.base_dir)

        print(f"\n{'='*60}")
        print("VALIDATION SUMMARY")
        print(f"{'='*60}")

        for dataset_name, result in validation_results.items():
            if 'error' in result:
                print(f"❌ {dataset_name}: ERROR - {result['error']}")
            else:
                status = "✅ PASS" if result['validation_passed'] else "⚠️  ISSUES"
                print(f"{status} {dataset_name}: {result['rows']} rows, {len(result['columns'])} columns")

        sys.exit(0)

    # Process symbols
    symbols_to_process = [args.symbol] if args.symbol else ['BTC', 'ETH']

    for symbol in symbols_to_process:
        total_count += 1
        if process_single_symbol(preprocessor, symbol, args.validate_only):
            success_count += 1

    # Final summary
    print(f"\n{'='*60}")
    print("PROCESSING COMPLETE")
    print(f"{'='*60}")
    print(f"Symbols processed: {total_count}")
    print(f"Successful: {success_count}")
    print(f"Failed: {total_count - success_count}")

    if success_count == total_count:
        print("🎉 All datasets processed successfully!")
        sys.exit(0)
    else:
        print("⚠️  Some datasets had issues during processing")
        sys.exit(1)

if __name__ == "__main__":
    main()
