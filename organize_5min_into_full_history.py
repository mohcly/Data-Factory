#!/usr/bin/env python3
"""
Organize 5-Minute Data into Full History Folders

This script moves the downloaded 5-minute data files into the corresponding
cryptocurrency folders within the 2020-2025_full_history aligned structure.

Result: Each cryptocurrency folder will contain both hourly and 5-minute data.

Usage:
    python organize_5min_into_full_history.py

Before:
    data/5min_full_history/btc/btc_5min.csv
    data/aligned_by_period/2020-2025_full_history/btc/btc_hourly.csv

After:
    data/aligned_by_period/2020-2025_full_history/btc/btc_hourly.csv
    data/aligned_by_period/2020-2025_full_history/btc/btc_5min.csv
    data/aligned_by_period/2020-2025_full_history/btc/btc_5min_metadata.json

Author: MVP Crypto Data Factory
Created: 2025-10-17
"""

import shutil
import json
from pathlib import Path
from typing import Dict, List
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FiveMinOrganizer:
    """Organize 5-minute data into full history folders."""

    def __init__(self):
        self.base_dir = Path("/Users/mohamedcoulibaly/MVP/Crypto/Data-factory")
        self.source_dir = self.base_dir / "data" / "5min_full_history"
        self.target_dir = self.base_dir / "data" / "aligned_by_period" / "2020-2025_full_history"

        # Verify directories exist
        if not self.source_dir.exists():
            raise FileNotFoundError(f"Source directory not found: {self.source_dir}")
        if not self.target_dir.exists():
            raise FileNotFoundError(f"Target directory not found: {self.target_dir}")

    def get_cryptocurrencies_to_move(self) -> List[str]:
        """Get list of cryptocurrencies that have 5-minute data to move."""
        crypto_folders = []

        # Find all cryptocurrency folders in source directory
        for item in self.source_dir.iterdir():
            if item.is_dir() and not item.name.startswith('.'):
                crypto_folders.append(item.name.upper())

        logger.info(f"Found {len(crypto_folders)} cryptocurrencies with 5-minute data: {crypto_folders}")
        return sorted(crypto_folders)

    def move_crypto_5min_data(self, symbol: str) -> bool:
        """Move 5-minute data files for a specific cryptocurrency."""
        try:
            # Source paths
            source_crypto_dir = self.source_dir / symbol.lower()
            source_csv = source_crypto_dir / f"{symbol.lower()}_5min.csv"
            source_metadata = source_crypto_dir / f"{symbol.lower()}_5min_metadata.json"

            # Target paths
            target_crypto_dir = self.target_dir / symbol.lower()
            target_csv = target_crypto_dir / f"{symbol.lower()}_5min.csv"
            target_metadata = target_crypto_dir / f"{symbol.lower()}_5min_metadata.json"

            # Verify source files exist
            if not source_csv.exists():
                logger.error(f"Source CSV not found: {source_csv}")
                return False
            if not source_metadata.exists():
                logger.error(f"Source metadata not found: {source_metadata}")
                return False

            # Verify target directory exists
            if not target_crypto_dir.exists():
                logger.error(f"Target directory not found: {target_crypto_dir}")
                return False

            # Check if files already exist in target
            if target_csv.exists():
                logger.warning(f"5-minute CSV already exists in target, overwriting: {target_csv}")
            if target_metadata.exists():
                logger.warning(f"5-minute metadata already exists in target, overwriting: {target_metadata}")

            # Move files
            logger.info(f"Moving {symbol} 5-minute data to full history folder...")
            shutil.move(str(source_csv), str(target_csv))
            shutil.move(str(source_metadata), str(target_metadata))

            # Verify move was successful
            if target_csv.exists() and target_metadata.exists():
                # Update metadata to reflect new location
                self.update_metadata_location(target_metadata, symbol)
                logger.info(f"✅ Successfully moved {symbol} 5-minute data")
                return True
            else:
                logger.error(f"❌ File move verification failed for {symbol}")
                return False

        except Exception as e:
            logger.error(f"Error moving 5-minute data for {symbol}: {e}")
            return False

    def update_metadata_location(self, metadata_file: Path, symbol: str) -> None:
        """Update metadata to reflect the new location."""
        try:
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)

            # Add location information
            metadata['organized_into'] = '2020-2025_full_history'
            metadata['final_location'] = str(metadata_file.parent.relative_to(self.base_dir))

            # Save updated metadata
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)

        except Exception as e:
            logger.warning(f"Could not update metadata for {symbol}: {e}")

    def verify_target_structure(self, symbol: str) -> Dict[str, bool]:
        """Verify that the target folder has both hourly and 5-minute data."""
        target_crypto_dir = self.target_dir / symbol.lower()

        verification = {
            'directory_exists': target_crypto_dir.exists(),
            'hourly_csv': (target_crypto_dir / f"{symbol.lower()}_hourly.csv").exists(),
            'hourly_metadata': (target_crypto_dir / f"{symbol.lower()}_metadata.json").exists(),
            '5min_csv': (target_crypto_dir / f"{symbol.lower()}_5min.csv").exists(),
            '5min_metadata': (target_crypto_dir / f"{symbol.lower()}_5min_metadata.json").exists()
        }

        return verification

    def organize_all_5min_data(self) -> Dict[str, Dict]:
        """Organize all 5-minute data into full history folders."""
        cryptocurrencies = self.get_cryptocurrencies_to_move()

        if not cryptocurrencies:
            logger.error("No cryptocurrencies found to organize!")
            return {}

        results = {}
        successful_moves = 0

        print("🚀 Organizing 5-Minute Data into Full History Folders")
        print("=" * 70)
        print(f"📊 Cryptocurrencies to organize: {len(cryptocurrencies)}")
        print(f"📁 Source: {self.source_dir}")
        print(f"📁 Target: {self.target_dir}")
        print("=" * 70)

        for symbol in cryptocurrencies:
            print(f"\n{'='*50}")
            print(f"Processing {symbol}...")
            print(f"{'='*50}")

            # Move the data
            success = self.move_crypto_5min_data(symbol)

            # Verify the final structure
            verification = self.verify_target_structure(symbol)

            results[symbol] = {
                'move_success': success,
                'verification': verification,
                'complete': success and all(verification.values())
            }

            if results[symbol]['complete']:
                successful_moves += 1
                print(f"✅ {symbol}: Successfully organized")
            else:
                print(f"❌ {symbol}: Organization failed")
                if not success:
                    print("   • File move failed")
                else:
                    missing_items = [k for k, v in verification.items() if not v]
                    print(f"   • Missing items: {', '.join(missing_items)}")

        return results, successful_moves

    def create_organization_summary(self, results: Dict[str, Dict], successful_moves: int) -> None:
        """Create a summary of the organization operation."""
        total_cryptos = len(results)

        # Create summary
        summary = {
            'organization_summary': {
                'total_cryptos': total_cryptos,
                'successful': successful_moves,
                'failed': total_cryptos - successful_moves,
                'success_rate': f"{successful_moves/total_cryptos*100:.1f}%" if total_cryptos > 0 else "0%"
            },
            'operation_details': {
                'source_directory': str(self.source_dir.relative_to(self.base_dir)),
                'target_directory': str(self.target_dir.relative_to(self.base_dir)),
                'moved_files_per_crypto': ['{symbol}_5min.csv', '{symbol}_5min_metadata.json']
            },
            'results': results,
            'timestamp': json.dumps({'organized_at': str(Path().cwd())}, default=str)[1:-1]  # Get current time
        }

        # Save summary
        summary_file = self.target_dir / "5min_organization_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)

        # Print summary
        print("\n" + "="*70)
        print("📊 5-MINUTE DATA ORGANIZATION SUMMARY")
        print("="*70)
        print(f"Total Cryptocurrencies: {summary['organization_summary']['total_cryptos']}")
        print(f"Successfully Organized: {summary['organization_summary']['successful']}")
        print(f"Failed: {summary['organization_summary']['failed']}")
        print(f"Success Rate: {summary['organization_summary']['success_rate']}")
        print(f"Files Moved Per Crypto: {len(summary['operation_details']['moved_files_per_crypto'])}")
        print(f"Target Directory: {self.target_dir}")
        print("="*70)

        # List failed organizations
        failed_cryptos = [symbol for symbol, result in results.items() if not result['complete']]
        if failed_cryptos:
            print("\n❌ Failed Organizations:")
            for symbol in failed_cryptos:
                print(f"   • {symbol}")

        # Show example of final structure
        if successful_moves > 0:
            example_symbol = next(symbol for symbol, result in results.items() if result['complete'])
            print(f"\n📁 Example Final Structure ({example_symbol}):")
            target_dir = self.target_dir / example_symbol.lower()
            files = list(target_dir.glob("*"))
            for file in sorted(files):
                size_mb = file.stat().st_size / (1024 * 1024)
                print(f"   • {file.name} ({size_mb:.1f} MB)")

        print("\n✅ 5-minute data organization completed!")
        print(f"📄 Summary saved to: {summary_file}")

    def cleanup_empty_source_directory(self) -> None:
        """Remove the now-empty source directory."""
        try:
            # Check if source directory is empty (except for the summary file)
            remaining_items = [item for item in self.source_dir.iterdir() if item.name != '5min_download_summary.json']

            if not remaining_items:
                # Move the summary file to the target directory for reference
                summary_file = self.source_dir / '5min_download_summary.json'
                if summary_file.exists():
                    target_summary = self.target_dir / 'original_5min_download_summary.json'
                    shutil.move(str(summary_file), str(target_summary))
                    logger.info(f"Moved download summary to: {target_summary}")

                # Remove the empty source directory
                shutil.rmtree(self.source_dir)
                logger.info(f"Removed empty source directory: {self.source_dir}")
            else:
                logger.warning(f"Source directory not empty, keeping it: {remaining_items}")

        except Exception as e:
            logger.warning(f"Could not cleanup source directory: {e}")

def main():
    """Main execution function."""
    try:
        organizer = FiveMinOrganizer()

        # Organize all 5-minute data
        results, successful_moves = organizer.organize_all_5min_data()

        # Create summary
        organizer.create_organization_summary(results, successful_moves)

        # Cleanup
        organizer.cleanup_empty_source_directory()

    except Exception as e:
        logger.error(f"Error during 5-minute data organization: {e}")
        print(f"\n❌ Error: {e}")
        raise

if __name__ == "__main__":
    main()
