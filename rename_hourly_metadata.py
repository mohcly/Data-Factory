#!/usr/bin/env python3
"""
Rename Hourly Metadata Files in Full History Folder

This script renames the hourly metadata files from '{symbol}_metadata.json'
to '{symbol}_1h_metadata.json' to distinguish them from 5-minute metadata files.

Usage:
    python rename_hourly_metadata.py

Process:
    - Finds all cryptocurrency folders in 2020-2025_full_history
    - Renames {symbol}_metadata.json to {symbol}_1h_metadata.json
    - Updates metadata content to reflect new naming
    - Provides summary of changes

Author: MVP Crypto Data Factory
Created: 2025-10-18
"""

import os
import json
from pathlib import Path
from typing import Dict, List

class MetadataRenamer:
    """Rename hourly metadata files to include timeframe suffix."""

    def __init__(self):
        self.base_dir = Path("/Users/mohamedcoulibaly/MVP/Crypto/Data-factory")
        self.target_dir = self.base_dir / "data" / "aligned_by_period" / "2020-2025_full_history"

        # Verify directory exists
        if not self.target_dir.exists():
            raise FileNotFoundError(f"Target directory not found: {self.target_dir}")

    def get_crypto_folders(self) -> List[str]:
        """Get list of all cryptocurrency folder names."""
        crypto_folders = []

        for item in self.target_dir.iterdir():
            if item.is_dir() and not item.name.startswith('.'):
                # Verify it has the expected files
                metadata_file = item / f"{item.name}_metadata.json"
                if metadata_file.exists():
                    crypto_folders.append(item.name.upper())

        crypto_folders.sort()
        print(f"Found {len(crypto_folders)} cryptocurrency folders with metadata files")
        return crypto_folders

    def rename_metadata_file(self, symbol: str) -> bool:
        """Rename the metadata file for a specific cryptocurrency."""
        try:
            symbol_lower = symbol.lower()
            crypto_dir = self.target_dir / symbol_lower

            # Current and new file paths
            old_file = crypto_dir / f"{symbol_lower}_metadata.json"
            new_file = crypto_dir / f"{symbol_lower}_1h_metadata.json"

            if not old_file.exists():
                print(f"❌ Source file not found: {old_file}")
                return False

            if new_file.exists():
                print(f"⚠️  Target file already exists, overwriting: {new_file}")

            # Read existing metadata
            with open(old_file, 'r') as f:
                metadata = json.load(f)

            # Update metadata to reflect new naming
            metadata['filename'] = f"{symbol_lower}_1h_metadata.json"
            metadata['timeframe'] = '1h'
            metadata['renamed_at'] = str(Path().cwd())
            metadata['previous_filename'] = f"{symbol_lower}_metadata.json"

            # Rename the file
            os.rename(str(old_file), str(new_file))

            # Write updated metadata
            with open(new_file, 'w') as f:
                json.dump(metadata, f, indent=2)

            print(f"✅ Renamed {symbol}: {old_file.name} → {new_file.name}")
            return True

        except Exception as e:
            print(f"❌ Error renaming metadata for {symbol}: {e}")
            return False

    def verify_final_structure(self, symbol: str) -> Dict[str, bool]:
        """Verify the final file structure for a cryptocurrency."""
        symbol_lower = symbol.lower()
        crypto_dir = self.target_dir / symbol_lower

        verification = {
            'directory_exists': crypto_dir.exists(),
            'hourly_csv': (crypto_dir / f"{symbol_lower}_hourly.csv").exists(),
            'old_metadata_gone': not (crypto_dir / f"{symbol_lower}_metadata.json").exists(),
            'new_metadata_exists': (crypto_dir / f"{symbol_lower}_1h_metadata.json").exists(),
            '5min_csv': (crypto_dir / f"{symbol_lower}_5min.csv").exists(),
            '5min_metadata': (crypto_dir / f"{symbol_lower}_5min_metadata.json").exists()
        }

        return verification

    def rename_all_metadata(self) -> Dict[str, Dict]:
        """Rename metadata files for all cryptocurrencies."""
        cryptocurrencies = self.get_crypto_folders()

        if not cryptocurrencies:
            print("No cryptocurrency folders found!")
            return {}

        results = {}
        successful_renames = 0

        print("🔄 Renaming Hourly Metadata Files")
        print("=" * 50)
        print(f"📊 Cryptocurrencies to process: {len(cryptocurrencies)}")
        print(f"📁 Target directory: {self.target_dir}")
        print("=" * 50)

        for symbol in cryptocurrencies:
            print(f"\nProcessing {symbol}...")

            # Rename the metadata file
            success = self.rename_metadata_file(symbol)

            # Verify the final structure
            verification = self.verify_final_structure(symbol)

            results[symbol] = {
                'rename_success': success,
                'verification': verification,
                'complete': success and verification['new_metadata_exists'] and verification['old_metadata_gone']
            }

            if results[symbol]['complete']:
                successful_renames += 1
                print(f"✅ {symbol}: Successfully renamed")
            else:
                print(f"❌ {symbol}: Rename failed")
                if not success:
                    print("   • File rename failed")
                else:
                    issues = []
                    if not verification['old_metadata_gone']:
                        issues.append("old metadata still exists")
                    if not verification['new_metadata_exists']:
                        issues.append("new metadata missing")
                    if issues:
                        print(f"   • Issues: {', '.join(issues)}")

        return results, successful_renames

    def create_rename_summary(self, results: Dict[str, Dict], successful_renames: int) -> None:
        """Create a summary of the rename operation."""
        total_cryptos = len(results)

        # Create summary
        summary = {
            'rename_summary': {
                'total_cryptos': total_cryptos,
                'successful': successful_renames,
                'failed': total_cryptos - successful_renames,
                'success_rate': f"{successful_renames/total_cryptos*100:.1f}%" if total_cryptos > 0 else "0%"
            },
            'operation_details': {
                'old_pattern': '{symbol}_metadata.json',
                'new_pattern': '{symbol}_1h_metadata.json',
                'target_directory': str(self.target_dir.relative_to(self.base_dir)),
                'files_renamed_per_crypto': 1,
                'metadata_updated': True
            },
            'results': results,
            'timestamp': json.dumps({'renamed_at': str(Path().cwd())}, default=str)[1:-1]
        }

        # Save summary
        summary_file = self.target_dir / "metadata_rename_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)

        # Print summary
        print("\n" + "="*60)
        print("📊 METADATA RENAME SUMMARY")
        print("="*60)
        print(f"Total Cryptocurrencies: {summary['rename_summary']['total_cryptos']}")
        print(f"Successfully Renamed: {summary['rename_summary']['successful']}")
        print(f"Failed: {summary['rename_summary']['failed']}")
        print(f"Success Rate: {summary['rename_summary']['success_rate']}")
        print(f"Pattern: {summary['operation_details']['old_pattern']} → {summary['operation_details']['new_pattern']}")
        print(f"Target Directory: {self.target_dir}")
        print("="*60)

        # List failed renames
        failed_cryptos = [symbol for symbol, result in results.items() if not result['complete']]
        if failed_cryptos:
            print("\n❌ Failed Renames:")
            for symbol in failed_cryptos:
                print(f"   • {symbol}")

        # Show example of final structure
        if successful_renames > 0:
            example_symbol = next(symbol for symbol, result in results.items() if result['complete'])
            print(f"\n📁 Example Final Structure ({example_symbol}):")
            crypto_dir = self.target_dir / example_symbol.lower()
            files = list(crypto_dir.glob("*.json"))
            for file in sorted(files):
                print(f"   • {file.name}")

        print("\n✅ Metadata rename operation completed!")
        print(f"📄 Summary saved to: {summary_file}")

def main():
    """Main execution function."""
    try:
        renamer = MetadataRenamer()

        # Rename all metadata files
        results, successful_renames = renamer.rename_all_metadata()

        # Create summary
        renamer.create_rename_summary(results, successful_renames)

    except Exception as e:
        print(f"❌ Error during metadata rename operation: {e}")
        raise

if __name__ == "__main__":
    main()




