#!/usr/bin/env python3
"""
Organize Cryptocurrency Data by Time Alignment

This script analyzes the date ranges of downloaded cryptocurrency data and organizes
them into folders based on their temporal alignment for easier analysis and backtesting.

Created: 2025-10-17
"""

import json
import shutil
from pathlib import Path
from datetime import datetime
import pandas as pd

class DataOrganizer:
    """Organize cryptocurrency data by time alignment."""

    def __init__(self):
        self.base_dir = Path("/Users/mohamedcoulibaly/MVP/Crypto/Data-factory")
        self.data_dir = self.base_dir / "data" / "top50_hourly"
        self.aligned_dir = self.base_dir / "data" / "aligned_by_period"

        # Create aligned directory
        self.aligned_dir.mkdir(parents=True, exist_ok=True)

    def extract_metadata(self):
        """Extract metadata from all cryptocurrency folders."""
        metadata_list = []

        # Find all metadata files
        metadata_files = list(self.data_dir.glob("*/*_metadata.json"))

        for metadata_file in metadata_files:
            try:
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)

                # Extract relevant information
                crypto_info = {
                    'symbol': metadata['symbol'],
                    'name': metadata['name'],
                    'market_cap_rank': metadata['market_cap_rank'],
                    'data_points': metadata['data_points'],
                    'start_date': metadata['start_date'],
                    'end_date': metadata['end_date'],
                    'start_datetime': pd.to_datetime(metadata['start_date']),
                    'end_datetime': pd.to_datetime(metadata['end_date']),
                    'days_of_data': (pd.to_datetime(metadata['end_date']) - pd.to_datetime(metadata['start_date'])).days,
                    'folder_path': metadata_file.parent
                }

                metadata_list.append(crypto_info)

            except Exception as e:
                print(f"Error reading {metadata_file}: {e}")
                continue

        return metadata_list

    def categorize_by_time_period(self, crypto_info):
        """Categorize cryptocurrency based on its time period."""
        start_date = crypto_info['start_datetime']
        end_date = crypto_info['end_datetime']
        days_of_data = crypto_info['days_of_data']

        # Define time period categories
        if start_date.year <= 2020 and days_of_data >= 2000:
            # Full historical data (2020-2025, 2000+ days)
            return "2020-2025_full_history"
        elif start_date.year >= 2024 and days_of_data >= 300:
            # Recent data (2024-2025, 300+ days)
            return "2024-2025_recent"
        elif start_date.year >= 2023 and days_of_data >= 800:
            # Mid-term data (2023-2025, 800+ days)
            return "2023-2025_mid_term"
        elif start_date.year >= 2021 and days_of_data >= 1200:
            # Established but started later (2021-2025, 1200+ days)
            return "2021-2025_established"
        elif start_date.year >= 2020 and days_of_data >= 500:
            # Good coverage started mid-2020s (2020-2025, 500+ days)
            return "2020-2025_good_coverage"
        elif days_of_data >= 180:
            # Moderate coverage (180+ days)
            return "moderate_coverage"
        else:
            # Limited data (< 180 days)
            return "limited_data"

    def create_period_folders(self):
        """Create folders for different time periods."""
        period_folders = {
            "2020-2025_full_history": {
                "description": "Complete historical data from 2020 to present (2000+ days)",
                "criteria": "Start date <= 2020 AND days >= 2000",
                "use_case": "Full historical analysis, long-term backtesting"
            },
            "2021-2025_established": {
                "description": "Established cryptocurrencies with substantial history (1200+ days)",
                "criteria": "Start date >= 2021 AND days >= 1200",
                "use_case": "Established token analysis, medium-term backtesting"
            },
            "2020-2025_good_coverage": {
                "description": "Good historical coverage from 2020 to present (500+ days)",
                "criteria": "Start date >= 2020 AND 500 <= days < 1200",
                "use_case": "General analysis, backtesting with good history"
            },
            "2023-2025_mid_term": {
                "description": "Mid-term data from 2023 to present (800+ days)",
                "criteria": "Start date >= 2023 AND days >= 800",
                "use_case": "Recent token analysis, short-term backtesting"
            },
            "2024-2025_recent": {
                "description": "Recent data from 2024 to present (300+ days)",
                "criteria": "Start date >= 2024 AND days >= 300",
                "use_case": "New token analysis, very recent performance"
            },
            "moderate_coverage": {
                "description": "Moderate historical coverage (180-499 days)",
                "criteria": "180 <= days < 500",
                "use_case": "Limited analysis, spot checking"
            },
            "limited_data": {
                "description": "Limited historical data (< 180 days)",
                "criteria": "days < 180",
                "use_case": "New tokens, preliminary analysis only"
            }
        }

        # Create folders and save descriptions
        for period, info in period_folders.items():
            period_dir = self.aligned_dir / period
            period_dir.mkdir(exist_ok=True)

            # Save folder description
            desc_file = period_dir / "folder_description.json"
            with open(desc_file, 'w') as f:
                json.dump(info, f, indent=2)

        return period_folders

    def organize_data(self, metadata_list):
        """Organize data into appropriate time period folders."""
        period_counts = {}

        for crypto_info in metadata_list:
            symbol = crypto_info['symbol']
            period = self.categorize_by_time_period(crypto_info)

            # Count for this period
            if period not in period_counts:
                period_counts[period] = []
            period_counts[period].append(symbol)

            # Source and destination paths
            source_dir = crypto_info['folder_path']
            dest_dir = self.aligned_dir / period / symbol.lower()

            print(f"📁 Organizing {symbol} ({crypto_info['days_of_data']} days) -> {period}")

            # Create destination directory
            dest_dir.mkdir(exist_ok=True)

            # Copy all files
            for file_path in source_dir.glob("*"):
                if file_path.is_file():
                    shutil.copy2(file_path, dest_dir / file_path.name)

        return period_counts

    def create_period_summaries(self, period_counts, metadata_list):
        """Create summary files for each period."""
        metadata_df = pd.DataFrame(metadata_list)

        for period, symbols in period_counts.items():
            period_dir = self.aligned_dir / period

            # Filter metadata for this period
            period_metadata = metadata_df[metadata_df['symbol'].isin(symbols)].copy()
            period_metadata = period_metadata.sort_values('market_cap_rank')

            # Create summary (convert pandas types to Python types for JSON serialization)
            summary = {
                "period": period,
                "total_cryptos": len(symbols),
                "cryptocurrencies": symbols,
                "statistics": {
                    "avg_data_points": float(period_metadata['data_points'].mean()),
                    "min_data_points": int(period_metadata['data_points'].min()),
                    "max_data_points": int(period_metadata['data_points'].max()),
                    "avg_days": float(period_metadata['days_of_data'].mean()),
                    "min_days": int(period_metadata['days_of_data'].min()),
                    "max_days": int(period_metadata['days_of_data'].max()),
                    "earliest_start": period_metadata['start_datetime'].min().strftime('%Y-%m-%d'),
                    "latest_start": period_metadata['start_datetime'].max().strftime('%Y-%m-%d'),
                    "earliest_end": period_metadata['end_datetime'].min().strftime('%Y-%m-%d'),
                    "latest_end": period_metadata['end_datetime'].max().strftime('%Y-%m-%d')
                }
            }

            # Save summary
            summary_file = period_dir / "period_summary.json"
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)

            # Save detailed CSV
            period_metadata['period'] = period
            csv_file = period_dir / "cryptocurrencies.csv"
            period_metadata.to_csv(csv_file, index=False)

    def create_master_index(self, period_counts):
        """Create a master index of all organized data."""
        master_index = {
            "organized_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "total_cryptocurrencies": sum(len(symbols) for symbols in period_counts.values()),
            "periods": {}
        }

        for period, symbols in period_counts.items():
            period_dir = self.aligned_dir / period
            summary_file = period_dir / "period_summary.json"

            try:
                with open(summary_file, 'r') as f:
                    period_summary = json.load(f)

                master_index["periods"][period] = {
                    "count": len(symbols),
                    "cryptocurrencies": sorted(symbols),
                    "description": period_summary.get("statistics", {}),
                    "folder_path": str(period_dir.relative_to(self.base_dir))
                }
            except Exception as e:
                print(f"Error reading summary for {period}: {e}")

        # Save master index
        master_index_file = self.aligned_dir / "master_index.json"
        with open(master_index_file, 'w') as f:
            json.dump(master_index, f, indent=2)

        return master_index

    def print_organization_summary(self, master_index):
        """Print a summary of the organization."""
        print("=" * 80)
        print("📊 CRYPTOCURRENCY DATA ORGANIZATION COMPLETE")
        print("=" * 80)
        print(f"Total Cryptocurrencies Organized: {master_index['total_cryptocurrencies']}")
        print(f"Organized At: {master_index['organized_at']}")
        print(f"Base Directory: {self.aligned_dir}")
        print()

        for period, info in master_index['periods'].items():
            print(f"📁 {period}")
            print(f"   Count: {info['count']} cryptocurrencies")
            print(f"   Description: {info.get('description', {}).get('avg_days', 'N/A'):.0f} avg days of data")
            print(f"   Top Cryptos: {', '.join(info['cryptocurrencies'][:5])}{'...' if len(info['cryptocurrencies']) > 5 else ''}")
            print()

        print("🎯 USE CASES:")
        print("• 2020-2025_full_history: Long-term analysis and backtesting")
        print("• 2021-2025_established: Established token analysis")
        print("• 2020-2025_good_coverage: General analysis with good history")
        print("• 2023-2025_mid_term: Recent token performance")
        print("• 2024-2025_recent: New token analysis")
        print("• moderate_coverage: Limited analysis")
        print("• limited_data: Preliminary analysis only")
        print()

        print("📄 Master Index: data/aligned_by_period/master_index.json")
        print("=" * 80)

def main():
    """Main execution function."""
    organizer = DataOrganizer()

    print("🔄 Starting cryptocurrency data organization by time alignment...")

    # Extract metadata
    metadata_list = organizer.extract_metadata()
    print(f"📊 Found {len(metadata_list)} cryptocurrencies to organize")

    # Create period folders
    period_folders = organizer.create_period_folders()
    print(f"📁 Created {len(period_folders)} time period folders")

    # Organize data
    period_counts = organizer.organize_data(metadata_list)
    print(f"✅ Data organization complete")

    # Create summaries
    organizer.create_period_summaries(period_counts, metadata_list)
    print("📋 Created period summaries")

    # Create master index
    master_index = organizer.create_master_index(period_counts)
    print("📖 Created master index")

    # Print summary
    organizer.print_organization_summary(master_index)

if __name__ == "__main__":
    main()
