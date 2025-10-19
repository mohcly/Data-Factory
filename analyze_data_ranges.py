#!/usr/bin/env python3
"""
Analyze Data Ranges for Downloaded Cryptocurrency Data

This script analyzes the date ranges of all downloaded cryptocurrency data
to check alignment and identify any gaps or inconsistencies.

Usage:
    python analyze_data_ranges.py
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

class DataRangeAnalyzer:
    """Analyze date ranges across all cryptocurrency datasets."""

    def __init__(self):
        self.base_dir = Path("/Users/mohamedcoulibaly/MVP/Crypto/Data-factory")
        self.data_dir = self.base_dir / "data" / "top50_hourly"

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
                    'downloaded_at': metadata['downloaded_at'],
                    'days_of_data': None,
                    'start_datetime': None,
                    'end_datetime': None
                }

                # Convert dates to datetime objects for analysis
                crypto_info['start_datetime'] = pd.to_datetime(crypto_info['start_date'])
                crypto_info['end_datetime'] = pd.to_datetime(crypto_info['end_date'])

                # Calculate days of data
                date_range = crypto_info['end_datetime'] - crypto_info['start_datetime']
                crypto_info['days_of_data'] = date_range.days

                metadata_list.append(crypto_info)

            except Exception as e:
                print(f"Error reading {metadata_file}: {e}")
                continue

        return metadata_list

    def analyze_ranges(self, metadata_list):
        """Analyze the date ranges for alignment and consistency."""
        if not metadata_list:
            print("No metadata found!")
            return {}

        # Convert to DataFrame for easier analysis
        df = pd.DataFrame(metadata_list)

        # Sort by market cap rank
        df = df.sort_values('market_cap_rank')

        # Calculate statistics
        analysis = {
            'total_cryptos': len(df),
            'earliest_start': df['start_datetime'].min(),
            'latest_start': df['start_datetime'].max(),
            'earliest_end': df['end_datetime'].min(),
            'latest_end': df['end_datetime'].max(),
            'min_data_points': df['data_points'].min(),
            'max_data_points': df['data_points'].max(),
            'avg_data_points': df['data_points'].mean(),
            'min_days': df['days_of_data'].min(),
            'max_days': df['days_of_data'].max(),
            'avg_days': df['days_of_data'].mean(),
            'data_points_per_day_avg': (df['data_points'] / df['days_of_data']).mean()
        }

        # Find cryptos with earliest and latest start dates
        earliest_start = df.loc[df['start_datetime'].idxmin()]
        latest_start = df.loc[df['start_datetime'].idxmax()]

        analysis['earliest_start_crypto'] = {
            'symbol': earliest_start['symbol'],
            'name': earliest_start['name'],
            'start_date': earliest_start['start_date']
        }

        analysis['latest_start_crypto'] = {
            'symbol': latest_start['symbol'],
            'name': latest_start['name'],
            'start_date': latest_start['start_date']
        }

        # Check for significant gaps
        start_date_range = (df['start_datetime'].max() - df['start_datetime'].min()).days
        end_date_range = (df['end_datetime'].max() - df['end_datetime'].min()).days

        analysis['start_date_spread_days'] = start_date_range
        analysis['end_date_spread_days'] = end_date_range

        # Identify cryptos with significantly different ranges
        start_date_std = df['start_datetime'].std()
        end_date_std = df['end_datetime'].std()

        analysis['start_date_std_days'] = start_date_std.days if pd.notna(start_date_std) else 0
        analysis['end_date_std_days'] = end_date_std.days if pd.notna(end_date_std) else 0

        return analysis, df

    def print_analysis_report(self, analysis, df):
        """Print a comprehensive analysis report."""
        print("=" * 80)
        print("📊 CRYPTOCURRENCY DATA RANGE ANALYSIS REPORT")
        print("=" * 80)
        print(f"Total Cryptocurrencies Analyzed: {analysis['total_cryptos']}")
        print()

        print("📅 DATE RANGE STATISTICS:")
        print("-" * 40)
        print(f"Start Date Range: {analysis['start_date_spread_days']} days")
        print(f"End Date Range: {analysis['end_date_spread_days']} days")
        print(f"Start Date Std Dev: {analysis['start_date_std_days']:.1f} days")
        print(f"End Date Std Dev: {analysis['end_date_std_days']:.1f} days")
        print()

        print("🏆 EARLIEST & LATEST START DATES:")
        print("-" * 40)
        earliest = analysis['earliest_start_crypto']
        latest = analysis['latest_start_crypto']
        print(f"Earliest: {earliest['symbol']} ({earliest['name']}) - {earliest['start_date']}")
        print(f"Latest:   {latest['symbol']} ({latest['name']}) - {latest['start_date']}")
        print()

        print("📈 DATA VOLUME STATISTICS:")
        print("-" * 40)
        print(f"Data Points Range: {analysis['min_data_points']:,} - {analysis['max_data_points']:,}")
        print(f"Average Data Points: {analysis['avg_data_points']:,.0f}")
        print(f"Days of Data Range: {analysis['min_days']} - {analysis['max_days']} days")
        print(f"Average Days: {analysis['avg_days']:.1f} days")
        print(f"Avg Data Points/Day: {analysis['data_points_per_day_avg']:.1f}")
        print()

        print("🔍 TOP 10 CRYPTOS BY DATA POINTS:")
        print("-" * 40)
        top_10 = df.nlargest(10, 'data_points')[['symbol', 'name', 'data_points', 'days_of_data', 'start_date']]
        for _, row in top_10.iterrows():
            print(f"{row['symbol']:6} ({row['name'][:15]:15}) - {row['data_points']:6,} points, {row['days_of_data']:4} days - Start: {row['start_date'][:10]}")
        print()

        print("⚠️  CRYPTOS WITH LEAST DATA:")
        print("-" * 40)
        bottom_10 = df.nsmallest(10, 'data_points')[['symbol', 'name', 'data_points', 'days_of_data', 'start_date']]
        for _, row in bottom_10.iterrows():
            print(f"{row['symbol']:6} ({row['name'][:15]:15}) - {row['data_points']:6,} points, {row['days_of_data']:4} days - Start: {row['start_date'][:10]}")
        print()

        # Check alignment
        print("🎯 ALIGNMENT ANALYSIS:")
        print("-" * 40)
        if analysis['start_date_spread_days'] > 365:
            print(f"❌ SIGNIFICANT START DATE SPREAD: {analysis['start_date_spread_days']} days")
            print("   Some cryptocurrencies have very different start dates")
        else:
            print(f"✅ GOOD START DATE ALIGNMENT: {analysis['start_date_spread_days']} days spread")

        if analysis['end_date_spread_days'] > 7:
            print(f"❌ END DATE MISALIGNMENT: {analysis['end_date_spread_days']} days spread")
            print("   End dates are not well aligned")
        else:
            print(f"✅ GOOD END DATE ALIGNMENT: {analysis['end_date_spread_days']} days spread")

        if analysis['start_date_std_days'] > 180:
            print(f"⚠️  HIGH START DATE VARIABILITY: {analysis['start_date_std_days']:.1f} days std dev")
        else:
            print(f"✅ LOW START DATE VARIABILITY: {analysis['start_date_std_days']:.1f} days std dev")

        print("=" * 80)

    def export_to_csv(self, df, analysis):
        """Export the analysis results to CSV."""
        output_file = self.base_dir / "data_range_analysis.csv"

        # Add analysis summary as first row
        summary_df = pd.DataFrame([{
            'symbol': 'ANALYSIS_SUMMARY',
            'name': 'Summary Statistics',
            'total_cryptos': analysis['total_cryptos'],
            'start_date_spread_days': analysis['start_date_spread_days'],
            'end_date_spread_days': analysis['end_date_spread_days'],
            'avg_data_points': analysis['avg_data_points'],
            'avg_days': analysis['avg_days'],
            'earliest_start': analysis['earliest_start'],
            'latest_end': analysis['latest_end']
        }])

        # Combine and export
        combined_df = pd.concat([summary_df, df], ignore_index=True)
        combined_df.to_csv(output_file, index=False)
        print(f"📄 Analysis exported to: {output_file}")

def main():
    """Main execution function."""
    analyzer = DataRangeAnalyzer()

    # Extract metadata
    metadata_list = analyzer.extract_metadata()
    print(f"Found {len(metadata_list)} cryptocurrency metadata files")

    if not metadata_list:
        print("No metadata files found!")
        return

    # Analyze ranges
    analysis, df = analyzer.analyze_ranges(metadata_list)

    # Print report
    analyzer.print_analysis_report(analysis, df)

    # Export to CSV
    analyzer.export_to_csv(df, analysis)

if __name__ == "__main__":
    main()
