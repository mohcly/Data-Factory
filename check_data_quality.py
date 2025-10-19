#!/usr/bin/env python3
"""
Cryptocurrency Data Quality Assessment

This script performs comprehensive quality checks on all cryptocurrency datasets
including data completeness, consistency, accuracy, and statistical properties.

Checks Performed:
- Data completeness (missing values, date ranges)
- Data consistency (OHLC relationships, volume validation)
- Data integrity (timestamp sequences, duplicates)
- Statistical properties (distributions, outliers)
- Cross-dataset alignment

Usage:
    python check_data_quality.py

Output:
    - Quality report for each cryptocurrency
    - Summary statistics across all datasets
    - Identified issues and recommendations

Author: MVP Crypto Data Factory
Created: 2025-10-18
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import json
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

class DataQualityChecker:
    """Comprehensive data quality assessment for cryptocurrency datasets."""

    def __init__(self):
        self.base_dir = Path("/Users/mohamedcoulibaly/MVP/Crypto/Data-factory")
        self.target_dir = self.base_dir / "data" / "aligned_by_period" / "2020-2025_full_history"

        # Quality thresholds
        self.thresholds = {
            'max_missing_pct': 1.0,  # Max 1% missing values
            'min_data_points_1h': 40000,  # Min 40K points for 1H data (5+ years)
            'min_data_points_5m': 500000,  # Min 500K points for 5M data
            'max_price_outlier_std': 10,  # Max 10 std devs from mean for price outliers
            'max_volume_zero_pct': 5.0,  # Max 5% zero volume periods
            'max_ohlc_violations_pct': 1.0,  # Max 1% OHLC logic violations
            'expected_1h_intervals': 8760,  # ~365*24 hours per year * 6 years
            'expected_5m_intervals': 105120,  # ~365*24*12 5min periods per year * 6 years
        }

    def get_crypto_datasets(self) -> List[Dict]:
        """Get all cryptocurrency datasets with their file paths."""
        datasets = []

        if not self.target_dir.exists():
            print(f"Target directory not found: {self.target_dir}")
            return datasets

        # Find all cryptocurrency folders
        for crypto_dir in self.target_dir.iterdir():
            if not crypto_dir.is_dir() or crypto_dir.name.startswith('.'):
                continue

            symbol = crypto_dir.name.upper()

            # Check for required files
            files = {
                '1h_csv': crypto_dir / f"{crypto_dir.name}_hourly.csv",
                '1h_metadata': crypto_dir / f"{crypto_dir.name}_1h_metadata.json",
                '5m_csv': crypto_dir / f"{crypto_dir.name}_5min.csv",
                '5m_metadata': crypto_dir / f"{crypto_dir.name}_5min_metadata.json"
            }

            if all(f.exists() for f in files.values()):
                datasets.append({
                    'symbol': symbol,
                    'directory': crypto_dir,
                    'files': files
                })

        print(f"Found {len(datasets)} complete cryptocurrency datasets")
        return datasets

    def load_dataset(self, file_path: Path, timeframe: str) -> Optional[pd.DataFrame]:
        """Load a dataset with proper error handling."""
        try:
            df = pd.read_csv(file_path)

            # Ensure timestamp column exists and is parsed
            if 'timestamp' not in df.columns:
                print(f"Warning: No timestamp column in {file_path}")
                return None

            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df = df.dropna(subset=['timestamp'])

            # Validate required OHLC columns
            required_cols = ['open', 'high', 'low', 'close']
            if not all(col in df.columns for col in required_cols):
                print(f"Warning: Missing OHLC columns in {file_path}")
                return None

            # Convert price columns to numeric
            for col in required_cols + (['volume'] if 'volume' in df.columns else []):
                df[col] = pd.to_numeric(df[col], errors='coerce')

            df['timeframe'] = timeframe
            df['symbol'] = file_path.parent.name.upper()

            return df.sort_values('timestamp').reset_index(drop=True)

        except Exception as e:
            print(f"Error loading {file_path}: {e}")
            return None

    def check_data_completeness(self, df: pd.DataFrame, timeframe: str) -> Dict:
        """Check data completeness metrics."""
        total_rows = len(df)
        completeness = {}

        # Check for missing values
        missing_data = df.isnull().sum()
        completeness['total_rows'] = total_rows
        completeness['missing_values'] = missing_data.to_dict()
        completeness['missing_pct'] = (missing_data / total_rows * 100).to_dict()

        # Check date range
        if not df.empty:
            completeness['date_range'] = {
                'start': df['timestamp'].min().strftime('%Y-%m-%d %H:%M:%S'),
                'end': df['timestamp'].max().strftime('%Y-%m-%d %H:%M:%S'),
                'days': (df['timestamp'].max() - df['timestamp'].min()).days
            }

            # Check for expected number of data points
            if timeframe == '1h':
                expected_points = self.thresholds['expected_1h_intervals']
                completeness['data_density'] = total_rows / expected_points if expected_points > 0 else 0
            elif timeframe == '5m':
                expected_points = self.thresholds['expected_5m_intervals']
                completeness['data_density'] = total_rows / expected_points if expected_points > 0 else 0
        else:
            completeness['date_range'] = None

        return completeness

    def check_data_consistency(self, df: pd.DataFrame) -> Dict:
        """Check data consistency (OHLC relationships, volume)."""
        consistency = {}

        # OHLC logic checks
        ohlc_violations = 0
        total_checks = len(df)

        for _, row in df.iterrows():
            # High should be >= max(open, close)
            # Low should be <= min(open, close)
            if row['high'] < max(row['open'], row['close']) or row['low'] > min(row['open'], row['close']):
                ohlc_violations += 1

        consistency['ohlc_violations'] = ohlc_violations
        consistency['ohlc_violations_pct'] = (ohlc_violations / total_checks * 100) if total_checks > 0 else 0

        # Volume checks
        if 'volume' in df.columns:
            zero_volume = (df['volume'] == 0).sum()
            negative_volume = (df['volume'] < 0).sum()
            consistency['zero_volume_count'] = zero_volume
            consistency['zero_volume_pct'] = (zero_volume / len(df) * 100) if len(df) > 0 else 0
            consistency['negative_volume_count'] = negative_volume
        else:
            consistency['volume_data'] = 'MISSING'

        # Price sanity checks
        price_cols = ['open', 'high', 'low', 'close']
        for col in price_cols:
            if col in df.columns:
                negative_prices = (df[col] <= 0).sum()
                consistency[f'{col}_negative_count'] = negative_prices
                consistency[f'{col}_negative_pct'] = (negative_prices / len(df) * 100) if len(df) > 0 else 0

        return consistency

    def check_data_integrity(self, df: pd.DataFrame, timeframe: str) -> Dict:
        """Check data integrity (duplicates, sequences, gaps)."""
        integrity = {}

        # Check for duplicates
        duplicates = df.duplicated(subset=['timestamp']).sum()
        integrity['duplicate_timestamps'] = duplicates
        integrity['duplicate_pct'] = (duplicates / len(df) * 100) if len(df) > 0 else 0

        # Check timestamp sequence
        if not df.empty:
            df_sorted = df.sort_values('timestamp')
            expected_interval = timedelta(hours=1) if timeframe == '1h' else timedelta(minutes=5)

            # Calculate gaps
            time_diffs = df_sorted['timestamp'].diff()
            gaps = (time_diffs > expected_interval).sum()
            integrity['timestamp_gaps'] = gaps
            integrity['expected_interval'] = str(expected_interval)

            # Check for irregular intervals
            irregular_intervals = ((time_diffs != expected_interval) & (~time_diffs.isnull())).sum()
            integrity['irregular_intervals'] = irregular_intervals
        else:
            integrity['timestamp_sequence'] = 'EMPTY_DATASET'

        return integrity

    def check_statistical_properties(self, df: pd.DataFrame) -> Dict:
        """Check statistical properties and outliers."""
        stats = {}

        price_cols = ['open', 'high', 'low', 'close']

        for col in price_cols:
            if col in df.columns and not df[col].empty:
                series = df[col].dropna()
                if len(series) > 0:
                    mean_val = series.mean()
                    std_val = series.std()
                    min_val = series.min()
                    max_val = series.max()

                    # Outlier detection (beyond 3 standard deviations)
                    outliers = ((series - mean_val).abs() > 3 * std_val).sum()
                    outlier_pct = (outliers / len(series) * 100) if len(series) > 0 else 0

                    stats[col] = {
                        'mean': float(mean_val),
                        'std': float(std_val),
                        'min': float(min_val),
                        'max': float(max_val),
                        'outliers_3std': int(outliers),
                        'outliers_pct': float(outlier_pct)
                    }

        # Volume statistics
        if 'volume' in df.columns and not df['volume'].empty:
            vol_series = df['volume'].dropna()
            if len(vol_series) > 0:
                stats['volume'] = {
                    'mean': float(vol_series.mean()),
                    'std': float(vol_series.std()),
                    'min': float(vol_series.min()),
                    'max': float(vol_series.max()),
                    'zero_count': int((vol_series == 0).sum())
                }

        return stats

    def assess_dataset_quality(self, dataset: Dict) -> Dict:
        """Perform complete quality assessment for a single dataset."""
        symbol = dataset['symbol']
        quality_report = {
            'symbol': symbol,
            'overall_quality': 'UNKNOWN',
            'issues': [],
            'warnings': [],
            'recommendations': []
        }

        # Load both timeframes
        df_1h = self.load_dataset(dataset['files']['1h_csv'], '1h')
        df_5m = self.load_dataset(dataset['files']['5m_csv'], '5m')

        if df_1h is None:
            quality_report['issues'].append("Failed to load 1H data")
            quality_report['overall_quality'] = 'CRITICAL'
            return quality_report

        if df_5m is None:
            quality_report['issues'].append("Failed to load 5M data")
            quality_report['overall_quality'] = 'CRITICAL'
            return quality_report

        # 1H Data Quality Checks
        quality_report['1h_data'] = {
            'completeness': self.check_data_completeness(df_1h, '1h'),
            'consistency': self.check_data_consistency(df_1h),
            'integrity': self.check_data_integrity(df_1h, '1h'),
            'statistics': self.check_statistical_properties(df_1h)
        }

        # 5M Data Quality Checks
        quality_report['5m_data'] = {
            'completeness': self.check_data_completeness(df_5m, '5m'),
            'consistency': self.check_data_consistency(df_5m),
            'integrity': self.check_data_integrity(df_5m, '5m'),
            'statistics': self.check_statistical_properties(df_5m)
        }

        # Cross-dataset validation
        quality_report['cross_validation'] = self.validate_cross_datasets(df_1h, df_5m)

        # Quality assessment
        quality_score = self.calculate_quality_score(quality_report)
        quality_report['overall_quality'] = quality_score['grade']
        quality_report['quality_score'] = quality_score['score']
        quality_report['issues'] = quality_score['issues']
        quality_report['warnings'] = quality_score['warnings']
        quality_report['recommendations'] = quality_score['recommendations']

        return quality_report

    def validate_cross_datasets(self, df_1h: pd.DataFrame, df_5m: pd.DataFrame) -> Dict:
        """Validate consistency between 1H and 5M datasets."""
        validation = {}

        if df_1h.empty or df_5m.empty:
            validation['status'] = 'CANNOT_VALIDATE'
            return validation

        # Check date range alignment
        h1_start = df_1h['timestamp'].min()
        h1_end = df_1h['timestamp'].max()
        m5_start = df_5m['timestamp'].min()
        m5_end = df_5m['timestamp'].max()

        validation['date_alignment'] = {
            '1h_range': f"{h1_start.strftime('%Y-%m-%d')} to {h1_end.strftime('%Y-%m-%d')}",
            '5m_range': f"{m5_start.strftime('%Y-%m-%d')} to {m5_end.strftime('%Y-%m-%d')}",
            'ranges_match': abs((h1_start - m5_start).days) <= 1 and abs((h1_end - m5_end).days) <= 1
        }

        # Check data point ratios (should be ~12:1 for 5min:1hour)
        expected_ratio = 12.0  # 60 minutes / 5 minutes = 12
        actual_ratio = len(df_5m) / len(df_1h) if len(df_1h) > 0 else 0
        validation['data_ratio'] = {
            'expected_5m_to_1h': expected_ratio,
            'actual_5m_to_1h': actual_ratio,
            'ratio_deviation_pct': abs(actual_ratio - expected_ratio) / expected_ratio * 100
        }

        return validation

    def calculate_quality_score(self, quality_report: Dict) -> Dict:
        """Calculate overall quality score and generate recommendations."""
        score = 100  # Start with perfect score
        issues = []
        warnings = []
        recommendations = []

        # Check 1H data quality
        h1_data = quality_report.get('1h_data', {})

        # Completeness checks
        completeness = h1_data.get('completeness', {})
        missing_pct = completeness.get('missing_pct', {})
        for col, pct in missing_pct.items():
            if pct > self.thresholds['max_missing_pct']:
                score -= 10
                issues.append(f"High missing values in 1H {col}: {pct:.1f}%")

        # Data points check
        total_rows_1h = completeness.get('total_rows', 0)
        if total_rows_1h < self.thresholds['min_data_points_1h']:
            score -= 20
            issues.append(f"Insufficient 1H data points: {total_rows_1h} < {self.thresholds['min_data_points_1h']}")

        # Consistency checks
        consistency = h1_data.get('consistency', {})
        ohlc_violations_pct = consistency.get('ohlc_violations_pct', 0)
        if ohlc_violations_pct > self.thresholds['max_ohlc_violations_pct']:
            score -= 15
            issues.append(f"OHLC logic violations in 1H: {ohlc_violations_pct:.1f}%")

        zero_volume_pct = consistency.get('zero_volume_pct', 0)
        if zero_volume_pct > self.thresholds['max_volume_zero_pct']:
            score -= 10
            warnings.append(f"High zero volume periods in 1H: {zero_volume_pct:.1f}%")

        # Integrity checks
        integrity = h1_data.get('integrity', {})
        duplicate_pct = integrity.get('duplicate_pct', 0)
        if duplicate_pct > 0.1:
            score -= 5
            warnings.append(f"Duplicate timestamps in 1H: {duplicate_pct:.1f}%")

        # Check 5M data quality
        m5_data = quality_report.get('5m_data', {})

        # Completeness checks for 5M
        m5_completeness = m5_data.get('completeness', {})
        total_rows_5m = m5_completeness.get('total_rows', 0)
        if total_rows_5m < self.thresholds['min_data_points_5m']:
            score -= 20
            issues.append(f"Insufficient 5M data points: {total_rows_5m} < {self.thresholds['min_data_points_5m']}")

        # Cross-validation
        cross_val = quality_report.get('cross_validation', {})
        date_alignment = cross_val.get('date_alignment', {})
        if not date_alignment.get('ranges_match', False):
            score -= 10
            warnings.append("Date ranges don't align between 1H and 5M data")

        ratio_info = cross_val.get('data_ratio', {})
        ratio_deviation = ratio_info.get('ratio_deviation_pct', 0)
        if ratio_deviation > 20:
            score -= 5
            warnings.append(f"Unexpected data ratio between 1H and 5M: {ratio_deviation:.1f}% deviation")

        # Determine grade
        if score >= 95:
            grade = 'EXCELLENT'
        elif score >= 85:
            grade = 'GOOD'
        elif score >= 70:
            grade = 'FAIR'
        elif score >= 50:
            grade = 'POOR'
        else:
            grade = 'CRITICAL'

        # Generate recommendations
        if issues:
            recommendations.append("Address critical data quality issues before analysis")
        if warnings:
            recommendations.append("Review data quality warnings for potential impacts")

        if score < 85:
            recommendations.append("Consider data cleaning and validation procedures")
        if not date_alignment.get('ranges_match', False):
            recommendations.append("Align date ranges between 1H and 5M datasets")

        return {
            'score': score,
            'grade': grade,
            'issues': issues,
            'warnings': warnings,
            'recommendations': recommendations
        }

    def run_quality_assessment(self) -> Dict:
        """Run quality assessment for all datasets."""
        datasets = self.get_crypto_datasets()

        if not datasets:
            return {'error': 'No datasets found'}

        print("🔍 Starting Data Quality Assessment")
        print("=" * 60)
        print(f"📊 Datasets to assess: {len(datasets)}")
        print("=" * 60)

        assessment_results = {}
        quality_distribution = {'EXCELLENT': 0, 'GOOD': 0, 'FAIR': 0, 'POOR': 0, 'CRITICAL': 0}

        for dataset in datasets:
            symbol = dataset['symbol']
            print(f"\n🔎 Assessing {symbol}...")

            quality_report = self.assess_dataset_quality(dataset)
            assessment_results[symbol] = quality_report

            grade = quality_report['overall_quality']
            quality_distribution[grade] += 1

            score = quality_report.get('quality_score', 0)
            issues_count = len(quality_report.get('issues', []))
            warnings_count = len(quality_report.get('warnings', []))

            print(f"   Quality: {grade} ({score}%) | Issues: {issues_count} | Warnings: {warnings_count}")

        # Generate summary report
        summary = self.generate_summary_report(assessment_results, quality_distribution)

        # Save detailed results
        output_file = self.base_dir / "data_quality_assessment.json"
        with open(output_file, 'w') as f:
            json.dump({
                'assessment_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'summary': summary,
                'detailed_results': assessment_results
            }, f, indent=2, default=str)

        self.print_summary_report(summary)

        print(f"\n📄 Detailed assessment saved to: {output_file}")

        return assessment_results

    def generate_summary_report(self, results: Dict, quality_distribution: Dict) -> Dict:
        """Generate summary statistics across all assessments."""
        summary = {
            'total_datasets': len(results),
            'quality_distribution': quality_distribution,
            'average_quality_score': 0,
            'datasets_by_quality': {},
            'most_common_issues': [],
            'critical_datasets': []
        }

        total_score = 0
        all_issues = []
        all_warnings = []

        for symbol, report in results.items():
            score = report.get('quality_score', 0)
            total_score += score

            grade = report['overall_quality']
            if grade not in summary['datasets_by_quality']:
                summary['datasets_by_quality'][grade] = []
            summary['datasets_by_quality'][grade].append(symbol)

            issues = report.get('issues', [])
            warnings = report.get('warnings', [])

            all_issues.extend(issues)
            all_warnings.extend(warnings)

            if grade in ['CRITICAL', 'POOR']:
                summary['critical_datasets'].append({
                    'symbol': symbol,
                    'grade': grade,
                    'score': score,
                    'issues': issues
                })

        summary['average_quality_score'] = total_score / len(results) if results else 0

        # Find most common issues
        from collections import Counter
        issue_counts = Counter(all_issues)
        summary['most_common_issues'] = issue_counts.most_common(5)

        return summary

    def print_summary_report(self, summary: Dict) -> None:
        """Print a formatted summary report."""
        print("\n" + "="*80)
        print("📊 CRYPTOCURRENCY DATA QUALITY ASSESSMENT SUMMARY")
        print("="*80)
        print(f"Total Datasets Assessed: {summary['total_datasets']}")
        print(f"Average Quality Score: {summary['average_quality_score']:.1f}%")
        print()

        print("🎯 QUALITY DISTRIBUTION:")
        for grade, count in summary['quality_distribution'].items():
            if count > 0:
                pct = (count / summary['total_datasets']) * 100
                print(f"   {grade}: {count} datasets ({pct:.1f}%)")
        print()

        if summary['critical_datasets']:
            print("🚨 CRITICAL DATASETS:")
            for dataset in summary['critical_datasets']:
                print(f"   ❌ {dataset['symbol']}: {dataset['grade']} ({dataset['score']}%)")
                for issue in dataset['issues'][:2]:  # Show first 2 issues
                    print(f"      • {issue}")
            print()

        if summary['most_common_issues']:
            print("🔍 MOST COMMON ISSUES:")
            for issue, count in summary['most_common_issues']:
                pct = (count / summary['total_datasets']) * 100
                print(f"   • {issue} ({count} datasets, {pct:.1f}%)")
            print()

        print("✅ ASSESSMENT COMPLETE")
        print("="*80)

def main():
    """Main execution function."""
    try:
        checker = DataQualityChecker()
        results = checker.run_quality_assessment()

        if 'error' in results:
            print(f"❌ Error: {results['error']}")
        else:
            print("🎯 Data quality assessment completed successfully!")

    except Exception as e:
        print(f"❌ Error during quality assessment: {e}")
        raise

if __name__ == "__main__":
    main()




