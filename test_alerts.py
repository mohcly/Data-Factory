#!/usr/bin/env python3
"""
Test script for liquidation alert system.
Demonstrates different alert types and sounds.
"""

import sys
import os

# Add the current directory to Python path to import our modules
sys.path.insert(0, os.path.dirname(__file__))

from realtime_liquidation_monitor import AlertSystem

def test_alerts():
    """Test all alert types."""
    print("🔔 TESTING LIQUIDATION ALERT SYSTEM")
    print("====================================")
    print()

    alert_system = AlertSystem(enabled=True)

    test_cases = [
        ("BTC", "LONG", 1000, "Small LONG liquidation (< $5K)"),
        ("ETH", "SHORT", 7500, "Medium SHORT liquidation ($5K-$25K)"),
        ("BNB", "LONG", 35000, "Large LONG liquidation ($25K-$100K)"),
        ("SOL", "SHORT", 150000, "Huge SHORT liquidation (>$100K)"),
    ]

    print("Testing different alert types:")
    print("------------------------------")

    for symbol, side, amount, description in test_cases:
        print(f"\n🧪 Testing: {description}")
        print(f"   Symbol: {symbol}, Side: {side}, Amount: ${amount:,.0f}")
        alert_system.alert_liquidation(symbol, side, amount)
        print("   ✅ Alert triggered!")
    print()
    print("🎵 ALERT SOUND LEGEND:")
    print("• Low tone: Small liquidations (< $5K)")
    print("• Medium tone: Medium liquidations ($5K-$25K)")
    print("• High tone: Large liquidations ($25K-$100K)")
    print("• Urgent tone: Huge liquidations (>$100K)")
    print()
    print("🎨 ALERT COLOR LEGEND:")
    print("• Yellow: Small/medium liquidations")
    print("• Blue: Medium liquidations")
    print("• Red: Large liquidations")
    print("• Red background: Huge liquidations")
    print()
    print("💡 To disable alerts, use: --no-alerts flag")

if __name__ == "__main__":
    test_alerts()
