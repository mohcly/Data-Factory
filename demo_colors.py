#!/usr/bin/env python3
"""
Demo of Colored Output for Liquidation Monitor

This script demonstrates the colored output that will be used
in the real-time liquidation monitor.
"""

# ANSI color codes
class Colors:
    BRIGHT_GREEN = '\033[92m'
    BOLD = '\033[1m'
    BRIGHT_RED = '\033[91m'
    BRIGHT_YELLOW = '\033[93m'
    BRIGHT_BLUE = '\033[94m'
    BRIGHT_CYAN = '\033[96m'
    BRIGHT_MAGENTA = '\033[95m'
    BG_YELLOW = '\033[43m'
    BG_RED = '\033[41m'
    BRIGHT_WHITE = '\033[97m'
    BLACK = '\033[30m'
    DIM = '\033[2m'
    RESET = '\033[0m'

def demo_colors():
    print("="*80)
    print("🎨 LIQUIDATION MONITOR COLOR DEMO")
    print("="*80)

    print("\n🚀 Startup Messages:")
    print(f"{Colors.BRIGHT_GREEN}{Colors.BOLD}🚀 Starting Real-Time Liquidation Monitor{Colors.RESET}")
    print(f"📊 Monitoring: {Colors.BRIGHT_CYAN}BTC, ETH, BNB{Colors.RESET}")
    print(f"⏰ Duration: {Colors.BRIGHT_BLUE}Indefinite (Ctrl+C to stop){Colors.RESET}")

    print("\n📊 Real-Time Dashboard:")
    print(f"{Colors.BRIGHT_MAGENTA}{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BRIGHT_MAGENTA}{Colors.BOLD}📊 REAL-TIME LIQUIDATION MONITOR{Colors.RESET}")
    print(f"{Colors.BRIGHT_MAGENTA}{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"⏰ Running since: {Colors.BRIGHT_CYAN}2025-10-18 14:30:00{Colors.RESET}")
    print(f"🔗 Connection: {Colors.BRIGHT_GREEN}{Colors.BOLD}✅ Connected{Colors.RESET}")
    print(f"💰 Total Liquidations: {Colors.BLACK}{Colors.BG_YELLOW}{Colors.BOLD}1,247{Colors.RESET}")

    print("
📈 Per-Symbol Statistics:"    print(f"{Colors.DIM}{'-' * 40}{Colors.RESET}")
    print(f"  {Colors.BRIGHT_BLUE}BTC    {Colors.BRIGHT_CYAN}  456 {Colors.BRIGHT_CYAN}$12,345,678 {Colors.RESET}Recent (30min): {Colors.BRIGHT_GREEN}23{Colors.RESET}")
    print(f"  {Colors.BRIGHT_BLUE}ETH    {Colors.BRIGHT_CYAN}  312 {Colors.BRIGHT_CYAN}$8,901,234  {Colors.RESET}Recent (30min): {Colors.BRIGHT_GREEN}15{Colors.RESET}")
    print(f"  {Colors.BRIGHT_BLUE}BNB    {Colors.BRIGHT_CYAN}  234 {Colors.BRIGHT_CYAN}$3,456,789  {Colors.RESET}Recent (30min): {Colors.BRIGHT_YELLOW}8{Colors.RESET}")

    print(f"\n💰 LARGE LIQUIDATION: {Colors.BRIGHT_CYAN}BTC{Colors.RESET} {Colors.BRIGHT_CYAN}$25,000.00{Colors.RESET} {Colors.BRIGHT_YELLOW}SHORT{Colors.RESET} {Colors.BRIGHT_CYAN}0.50{Colors.RESET} @ {Colors.BRIGHT_CYAN}$50000.00{Colors.RESET}")

    print("
🏁 Final Summary:"    print(f"{Colors.BRIGHT_MAGENTA}{Colors.BOLD}{'='*80}{Colors.RESET}")
    print(f"{Colors.BRIGHT_GREEN}{Colors.BOLD}🏁 LIQUIDATION MONITORING SESSION COMPLETE{Colors.RESET}")
    print(f"{Colors.BRIGHT_MAGENTA}{Colors.BOLD}{'='*80}{Colors.RESET}")

    runtime_str = "2:30:15"
    total_liq = "1,247"
    avg_per_hour = "14.2"

    print(f"⏰ Session Duration: {Colors.BRIGHT_CYAN}{runtime_str}{Colors.RESET}")
    print(f"💰 Total Liquidations Captured: {Colors.BLACK}{Colors.BG_YELLOW}{Colors.BOLD}{total_liq}{Colors.RESET}")
    print(f"📊 Average Liquidations/Hour: {Colors.BRIGHT_CYAN}{avg_per_hour}{Colors.RESET}")

    print("
🎯 Color Legend:"    print(f"  {Colors.BRIGHT_GREEN}{Colors.BOLD}Green Bold{Colors.RESET}: Success messages, high activity")
    print(f"  {Colors.BRIGHT_RED}{Colors.BOLD}Red Bold{Colors.RESET}: Errors and critical issues")
    print(f"  {Colors.BRIGHT_YELLOW}Yellow{Colors.RESET}: Warnings and moderate activity")
    print(f"  {Colors.BRIGHT_BLUE}Blue{Colors.RESET}: Info messages and symbol names")
    print(f"  {Colors.BRIGHT_CYAN}Cyan{Colors.RESET}: Data values and numbers")
    print(f"  {Colors.BRIGHT_MAGENTA}{Colors.BOLD}Magenta Bold{Colors.RESET}: Headers and section titles")
    print(f"  {Colors.BLACK}{Colors.BG_YELLOW}{Colors.BOLD}Yellow BG{Colors.RESET}: Important highlights and totals")
    print(f"  {Colors.BRIGHT_WHITE}{Colors.BG_RED}{Colors.BOLD}Red BG{Colors.RESET}: Critical alerts")

    print("
✅ Color system ready for liquidation monitor!"    print("="*80)

if __name__ == "__main__":
    demo_colors()




