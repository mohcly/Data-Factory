#!/usr/bin/env python3
"""
Simple WebSocket test to check Binance connectivity
"""

import asyncio
import websockets
import json
import time

async def test_connection():
    symbol = 'BTCUSDT'
    base_symbol = symbol.upper().replace('USDT', '')

    # Test different depth stream formats
    test_streams = [
        f"{base_symbol.lower()}usdt@depth5",  # Basic depth
        f"{base_symbol.lower()}usdt@depth5@1000ms",  # With interval
        f"{base_symbol.lower()}usdt@ticker"  # Ticker for comparison
    ]

    for stream_name in test_streams:
        url = f'wss://fstream.binance.com/ws/{stream_name}'
        print(f"\n🧪 Testing stream: {stream_name}")
        print(f"URL: {url}")

        try:
            async with websockets.connect(url) as ws:
                print("✅ Connected successfully")

                message_count = 0
                start_time = time.time()

                while time.time() - start_time < 10:  # Test for 10 seconds per stream
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=3.0)
                        message_count += 1
                        print(f"📨 Message {message_count}: {message[:80]}...")

                        try:
                            data = json.loads(message)
                            msg_type = data.get('e', 'unknown')
                            print(f"   Type: {msg_type}")
                            if message_count >= 2:  # Got at least 2 messages
                                break
                        except:
                            print("   Not JSON")

                    except asyncio.TimeoutError:
                        print("⏰ Timeout - no message received")
                        break

                print(f"Total messages for {stream_name}: {message_count}")
                if message_count > 0:
                    print(f"✅ Stream {stream_name} is working!")
                    return  # Found a working stream

        except Exception as e:
            print(f"❌ Connection failed for {stream_name}: {e}")

    print("\n❌ No streams received messages")

    try:
        async with websockets.connect(url) as ws:
            print("✅ Connected successfully")

            start_time = time.time()
            message_count = 0

            while time.time() - start_time < 30:  # Test for 30 seconds
                try:
                    # Wait for message with timeout
                    message = await asyncio.wait_for(ws.recv(), timeout=5.0)

                    message_count += 1
                    print(f"📨 Message {message_count}: {message[:100]}...")

                    # Try to parse
                    try:
                        data = json.loads(message)
                        msg_type = data.get('e', 'unknown')
                        print(f"   Type: {msg_type}")

                        if msg_type == 'depthUpdate':
                            bids = len(data.get('b', []))
                            asks = len(data.get('a', []))
                            print(f"   Bids: {bids}, Asks: {asks}")
                            break  # We got a real orderbook update!

                    except json.JSONDecodeError:
                        print("   Not JSON format")

                except asyncio.TimeoutError:
                    print("⏰ Timeout waiting for message")
                    break

            print(f"Total messages received: {message_count}")

    except Exception as e:
        print(f"❌ Connection failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_connection())
