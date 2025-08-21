#!/usr/bin/env python3
"""
Simple Test for Nexmark Query 8
Calls the simplified handler with 12-hour window
"""

import json
import time
import sys
import os

# Add the current directory to Python path to import the handler
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from handler import NexmarkQuery8Streaming

def test_simple_performance():
    """Test simple performance with 12-hour window"""
    
    print("=== Nexmark Query 8 - Simple Performance Test ===")
    print("Testing with 12-hour window (43,200 entries)")
    print()
    
    # Initialize the query processor
    try:
        query8 = NexmarkQuery8Streaming(
            redis_host='128.110.217.212',
            redis_port=6379,
            redis_db=0
        )
        print("✅ Connected to Redis server")
    except Exception as e:
        print(f"❌ Failed to connect to Redis: {e}")
        return False
    
    # Test auction ID
    auction_id = "simple_test_auction"
    
    # Step 1: Initialize 12-hour window
    print("1. Initializing 12-hour window...")
    start_time = time.time()
    init_result = query8.initialize_12_hour_window(auction_id)
    init_time = time.time() - start_time
    
    print(f"   ✅ Initialization completed in {init_time:.2f}s")
    print(f"   📊 Entries: {init_result['entries']:,}")
    print(f"   💾 Size: {init_result['size_kb']:.1f} KB")
    print()
    
    # Step 2: Process some bids and measure timing
    print("2. Processing bids and measuring timing...")
    print("Format: Bid | GET | COMPUTE | PUT | TOTAL | State Size | Entries")
    print("-" * 80)
    
    for i in range(10):
        # Create bid event
        bid_event = {
            "auction": {
                "id": auction_id,
                "seller_id": "seller_simple",
                "reserve": 100.0,
                "type": "Regular"
            },
            "person": {
                "id": f"bidder_simple_{i}",
                "name": f"Simple Bidder {i}",
                "email": f"simple{i}@example.com"
            },
            "bid_amount": 100.0 + (i * 10),
            "bid_time": int(time.time())
        }
        
        # Process bid
        result = query8.process_bid_event(bid_event, auction_id)
        
        if "error" not in result:
            timing = result.get('timing', {})
            get_time = timing.get('get_time', 0)
            compute_time = timing.get('compute_time', 0)
            put_time = timing.get('put_time', 0)
            total_time = timing.get('total_time', 0)
            
            state_size = result.get('state_size_kb', 0)
            entries = result.get('window_statistics', {}).get('total_entries', 0)
            
            print(f"Bid {i+1:2d} | {get_time:.4f}s | {compute_time:.4f}s | {put_time:.4f}s | {total_time:.4f}s | {state_size:.1f}KB | {entries}")
        else:
            print(f"❌ Error in bid {i+1}: {result.get('error')}")
    
    print("-" * 80)
    print("✅ Simple performance test completed!")
    print()
    print("This shows the actual timing for your 12-hour window application:")
    print("- GET: Retrieve 43,200-entry state from Redis")
    print("- COMPUTE: Update window (remove old, add new)")
    print("- PUT: Store updated state back to Redis")
    print("- TOTAL: Complete processing time")
    
    return True

if __name__ == "__main__":
    print("Nexmark Query 8 - Simple Performance Test")
    print("=" * 60)
    print()
    
    if test_simple_performance():
        print("\n🎉 Simple performance test completed!")
        print("You now have the real timing measurements for your 12-hour window.")
    else:
        print("\n❌ Simple performance test failed.")
        sys.exit(1) 