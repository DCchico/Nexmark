#!/usr/bin/env python3
"""
Simple Performance Test for Nexmark Query 8
Calls the original function with 12-hour window and shows real timing
"""

import json
import time
import sys
import os

# Add the current directory to Python path to import the handler
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from handler import NexmarkQuery8Streaming

def test_real_performance():
    """Test real performance with 12-hour window"""
    
    print("=== Nexmark Query 8 - Real Performance Test ===")
    print("Testing with 12-hour window (43,200 seconds)")
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
    
    # Initialize 12-hour window first
    print("Initializing 12-hour window...")
    init_result = query8.initialize_12_hour_window("real_test")
    print(f"Initialization result: {init_result}")
    print()
    
    # Test parameters for real scale
    auction_id = "real_auction_12345"
    bids_per_second = 10  # Realistic rate
    test_duration_seconds = 60  # Test for 1 minute
    
    print(f"Test configuration:")
    print(f"  - Auction ID: {auction_id}")
    print(f"  - Bids per second: {bids_per_second}")
    print(f"  - Test duration: {test_duration_seconds} seconds")
    print(f"  - Total bids: {bids_per_second * test_duration_seconds}")
    print()
    
    # Process bids at realistic rate
    print("Processing bids at realistic rate...")
    print("Format: Bid | Retrieval | Computation | Storage | Total | State Size | Bids in Window")
    print("-" * 100)
    
    base_time = int(time.time())
    total_bids = bids_per_second * test_duration_seconds
    
    for i in range(total_bids):
        # Create bid event
        bid_time = base_time + (i // bids_per_second)  # 1 second per bid
        
        bid_event = {
            "auction": {
                "id": auction_id,
                "seller_id": "seller_real",
                "reserve": 100.0,
                "type": "Regular"
            },
            "person": {
                "id": f"bidder_real_{i}",
                "name": f"Real Bidder {i}",
                "email": f"real{i}@example.com"
            },
            "bid_amount": 100.0 + (i * 0.1),
            "bid_time": bid_time
        }
        
        sequence_id = int(time.time() * 1000) + i
        
        try:
            result = query8.process_bid_event_ordered(
                bid_event, 
                stream_id="real_test", 
                sequence_id=sequence_id
            )
            
            if "error" not in result:
                # Extract timing from the handler's built-in metrics
                metrics = result.get('metrics', {})
                timings = metrics.get('timings', {})
                
                retrieval_time = timings.get('retrieval', 0)
                computation_time = timings.get('processing', 0)
                storage_time = timings.get('storage', 0)
                total_time = result.get('processing_time', 0)
                
                # Extract state size and window info
                state_size = metrics.get('sizes', {}).get('stored_kb', 0)
                window_stats = result.get('window_statistics', {})
                bids_in_window = window_stats.get('bids_in_window', 0)
                
                # Print every 10th bid to avoid spam
                if i % 10 == 0:
                    print(f"Bid {i+1:4d} | {retrieval_time:.4f}s | {computation_time:.4f}s | {storage_time:.4f}s | {total_time:.4f}s | {state_size:.1f}KB | {bids_in_window}")
            else:
                print(f"❌ Error in bid {i+1}: {result.get('error')}")
                
        except Exception as e:
            print(f"❌ Exception in bid {i+1}: {e}")
    
    print("-" * 100)
    print("✅ Real performance test completed!")
    print()
    print("This shows the actual timing for:")
    print("- State retrieval (Redis GET) - getting 12-hour window")
    print("- Computation (processing window with 43,200+ entries)")
    print("- State storage (Redis PUT) - storing updated window")
    print("- Total processing time")
    print("- State size growth as window fills")
    
    return True

if __name__ == "__main__":
    print("Nexmark Query 8 - Real Performance Test")
    print("=" * 60)
    print()
    
    if test_real_performance():
        print("\n🎉 Real performance test completed!")
        print("You now have the actual timing measurements for your 12-hour window application.")
    else:
        print("\n❌ Real performance test failed.")
        sys.exit(1) 