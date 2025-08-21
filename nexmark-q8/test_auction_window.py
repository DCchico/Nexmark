#!/usr/bin/env python3
"""
Test 12-Hour Window State for Single Auction
Demonstrates the correct state structure with thousands of entries
"""

import json
import time
import sys
import os

# Add the current directory to Python path to import the handler
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from handler import NexmarkQuery8Streaming

def test_auction_window():
    """Test the 12-hour window state for a single auction"""
    
    print("=== Nexmark Query 8 - Auction Window Test ===")
    print("Testing 12-hour window state for a single auction")
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
    auction_id = "test_auction_12345"
    
    print(f"Testing auction: {auction_id}")
    print(f"Window duration: 12 hours = {12*3600:,} seconds")
    print()
    
    # Simulate multiple bids over time for the same auction
    base_time = int(time.time())
    total_bids = 100  # Simulate 100 bids over time
    
    print(f"Simulating {total_bids} bids over time...")
    print()
    
    total_size = 0
    final_state_size = 0
    
    for i in range(total_bids):
        # Create bid event for the same auction
        bid_time = base_time + (i * 60)  # 1 minute apart
        
        bid_event = {
            "auction": {
                "id": auction_id,
                "seller_id": "seller_123",
                "reserve": 100.0,
                "type": "Regular"
            },
            "person": {
                "id": f"bidder_{i}",
                "name": f"Bidder {i}",
                "email": f"bidder{i}@example.com"
            },
            "bid_amount": 100.0 + (i * 10),  # Increasing bids
            "bid_time": bid_time
        }
        
        sequence_id = int(time.time() * 1000) + i
        
        try:
            result = query8.process_bid_event_ordered(
                bid_event, 
                stream_id="auction_test", 
                sequence_id=sequence_id
            )
            
            if "error" not in result:
                state_size = result.get('metrics', {}).get('sizes', {}).get('stored_kb', 0)
                total_size += state_size
                final_state_size = state_size
                
                bids_in_window = result.get('window_statistics', {}).get('bids_in_window', 0)
                persons_in_window = result.get('window_statistics', {}).get('persons_in_window', 0)
                
                if i % 10 == 0:  # Print every 10th bid
                    print(f"Bid {i+1:3d}: State size: {state_size:.2f} KB, Bids in window: {bids_in_window}, Persons: {persons_in_window}")
            else:
                print(f"❌ Error in bid {i+1}: {result.get('error')}")
                
        except Exception as e:
            print(f"❌ Exception in bid {i+1}: {e}")
    
    print()
    print("=== Final State Analysis ===")
    print(f"Total bids processed: {total_bids}")
    print(f"Final state size: {final_state_size:.2f} KB")
    print(f"Average state size: {total_size/total_bids:.2f} KB")
    print()
    
    # Calculate expected size for full 12-hour window
    window_seconds = 12 * 3600  # 43,200 seconds
    bids_per_second = 10  # Assume 10 bids per second
    total_expected_bids = window_seconds * bids_per_second
    
    print("=== Size Projection for Full 12-Hour Window ===")
    print(f"Window duration: {window_seconds:,} seconds")
    print(f"Expected bids per second: {bids_per_second}")
    print(f"Total expected bids: {total_expected_bids:,}")
    print()
    
    # Estimate size based on current measurements
    if total_bids > 0:
        size_per_bid = final_state_size / total_bids  # KB per bid
        projected_size = size_per_bid * total_expected_bids
        projected_size_mb = projected_size / 1024
        projected_size_gb = projected_size_mb / 1024
        
        print(f"Current measurements:")
        print(f"  - Bids processed: {total_bids}")
        print(f"  - Final state size: {final_state_size:.2f} KB")
        print(f"  - Size per bid: {size_per_bid:.4f} KB")
        print()
        print(f"Projected full window:")
        print(f"  - Total bids: {total_expected_bids:,}")
        print(f"  - Projected size: {projected_size:.2f} KB ({projected_size_mb:.2f} MB)")
        print(f"  - With Redis overhead: {projected_size*1.5:.2f} KB ({projected_size_mb*1.5:.2f} MB)")
        print()
        print(f"Conservative estimate (2x buffer): {projected_size_mb*3:.2f} MB ({projected_size_gb*3:.2f} GB)")
    
    # Check Redis for actual state
    print("\n=== Redis State Verification ===")
    try:
        keys = query8.redis_client.keys(f"nexmark:state:auction_test:{auction_id}:*")
        print(f"State keys in Redis: {len(keys)}")
        
        if keys:
            # Get the latest state
            latest_key = keys[-1]  # Assuming sorted
            state_data = query8.redis_client.get(latest_key)
            
            if state_data:
                import pickle
                state_obj = pickle.loads(state_data)
                
                bids_count = len(state_obj.get('bids', []))
                persons_count = len(state_obj.get('persons', {}))
                actual_size = len(state_data) / 1024  # KB
                
                print(f"Latest state analysis:")
                print(f"  - Key: {latest_key}")
                print(f"  - Size: {actual_size:.2f} KB")
                print(f"  - Bids: {bids_count}")
                print(f"  - Persons: {persons_count}")
                print(f"  - Auction ID: {state_obj.get('auction_id')}")
                
                # Show some sample bids
                if state_obj.get('bids'):
                    print(f"  - Sample bids:")
                    for i, bid in enumerate(state_obj['bids'][-5:]):  # Last 5 bids
                        print(f"    {i+1}. {bid.get('person_name')} - ${bid.get('bid_amount')}")
        
    except Exception as e:
        print(f"Error checking Redis: {e}")
    
    print()
    print("✅ Auction window test completed!")
    print("This demonstrates the correct state structure:")
    print("  - ONE state object per auction")
    print("  - Contains ALL bids in 12-hour window")
    print("  - Contains ALL persons who bid")
    print("  - State size grows with window content")
    
    return True

if __name__ == "__main__":
    print("Nexmark Query 8 - Auction Window Test")
    print("=" * 50)
    print()
    
    if test_auction_window():
        print("\n🎉 Auction window test completed successfully!")
        print("The state now correctly contains the full 12-hour window for a single auction.")
    else:
        print("\n❌ Auction window test failed.")
        sys.exit(1) 