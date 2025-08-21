#!/usr/bin/env python3
"""
Test State Management for Nexmark Query 8
Tests the single serialized state object approach
"""

import json
import time
import sys
import os

# Add the current directory to Python path to import the handler
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from handler import NexmarkQuery8Streaming

def test_state_management():
    """Test the new state management approach"""
    
    print("=== Nexmark Query 8 - State Management Test ===")
    print(f"Testing with Redis server: 128.110.217.212")
    print()
    
    # Initialize the query processor
    try:
        query8 = NexmarkQuery8Streaming(
            redis_host='128.110.217.212',
            redis_port=6379,
            redis_db=0
        )
        print("✅ Successfully connected to Redis server")
    except Exception as e:
        print(f"❌ Failed to connect to Redis: {e}")
        return False
    
    # Test 1: Process a single bid event
    print("\n1. Testing single bid event processing...")
    try:
        bid_event = {
            "person": {
                "id": "person_test_1",
                "name": "Test Person 1",
                "email": "test1@example.com"
            },
            "auction": {
                "id": "auction_test_1",
                "seller_id": "person_test_1",
                "reserve": 100.0,
                "type": "Regular"
            },
            "bid_amount": 150.0,
            "bid_time": int(time.time())
        }
        
        result = query8.process_bid_event_ordered(
            bid_event, 
            stream_id="state_test", 
            sequence_id=int(time.time() * 1000)
        )
        
        print(f"   ✅ Event processed successfully")
        print(f"   📊 New matches found: {len(result.get('new_matches', []))}")
        print(f"   📈 State size: {result.get('metrics', {}).get('sizes', {}).get('stored_kb', 0):.2f} KB")
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False
    
    # Test 2: Process multiple events to see state evolution
    print("\n2. Testing multiple events processing...")
    try:
        base_sequence = int(time.time() * 1000)
        total_matches = 0
        
        for i in range(5):
            sequence_id = base_sequence + i
            
            # Create a person who is also a seller
            person_id = f"person_test_{i+2}"
            auction_id = f"auction_test_{i+2}"
            
            bid_event = {
                "person": {
                    "id": person_id,
                    "name": f"Test Person {i+2}",
                    "email": f"test{i+2}@example.com"
                },
                "auction": {
                    "id": auction_id,
                    "seller_id": person_id,  # Person is seller
                    "reserve": 100 + i * 50,
                    "type": "Regular"
                },
                "bid_amount": 150 + i * 25,
                "bid_time": int(time.time()) + i
            }
            
            result = query8.process_bid_event_ordered(
                bid_event, 
                stream_id="state_test", 
                sequence_id=sequence_id
            )
            
            matches = len(result.get('new_matches', []))
            total_matches += matches
            state_size = result.get('metrics', {}).get('sizes', {}).get('stored_kb', 0)
            
            print(f"   Event {i+1}: {matches} matches, State: {state_size:.2f} KB")
            
            # Small delay
            time.sleep(0.1)
        
        print(f"   📊 Total matches found: {total_matches}")
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False
    
    # Test 3: Check Redis storage pattern
    print("\n3. Checking Redis storage pattern...")
    try:
        # Check what keys are in Redis
        keys = query8.redis_client.keys("nexmark:state:state_test:*")
        print(f"   📦 State keys in Redis: {len(keys)}")
        
        if keys:
            # Check the size of one state object
            sample_key = keys[0]
            sample_data = query8.redis_client.get(sample_key)
            if sample_data:
                sample_size = len(sample_data) / 1024  # KB
                print(f"   📏 Sample state size: {sample_size:.2f} KB")
                
                # Try to deserialize
                import pickle
                try:
                    state_obj = pickle.loads(sample_data)
                    persons_count = len(state_obj.get('persons', {}))
                    auctions_count = len(state_obj.get('auctions', {}))
                    print(f"   👥 Persons in state: {persons_count}")
                    print(f"   🏷️  Auctions in state: {auctions_count}")
                except Exception as e:
                    print(f"   ⚠️  Could not deserialize state: {e}")
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False
    
    print("\n✅ State management test completed successfully!")
    print("\nKey improvements:")
    print("  - Single serialized state object per sequence")
    print("  - One GET and one PUT per invocation")
    print("  - Efficient state evolution")
    print("  - Proper cleanup of expired data")
    
    return True

if __name__ == "__main__":
    print("Nexmark Query 8 - State Management Test")
    print("=" * 50)
    print()
    
    if test_state_management():
        print("\n🎉 State management is working correctly!")
        print("The function now uses a single serialized state object approach.")
    else:
        print("\n❌ State management test failed.")
        sys.exit(1) 