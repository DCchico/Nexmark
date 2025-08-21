#!/usr/bin/env python3
"""
Measure actual state sizes for Nexmark Query 8
Compare with calculator predictions
"""

import pickle
import time
import sys
import os

# Add the current directory to Python path to import the handler
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from handler import NexmarkQuery8Streaming

def measure_state_sizes():
    """Measure actual state sizes"""
    
    print("=== Nexmark Query 8 - State Size Measurement ===")
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
    
    # Create sample data structures
    current_time = int(time.time())
    
    # Sample person
    sample_person = {
        'id': 'person_12345',
        'name': 'John Doe',
        'email': 'john.doe@example.com',
        'timestamp': current_time,
        'region': 'US',
        'income': 75000,
        'interests': ['electronics', 'sports', 'books']
    }
    
    # Sample auction
    sample_auction = {
        'id': 'auction_67890',
        'seller_id': 'person_12345',
        'reserve': 150.0,
        'type': 'Regular',
        'timestamp': current_time,
        'category': 'electronics',
        'description': 'High-quality laptop for sale',
        'quantity': 1,
        'location': 'US'
    }
    
    # Sample state object (empty)
    empty_state = {
        'persons': {},
        'auctions': {},
        'matches': [],
        'statistics': {
            'total_persons': 0,
            'total_auctions': 0,
            'total_matches': 0
        },
        'window_start': current_time - 43200,  # 12 hours ago
        'window_end': current_time
    }
    
    # Sample state object (with data)
    populated_state = {
        'persons': {
            'person_1': sample_person,
            'person_2': {**sample_person, 'id': 'person_2', 'name': 'Jane Doe'},
            'person_3': {**sample_person, 'id': 'person_3', 'name': 'Bob Smith'}
        },
        'auctions': {
            'auction_1': sample_auction,
            'auction_2': {**sample_auction, 'id': 'auction_2', 'reserve': 200.0},
            'auction_3': {**sample_auction, 'id': 'auction_3', 'reserve': 300.0}
        },
        'matches': [
            {
                'person_id': 'person_1',
                'person_name': 'John Doe',
                'auction_id': 'auction_1',
                'reserve': 150.0,
                'person_timestamp': current_time,
                'auction_timestamp': current_time
            }
        ],
        'statistics': {
            'total_persons': 3,
            'total_auctions': 3,
            'total_matches': 1
        },
        'window_start': current_time - 43200,
        'window_end': current_time
    }
    
    # Measure sizes
    person_size = len(pickle.dumps(sample_person))
    auction_size = len(pickle.dumps(sample_auction))
    empty_state_size = len(pickle.dumps(empty_state))
    populated_state_size = len(pickle.dumps(populated_state))
    
    print("=== Actual Measurements ===")
    print(f"Person Entry: {person_size:,} bytes ({person_size/1024:.2f} KB)")
    print(f"Auction Entry: {auction_size:,} bytes ({auction_size/1024:.2f} KB)")
    print(f"Empty State: {empty_state_size:,} bytes ({empty_state_size/1024:.2f} KB)")
    print(f"Populated State (3 persons, 3 auctions, 1 match): {populated_state_size:,} bytes ({populated_state_size/1024:.2f} KB)")
    print()
    
    # Calculate growth per entry
    state_growth = populated_state_size - empty_state_size
    avg_person_contribution = state_growth / 3  # 3 persons added
    avg_auction_contribution = state_growth / 3  # 3 auctions added
    
    print("=== Growth Analysis ===")
    print(f"State growth with 3 persons + 3 auctions: {state_growth:,} bytes")
    print(f"Average contribution per person: {avg_person_contribution:.0f} bytes")
    print(f"Average contribution per auction: {avg_auction_contribution:.0f} bytes")
    print()
    
    # Compare with calculator predictions
    print("=== Comparison with Calculator ===")
    calculator_person = 177
    calculator_auction = 216
    calculator_state = 163
    
    print(f"Calculator vs Actual:")
    print(f"  Person: {calculator_person} vs {person_size} bytes (diff: {person_size - calculator_person:+d})")
    print(f"  Auction: {calculator_auction} vs {auction_size} bytes (diff: {auction_size - calculator_auction:+d})")
    print(f"  Empty State: {calculator_state} vs {empty_state_size} bytes (diff: {empty_state_size - calculator_state:+d})")
    print()
    
    # Test actual state from Redis
    print("=== Testing Actual State from Redis ===")
    try:
        # Process a test event to create a real state
        test_event = {
            "person": sample_person,
            "auction": sample_auction,
            "bid_amount": 150.0,
            "bid_time": current_time
        }
        
        result = query8.process_bid_event_ordered(
            test_event, 
            stream_id="size_test", 
            sequence_id=int(time.time() * 1000)
        )
        
        stored_size = result.get('metrics', {}).get('sizes', {}).get('stored_kb', 0) * 1024  # Convert KB to bytes
        print(f"Actual stored state size: {stored_size:.0f} bytes ({stored_size/1024:.2f} KB)")
        
        # Check Redis directly
        keys = query8.redis_client.keys("nexmark:state:size_test:*")
        if keys:
            sample_key = keys[0]
            sample_data = query8.redis_client.get(sample_key)
            if sample_data:
                actual_size = len(sample_data)
                print(f"Redis stored size: {actual_size:,} bytes ({actual_size/1024:.2f} KB)")
                
                # Try to deserialize and analyze
                try:
                    state_obj = pickle.loads(sample_data)
                    persons_count = len(state_obj.get('persons', {}))
                    auctions_count = len(state_obj.get('auctions', {}))
                    matches_count = len(state_obj.get('matches', []))
                    print(f"State contents: {persons_count} persons, {auctions_count} auctions, {matches_count} matches")
                except Exception as e:
                    print(f"Could not deserialize: {e}")
        
    except Exception as e:
        print(f"Error testing actual state: {e}")
    
    print()
    print("=== Conclusion ===")
    print("The state size discrepancy is likely due to:")
    print("1. Different data structures in actual vs calculator")
    print("2. Pickle serialization overhead")
    print("3. Dynamic state evolution during processing")
    print("4. Additional metadata in real state objects")
    
    return True

if __name__ == "__main__":
    print("Nexmark Query 8 - State Size Measurement")
    print("=" * 50)
    print()
    
    if measure_state_sizes():
        print("\n✅ State size measurement completed!")
    else:
        print("\n❌ State size measurement failed.")
        sys.exit(1) 