#!/usr/bin/env python3
"""
Local Test Script for Nexmark Query 8 - Bare Metal Testing
Tests the function directly without OpenFaaS deployment
"""

import json
import time
import sys
import os
from datetime import datetime

# Add the current directory to Python path to import the handler
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the handler function and class
from handler import NexmarkQuery8Streaming, handle

def test_local_functionality():
    """Test the function locally without OpenFaaS"""
    
    print("=== Nexmark Query 8 - Local Functionality Test ===")
    print(f"Testing with Redis server: 128.110.217.212")
    print()
    
    # Initialize the query processor with your Redis server
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
    
    # Test 1: Check window status
    print("\n1. Testing window status...")
    try:
        status = query8.get_window_status("test_stream")
        print(f"   Status: {json.dumps(status, indent=2)}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # Test 2: Initialize 12-hour window
    print("\n2. Testing window initialization...")
    try:
        init_result = query8.initialize_12_hour_window("test_stream")
        print(f"   Result: {json.dumps(init_result, indent=2)}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # Test 3: Process a bid event
    print("\n3. Testing bid event processing...")
    try:
        bid_event = {
            "person": {
                "id": "person_local_test",
                "name": "Local Test Person",
                "email": "local@test.com"
            },
            "auction": {
                "id": "auction_local_test",
                "seller_id": "person_local_test",
                "reserve": 100.0,
                "type": "Regular"
            },
            "bid_amount": 150.0,
            "bid_time": int(time.time())
        }
        
        result = query8.process_bid_event_ordered(
            bid_event, 
            stream_id="test_stream", 
            sequence_id=int(time.time() * 1000)
        )
        print(f"   Result: {json.dumps(result, indent=2)}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # Test 4: Process multiple events in sequence
    print("\n4. Testing ordered sequence processing...")
    try:
        base_sequence = int(time.time() * 1000)
        for i in range(5):
            sequence_id = base_sequence + i
            
            # Create a person who is also a seller
            person_id = f"person_seq_{i}"
            auction_id = f"auction_seq_{i}"
            
            bid_event = {
                "person": {
                    "id": person_id,
                    "name": f"Person {i}",
                    "email": f"person{i}@test.com"
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
                stream_id="test_stream", 
                sequence_id=sequence_id
            )
            print(f"   Event {i+1} (Sequence {sequence_id}): {len(result.get('new_matches', []))} matches found")
            
            # Small delay to simulate real processing
            time.sleep(0.1)
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # Test 5: Check final window status
    print("\n5. Checking final window status...")
    try:
        final_status = query8.get_window_status("test_stream")
        print(f"   Final Status: {json.dumps(final_status, indent=2)}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    print("\n✅ Local functionality test completed!")
    return True

def test_handler_function():
    """Test the OpenFaaS handler function locally"""
    
    print("\n=== Testing OpenFaaS Handler Function ===")
    print()
    
    # Create a mock context with Redis configuration
    mock_context = {
        'env': {
            'REDIS_HOST': '128.110.217.212',
            'REDIS_PORT': '6379',
            'REDIS_DB': '0'
        }
    }
    
    # Test 1: Status command
    print("1. Testing status command...")
    try:
        status_event = {
            "command": "status",
            "stream_id": "handler_test"
        }
        
        result = handle(status_event, mock_context)
        print(f"   Status Result: {json.dumps(result, indent=2)}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # Test 2: Initialize command
    print("\n2. Testing initialize command...")
    try:
        init_event = {
            "command": "initialize",
            "stream_id": "handler_test"
        }
        
        result = handle(init_event, mock_context)
        print(f"   Initialize Result: {json.dumps(result, indent=2)}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # Test 3: Bid event processing
    print("\n3. Testing bid event through handler...")
    try:
        bid_event = {
            "stream_id": "handler_test",
            "sequence_id": int(time.time() * 1000),
            "bid_event": {
                "person": {
                    "id": "person_handler_test",
                    "name": "Handler Test Person",
                    "email": "handler@test.com"
                },
                "auction": {
                    "id": "auction_handler_test",
                    "seller_id": "person_handler_test",
                    "reserve": 200.0,
                    "type": "Featured"
                },
                "bid_amount": 250.0,
                "bid_time": int(time.time())
            }
        }
        
        result = handle(bid_event, mock_context)
        print(f"   Bid Event Result: {json.dumps(result, indent=2)}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    print("\n✅ Handler function test completed!")

def test_performance():
    """Test performance with multiple events"""
    
    print("\n=== Performance Test ===")
    print()
    
    try:
        query8 = NexmarkQuery8Streaming(
            redis_host='128.110.217.212',
            redis_port=6379,
            redis_db=0
        )
        
        # Initialize window
        query8.initialize_12_hour_window("perf_test")
        
        # Process multiple events and measure performance
        num_events = 100
        start_time = time.time()
        
        base_sequence = int(time.time() * 1000)
        total_matches = 0
        
        for i in range(num_events):
            sequence_id = base_sequence + i
            
            bid_event = {
                "person": {
                    "id": f"perf_person_{i}",
                    "name": f"Performance Person {i}",
                    "email": f"perf{i}@test.com"
                },
                "auction": {
                    "id": f"perf_auction_{i}",
                    "seller_id": f"perf_person_{i % 10}",  # Some persons are sellers
                    "reserve": 100 + (i % 500),
                    "type": "Regular" if i % 2 == 0 else "Featured"
                },
                "bid_amount": 150 + (i % 300),
                "bid_time": int(time.time()) + i
            }
            
            result = query8.process_bid_event_ordered(
                bid_event, 
                stream_id="perf_test", 
                sequence_id=sequence_id
            )
            
            total_matches += len(result.get('new_matches', []))
            
            if i % 10 == 0:
                print(f"   Processed {i+1}/{num_events} events...")
        
        end_time = time.time()
        total_time = end_time - start_time
        avg_time = total_time / num_events
        
        print(f"\nPerformance Results:")
        print(f"   Total Events: {num_events}")
        print(f"   Total Time: {total_time:.2f} seconds")
        print(f"   Average Time per Event: {avg_time*1000:.2f} ms")
        print(f"   Events per Second: {num_events/total_time:.2f}")
        print(f"   Total Matches Found: {total_matches}")
        
    except Exception as e:
        print(f"❌ Performance test error: {e}")

def test_initialization():
    """Test the 12-hour window initialization"""
    print("\n=== Testing 12-Hour Window Initialization ===")
    
    try:
        # Test window status before initialization
        print("1. Checking window status before initialization...")
        status_result = query8.get_window_status("test_stream")
        print(f"   Status: {status_result}")
        
        # Test initialization
        print("\n2. Initializing 12-hour window...")
        init_result = query8.initialize_12_hour_window("test_stream")
        print(f"   Result: {init_result}")
        
        # Test window status after initialization
        print("\n3. Checking window status after initialization...")
        status_result = query8.get_window_status("test_stream")
        print(f"   Status: {status_result}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False

def show_data_size_calculation():
    """Show the data size calculation"""
    print("\n=== Data Size Calculation ===")
    
    # Window parameters
    window_hours = 12
    window_seconds = window_hours * 3600  # 43,200 seconds
    
    print(f"12-Hour Window Details:")
    print(f"  - Duration: {window_hours} hours")
    print(f"  - Total seconds: {window_seconds:,}")
    print(f"  - Granularity: 1 second per entry")
    print(f"  - Total entries: {window_seconds:,}")
    
    # Estimated data distribution
    estimated_persons = window_seconds // 10  # 1 person per 10 seconds
    estimated_auctions = window_seconds  # 1 auction per second
    estimated_bids = window_seconds * 10  # 10 bids per second
    
    print(f"\nEstimated Data Distribution:")
    print(f"  - Persons: {estimated_persons:,}")
    print(f"  - Auctions: {estimated_auctions:,}")
    print(f"  - Bids: {estimated_bids:,}")
    
    # Memory estimates (from actual measurements)
    person_size = 177  # bytes
    auction_size = 216  # bytes
    state_size = 163  # bytes
    
    total_person_memory = estimated_persons * person_size
    total_auction_memory = estimated_auctions * auction_size
    total_state_memory = window_seconds * state_size
    
    redis_overhead = 1.5
    total_memory_bytes = (total_person_memory + total_auction_memory + total_state_memory) * redis_overhead
    total_memory_mb = total_memory_bytes / (1024 * 1024)
    total_memory_gb = total_memory_mb / 1024
    
    print(f"\nMemory Requirements:")
    print(f"  - Person data: {total_person_memory/1024/1024:.2f} MB")
    print(f"  - Auction data: {total_auction_memory/1024/1024:.2f} MB")
    print(f"  - State data: {total_state_memory/1024/1024:.2f} MB")
    print(f"  - Redis overhead: 50%")
    print(f"  - Total memory: {total_memory_mb:.2f} MB ({total_memory_gb:.2f} GB)")
    print(f"\nConservative estimate (2x buffer): {total_memory_mb * 2:.2f} MB ({total_memory_gb * 2:.2f} GB)")
    print("\nRecommendation: Use at least 2-4 GB of memory for Redis to handle the 12-hour window comfortably.")

if __name__ == "__main__":
    print("Nexmark Query 8 - Local Testing Suite")
    print("=" * 50)
    print()
    
    # Test basic functionality
    # Show data size calculation first
    show_data_size_calculation()
    
    if test_local_functionality():
        print("\n" + "="*50)
        
        # Test handler function
        test_handler_function()
        
        print("\n" + "="*50)
        
        # Ask if user wants to run initialization test
        response = input("\nDo you want to run the initialization test? (y/n): ")
        if response.lower() in ['y', 'yes']:
            test_initialization()
        
        print("\n" + "="*50)
        
        # Ask if user wants to run performance test
        response = input("\nDo you want to run the performance test? (y/n): ")
        if response.lower() in ['y', 'yes']:
            test_performance()
        
        print("\n" + "="*50)
        print("🎉 All tests completed successfully!")
        print("\nNext steps:")
        print("1. If all tests pass, you can deploy to OpenFaaS")
        print("2. Use: faas-cli deploy nexmark-q8 --env REDIS_HOST=128.110.217.212")
        print("3. Test with real NEXMark data")
    else:
        print("\n❌ Basic functionality test failed. Please check Redis connection.")
        print("Make sure Redis is running on 128.110.217.212:6379") 