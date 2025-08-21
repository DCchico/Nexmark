#!/usr/bin/env python3
"""
Data Size Calculator for Nexmark Query 8 - 12-Hour Window
Calculates memory requirements for a full 12-hour window with 1-second granularity
"""

import sys
import pickle
import time
from datetime import datetime, timedelta
import json

def calculate_window_size():
    """Calculate the size of a 12-hour window with 1-second granularity"""
    
    # Window parameters
    window_hours = 12
    window_seconds = window_hours * 3600  # 43,200 seconds
    granularity_seconds = 1
    
    print("=== Nexmark Query 8 - Data Size Calculator ===")
    print(f"Window Duration: {window_hours} hours")
    print(f"Total Seconds: {window_seconds:,}")
    print(f"Granularity: {granularity_seconds} second")
    print(f"Total Entries: {window_seconds:,}")
    print()
    
    # Sample data structures
    sample_person = {
        'id': 'person_12345',
        'name': 'John Doe',
        'email': 'john.doe@example.com',
        'timestamp': int(time.time()),
        'region': 'US',
        'income': 75000,
        'interests': ['electronics', 'sports', 'books']
    }
    
    sample_auction = {
        'id': 'auction_67890',
        'seller_id': 'person_12345',
        'reserve': 150.0,
        'type': 'Regular',
        'timestamp': int(time.time()),
        'category': 'electronics',
        'description': 'High-quality laptop for sale',
        'quantity': 1,
        'location': 'US'
    }
    
    sample_state = {
        'persons': {},
        'auctions': {},
        'matches': [],
        'statistics': {
            'total_persons': 0,
            'total_auctions': 0,
            'total_matches': 0
        },
        'window_start': int(time.time()) - window_seconds,
        'window_end': int(time.time())
    }
    
    # Calculate sizes
    person_size = len(pickle.dumps(sample_person))
    auction_size = len(pickle.dumps(sample_auction))
    state_size = len(pickle.dumps(sample_state))
    
    print("=== Sample Data Sizes ===")
    print(f"Person Entry: {person_size:,} bytes ({person_size/1024:.2f} KB)")
    print(f"Auction Entry: {auction_size:,} bytes ({auction_size/1024:.2f} KB)")
    print(f"State Object: {state_size:,} bytes ({state_size/1024:.2f} KB)")
    print()
    
    # Estimate data distribution
    # Based on NEXMark generator: ~1 person per 10 calls, ~1 auction per call, ~10 bids per call
    total_calls = window_seconds
    estimated_persons = total_calls // 10  # 1 person per 10 seconds
    estimated_auctions = total_calls  # 1 auction per second
    estimated_bids = total_calls * 10  # 10 bids per second
    
    print("=== Estimated Data Distribution ===")
    print(f"Total Time Window: {window_seconds:,} seconds")
    print(f"Estimated Persons: {estimated_persons:,}")
    print(f"Estimated Auctions: {estimated_auctions:,}")
    print(f"Estimated Bids: {estimated_bids:,}")
    print()
    
    # Calculate total memory requirements
    total_person_memory = estimated_persons * person_size
    total_auction_memory = estimated_auctions * auction_size
    total_state_memory = window_seconds * state_size  # One state per second
    
    # Redis overhead (approximately 50% for keys, metadata, etc.)
    redis_overhead = 1.5
    
    total_memory_bytes = (total_person_memory + total_auction_memory + total_state_memory) * redis_overhead
    total_memory_mb = total_memory_bytes / (1024 * 1024)
    total_memory_gb = total_memory_mb / 1024
    
    print("=== Memory Requirements ===")
    print(f"Person Data: {total_person_memory:,} bytes ({total_person_memory/1024/1024:.2f} MB)")
    print(f"Auction Data: {total_auction_memory:,} bytes ({total_auction_memory/1024/1024:.2f} MB)")
    print(f"State Data: {total_state_memory:,} bytes ({total_state_memory/1024/1024:.2f} MB)")
    print(f"Redis Overhead: 50%")
    print(f"Total Memory: {total_memory_bytes:,} bytes ({total_memory_mb:.2f} MB, {total_memory_gb:.2f} GB)")
    print()
    
    # Alternative scenarios
    print("=== Alternative Scenarios ===")
    
    # High activity scenario (10x more data)
    high_activity_memory = total_memory_bytes * 10
    print(f"High Activity (10x): {high_activity_memory/1024/1024:.2f} MB ({high_activity_memory/1024/1024/1024:.2f} GB)")
    
    # Low activity scenario (0.1x data)
    low_activity_memory = total_memory_bytes * 0.1
    print(f"Low Activity (0.1x): {low_activity_memory/1024/1024:.2f} MB")
    
    # Conservative estimate (2x buffer)
    conservative_memory = total_memory_bytes * 2
    print(f"Conservative (2x buffer): {conservative_memory/1024/1024:.2f} MB ({conservative_memory/1024/1024/1024:.2f} GB)")
    print()
    
    # Performance considerations
    print("=== Performance Considerations ===")
    print(f"Redis Operations per Second: ~{estimated_bids:,}")
    print(f"State Retrievals per Second: ~{estimated_bids:,}")
    print(f"State Stores per Second: ~{estimated_bids:,}")
    print(f"Memory Bandwidth: ~{total_memory_bytes/3600/1024/1024:.2f} MB/s (hourly)")
    print()
    
    return {
        'window_seconds': window_seconds,
        'estimated_persons': estimated_persons,
        'estimated_auctions': estimated_auctions,
        'estimated_bids': estimated_bids,
        'total_memory_mb': total_memory_mb,
        'total_memory_gb': total_memory_gb,
        'conservative_memory_gb': conservative_memory/1024/1024/1024
    }

def generate_initial_window_data():
    """Generate sample data for initial 12-hour window"""
    
    window_seconds = 12 * 3600
    current_time = int(time.time())
    
    print("=== Generating Initial Window Data ===")
    print(f"Creating {window_seconds:,} seconds of initial data...")
    
    initial_data = []
    
    # Generate data for each second in the window
    for i in range(window_seconds):
        timestamp = current_time - window_seconds + i
        
        # Create some sample data for this timestamp
        if i % 10 == 0:  # Person every 10 seconds
            person_data = {
                'id': f'person_{i}',
                'name': f'Person {i}',
                'email': f'person{i}@example.com',
                'timestamp': timestamp
            }
            initial_data.append({
                'type': 'person',
                'data': person_data,
                'timestamp': timestamp
            })
        
        if i % 5 == 0:  # Auction every 5 seconds
            auction_data = {
                'id': f'auction_{i}',
                'seller_id': f'person_{i//10 * 10}',  # Link to a person
                'reserve': 100 + (i % 500),
                'type': 'Regular' if i % 2 == 0 else 'Featured',
                'timestamp': timestamp
            }
            initial_data.append({
                'type': 'auction',
                'data': auction_data,
                'timestamp': timestamp
            })
    
    print(f"Generated {len(initial_data):,} initial data entries")
    
    # Calculate size of initial data
    total_size = 0
    for entry in initial_data:
        total_size += len(pickle.dumps(entry))
    
    print(f"Initial data size: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")
    
    return initial_data, total_size

if __name__ == "__main__":
    # Calculate window size
    results = calculate_window_size()
    
    # Generate initial data
    initial_data, initial_size = generate_initial_window_data()
    
    print("=== Summary ===")
    print(f"12-hour window with 1-second granularity: {results['window_seconds']:,} entries")
    print(f"Estimated memory usage: {results['total_memory_mb']:.2f} MB ({results['total_memory_gb']:.2f} GB)")
    print(f"Conservative estimate: {results['conservative_memory_gb']:.2f} GB")
    print(f"Initial data size: {initial_size/1024/1024:.2f} MB")
    print()
    print("Recommendation: Use at least 2-4 GB of memory for Redis to handle the 12-hour window comfortably.") 