#!/usr/bin/env python3
"""
Pure Computation Latency Test for Nexmark Query 8
Tests computation time for single auction with 12-hour window of bids
Matching against a single person (seller)
"""

import time
import random
from typing import List, Dict, Any

def generate_single_auction_bids(num_bids: int = 43200):
    """Generate 12-hour window of bids for a single auction"""
    
    print(f"Generating {num_bids:,} bids for single auction over 12-hour window...")
    
    # Single auction and seller
    auction_id = "test_auction_12345"
    seller_id = "seller_person_123"
    
    bids = []
    current_time = int(time.time())
    window_start = current_time - (12 * 3600)  # 12 hours ago
    
    # Generate bids over the 12-hour window
    for i in range(num_bids):
        timestamp = window_start + (i * 1)  # 1 second intervals
        
        bid = {
            'auction_id': auction_id,
            'seller_id': seller_id,
            'bidder_id': f'bidder_{i}',
            'bidder_name': f'Bidder {i}',
            'bid_amount': 100 + random.randint(0, 1000),
            'timestamp': timestamp
        }
        
        bids.append(bid)
    
    print(f"Generated {len(bids):,} bids for auction {auction_id}")
    print(f"Seller: {seller_id}")
    print(f"Time window: {window_start} to {current_time}")
    print()
    
    return bids, auction_id, seller_id

def compute_query8_single_auction(bids: List[Dict[str, Any]], seller_id: str):
    """Compute Query 8 for single auction: Monitor seller's activity"""
    
    print(f"Computing Query 8: Monitor seller {seller_id}...")
    
    # Find all bids where the bidder is the seller (person bidding on their own auction)
    seller_bids = []
    
    for bid in bids:
        if bid['bidder_id'] == seller_id:
            seller_bid = {
                'auction_id': bid['auction_id'],
                'seller_id': bid['seller_id'],
                'bid_amount': bid['bid_amount'],
                'timestamp': bid['timestamp']
            }
            seller_bids.append(seller_bid)
    
    print(f"Found {len(seller_bids):,} bids where seller is bidding on their own auction")
    return seller_bids

def test_single_auction_computation():
    """Test computation latency for single auction with different bid counts"""
    
    print("=== Nexmark Query 8 - Single Auction Computation Test ===")
    print("Testing computation time for single auction with 12-hour window of bids")
    print()
    
    # Test different bid counts
    bid_counts = [
        1000,      # 1K bids
        5000,      # 5K bids
        10000,     # 10K bids
        25000,     # 25K bids
        43200,     # Full 12-hour window (1 bid/second)
        50000,     # 50K bids
        100000     # 100K bids (stress test)
    ]
    
    results = []
    
    for num_bids in bid_counts:
        print(f"Testing with {num_bids:,} bids...")
        
        # Generate bids
        gen_start = time.time()
        bids, auction_id, seller_id = generate_single_auction_bids(num_bids)
        gen_time = time.time() - gen_start
        
        # Compute results
        compute_start = time.time()
        seller_bids = compute_query8_single_auction(bids, seller_id)
        compute_time = time.time() - compute_start
        
        result = {
            'bids': num_bids,
            'seller_bids': len(seller_bids),
            'gen_time': gen_time,
            'compute_time': compute_time,
            'total_time': gen_time + compute_time
        }
        
        results.append(result)
        
        print(f"  ✅ Generation: {gen_time:.4f}s")
        print(f"  ✅ Computation: {compute_time:.4f}s")
        print(f"  ✅ Total: {result['total_time']:.4f}s")
        print(f"  📊 Seller bids found: {len(seller_bids):,}")
        print()
    
    # Print summary
    print("=" * 80)
    print("SINGLE AUCTION COMPUTATION SUMMARY")
    print("=" * 80)
    print("Bids | Seller Bids | Gen(s) | Compute(s) | Total(s) | Rate(bids/s)")
    print("-" * 80)
    
    for result in results:
        rate = result['bids'] / result['compute_time'] if result['compute_time'] > 0 else 0
        print(f"{result['bids']:5,} | {result['seller_bids']:11,} | {result['gen_time']:6.4f} | {result['compute_time']:9.4f} | {result['total_time']:7.4f} | {rate:10.0f}")
    
    print("-" * 80)
    print()
    
    # Performance analysis
    print("PERFORMANCE ANALYSIS:")
    print("-" * 25)
    
    # Find the full 12-hour window result
    full_window = next((r for r in results if r['bids'] == 43200), None)
    if full_window:
        print(f"Full 12-hour window ({full_window['bids']:,} bids):")
        print(f"  - Generation time: {full_window['gen_time']:.4f}s")
        print(f"  - Computation time: {full_window['compute_time']:.4f}s")
        print(f"  - Total time: {full_window['total_time']:.4f}s")
        print(f"  - Computation rate: {full_window['bids']/full_window['compute_time']:.0f} bids/second")
        print(f"  - Seller bids found: {full_window['seller_bids']:,}")
        print()
    
    # Scaling analysis
    print("Scaling analysis:")
    for i in range(1, len(results)):
        prev = results[i-1]
        curr = results[i]
        
        bids_ratio = curr['bids'] / prev['bids']
        time_ratio = curr['compute_time'] / prev['compute_time']
        
        print(f"  {prev['bids']:,} → {curr['bids']:,} bids: {bids_ratio:.1f}x bids, {time_ratio:.1f}x time")
    
    print()
    print("✅ Single auction computation test completed!")
    print("This shows the pure computation time for processing bids in a 12-hour window.")

if __name__ == "__main__":
    print("Nexmark Query 8 - Single Auction Computation Test")
    print("=" * 60)
    print()
    
    test_single_auction_computation() 