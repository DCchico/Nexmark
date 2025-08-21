#!/usr/bin/env python3
"""
Performance Test for Nexmark Query 8
Measures timing for the three main steps:
1. State retrieval (GET from Redis)
2. Computation (processing and window management)
3. State storage (PUT to Redis)
"""

import json
import time
import sys
import os
import statistics
from typing import List, Dict, Any

# Add the current directory to Python path to import the handler
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from handler import NexmarkQuery8Streaming

class PerformanceMetrics:
    """Collects and analyzes performance metrics"""
    
    def __init__(self):
        self.retrieval_times = []
        self.computation_times = []
        self.storage_times = []
        self.total_times = []
        self.state_sizes = []
        self.bid_counts = []
        self.person_counts = []
    
    def add_measurement(self, result: Dict[str, Any]):
        """Add a measurement from a function result"""
        metrics = result.get('metrics', {})
        timings = metrics.get('timings', {})
        
        # Extract timing data
        retrieval_time = timings.get('retrieval', 0)
        computation_time = timings.get('processing', 0)
        storage_time = timings.get('storage', 0)
        total_time = result.get('processing_time', 0)
        
        # Extract size data
        stored_size = metrics.get('sizes', {}).get('stored_kb', 0)
        
        # Extract window data
        window_stats = result.get('window_statistics', {})
        bid_count = window_stats.get('bids_in_window', 0)
        person_count = window_stats.get('persons_in_window', 0)
        
        # Store measurements
        self.retrieval_times.append(retrieval_time)
        self.computation_times.append(computation_time)
        self.storage_times.append(storage_time)
        self.total_times.append(total_time)
        self.state_sizes.append(stored_size)
        self.bid_counts.append(bid_count)
        self.person_counts.append(person_count)
    
    def print_summary(self):
        """Print comprehensive performance summary"""
        if not self.total_times:
            print("No measurements available")
            return
        
        print("\n" + "="*80)
        print("PERFORMANCE ANALYSIS SUMMARY")
        print("="*80)
        
        # Timing analysis
        print("\n📊 TIMING BREAKDOWN (seconds):")
        print("-" * 50)
        
        steps = [
            ("State Retrieval", self.retrieval_times),
            ("Computation", self.computation_times),
            ("State Storage", self.storage_times),
            ("Total Processing", self.total_times)
        ]
        
        for step_name, times in steps:
            if times:
                avg_time = statistics.mean(times)
                min_time = min(times)
                max_time = max(times)
                median_time = statistics.median(times)
                
                print(f"{step_name:20s}:")
                print(f"  Average: {avg_time:.6f}s")
                print(f"  Median:  {median_time:.6f}s")
                print(f"  Min:     {min_time:.6f}s")
                print(f"  Max:     {max_time:.6f}s")
                print()
        
        # Percentage breakdown
        print("📈 PERCENTAGE BREAKDOWN:")
        print("-" * 30)
        avg_retrieval = statistics.mean(self.retrieval_times) if self.retrieval_times else 0
        avg_computation = statistics.mean(self.computation_times) if self.computation_times else 0
        avg_storage = statistics.mean(self.storage_times) if self.storage_times else 0
        avg_total = statistics.mean(self.total_times) if self.total_times else 0
        
        if avg_total > 0:
            retrieval_pct = (avg_retrieval / avg_total) * 100
            computation_pct = (avg_computation / avg_total) * 100
            storage_pct = (avg_storage / avg_total) * 100
            
            print(f"State Retrieval: {retrieval_pct:.1f}%")
            print(f"Computation:     {computation_pct:.1f}%")
            print(f"State Storage:   {storage_pct:.1f}%")
            print(f"Total:           100.0%")
        
        # State size analysis
        print("\n💾 STATE SIZE ANALYSIS:")
        print("-" * 30)
        if self.state_sizes:
            avg_size = statistics.mean(self.state_sizes)
            min_size = min(self.state_sizes)
            max_size = max(self.state_sizes)
            
            print(f"Average state size: {avg_size:.2f} KB")
            print(f"Min state size:     {min_size:.2f} KB")
            print(f"Max state size:     {max_size:.2f} KB")
            print(f"Size growth:        {max_size - min_size:.2f} KB")
        
        # Window content analysis
        print("\n🪟 WINDOW CONTENT ANALYSIS:")
        print("-" * 35)
        if self.bid_counts:
            avg_bids = statistics.mean(self.bid_counts)
            max_bids = max(self.bid_counts)
            print(f"Average bids in window: {avg_bids:.1f}")
            print(f"Max bids in window:     {max_bids}")
        
        if self.person_counts:
            avg_persons = statistics.mean(self.person_counts)
            max_persons = max(self.person_counts)
            print(f"Average persons in window: {avg_persons:.1f}")
            print(f"Max persons in window:     {max_persons}")
        
        # Performance insights
        print("\n🔍 PERFORMANCE INSIGHTS:")
        print("-" * 25)
        
        if avg_total > 0:
            if avg_retrieval > avg_computation and avg_retrieval > avg_storage:
                print("⚠️  State retrieval is the bottleneck")
            elif avg_computation > avg_retrieval and avg_computation > avg_storage:
                print("⚠️  Computation is the bottleneck")
            elif avg_storage > avg_retrieval and avg_storage > avg_computation:
                print("⚠️  State storage is the bottleneck")
            else:
                print("✅ Processing is well-balanced")
        
        if self.state_sizes and max(self.state_sizes) > 1000:  # > 1MB
            print("⚠️  Large state sizes detected - consider optimization")
        
        if avg_total > 1.0:  # > 1 second
            print("⚠️  Processing time is high - consider optimization")
        elif avg_total < 0.1:  # < 100ms
            print("✅ Processing time is excellent")
        else:
            print("✅ Processing time is acceptable")

def test_performance_breakdown():
    """Test performance breakdown of the three main steps"""
    
    print("=== Nexmark Query 8 - Performance Breakdown Test ===")
    print("Measuring: State Retrieval | Computation | State Storage")
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
    
    # Initialize metrics collector
    metrics = PerformanceMetrics()
    
    # Test parameters
    auction_id = "perf_test_auction"
    num_bids = 50  # Number of bids to process
    warmup_bids = 5  # Warmup bids (discarded from metrics)
    
    print(f"Test configuration:")
    print(f"  - Auction ID: {auction_id}")
    print(f"  - Total bids: {num_bids}")
    print(f"  - Warmup bids: {warmup_bids}")
    print(f"  - Measured bids: {num_bids - warmup_bids}")
    print()
    
    # Process bids and collect metrics
    print("Processing bids and collecting metrics...")
    print()
    
    base_time = int(time.time())
    
    for i in range(num_bids):
        # Create bid event
        bid_time = base_time + (i * 30)  # 30 seconds apart
        
        bid_event = {
            "auction": {
                "id": auction_id,
                "seller_id": "seller_perf",
                "reserve": 100.0,
                "type": "Regular"
            },
            "person": {
                "id": f"bidder_perf_{i}",
                "name": f"Performance Bidder {i}",
                "email": f"perf{i}@example.com"
            },
            "bid_amount": 100.0 + (i * 5),
            "bid_time": bid_time
        }
        
        sequence_id = int(time.time() * 1000) + i
        
        try:
            # Process the bid
            start_time = time.time()
            result = query8.process_bid_event_ordered(
                bid_event, 
                stream_id="performance_test", 
                sequence_id=sequence_id
            )
            end_time = time.time()
            
            # Add actual processing time
            result['processing_time'] = end_time - start_time
            
            if "error" not in result:
                # Collect metrics (skip warmup bids)
                if i >= warmup_bids:
                    metrics.add_measurement(result)
                
                # Print progress
                if i % 10 == 0:
                    retrieval_time = result.get('metrics', {}).get('timings', {}).get('retrieval', 0)
                    computation_time = result.get('metrics', {}).get('timings', {}).get('processing', 0)
                    storage_time = result.get('metrics', {}).get('timings', {}).get('storage', 0)
                    total_time = result.get('processing_time', 0)
                    state_size = result.get('metrics', {}).get('sizes', {}).get('stored_kb', 0)
                    
                    print(f"Bid {i+1:2d}: R={retrieval_time:.4f}s | C={computation_time:.4f}s | S={storage_time:.4f}s | T={total_time:.4f}s | Size={state_size:.1f}KB")
            else:
                print(f"❌ Error in bid {i+1}: {result.get('error')}")
                
        except Exception as e:
            print(f"❌ Exception in bid {i+1}: {e}")
    
    # Print detailed performance summary
    metrics.print_summary()
    
    # Additional analysis
    print("\n" + "="*80)
    print("DETAILED STEP ANALYSIS")
    print("="*80)
    
    # State retrieval analysis
    print("\n🔍 STATE RETRIEVAL ANALYSIS:")
    print("-" * 30)
    if metrics.retrieval_times:
        retrieval_times = metrics.retrieval_times
        print(f"First retrieval:  {retrieval_times[0]:.6f}s (no previous state)")
        print(f"Last retrieval:   {retrieval_times[-1]:.6f}s (with previous state)")
        
        if len(retrieval_times) > 1:
            avg_with_state = statistics.mean(retrieval_times[1:])
            print(f"Average with state: {avg_with_state:.6f}s")
            print(f"Improvement:      {(retrieval_times[0] - avg_with_state):.6f}s")
    
    # Computation analysis
    print("\n🔍 COMPUTATION ANALYSIS:")
    print("-" * 30)
    if metrics.computation_times:
        comp_times = metrics.computation_times
        print(f"First computation: {comp_times[0]:.6f}s (empty window)")
        print(f"Last computation:  {comp_times[-1]:.6f}s (full window)")
        
        if len(comp_times) > 1:
            # Check if computation time grows with window size
            first_half = statistics.mean(comp_times[:len(comp_times)//2])
            second_half = statistics.mean(comp_times[len(comp_times)//2:])
            growth = second_half - first_half
            print(f"Computation growth: {growth:.6f}s")
            
            if growth > 0.001:  # 1ms threshold
                print("⚠️  Computation time increases with window size")
            else:
                print("✅ Computation time is stable")
    
    # Storage analysis
    print("\n🔍 STATE STORAGE ANALYSIS:")
    print("-" * 30)
    if metrics.storage_times and metrics.state_sizes:
        storage_times = metrics.storage_times
        state_sizes = metrics.state_sizes
        
        print(f"First storage:    {storage_times[0]:.6f}s (size: {state_sizes[0]:.1f}KB)")
        print(f"Last storage:     {storage_times[-1]:.6f}s (size: {state_sizes[-1]:.1f}KB)")
        
        # Calculate storage rate
        if len(storage_times) > 1:
            size_growth = state_sizes[-1] - state_sizes[0]
            time_growth = storage_times[-1] - storage_times[0]
            
            if time_growth > 0:
                storage_rate = size_growth / time_growth  # KB/s
                print(f"Storage rate:     {storage_rate:.1f} KB/s")
    
    print("\n✅ Performance breakdown test completed!")
    return True

def test_single_step_performance():
    """Test performance of individual steps in isolation"""
    
    print("\n" + "="*80)
    print("INDIVIDUAL STEP PERFORMANCE TEST")
    print("="*80)
    
    try:
        query8 = NexmarkQuery8Streaming(
            redis_host='128.110.217.212',
            redis_port=6379,
            redis_db=0
        )
        
        # Test state retrieval performance
        print("\n🔍 Testing State Retrieval Performance...")
        retrieval_times = []
        
        for i in range(10):
            start_time = time.time()
            # Try to retrieve a non-existent state
            state, size, details = query8.get_previous_state("test", i, "test_auction")
            end_time = time.time()
            retrieval_times.append(end_time - start_time)
        
        avg_retrieval = statistics.mean(retrieval_times)
        print(f"Average retrieval time (no state): {avg_retrieval:.6f}s")
        
        # Test state storage performance
        print("\n🔍 Testing State Storage Performance...")
        storage_times = []
        
        test_state = {
            'auction_id': 'test_storage',
            'bids': [{'bid_amount': 100, 'timestamp': time.time()}] * 1000,  # 1000 bids
            'persons': {'test_person': {'id': 'test', 'name': 'Test'}},
            'statistics': {'total_bids': 1000, 'total_persons': 1},
            'window_start': time.time() - 43200,
            'window_end': time.time()
        }
        
        for i in range(10):
            start_time = time.time()
            success, size, details = query8.store_state("test", i, test_state, "test_storage")
            end_time = time.time()
            storage_times.append(end_time - start_time)
        
        avg_storage = statistics.mean(storage_times)
        print(f"Average storage time (1000 bids): {avg_storage:.6f}s")
        print(f"State size: {len(test_state.get('bids', []))} bids = {size/1024:.1f}KB")
        
    except Exception as e:
        print(f"❌ Error in individual step test: {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("Nexmark Query 8 - Performance Breakdown Test")
    print("=" * 80)
    print()
    
    success = True
    
    # Run main performance test
    if test_performance_breakdown():
        print("\n✅ Main performance test completed")
    else:
        print("\n❌ Main performance test failed")
        success = False
    
    # Run individual step test
    if test_single_step_performance():
        print("\n✅ Individual step test completed")
    else:
        print("\n❌ Individual step test failed")
        success = False
    
    if success:
        print("\n🎉 All performance tests completed successfully!")
        print("\nKey insights:")
        print("- State retrieval time (Redis GET)")
        print("- Computation time (window processing)")
        print("- State storage time (Redis PUT)")
        print("- Performance bottlenecks and optimization opportunities")
    else:
        print("\n❌ Some performance tests failed.")
        sys.exit(1) 