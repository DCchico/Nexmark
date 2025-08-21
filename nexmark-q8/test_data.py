#!/usr/bin/env python3
"""
Test data generator for Nexmark Query 8 - Streaming Implementation
Generates sample XML auction data and bid events compatible with NEXMark format
"""

import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import random
import time
import json

def generate_test_xml(num_auctions=5, num_bids=20, num_persons=10):
    """Generate test XML data in NEXMark format with seller information"""
    
    # Create root element
    root = ET.Element("site")
    
    # Generate persons
    for i in range(num_persons):
        person = ET.SubElement(root, "person")
        person.set("id", f"person_{i}")
        
        # Add person details
        name = ET.SubElement(person, "name")
        name.text = f"Person {i}"
        
        email = ET.SubElement(person, "emailaddress")
        email.text = f"person{i}@example.com"
    
    # Generate open auctions with seller information
    open_auctions = ET.SubElement(root, "open_auctions")
    
    current_time = int(time.time())
    
    for i in range(num_auctions):
        auction = ET.SubElement(open_auctions, "open_auction")
        auction.set("id", f"auction_{i}")
        
        # Add auction details
        interval = ET.SubElement(auction, "interval")
        start = ET.SubElement(interval, "start")
        start.text = str(current_time - random.randint(0, 3600))  # Started within last hour
        
        end = ET.SubElement(interval, "end")
        end.text = str(current_time + random.randint(3600, 86400))  # Ends within next day
        
        type_elem = ET.SubElement(auction, "type")
        type_elem.text = random.choice(["Regular", "Featured"])
        
        # Add seller information (this is crucial for Query 8)
        seller = ET.SubElement(auction, "seller")
        person_ref = ET.SubElement(seller, "person_ref")
        seller_id = f"person_{random.randint(0, num_persons-1)}"
        person_ref.set("person", seller_id)
        
        # Add reserve price (important for Query 8)
        if random.choice([True, False]):  # 50% chance of having reserve
            reserve = ET.SubElement(auction, "reserve")
            reserve.text = str(random.randint(50, 1000))
        
        # Add some bids to this auction
        for j in range(random.randint(1, 5)):
            bidder = ET.SubElement(auction, "bidder")
            
            time_elem = ET.SubElement(bidder, "time")
            time_elem.text = str(current_time - random.randint(0, 1800))  # Bid within last 30 min
            
            person_ref = ET.SubElement(bidder, "person_ref")
            person_ref.set("person", f"person_{random.randint(0, num_persons-1)}")
            
            bid = ET.SubElement(bidder, "bid")
            bid.text = f"{random.randint(10, 500)}.00"
    
    # Generate additional bids
    for i in range(num_bids):
        auction_id = random.randint(0, num_auctions-1)
        auction = open_auctions[auction_id]
        
        bidder = ET.SubElement(auction, "bidder")
        
        time_elem = ET.SubElement(bidder, "time")
        time_elem.text = str(current_time - random.randint(0, 1800))
        
        person_ref = ET.SubElement(bidder, "person_ref")
        person_ref.set("person", f"person_{random.randint(0, num_persons-1)}")
        
        bid = ET.SubElement(bidder, "bid")
        bid.text = f"{random.randint(10, 500)}.00"
    
    return ET.tostring(root, encoding='unicode')

def generate_bid_event(person_id=None, auction_id=None, sequence_id=None, stream_id="bid_stream"):
    """Generate a single bid event for testing with ordered processing"""
    current_time = int(time.time())
    
    if person_id is None:
        person_id = f"person_{random.randint(0, 20)}"
    
    if auction_id is None:
        auction_id = f"auction_{random.randint(0, 10)}"
    
    if sequence_id is None:
        sequence_id = int(current_time * 1000) + random.randint(0, 999)  # Millisecond timestamp + random offset
    
    # Generate person data
    person_data = {
        "id": person_id,
        "name": f"Person {person_id.split('_')[1]}",
        "email": f"{person_id}@example.com",
        "timestamp": current_time
    }
    
    # Generate auction data with seller
    seller_id = f"person_{random.randint(0, 20)}"
    auction_data = {
        "id": auction_id,
        "seller_id": seller_id,
        "reserve": random.randint(50, 1000) if random.choice([True, False]) else None,
        "type": random.choice(["Regular", "Featured"]),
        "timestamp": current_time
    }
    
    bid_event = {
        "stream_id": stream_id,
        "sequence_id": sequence_id,
        "bid_event": {
            "person": person_data,
            "auction": auction_data,
            "bid_amount": random.randint(10, 500),
            "bid_time": current_time
        }
    }
    
    return bid_event

def generate_sample_data():
    """Generate and return sample data for testing"""
    xml_data = generate_test_xml(num_auctions=10, num_bids=50, num_persons=20)
    
    # Create test event for XML processing
    xml_event = {
        "xml_data": xml_data
    }
    
    # Create test event for bid processing
    bid_event = generate_bid_event()
    
    return {
        "xml_event": xml_event,
        "bid_event": bid_event
    }

def generate_streaming_test_sequence():
    """Generate a sequence of bid events to test streaming functionality with ordered processing"""
    events = []
    base_sequence = int(time.time() * 1000)
    
    # Generate a sequence of bid events with some matching person-seller relationships
    for i in range(10):
        # Create some persons who are also sellers
        person_id = f"person_{i}"
        auction_id = f"auction_{i}"
        sequence_id = base_sequence + i
        
        # First event: person becomes a seller
        seller_event = generate_bid_event(person_id, auction_id, sequence_id)
        seller_event["bid_event"]["auction"]["seller_id"] = person_id  # Person is seller
        events.append(seller_event)
        
        # Second event: someone bids on that auction
        bid_event = generate_bid_event(f"bidder_{i}", auction_id, sequence_id + 1)
        events.append(bid_event)
    
    return events

def generate_ordered_test_sequence(num_events=20):
    """Generate a sequence of ordered bid events for testing in-order processing"""
    events = []
    base_sequence = int(time.time() * 1000)
    stream_id = "test_stream"
    
    for i in range(num_events):
        sequence_id = base_sequence + i
        
        # Create events with increasing sequence IDs
        event = generate_bid_event(
            person_id=f"person_{i % 5}",  # Cycle through 5 persons
            auction_id=f"auction_{i % 3}",  # Cycle through 3 auctions
            sequence_id=sequence_id,
            stream_id=stream_id
        )
        
        # Make some persons sellers of their own auctions
        if i % 3 == 0:  # Every 3rd event
            event["bid_event"]["auction"]["seller_id"] = event["bid_event"]["person"]["id"]
        
        events.append(event)
    
    return events

if __name__ == "__main__":
    print("Nexmark Query 8 Test Data Generator")
    print("=" * 50)
    
    # Generate sample XML
    print("\n1. Sample NEXMark XML data:")
    print("-" * 30)
    xml_data = generate_test_xml()
    print(xml_data[:500] + "..." if len(xml_data) > 500 else xml_data)
    
    # Generate sample bid event
    print("\n2. Sample bid event (with ordered processing):")
    print("-" * 30)
    bid_event = generate_bid_event()
    print(json.dumps(bid_event, indent=2))
    
    # Generate streaming test sequence
    print("\n3. Streaming test sequence (first 2 events):")
    print("-" * 30)
    events = generate_streaming_test_sequence()
    for i, event in enumerate(events[:2]):
        print(f"Event {i+1}:")
        print(json.dumps(event, indent=2))
        print()
    
    # Generate ordered test sequence
    print("\n4. Ordered test sequence (first 3 events):")
    print("-" * 30)
    ordered_events = generate_ordered_test_sequence(5)
    for i, event in enumerate(ordered_events[:3]):
        print(f"Ordered Event {i+1} (Sequence {event['sequence_id']}):")
        print(json.dumps(event, indent=2))
        print()
    
    print("\n5. Usage examples:")
    print("-" * 30)
    print("# Test ordered bid event processing:")
    print("echo '{\"stream_id\": \"test_stream\", \"sequence_id\": 123456, \"bid_event\": {...}}' | faas-cli invoke nexmark-q8")
    print()
    print("# Test XML processing:")
    print("echo '{\"xml_data\": \"<?xml version=\\\"1.0\\\"?><site>...</site>\"}' | faas-cli invoke nexmark-q8")
    print()
    print("# Test with Redis (make sure Redis is running):")
    print("docker run -d -p 6379:6379 redis:alpine")
    print("echo '{\"stream_id\": \"test_stream\", \"sequence_id\": 123456, \"bid_event\": {...}}' | faas-cli invoke nexmark-q8")
    print()
    print("# Test ordered sequence processing:")
    print("python -c \"import test_data; import json; events = test_data.generate_ordered_test_sequence(5); [print(json.dumps(e)) for e in events]\" | while read line; do echo \"$line\" | faas-cli invoke nexmark-q8; done") 