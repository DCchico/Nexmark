import json
import time
import pickle
import logging
import sys
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class NexmarkQuery8Streaming:
    """Nexmark Query 8 - Monitor New Users with 12-hour sliding window"""
    
    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.window_hours = 12
        self.window_seconds = self.window_hours * 3600  # 43,200 seconds
        
        # Initialize Redis client
        try:
            import redis
            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                decode_responses=False  # For pickle serialization
            )
            logger.info(f"Connected to Redis at {redis_host}:{redis_port}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def _get_current_timestamp(self) -> int:
        """Get current timestamp in seconds"""
        return int(time.time())
    
    def _is_within_window(self, timestamp: int, current_time: int) -> bool:
        """Check if timestamp is within the 12-hour window"""
        return current_time - timestamp <= self.window_seconds
    
    def initialize_12_hour_window(self, auction_id: str):
        """Initialize 12-hour window with 43,200 entries - ONE Redis operation"""
        current_time = self._get_current_timestamp()
        window_start = current_time - self.window_seconds
        
        logger.info(f"Initializing 12-hour window for auction {auction_id}")
        logger.info(f"Window: {window_start} to {current_time} ({self.window_seconds:,} seconds)")
        
        # Generate 43,200 entries (one per second)
        window_data = []
        for i in range(self.window_seconds):
            timestamp = window_start + i
            
            # Generate one entry per second (43,200 total)
            entry_data = {
                'id': f'entry_{i}',
                'timestamp': timestamp,
                'value': 100 + (i % 1000),  # Some value for each entry
                'type': 'data' if i % 2 == 0 else 'event'  # Alternate types
            }
            
            window_data.append({
                'type': 'entry',
                'data': entry_data,
                'timestamp': timestamp
            })
        
        # Create the state object
        state = {
            'auction_id': auction_id,
            'window_start': window_start,
            'window_end': current_time,
            'entries': window_data,
            'statistics': {
                'total_entries': len(window_data),
                'data_entries': len([e for e in window_data if e['data']['type'] == 'data']),
                'event_entries': len([e for e in window_data if e['data']['type'] == 'event'])
            }
        }
        
        # Store in ONE Redis operation
        state_key = f"nexmark:window:{auction_id}"
        serialized_state = pickle.dumps(state)
        self.redis_client.set(state_key, serialized_state)
        
        logger.info(f"Stored 12-hour window: {len(window_data):,} entries, {len(serialized_state)/1024:.1f} KB")
        
        return {
            "status": "initialized",
            "auction_id": auction_id,
            "entries": len(window_data),
            "size_kb": len(serialized_state) / 1024,
            "window_start": window_start,
            "window_end": current_time
        }
    
    def process_bid_event(self, bid_data: Dict[str, Any], auction_id: str) -> Dict[str, Any]:
        """Process a bid event - GET state, update window, PUT state back"""
        current_time = self._get_current_timestamp()
        
        # Step 1: GET state from Redis
        get_start = time.time()
        state_key = f"nexmark:window:{auction_id}"
        state_data = self.redis_client.get(state_key)
        get_time = time.time() - get_start
        
        if state_data:
            state = pickle.loads(state_data)
        else:
            # Create new state if not exists
            state = {
                'auction_id': auction_id,
                'window_start': current_time - self.window_seconds,
                'window_end': current_time,
                'entries': [],
                'statistics': {'total_entries': 0, 'persons': 0, 'auctions': 0}
            }
        
        # Step 2: Update window (remove old entries, add new bid)
        compute_start = time.time()
        
        # Remove entries older than 12 hours
        state['entries'] = [e for e in state['entries'] if self._is_within_window(e['timestamp'], current_time)]
        
        # Add new bid entry
        person_data = bid_data.get('person', {})
        bid_entry = {
            'type': 'bid',
            'data': {
                'person_id': person_data.get('id'),
                'person_name': person_data.get('name', 'Unknown'),
                'bid_amount': bid_data.get('bid_amount', 0.0),
                'timestamp': current_time
            },
            'timestamp': current_time
        }
        state['entries'].append(bid_entry)
        
        # Update window timestamps
        state['window_start'] = current_time - self.window_seconds
        state['window_end'] = current_time
        
        # Update statistics
        state['statistics']['total_entries'] = len(state['entries'])
        state['statistics']['data_entries'] = len([e for e in state['entries'] if e['type'] == 'entry' and e['data']['type'] == 'data'])
        state['statistics']['event_entries'] = len([e for e in state['entries'] if e['type'] == 'entry' and e['data']['type'] == 'event'])
        state['statistics']['bids'] = len([e for e in state['entries'] if e['type'] == 'bid'])
        
        compute_time = time.time() - compute_start
        
        # Step 3: PUT state back to Redis
        put_start = time.time()
        serialized_state = pickle.dumps(state)
        self.redis_client.set(state_key, serialized_state)
        put_time = time.time() - put_start
        
        # Monitor new users (Query 8 logic)
        new_users = []
        for entry in state['entries']:
            if entry['type'] == 'person':
                person_id = entry['data']['id']
                # Check if this person is a seller in any auction
                for auction_entry in state['entries']:
                    if auction_entry['type'] == 'auction' and auction_entry['data']['seller_id'] == person_id:
                        new_users.append({
                            'person_id': person_id,
                            'person_name': entry['data']['name'],
                            'auction_id': auction_entry['data']['id'],
                            'reserve': auction_entry['data']['reserve']
                        })
        
        return {
            "query": "Nexmark Query 8 - Monitor New Users",
            "timestamp": current_time,
            "auction_id": auction_id,
            "window_hours": self.window_hours,
            "new_users": new_users,
            "window_statistics": {
                "total_entries": state['statistics']['total_entries'],
                "data_entries": state['statistics']['data_entries'],
                "event_entries": state['statistics']['event_entries'],
                "bids": state['statistics']['bids'],
                "window_start": state['window_start'],
                "window_end": state['window_end']
            },
            "timing": {
                "get_time": get_time,
                "compute_time": compute_time,
                "put_time": put_time,
                "total_time": get_time + compute_time + put_time
            },
            "state_size_kb": len(serialized_state) / 1024
        }
    
    def get_window_status(self, auction_id: str) -> Dict[str, Any]:
        """Get current window status"""
        state_key = f"nexmark:window:{auction_id}"
        state_data = self.redis_client.get(state_key)
        
        if state_data:
            state = pickle.loads(state_data)
            return {
                "status": "exists",
                "auction_id": auction_id,
                "entries": state['statistics']['total_entries'],
                "size_kb": len(state_data) / 1024,
                "window_start": state['window_start'],
                "window_end": state['window_end']
            }
        else:
            return {
                "status": "not_found",
                "auction_id": auction_id
            }

def handle(req, context):
    """OpenFaaS handler function"""
    try:
        # Parse request
        if isinstance(req, bytes):
            req = req.decode('utf-8')
        
        request_data = json.loads(req) if isinstance(req, str) else req
        
        # Get Redis configuration from environment
        redis_host = context.env.get('REDIS_HOST', 'localhost')
        redis_port = int(context.env.get('REDIS_PORT', 6379))
        redis_db = int(context.env.get('REDIS_DB', 0))
        
        # Initialize query processor
        query8 = NexmarkQuery8Streaming(redis_host, redis_port, redis_db)
        
        # Handle different commands
        command = request_data.get('command')
        
        if command == 'initialize':
            auction_id = request_data.get('auction_id', 'default_auction')
            result = query8.initialize_12_hour_window(auction_id)
            return json.dumps(result)
        
        elif command == 'status':
            auction_id = request_data.get('auction_id', 'default_auction')
            result = query8.get_window_status(auction_id)
            return json.dumps(result)
        
        else:
            # Process bid event
            auction_id = request_data.get('auction', {}).get('id')
            if not auction_id:
                return json.dumps({"error": "No auction ID provided"})
            
            result = query8.process_bid_event(request_data, auction_id)
            return json.dumps(result)
    
    except Exception as e:
        logger.error(f"Handler error: {e}")
        return json.dumps({"error": str(e)})
