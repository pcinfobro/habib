"""
Kafka Streaming Module
Handles real-time data processing using Kafka
"""

import json
import logging
from typing import Dict, List, Callable, Optional
from datetime import datetime
import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import threading
import time
from dataclasses import dataclass, asdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class StreamingMetrics:
    """Metrics for streaming operations"""
    messages_produced: int = 0
    messages_consumed: int = 0
    processing_errors: int = 0
    last_processed_timestamp: Optional[datetime] = None

class KafkaStreamProcessor:
    """Kafka stream processor for real-time data processing"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.metrics = StreamingMetrics()
        self.running = False
        self.consumer_thread = None
        
        # Initialize Kafka producer
        self.producer = self._create_producer()
        
        # Processing functions registry
        self.processors = {}
    
    def _create_producer(self) -> KafkaProducer:
        """Create Kafka producer"""
        producer_config = {
            'bootstrap_servers': self.config.get('bootstrap_servers', ['localhost:9092']),
            'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
            'key_serializer': lambda k: str(k).encode('utf-8') if k else None,
            'acks': self.config.get('acks', 'all'),
            'retries': self.config.get('retries', 3),
            'batch_size': self.config.get('batch_size', 16384),
            'linger_ms': self.config.get('linger_ms', 10)
        }
        
        return KafkaProducer(**producer_config)
    
    def _create_consumer(self, topics: List[str]) -> KafkaConsumer:
        """Create Kafka consumer"""
        consumer_config = {
            'bootstrap_servers': self.config.get('bootstrap_servers', ['localhost:9092']),
            'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
            'key_deserializer': lambda k: k.decode('utf-8') if k else None,
            'group_id': self.config.get('consumer_group', 'ecommerce-processor'),
            'auto_offset_reset': self.config.get('auto_offset_reset', 'latest'),
            'enable_auto_commit': self.config.get('enable_auto_commit', True),
            'auto_commit_interval_ms': self.config.get('auto_commit_interval_ms', 1000)
        }
        
        return KafkaConsumer(*topics, **consumer_config)
    
    def register_processor(self, topic: str, processor_func: Callable):
        """Register a processing function for a specific topic"""
        self.processors[topic] = processor_func
        self.logger.info(f"Registered processor for topic: {topic}")
    
    def send_batch(self, topic: str, records: List[Dict], key_field: Optional[str] = None):
        """Send a batch of records to Kafka topic"""
        try:
            successful_sends = 0
            
            for record in records:
                try:
                    # Extract key if specified
                    key = record.get(key_field) if key_field else None
                    
                    # Add metadata
                    enriched_record = {
                        **record,
                        'timestamp': datetime.now().isoformat(),
                        'source': 'streamlit-upload',
                        'batch_id': datetime.now().strftime('%Y%m%d_%H%M%S')
                    }
                    
                    future = self.producer.send(topic, value=enriched_record, key=key)
                    # Don't wait for each message individually for better performance
                    successful_sends += 1
                    
                except Exception as e:
                    self.logger.error(f"Failed to send record to {topic}: {e}")
                    self.metrics.processing_errors += 1
            
            # Flush to ensure all messages are sent
            self.producer.flush()
            
            self.metrics.messages_produced += successful_sends
            self.logger.info(f"Successfully sent {successful_sends}/{len(records)} records to {topic}")
            
            return successful_sends
            
        except Exception as e:
            self.logger.error(f"Batch send failed for topic {topic}: {e}")
            return 0

    def produce_message(self, topic: str, message: Dict, key: Optional[str] = None):
        """Produce a message to Kafka topic"""
        try:
            # Add metadata
            enriched_message = {
                **message,
                'timestamp': datetime.now().isoformat(),
                'source': 'data-pipeline'
            }
            
            future = self.producer.send(topic, value=enriched_message, key=key)
            
            # Wait for message to be sent
            record_metadata = future.get(timeout=10)
            
            self.metrics.messages_produced += 1
            self.logger.debug(f"Message sent to {topic}: partition {record_metadata.partition}, offset {record_metadata.offset}")
            
        except KafkaError as e:
            self.logger.error(f"Failed to produce message to {topic}: {str(e)}")
            raise
    
    def produce_batch(self, topic: str, messages: List[Dict], keys: Optional[List[str]] = None):
        """Produce multiple messages to Kafka topic"""
        if keys and len(keys) != len(messages):
            raise ValueError("Number of keys must match number of messages")
        
        for i, message in enumerate(messages):
            key = keys[i] if keys else None
            self.produce_message(topic, message, key)
        
        # Flush to ensure all messages are sent
        self.producer.flush()
        self.logger.info(f"Produced batch of {len(messages)} messages to {topic}")
    
    def start_consumer(self, topics: List[str]):
        """Start consuming messages from specified topics"""
        if self.running:
            self.logger.warning("Consumer is already running")
            return
        
        self.running = True
        self.consumer_thread = threading.Thread(
            target=self._consume_messages,
            args=(topics,),
            daemon=True
        )
        self.consumer_thread.start()
        self.logger.info(f"Started consumer for topics: {topics}")
    
    def stop_consumer(self):
        """Stop the consumer"""
        self.running = False
        if self.consumer_thread:
            self.consumer_thread.join(timeout=5)
        self.logger.info("Consumer stopped")
    
    def _consume_messages(self, topics: List[str]):
        """Internal method to consume messages"""
        consumer = self._create_consumer(topics)
        
        try:
            while self.running:
                message_batch = consumer.poll(timeout_ms=1000)
                
                for topic_partition, messages in message_batch.items():
                    topic = topic_partition.topic
                    
                    for message in messages:
                        try:
                            self._process_message(topic, message.value, message.key)
                            self.metrics.messages_consumed += 1
                            self.metrics.last_processed_timestamp = datetime.now()
                            
                        except Exception as e:
                            self.logger.error(f"Error processing message from {topic}: {str(e)}")
                            self.metrics.processing_errors += 1
                
        except Exception as e:
            self.logger.error(f"Consumer error: {str(e)}")
        finally:
            consumer.close()
    
    def _process_message(self, topic: str, message: Dict, key: Optional[str]):
        """Process a single message"""
        if topic in self.processors:
            processor_func = self.processors[topic]
            processed_message = processor_func(message, key)
            
            # If processor returns a result, send it to output topic
            output_topic = self.config.get('output_topics', {}).get(topic)
            if processed_message and output_topic:
                self.produce_message(output_topic, processed_message, key)
        else:
            self.logger.warning(f"No processor registered for topic: {topic}")
    
    def get_metrics(self) -> Dict:
        """Get streaming metrics"""
        return asdict(self.metrics)
    
    def close(self):
        """Close producer and consumer"""
        self.stop_consumer()
        if self.producer:
            self.producer.close()

class EcommerceStreamProcessor:
    """Specialized stream processor for e-commerce data"""
    
    def __init__(self, kafka_config: Dict):
        self.kafka_processor = KafkaStreamProcessor(kafka_config)
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Register processors
        self._register_processors()
    
    def _register_processors(self):
        """Register e-commerce specific processors"""
        self.kafka_processor.register_processor('raw-orders', self.process_order)
        self.kafka_processor.register_processor('raw-customers', self.process_customer)
        self.kafka_processor.register_processor('raw-products', self.process_product)
    
    def process_order(self, message: Dict, key: Optional[str]) -> Dict:
        """Process order messages"""
        try:
            # Extract order data
            order_data = message.copy()
            
            # Add processing timestamp
            order_data['processed_at'] = datetime.now().isoformat()
            
            # Validate required fields
            required_fields = ['order_id', 'customer_id', 'order_amount']
            for field in required_fields:
                if field not in order_data:
                    raise ValueError(f"Missing required field: {field}")
            
            # Convert order amount to float
            order_data['order_amount'] = float(order_data['order_amount'])
            
            # Add order classification
            if order_data['order_amount'] > 1000:
                order_data['order_type'] = 'high_value'
            elif order_data['order_amount'] > 100:
                order_data['order_type'] = 'medium_value'
            else:
                order_data['order_type'] = 'low_value'
            
            # Calculate order metrics
            items = order_data.get('items', [])
            order_data['total_items'] = len(items)
            order_data['average_item_price'] = order_data['order_amount'] / max(len(items), 1)
            
            self.logger.debug(f"Processed order: {order_data['order_id']}")
            return order_data
            
        except Exception as e:
            self.logger.error(f"Error processing order: {str(e)}")
            raise
    
    def process_customer(self, message: Dict, key: Optional[str]) -> Dict:
        """Process customer messages"""
        try:
            customer_data = message.copy()
            
            # Add processing timestamp
            customer_data['processed_at'] = datetime.now().isoformat()
            
            # Standardize email
            if 'email' in customer_data:
                customer_data['email'] = customer_data['email'].lower().strip()
            
            # Standardize phone
            if 'phone' in customer_data:
                phone = customer_data['phone']
                # Simple phone standardization
                digits = ''.join(filter(str.isdigit, str(phone)))
                if len(digits) == 10:
                    customer_data['phone'] = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
            
            # Add customer segment
            order_count = customer_data.get('total_orders', 0)
            total_spent = customer_data.get('total_spent', 0)
            
            if order_count >= 10 and total_spent >= 1000:
                customer_data['segment'] = 'vip'
            elif order_count >= 5 or total_spent >= 500:
                customer_data['segment'] = 'regular'
            else:
                customer_data['segment'] = 'new'
            
            self.logger.debug(f"Processed customer: {customer_data.get('customer_id')}")
            return customer_data
            
        except Exception as e:
            self.logger.error(f"Error processing customer: {str(e)}")
            raise
    
    def process_product(self, message: Dict, key: Optional[str]) -> Dict:
        """Process product messages"""
        try:
            product_data = message.copy()
            
            # Add processing timestamp
            product_data['processed_at'] = datetime.now().isoformat()
            
            # Standardize product name
            if 'name' in product_data:
                product_data['name'] = product_data['name'].strip().title()
            
            # Add price category
            price = float(product_data.get('price', 0))
            if price > 500:
                product_data['price_category'] = 'premium'
            elif price > 100:
                product_data['price_category'] = 'mid_range'
            else:
                product_data['price_category'] = 'budget'
            
            # Calculate discount percentage if applicable
            original_price = product_data.get('original_price')
            if original_price and original_price > price:
                discount_pct = ((original_price - price) / original_price) * 100
                product_data['discount_percentage'] = round(discount_pct, 2)
            
            self.logger.debug(f"Processed product: {product_data.get('product_id')}")
            return product_data
            
        except Exception as e:
            self.logger.error(f"Error processing product: {str(e)}")
            raise
    
    def start_processing(self, input_topics: List[str]):
        """Start processing messages from input topics"""
        self.kafka_processor.start_consumer(input_topics)
        self.logger.info("E-commerce stream processing started")
    
    def stop_processing(self):
        """Stop processing"""
        self.kafka_processor.stop_consumer()
        self.logger.info("E-commerce stream processing stopped")
    
    def send_order_data(self, order_data: Dict):
        """Send order data to Kafka"""
        self.kafka_processor.produce_message('raw-orders', order_data, order_data.get('order_id'))
    
    def send_customer_data(self, customer_data: Dict):
        """Send customer data to Kafka"""
        self.kafka_processor.produce_message('raw-customers', customer_data, customer_data.get('customer_id'))
    
    def send_product_data(self, product_data: Dict):
        """Send product data to Kafka"""
        self.kafka_processor.produce_message('raw-products', product_data, product_data.get('product_id'))
    
    def get_metrics(self) -> Dict:
        """Get processing metrics"""
        return self.kafka_processor.get_metrics()

# Example usage
if __name__ == "__main__":
    # Kafka configuration
    kafka_config = {
        'bootstrap_servers': ['localhost:9092'],
        'consumer_group': 'ecommerce-processor',
        'output_topics': {
            'raw-orders': 'processed-orders',
            'raw-customers': 'processed-customers',
            'raw-products': 'processed-products'
        }
    }
    
    # Initialize processor
    processor = EcommerceStreamProcessor(kafka_config)
    
    # Example: Send some test data
    test_order = {
        'order_id': 'ORD-001',
        'customer_id': 'CUST-001',
        'order_amount': 250.50,
        'items': [
            {'product_id': 'PROD-001', 'quantity': 2, 'price': 125.25}
        ]
    }
    
    processor.send_order_data(test_order)
    
    # Start processing (in a real scenario, this would run continuously)
    # processor.start_processing(['raw-orders', 'raw-customers', 'raw-products'])
    
    print("Kafka streaming setup complete. Metrics:", processor.get_metrics())
