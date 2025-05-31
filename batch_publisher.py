#!/usr/bin/env python

"""
Example of batch publishing messages to Google Cloud Pub/Sub.
This example demonstrates how to publish multiple messages efficiently
using batch settings and async publishing with futures.
"""

import os
import time
from concurrent import futures
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import BatchSettings

def batch_publish_with_settings(project_id: str, topic_id: str, messages: list):
    """Publish messages using batch settings for better performance."""
    
    # Configure batch settings
    # Publish when we have 10 messages, 1KB of data, or after 1 second
    batch_settings = BatchSettings(
        max_messages=10,    # Maximum messages per batch
        max_bytes=1024,     # Maximum bytes per batch (1KB)
        max_latency=1       # Maximum delay in seconds
    )
    
    # Create publisher with batch settings
    publisher = pubsub_v1.PublisherClient(batch_settings=batch_settings)
    topic_path = publisher.topic_path(project_id, topic_id)
    
    print(f"Publishing {len(messages)} messages with batch settings...")
    print(f"Batch settings: max_messages={batch_settings.max_messages}, "
          f"max_bytes={batch_settings.max_bytes}, max_latency={batch_settings.max_latency}s")
    print("-" * 60)
    
    publish_futures = []
    
    def get_callback(future, data):
        """Callback function for handling publish results."""
        def callback(future):
            try:
                message_id = future.result(timeout=30)
                print(f"✓ Published: '{data}' (ID: {message_id})")
            except Exception as e:
                print(f"✗ Failed to publish '{data}': {e}")
        return callback
    
    # Publish all messages
    start_time = time.time()
    for i, message in enumerate(messages):
        data = message.encode('utf-8')
        
        # Publish message and get future
        future = publisher.publish(topic_path, data)
        
        # Add callback to handle result
        future.add_done_callback(get_callback(future, message))
        publish_futures.append(future)
        
        print(f"Queued message {i+1}: '{message}'")
    
    # Wait for all messages to be published
    print(f"\nWaiting for {len(publish_futures)} messages to be published...")
    futures.as_completed(publish_futures)
    
    # Stop the publisher to flush any remaining messages
    publisher.stop()
    
    end_time = time.time()
    print(f"\nAll messages published in {end_time - start_time:.2f} seconds")

def publish_with_attributes(project_id: str, topic_id: str):
    """Publish messages with custom attributes in batches."""
    
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    
    # Messages with different attributes
    messages_with_attributes = [
        ("Order #1001 processed", {"order_id": "1001", "status": "processed", "priority": "high"}),
        ("Order #1002 shipped", {"order_id": "1002", "status": "shipped", "priority": "normal"}),
        ("Order #1003 cancelled", {"order_id": "1003", "status": "cancelled", "priority": "low"}),
        ("System health check", {"type": "health_check", "component": "api", "status": "ok"}),
        ("User login event", {"type": "user_event", "action": "login", "user_id": "user123"}),
    ]
    
    print(f"\nPublishing {len(messages_with_attributes)} messages with attributes...")
    print("-" * 60)
    
    publish_futures = []
    
    for message_data, attributes in messages_with_attributes:
        data = message_data.encode('utf-8')
        
        # Publish with attributes
        future = publisher.publish(topic_path, data, **attributes)
        publish_futures.append(future)
        
        print(f"Queued: '{message_data}'")
        print(f"  Attributes: {attributes}")
    
    # Wait for all messages to be published
    print(f"\nWaiting for messages to be published...")
    for future in publish_futures:
        try:
            message_id = future.result(timeout=30)
            print(f"✓ Message published (ID: {message_id})")
        except Exception as e:
            print(f"✗ Failed to publish message: {e}")

def main():
    # Set your project ID
    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT', 'your-project-id')
    topic_id = 'MyTopic'
    
    print(f"Batch Publisher Example")
    print(f"Project ID: {project_id}")
    print(f"Topic: {topic_id}")
    print("=" * 60)
    
    # Generate sample messages
    sample_messages = [
        f"Batch message #{i+1}: Hello from batch publisher!"
        for i in range(15)
    ]
    
    # Add some varied messages
    sample_messages.extend([
        "Important notification: System maintenance scheduled",
        "Weather update: Sunny, 75°F",
        "Breaking news: New feature released!",
        "Reminder: Meeting at 3 PM",
        "Status update: All systems operational"
    ])
    
    try:
        # Example 1: Batch publishing with settings
        batch_publish_with_settings(project_id, topic_id, sample_messages)
        
        # Wait a moment between examples
        time.sleep(2)
        
        # Example 2: Publishing with attributes
        publish_with_attributes(project_id, topic_id)
        
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure your topic exists and you have proper authentication set up.")

if __name__ == "__main__":
    main()