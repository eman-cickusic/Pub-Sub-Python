#!/usr/bin/env python

"""
Simple example of subscribing to messages from Google Cloud Pub/Sub.
This is a minimal example showing the basic concepts.
"""

import os
import time
from google.cloud import pubsub_v1

def receive_messages(project_id: str, subscription_id: str, timeout: float = 30.0):
    """Receive messages from a Pub/Sub subscription."""
    
    # Create a subscriber client
    subscriber = pubsub_v1.SubscriberClient()
    
    # Create the subscription path
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    
    def callback(message):
        """Callback function to process received messages."""
        print(f"Received message: {message.data.decode('utf-8')}")
        
        # Print message attributes if any
        if message.attributes:
            print("Message attributes:")
            for key, value in message.attributes.items():
                print(f"  {key}: {value}")
        
        print(f"Message ID: {message.message_id}")
        print(f"Publish time: {message.publish_time}")
        print("-" * 40)
        
        # Acknowledge the message so it won't be sent again
        message.ack()
    
    print(f"Listening for messages on {subscription_path}...")
    print(f"Timeout: {timeout} seconds")
    print("Press Ctrl+C to stop listening")
    print("=" * 50)
    
    # Start receiving messages
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    
    # Keep the subscriber running
    with subscriber:
        try:
            # Block until timeout or KeyboardInterrupt
            streaming_pull_future.result(timeout=timeout)
        except KeyboardInterrupt:
            print("\nStopping subscriber...")
            streaming_pull_future.cancel()  # Trigger shutdown
            streaming_pull_future.result()  # Block until shutdown complete
        except Exception as e:
            print(f"Error occurred: {e}")
            streaming_pull_future.cancel()

def main():
    # Set your project ID - replace with your actual project ID
    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT', 'your-project-id')
    subscription_id = 'MySub'
    
    print(f"Simple Pub/Sub Subscriber")
    print(f"Project ID: {project_id}")
    print(f"Subscription: {subscription_id}")
    print()
    
    # Start receiving messages
    receive_messages(project_id, subscription_id, timeout=60.0)

if __name__ == "__main__":
    main()