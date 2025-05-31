#!/usr/bin/env python

"""
Simple example of publishing messages to Google Cloud Pub/Sub.
This is a minimal example showing the basic concepts.
"""

import os
from google.cloud import pubsub_v1

def publish_message(project_id: str, topic_id: str, message: str):
    """Publish a single message to a Pub/Sub topic."""
    
    # Create a publisher client
    publisher = pubsub_v1.PublisherClient()
    
    # Create the topic path
    topic_path = publisher.topic_path(project_id, topic_id)
    
    # Convert message to bytes
    message_data = message.encode('utf-8')
    
    # Publish the message
    future = publisher.publish(topic_path, message_data)
    
    # Get the message ID
    message_id = future.result()
    
    print(f"Published message '{message}' to {topic_path}")
    print(f"Message ID: {message_id}")

def main():
    # Set your project ID - replace with your actual project ID
    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT', 'your-project-id')
    topic_id = 'MyTopic'
    
    # Messages to publish
    messages = [
        "Hello, World!",
        "This is a test message",
        "Pub/Sub is awesome!",
        "Message from simple publisher"
    ]
    
    print(f"Publishing messages to topic: {topic_id}")
    print(f"Project ID: {project_id}")
    print("-" * 50)
    
    # Publish each message
    for i, message in enumerate(messages, 1):
        print(f"\nPublishing message {i}:")
        publish_message(project_id, topic_id, message)

if __name__ == "__main__":
    main()