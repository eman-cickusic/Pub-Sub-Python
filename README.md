# Google Cloud Pub/Sub Python Tutorial

A complete tutorial on working with Google Cloud Pub/Sub using Python. This repository contains all the code and instructions to create topics, subscriptions, publish messages, and consume messages using the Google Cloud Pub/Sub service.

## Video

https://youtu.be/rWaqYESc_4E

## Overview

Google Cloud Pub/Sub is an asynchronous global messaging service that allows applications to exchange messages reliably, quickly, and asynchronously. This tutorial demonstrates:

- Creating and managing Pub/Sub topics and subscriptions
- Publishing messages to topics
- Consuming messages from subscriptions using pull subscribers
- Basic operations with the Python client library

## Architecture

```
Publisher → Topic → Subscription → Subscriber
```

- **Publisher**: Sends messages to a topic
- **Topic**: A named channel for messages
- **Subscription**: Connects to a topic to receive messages
- **Subscriber**: Receives and processes messages from a subscription

## Prerequisites

- Google Cloud Platform account
- Python 3.6 or higher
- `gcloud` CLI installed and configured
- A GCP project with Pub/Sub API enabled

## Setup Instructions

### 1. Clone this repository

```bash
git clone https://github.com/yourusername/gcp-pubsub-python-tutorial.git
cd gcp-pubsub-python-tutorial
```

### 2. Set up Python virtual environment

```bash
# Install virtualenv (if not already installed)
sudo apt-get install -y virtualenv

# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Set up Google Cloud credentials

```bash
# Set your project ID
export GOOGLE_CLOUD_PROJECT="your-project-id"

# Authenticate with Google Cloud
gcloud auth login
gcloud config set project $GOOGLE_CLOUD_PROJECT
```

## Usage Examples

### Creating a Topic

```bash
python publisher.py $GOOGLE_CLOUD_PROJECT create MyTopic
```

### Listing Topics

```bash
python publisher.py $GOOGLE_CLOUD_PROJECT list
```

### Creating a Subscription

```bash
python subscriber.py $GOOGLE_CLOUD_PROJECT create MyTopic MySub
```

### Publishing Messages

Using the Python script:
```bash
python publisher.py $GOOGLE_CLOUD_PROJECT publish MyTopic
```

Using gcloud CLI:
```bash
gcloud pubsub topics publish MyTopic --message "Hello World"
gcloud pubsub topics publish MyTopic --message "Publisher's name is John"
gcloud pubsub topics publish MyTopic --message "Publisher likes to eat pizza"
gcloud pubsub topics publish MyTopic --message "Publisher thinks Pub/Sub is awesome"
```

### Receiving Messages

```bash
python subscriber.py $GOOGLE_CLOUD_PROJECT receive MySub
```

Press `Ctrl+C` to stop listening for messages.

## File Structure

```
├── README.md
├── requirements.txt
├── publisher.py          # Topic management and publishing
├── subscriber.py         # Subscription management and message receiving
├── setup.py             # Package setup
├── .gitignore           # Git ignore rules
└── examples/            # Additional examples
    ├── simple_publisher.py
    ├── simple_subscriber.py
    └── batch_publisher.py
```

## Key Concepts

### Topics
- A named resource to which messages are sent
- Publishers send messages to topics
- Topics are created before publishing messages

### Subscriptions
- A named resource representing a stream of messages from a topic
- Subscribers receive messages from subscriptions
- Multiple subscriptions can be created for a single topic

### Message Flow
1. Create a topic
2. Create a subscription to the topic
3. Publish messages to the topic
4. Pull messages from the subscription

### Message Persistence
- Messages are persisted for up to 7 days if not acknowledged
- Subscribers must acknowledge messages within a configurable time window

## Advanced Features

### Custom Attributes
You can publish messages with custom attributes:

```bash
python publisher.py $GOOGLE_CLOUD_PROJECT publish-with-custom-attributes MyTopic
```

### Batch Settings
Configure batch settings for better performance:

```bash
python publisher.py $GOOGLE_CLOUD_PROJECT publish-with-batch-settings MyTopic
```

### Error Handling
Handle publishing errors gracefully:

```bash
python publisher.py $GOOGLE_CLOUD_PROJECT publish-with-error-handler MyTopic
```

### Flow Control
Control message flow on the subscriber side:

```bash
python subscriber.py $GOOGLE_CLOUD_PROJECT receive-flow-control MySub
```

## Troubleshooting

### Common Issues

1. **Authentication Error**: Ensure you're authenticated with Google Cloud
   ```bash
   gcloud auth login
   ```

2. **Permission Denied**: Ensure the Pub/Sub API is enabled
   ```bash
   gcloud services enable pubsub.googleapis.com
   ```

3. **Project Not Set**: Set your project ID
   ```bash
   gcloud config set project YOUR_PROJECT_ID
   ```

### Verification Commands

Check your setup:
```bash
# Verify authentication
gcloud auth list

# Verify project
gcloud config list project

# List existing topics
gcloud pubsub topics list

# List existing subscriptions
gcloud pubsub subscriptions list
```

## Best Practices

1. **Use Virtual Environments**: Always use virtual environments for Python projects
2. **Error Handling**: Implement proper error handling in production code
3. **Message Acknowledgment**: Always acknowledge messages after processing
4. **Resource Cleanup**: Delete unused topics and subscriptions to avoid charges
5. **Monitoring**: Use Google Cloud Monitoring to track Pub/Sub metrics

## Cleanup

To avoid charges, clean up resources when done:

```bash
# Delete subscription
python subscriber.py $GOOGLE_CLOUD_PROJECT delete MySub

# Delete topic
python publisher.py $GOOGLE_CLOUD_PROJECT delete MyTopic
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Additional Resources

- [Google Cloud Pub/Sub Documentation](https://cloud.google.com/pubsub/docs)
- [Python Client Library Reference](https://googleapis.dev/python/pubsub/latest/)
- [Pub/Sub Best Practices](https://cloud.google.com/pubsub/docs/publisher)
- [Pricing Information](https://cloud.google.com/pubsub/pricing)

## Support

If you encounter any issues or have questions:
1. Check the [troubleshooting section](#troubleshooting)
2. Review the [Google Cloud Pub/Sub documentation](https://cloud.google.com/pubsub/docs)
3. Open an issue in this repository
