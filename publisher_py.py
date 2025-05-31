#!/usr/bin/env python

# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This application demonstrates how to perform basic operations on topics
with the Cloud Pub/Sub API.

For more information, see the README.md under /pubsub and the documentation
at https://cloud.google.com/pubsub/docs.
"""

import argparse
import os
from concurrent import futures
from typing import Callable

from google.cloud import pubsub_v1


def list_topics(project_id: str) -> None:
    """Lists all Pub/Sub topics in the given project."""
    # [START pubsub_list_topics]
    publisher = pubsub_v1.PublisherClient()
    project_path = f"projects/{project_id}"

    for topic in publisher.list_topics(request={"project": project_path}):
        print(topic)
    # [END pubsub_list_topics]


def create_topic(project_id: str, topic_id: str) -> None:
    """Create a new Pub/Sub topic."""
    # [START pubsub_quickstart_create_topic]
    # [START pubsub_create_topic]
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    topic = publisher.create_topic(request={"name": topic_path})

    print(f"Created topic: {topic.name}")
    # [END pubsub_create_topic]
    # [END pubsub_quickstart_create_topic]


def delete_topic(project_id: str, topic_id: str) -> None:
    """Deletes an existing Pub/Sub topic."""
    # [START pubsub_delete_topic]
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    publisher.delete_topic(request={"topic": topic_path})

    print(f"Topic deleted: {topic_path}")
    # [END pubsub_delete_topic]


def publish_messages(project_id: str, topic_id: str) -> None:
    """Publishes multiple messages to a Pub/Sub topic."""
    # [START pubsub_quickstart_publisher]
    # [START pubsub_publish]
    publisher = pubsub_v1.PublisherClient()
    # The `topic_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/topics/{topic_id}`
    topic_path = publisher.topic_path(project_id, topic_id)

    for n in range(1, 10):
        data = f"Message number {n}"
        # Data must be a bytestring
        data = data.encode("utf-8")
        # When you publish a message, the client returns a future.
        future = publisher.publish(topic_path, data)
        print(future.result())

    print(f"Published messages to {topic_path}.")
    # [END pubsub_publish]
    # [END pubsub_quickstart_publisher]


def publish_messages_with_custom_attributes(project_id: str, topic_id: str) -> None:
    """Publishes multiple messages with custom attributes
    to a Pub/Sub topic."""
    # [START pubsub_publish_custom_attributes]
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    for n in range(1, 10):
        data = f"Message number {n}"
        # Data must be a bytestring
        data = data.encode("utf-8")
        # Add two attributes, origin and username, to the message
        future = publisher.publish(
            topic_path, data, origin="python-sample", username="gcp"
        )
        print(future.result())

    print(f"Published messages with custom attributes to {topic_path}.")
    # [END pubsub_publish_custom_attributes]


def publish_messages_with_futures(project_id: str, topic_id: str) -> None:
    """Publishes multiple messages to a Pub/Sub topic and prints their
    message IDs."""
    # [START pubsub_publish_with_futures]
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    publish_futures = []

    def get_callback(
        publish_future: pubsub_v1.publisher.futures.Future, data: str
    ) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
        def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
            try:
                # Wait 60 seconds for the publish call to succeed.
                print(publish_future.result(timeout=60))
            except futures.TimeoutError:
                print(f"Publishing {data} timed out.")

        return callback

    for i in range(10):
        data = f"Message number {i}"
        # Data must be a bytestring
        data_bytes = data.encode("utf-8")
        # When you publish a message, the client returns a future.
        publish_future = publisher.publish(topic_path, data_bytes)
        # Non-blocking. Publish failures are handled in the callback function.
        publish_future.add_done_callback(get_callback(publish_future, data))
        publish_futures.append(publish_future)

    # Wait for all the publish futures to complete.
    futures.as_completed(publish_futures)

    print(f"Published messages with futures to {topic_path}")
    # [END pubsub_publish_with_futures]


def publish_messages_with_error_handler(project_id: str, topic_id: str) -> None:
    """Publishes multiple messages to a Pub/Sub topic with an error handler."""
    # [START pubsub_publish_with_error_handler]
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    publish_futures = []

    def get_callback(
        publish_future: pubsub_v1.publisher.futures.Future, data: str
    ) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
        def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
            try:
                # Wait 60 seconds for the publish call to succeed.
                print(publish_future.result(timeout=60))
            except futures.TimeoutError:
                print(f"Publishing {data} timed out.")

        return callback

    for i in range(10):
        data = f"Message number {i}"
        # Data must be a bytestring
        data_bytes = data.encode("utf-8")
        # When you publish a message, the client returns a future.
        publish_future = publisher.publish(topic_path, data_bytes)
        # Non-blocking. Publish failures are handled in the callback function.
        publish_future.add_done_callback(get_callback(publish_future, data))
        publish_futures.append(publish_future)

    # Wait for all the publish futures to complete.
    futures.as_completed(publish_futures)

    print(f"Published messages with error handler to {topic_path}")
    # [END pubsub_publish_with_error_handler]


def publish_messages_with_batch_settings(project_id: str, topic_id: str) -> None:
    """Publishes multiple messages to a Pub/Sub topic with batch settings."""
    # [START pubsub_publisher_batch_settings]
    from google.cloud.pubsub_v1 import PublisherClient
    from google.cloud.pubsub_v1.types import BatchSettings

    # TODO(developer): Choose an existing topic.
    # project_id = "your-project-id"
    # topic_id = "your-topic-id"

    # Configure the batch to publish as soon as there are 10 messages
    # or 1 KiB of data, or 1 second has passed.
    batch_settings = BatchSettings(max_messages=10, max_bytes=1024, max_latency=1)
    publisher = PublisherClient(batch_settings)
    topic_path = publisher.topic_path(project_id, topic_id)

    # Resolve the publish future in a separate thread.
    def callback(future: pubsub_v1.publisher.futures.Future) -> None:
        message_id = future.result()
        print(message_id)

    for n in range(1, 10):
        data = f"Message number {n}"
        # Data must be a bytestring
        data = data.encode("utf-8")
        future = publisher.publish(topic_path, data)
        # Publish failures are handled in the callback function.
        future.add_done_callback(callback)

    # Wait for all messages to be published.
    publisher.stop()
    print(f"Published messages with batch settings to {topic_path}.")
    # [END pubsub_publisher_batch_settings]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("project_id", help="Your Google Cloud project ID")

    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("list", help="Lists all Pub/Sub topics in the given project.")
    subparsers.add_parser("create", help="Create a new Pub/Sub topic.")
    subparsers.add_parser("delete", help="Deletes an existing Pub/Sub topic.")
    subparsers.add_parser("publish", help="Publishes multiple messages to a Pub/Sub topic.")
    subparsers.add_parser(
        "publish-with-custom-attributes",
        help="Publishes multiple messages with custom attributes to a Pub/Sub topic.",
    )
    subparsers.add_parser(
        "publish-with-futures",
        help="Publishes multiple messages to a Pub/Sub topic and prints their message IDs.",
    )
    subparsers.add_parser(
        "publish-with-error-handler",
        help="Publishes multiple messages to a Pub/Sub topic with an error handler.",
    )
    subparsers.add_parser(
        "publish-with-batch-settings",
        help="Publishes multiple messages to a Pub/Sub topic with batch settings.",
    )

    args = parser.parse_args()

    if args.command == "list":
        list_topics(args.project_id)
    elif args.command == "create":
        create_topic(args.project_id, "MyTopic")
    elif args.command == "delete":
        delete_topic(args.project_id, "MyTopic")
    elif args.command == "publish":
        publish_messages(args.project_id, "MyTopic")
    elif args.command == "publish-with-custom-attributes":
        publish_messages_with_custom_attributes(args.project_id, "MyTopic")
    elif args.command == "publish-with-futures":
        publish_messages_with_futures(args.project_id, "MyTopic")
    elif args.command == "publish-with-error-handler":
        publish_messages_with_error_handler(args.project_id, "MyTopic")
    elif args.command == "publish-with-batch-settings":
        publish_messages_with_batch_settings(args.project_id, "MyTopic")