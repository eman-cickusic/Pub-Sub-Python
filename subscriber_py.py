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

"""This application demonstrates how to perform basic operations on
subscriptions with the Cloud Pub/Sub API.

For more information, see the README.md under /pubsub and the documentation
at https://cloud.google.com/pubsub/docs.
"""

import argparse
import os
from concurrent.futures import ThreadPoolExecutor
import time
from typing import Callable

from google.cloud import pubsub_v1


def list_subscriptions_in_topic(project_id: str, topic_id: str) -> None:
    """Lists all subscriptions for a given topic."""
    # [START pubsub_list_topic_subscriptions]
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    response = publisher.list_topic_subscriptions(request={"topic": topic_path})
    for subscription in response:
        print(subscription)
    # [END pubsub_list_topic_subscriptions]


def list_subscriptions_in_project(project_id: str) -> None:
    """Lists all subscriptions in the current project."""
    # [START pubsub_list_subscriptions]
    subscriber = pubsub_v1.SubscriberClient()
    project_path = f"projects/{project_id}"

    # Wrap the subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        for subscription in subscriber.list_subscriptions(
            request={"project": project_path}
        ):
            print(subscription.name)
    # [END pubsub_list_subscriptions]


def create_subscription(project_id: str, topic_id: str, subscription_id: str) -> None:
    """Create a new pull subscription on the given topic."""
    # [START pubsub_create_pull_subscription]
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = subscriber.topic_path(project_id, topic_id)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    # Wrap the subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        subscription = subscriber.create_subscription(
            request={"name": subscription_path, "topic": topic_path}
        )

    print(f"Subscription created: {subscription.name}")
    # [END pubsub_create_pull_subscription]


def create_push_subscription(
    project_id: str, topic_id: str, subscription_id: str, endpoint: str
) -> None:
    """Create a new push subscription on the given topic."""
    # [START pubsub_create_push_subscription]
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = subscriber.topic_path(project_id, topic_id)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    push_config = pubsub_v1.PushConfig(push_endpoint=endpoint)

    # Wrap the subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        subscription = subscriber.create_subscription(
            request={
                "name": subscription_path,
                "topic": topic_path,
                "push_config": push_config,
            }
        )

    print(f"Push subscription created: {subscription.name}")
    print(f"Endpoint for subscription is: {endpoint}")
    # [END pubsub_create_push_subscription]


def delete_subscription(project_id: str, subscription_id: str) -> None:
    """Deletes an existing Pub/Sub topic."""
    # [START pubsub_delete_subscription]
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    # Wrap the subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        subscriber.delete_subscription(request={"subscription": subscription_path})

    print(f"Subscription deleted: {subscription_path}")
    # [END pubsub_delete_subscription]


def update_push_subscription(
    project_id: str, topic_id: str, subscription_id: str, endpoint: str
) -> None:
    """
    Updates an existing Pub/Sub subscription's push endpoint URL.
    Note that certain properties of a subscription, such as
    its topic, are not modifiable.
    """
    # [START pubsub_update_push_subscription]
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    push_config = pubsub_v1.PushConfig(push_endpoint=endpoint)

    subscription = pubsub_v1.Subscription(
        name=subscription_path, push_config=push_config
    )

    update_mask = {"paths": {"push_config"}}

    # Wrap the subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        result = subscriber.update_subscription(
            request={"subscription": subscription, "update_mask": update_mask}
        )

    print(f"Subscription updated: {result}")
    print(f"New endpoint for subscription is: {endpoint}")
    # [END pubsub_update_push_subscription]


def receive_messages(project_id: str, subscription_id: str, timeout: float = None) -> None:
    """Receives messages from a pull subscription."""
    # [START pubsub_subscriber_async_pull]
    # [START pubsub_quickstart_subscriber]
    subscriber = pubsub_v1.SubscriberClient()
    # The `subscription_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/subscriptions/{subscription_id}`
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        print(f"Received {message}.")
        message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=timeout)
        except KeyboardInterrupt:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.
    # [END pubsub_subscriber_async_pull]
    # [END pubsub_quickstart_subscriber]


def receive_messages_with_custom_attributes(
    project_id: str, subscription_id: str, timeout: float = None
) -> None:
    """Receives messages from a pull subscription."""
    # [START pubsub_subscriber_async_pull_custom_attributes]
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        print(f"Received {message.data!r}.")
        if message.attributes:
            print("Attributes:")
            for key in message.attributes:
                value = message.attributes.get(key)
                print(f"{key}: {value}")
        message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=timeout)
        except KeyboardInterrupt:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.
    # [END pubsub_subscriber_async_pull_custom_attributes]


def receive_messages_with_flow_control(project_id: str, subscription_id: str) -> None:
    """Receives messages from a pull subscription with flow control."""
    # [START pubsub_subscriber_flow_control]
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        print(f"Received {message.data!r}.")
        message.ack()

    # Limit the subscriber to only have ten outstanding messages at a time.
    flow_control = pubsub_v1.types.FlowControl(max_messages=10)
    streaming_pull_future = subscriber.subscribe(
        subscription_path, callback=callback, flow_control=flow_control
    )
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=5.0)
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.
    # [END pubsub_subscriber_flow_control]


def synchronous_pull(project_id: str, subscription_id: str) -> None:
    """Pulling messages synchronously."""
    # [START pubsub_subscriber_sync_pull]
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    NUM_MESSAGES = 3

    # Wrap the subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        # The subscriber pulls a specific number of messages. The actual
        # number of messages pulled may be smaller than max_messages.
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": NUM_MESSAGES}
        )

        ack_ids = []
        for received_message in response.received_messages:
            print(f"Received: {received_message.message.data}")
            ack_ids.append(received_message.ack_id)

        # Acknowledges the received messages so they will not be sent again.
        subscriber.acknowledge(
            request={"subscription": subscription_path, "ack_ids": ack_ids}
        )

        print(
            f"Received and acknowledged {len(response.received_messages)} messages from {subscription_path}."
        )
    # [END pubsub_subscriber_sync_pull]


def listen_for_errors(project_id: str, subscription_id: str, timeout: float = None) -> None:
    """Receives messages and catches errors from a pull subscription."""
    # [START pubsub_subscriber_error_listener]
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        print(f"Received {message.data!r}.")
        message.ack()

    streaming_pull_future = subscriber.subscribe(
        subscription_path, callback=callback
    )
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=timeout)
        except KeyboardInterrupt:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.
        except Exception as e:
            print(f"Listening for messages on {subscription_path} threw an exception: {e}.")
            streaming_pull_future.cancel()  # Trigger the shutdown.
    # [END pubsub_subscriber_error_listener]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("project_id", help="Your Google Cloud project ID")

    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser(
        "list_in_topic", help="Lists all subscriptions for a given topic."
    )
    subparsers.add_parser(
        "list_in_project", help="Lists all subscriptions in the current project."
    )
    subparsers.add_parser(
        "create", help="Create a new pull subscription on the given topic."
    )
    subparsers.add_parser(
        "create-push", help="Create a new push subscription on the given topic."
    )
    subparsers.add_parser("delete", help="Deletes an existing Pub/Sub topic.")
    subparsers.add_parser(
        "update",
        help="Updates an existing Pub/Sub subscription's push endpoint URL. "
        "Note that certain properties of a subscription, such as its topic, "
        "are not modifiable.",
    )
    subparsers.add_parser(
        "receive", help="Receives messages from a pull subscription."
    )
    subparsers.add_parser(
        "receive-custom-attributes",
        help="Receives messages from a pull subscription.",
    )
    subparsers.add_parser(
        "receive-flow-control",
        help="Receives messages from a pull subscription with flow control.",
    )
    subparsers.add_parser(
        "receive-synchronously", help="Pulling messages synchronously."
    )
    subparsers.add_parser(
        "listen_for_errors",
        help="Receives messages and catches errors from a pull subscription.",
    )

    args = parser.parse_args()

    if args.command == "list_in_topic":
        list_subscriptions_in_topic(args.project_id, "MyTopic")
    elif args.command == "list_in_project":
        list_subscriptions_in_project(args.project_id)
    elif args.command == "create":
        create_subscription(args.project_id, "MyTopic", "MySub")
    elif args.command == "create-push":
        create_push_subscription(
            args.project_id, "MyTopic", "MySub", "https://example.com/push"
        )
    elif args.command == "delete":
        delete_subscription(args.project_id, "MySub")
    elif args.command == "update":
        update_push_subscription(
            args.project_id, "MyTopic", "MySub", "https://example.com/push"
        )
    elif args.command == "receive":
        receive_messages(args.project_id, "MySub", timeout=30.0)
    elif args.command == "receive-custom-attributes":
        receive_messages_with_custom_attributes(args.project_id, "MySub", timeout=30.0)
    elif args.command == "receive-flow-control":
        receive_messages_with_flow_control(args.project_id, "MySub")
    elif args.command == "receive-synchronously":
        synchronous_pull(args.project_id, "MySub")
    elif args.command == "listen_for_errors":
        listen_for_errors(args.project_id, "MySub", timeout=30.0)