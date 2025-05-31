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

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="gcp-pubsub-python-tutorial",
    version="1.0.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A complete tutorial on working with Google Cloud Pub/Sub using Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/gcp-pubsub-python-tutorial",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Internet",
    ],
    python_requires='>=3.6',
    install_requires=[
        "google-cloud-pubsub>=2.18.0",
        "google-auth>=2.0.0",
        "google-auth-oauthlib>=0.5.0",
        "google-auth-httplib2>=0.1.0",
    ],
    entry_points={
        'console_scripts': [
            'pubsub-publisher=publisher:main',
            'pubsub-subscriber=subscriber:main',
        ],
    },
)