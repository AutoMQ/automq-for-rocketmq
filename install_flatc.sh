#!/bin/bash
#
# Copyright 2024, AutoMQ CO.,LTD.
#
# Use of this software is governed by the Business Source License
# included in the file BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
#

set -e

if [ "$EUID" -ne 0 ]
  then echo "To install dependencies, you need to run as root. Please try running with sudo: sudo $0"
fi

if [ ! -f /usr/local/bin/flatc ]; then
    apt update
    apt install -y unzip clang
    arch=$(uname -m)
    if [ "$arch" == "arm64" ]; then
        echo "Host arch is arm64"
        wget -O flatc.zip https://github.com/google/flatbuffers/releases/download/v23.5.26/Mac.flatc.binary.zip
    elif [ "$arch" == "x86_64" ]; then
        echo "Host arch is amd64"
        wget -O flatc.zip https://github.com/google/flatbuffers/releases/download/v23.5.26/Linux.flatc.binary.clang++-12.zip
    else
        echo "Unsupported arch"
        exit 1
    fi
    unzip flatc.zip
    mv flatc /usr/local/bin/
    rm flatc.zip
    echo "install flatc successfully"
else
    echo "flatc exists"
fi
