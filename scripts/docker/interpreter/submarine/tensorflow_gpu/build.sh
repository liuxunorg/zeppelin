#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

bin=$(dirname "${BASH_SOURCE-$0}")
bin=$(cd "${bin}">/dev/null; pwd)

# In order to avoid repeated downloads every time you make a mirror,
# So first download hadoop and zeppelin to the local
HADOOP_VERSION="3.1.1"
Z_VERSION="0.9.0-SNAPSHOT"

# download zeppelin from remote or copy by local
echo "download zeppelin-${Z_VERSION}"

# From the local copy please open the note below
# copy ../../../../../zeppelin-distribution/target/zeppelin-${Z_VERSION}.tar.gz .

# Download from remote please open the note below
wget http://archive.apache.org/dist/zeppelin/zeppelin-${Z_VERSION}/zeppelin-${Z_VERSION}-bin-all.tgz

echo "download hadoop-${HADOOP_VERSION}"
wget http://mirrors.hust.edu.cn/apache/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz

docker build -t zeppelin-submarine/tensorflow1.8-gpu:1.0.0 .
