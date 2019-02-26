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

cd /Users/liuxun/NetEase/zeppelin

function clean() {
    mvn clean
    cp -r /Users/liuxun/Github/zeppelin-web-bak/bower_components /Users/liuxun/NetEase/zeppelin/zeppelin-web/
    cp -r /Users/liuxun/Github/zeppelin-web-bak/node /Users/liuxun/NetEase/zeppelin/zeppelin-web/
    #cp -r /Users/liuxun/Github/zeppelin-web-bak/node_modules /Users/liuxun/NetEase/zeppelin/zeppelin-web/
}

function updateGit(){
    git fetch upstream
    git checkout master
    git merge upstream/master
    git push origin master
    git checkout ZEPPLEIN-3610
    git rebase master
}

function buildDist() {
    mvn package -Pbuild-distr -DskipTests -Pspark-2.1 -Phadoop-2.7 -Pyarn -Ppyspark -Psparkr -Pscala-2.10 -Dcheckstyle.skip=true -Denforcer.skip=true -DskipRat=true -X
}

function buildWeb() {
    mvn package -pl zeppelin-web -am -Pbuild-distr -DskipTests -Pspark-2.1 -Phadoop-2.7 -Pyarn -Ppyspark -Psparkr -Pscala-2.10 -Dcheckstyle.skip=true -Denforcer.skip=true -DskipRat=true -X
}

function buildServer() {
    mvn package -pl zeppelin-server -am -Pbuild-distr -DskipTests -Pspark-2.1 -Phadoop-2.7 -Pyarn -Ppyspark -Psparkr -Pscala-2.10 -Dcheckstyle.skip=true -Denforcer.skip=true -DskipRat=true -X
}

function buildZengine() {
    mvn package -pl zeppelin-zengine -am -Pbuild-distr -DskipTests -Pspark-2.1 -Phadoop-2.7 -Pyarn -Ppyspark -Psparkr -Pscala-2.10 -Dcheckstyle.skip=true -Denforcer.skip=true -DskipRat=true -X
}

function buildSubmarine() {
  mvn package -pl submarine -am -Pbuild-distr -DskipTests -Dcheckstyle.skip=true -Denforcer.skip=true -DskipRat=true -X
}
#mvn package -pl zeppelin-server -am -Pbuild-distr -DskipTests -Pspark-2.1 -Phadoop-2.7 -Pyarn -Ppyspark -Psparkr -Pscala-2.10 -X

#cp /Users/liuxun/NetEase/zeppelin/zeppelin-server/target/zeppelin-server-0.8.0-SNAPSHOT.jar /Users/liuxun/NetEase/zeppelin-0.8.0-debug/lib/

function scpJar() {
#    scp -i ~/.ssh/hzliuxun -P 1046 /Users/liuxun/Github/zeppelin/zeppelin-interpreter/target/zeppelin-interpreter-0.8.1-SNAPSHOT.jar  hzliuxun@10.120.196.234:/tmp
#    scp -i ~/.ssh/hzliuxun -P 1046 /Users/liuxun/Github/zeppelin/zeppelin-server/target/zeppelin-server-0.8.1-SNAPSHOT.jar  hzliuxun@10.120.196.234:/tmp
#    scp -i ~/.ssh/hzliuxun -P 1046 /Users/liuxun/Github/zeppelin/zeppelin-zengine/target/zeppelin-zengine-0.8.1-SNAPSHOT.jar  hzliuxun@10.120.196.234:/tmp

#    scp -i ~/.ssh/hzliuxun -P 1046 /Users/liuxun/Github/zeppelin/zeppelin-interpreter/target/zeppelin-interpreter-0.8.1-SNAPSHOT.jar  hzliuxun@10.120.196.235:/tmp
#    scp -i ~/.ssh/hzliuxun -P 1046 /Users/liuxun/Github/zeppelin/zeppelin-server/target/zeppelin-server-0.8.1-SNAPSHOT.jar  hzliuxun@10.120.196.235:/tmp
#    scp -i ~/.ssh/hzliuxun -P 1046 /Users/liuxun/Github/zeppelin/zeppelin-zengine/target/zeppelin-zengine-0.8.1-SNAPSHOT.jar  hzliuxun@10.120.196.235:/tmp

#    scp -i ~/.ssh/hzliuxun -P 1046 /Users/liuxun/MachineLearning/zeppelin/zeppelin-interpreter/target/zeppelin-interpreter-0.9.0-SNAPSHOT.jar  hzliuxun@10.120.196.234:/tmp
#    scp -i ~/.ssh/hzliuxun -P 1046 /Users/liuxun/MachineLearning/zeppelin/zeppelin-server/target/zeppelin-server-0.9.0-SNAPSHOT.jar  hzliuxun@10.120.196.234:/tmp
#    scp -i ~/.ssh/hzliuxun -P 1046 /Users/liuxun/MachineLearning/zeppelin/zeppelin-zengine/target/zeppelin-zengine-0.9.0-SNAPSHOT.jar  hzliuxun@10.120.196.234:/tmp
     scp -i ~/.ssh/hzliuxun -P 1046 /Users/liuxun/MachineLearning/zeppelin/interpreter/submarine/zeppelin-submarine-0.9.0-SNAPSHOT.jar hzliuxun@10.120.196.234:/tmp
}

#clean

scpJar
exit;

#git pull origin branch-0.8-0.3.1:branch-0.8-0.3.1

#cp /tmp/zeppelin-zengine-0.8.1-SNAPSHOT.jar /home/hadoop/zeppelin-current/lib/
#cp /tmp/zeppelin-server-0.8.1-SNAPSHOT.jar /home/hadoop/zeppelin-current/lib/
#cp /tmp/zeppelin-interpreter-0.8.1-SNAPSHOT.jar /home/hadoop/zeppelin-current/lib/interpreter/

#scp -i ~/.ssh/hzliuxun -P 1046 /Users/liuxun/NetEase/zeppelin/zeppelin-web/target/zeppelin-web-0.8.0-SNAPSHOT.war hzliuxun@10.120.196.236:/tmp
#scp -i ~/.ssh/hzliuxun -P 1046 target/dolphin-service.jar hzliuxun@10.120.196.232:/home/hadoop/dolphin/dolphin-service
