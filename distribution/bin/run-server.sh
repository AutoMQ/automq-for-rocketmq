#!/usr/bin/env bash

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

#===========================================================================================
# Java Environment Setting
#===========================================================================================
error_exit ()
{
    echo "ERROR: $1 !!"
    exit 1
}

find_java_home()
{
    case "`uname`" in
        Darwin)
          if [ -n "$JAVA_HOME" ]; then
              JAVA_HOME=$JAVA_HOME
              return
          fi
            JAVA_HOME=$(/usr/libexec/java_home)
        ;;
        *)
            JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
        ;;
    esac
}

find_java_home

[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=$HOME/jdk/java
[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=/usr/java
[ ! -e "$JAVA_HOME/bin/java" ] && error_exit "Please set the JAVA_HOME variable in your environment, We need java(x64)!"

export JAVA_HOME
export JAVA="$JAVA_HOME/bin/java"
export BASE_DIR=$(dirname $0)/..
export CLASSPATH=.:${BASE_DIR}/conf:${BASE_DIR}/lib/*:${CLASSPATH}

#===========================================================================================
# JVM Configuration
#===========================================================================================
# The RAMDisk initializing size in MB on Darwin OS for gc-log
DIR_SIZE_IN_MB=600

choose_gc_log_directory()
{
    case "`uname`" in
        Darwin)
            if [ ! -d "/Volumes/RAMDisk" ]; then
                # create ram disk on Darwin systems as gc-log directory
                DEV=`hdiutil attach -nomount ram://$((2 * 1024 * DIR_SIZE_IN_MB))` > /dev/null
                diskutil eraseVolume HFS+ RAMDisk ${DEV} > /dev/null
                echo "Create RAMDisk /Volumes/RAMDisk for gc logging on Darwin OS."
            fi
            GC_LOG_DIR="/Volumes/RAMDisk"
        ;;
        *)
            if [ -d "$HOME/logs" ]; then
                GC_LOG_DIR="$HOME/logs"
            else
                mkdir "/tmp/rocketmq"
                GC_LOG_DIR="/tmp/rocketmq"
            fi
        ;;
    esac
}

choose_gc_options()
{
    # Example of JAVA_MAJOR_VERSION value : '1', '9', '10', '11', ...
    # '1' means releases befor Java 9
    JAVA_MAJOR_VERSION=$("$JAVA" -version 2>&1 | awk -F '"' '/version/ {print $2}' | awk -F '.' '{print $1}' | awk -F '-' '{print $1}')
    if [ -z "$JAVA_MAJOR_VERSION" ] || [ "$JAVA_MAJOR_VERSION" -lt "9" ] ; then
      JAVA_OPT="${JAVA_OPT} -server -Xms4g -Xmx4g -Xmn2g -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=320m -XX:MaxDirectMemorySize=4g"
      JAVA_OPT="${JAVA_OPT} -XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled -XX:SoftRefLRUPolicyMSPerMB=0 -XX:+CMSClassUnloadingEnabled -XX:SurvivorRatio=8 -XX:-UseParNewGC"
      JAVA_OPT="${JAVA_OPT} -verbose:gc -Xloggc:${GC_LOG_DIR}/rmq_srv_gc_%p_%t.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps"
      JAVA_OPT="${JAVA_OPT} -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=30m"
    else
#    elif [ -z "$JAVA_MAJOR_VERSION" ] || [ "$JAVA_MAJOR_VERSION" -lt "11" ]; then
      JAVA_OPT="${JAVA_OPT} -server -Xms4g -Xmx4g -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=320m"
      JAVA_OPT="${JAVA_OPT} -XX:+UseG1GC -XX:G1HeapRegionSize=16m -XX:G1ReservePercent=25 -XX:InitiatingHeapOccupancyPercent=30 -XX:SoftRefLRUPolicyMSPerMB=0"
      JAVA_OPT="${JAVA_OPT} -Xlog:gc*:file=${GC_LOG_DIR}/rmq_srv_gc_%p_%t.log:time,tags:filecount=5,filesize=30M"
#    else
#      JAVA_OPT="${JAVA_OPT} -server -Xms4g -Xmx4g -Xmn2g -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=320m -XX:MaxDirectMemorySize=4g"
#      JAVA_OPT="${JAVA_OPT} -XX:+UseZGC -XX:ZAllocationSpikeTolerance=3 -XX:ParallelGCThreads=8 -XX:CICompilerCount=3 -XX:-RestrictContended -XX:+AlwaysPreTouch -XX:+ExplicitGCInvokesConcurrent"
#      JAVA_OPT="${JAVA_OPT} -Xlog:gc*:file=${GC_LOG_DIR}/rmq_srv_gc_%p_%t.log:time,tags:filecount=5,filesize=30M"
    fi
}

choose_gc_log_directory
choose_gc_options

if [ -z "$LOG4J_CONF" ] ; then
    LOG4J_CONF="${BASE_DIR}/conf/log4j2.xml"
fi

JAVA_OPT="${JAVA_OPT} -XX:-OmitStackTraceInFastThrow"
JAVA_OPT="${JAVA_OPT} -XX:-UseLargePages"
JAVA_OPT="${JAVA_OPT} -Xdebug -Xrunjdwp:transport=dt_socket,address=*:9555,server=y,suspend=n"
JAVA_OPT="${JAVA_OPT} -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${GC_LOG_DIR}/oomheapdump.hprof"
JAVA_OPT="${JAVA_OPT} ${JAVA_OPT_EXT}"
JAVA_OPT="${JAVA_OPT} -cp ${CLASSPATH}"
# https://logging.apache.org/log4j/2.x/manual/configuration.html#automatic-configuration
JAVA_OPT="${JAVA_OPT} -Dlog4j.configurationFile=file:${LOG4J_CONF}"

"$JAVA" ${JAVA_OPT} $@
