#!/usr/bin/env bash

## Set this to the home directory of spark
export SPARK_HOME="/Users/matteusklich/YD/git-repos/spark"

if [ -z "$SPARK_HOME" ]; then
    echo "SPARK_HOME not set, go to conf/spark-env.sh and add the path to spark"
    exit
fi

## This will add two tbana jars (main jar, and examples jar) to the spark-shell
PATH_TO_TBANA=$(pwd)
EXAMPLES_JAR=$(ls "$PATH_TO_TBANA/build/libs/tbana-"*[0-9T]-examples.jar)
TBANA_JAR=$(ls "$PATH_TO_TBANA/build/libs/tbana-"*[0-9T].jar)
DEPS_JARS=$(ls -d $PATH_TO_TBANA/lib_managed/*.jar | paste -sd "," -)
ADD_JARS="$TBANA_JAR,$EXAMPLES_JAR,$DEPS_JARS"
export ADD_JARS
export CLASSPATH=$CLASSPATH:$TBANA_JAR
