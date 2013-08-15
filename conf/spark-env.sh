#!/usr/bin/env bash

if [ -z "$SPARK_HOME" ]; then
    echo "SPARK_HOME not set"
    exit
fi

if [ -z "$TBANA_HOME" ]; then
    echo "TBANA_HOME not set"
    exit
fi

## This will add two tbana jars (main jar, and examples jar) to the spark-shell
PATH_TO_TBANA=${TBANA_HOME}
EXAMPLES_JAR=$(ls "$PATH_TO_TBANA/build/libs/tbana-"*[0-9T]-examples.jar)
TBANA_JAR=$(ls "$PATH_TO_TBANA/build/libs/tbana-"*[0-9T].jar)
DEPS_JARS=$(ls -d $PATH_TO_TBANA/lib_managed/*.jar | paste -sd "," -)
ADD_JARS="$TBANA_JAR,$EXAMPLES_JAR,$DEPS_JARS"
export ADD_JARS

DEPS_JARS_COLON_SEPARATED=$(ls -d $PATH_TO_TBANA/lib_managed/*.jar | paste -sd ":" -)
export SPARK_CLASSPATH=$SPARK_CLASSPATH:$EXAMPLES_JAR:$TBANA_JAR:$DEPS_JARS_COLON_SEPARATED
