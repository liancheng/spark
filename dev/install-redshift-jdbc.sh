#!/usr/bin/env bash

set -e

SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd )"
SPARK_ROOT_DIR="$(dirname "$SCRIPT_DIR")"

cd /tmp

VERSION='1.1.17.1017'
FILENAME="RedshiftJDBC4-$VERSION.jar"

wget "https://s3.amazonaws.com/redshift-downloads/drivers/$FILENAME"

$SPARK_ROOT_DIR/build/mvn install:install-file \
    -Dfile=$FILENAME \
    -DgroupId=com.amazonaws \
    -DartifactId=redshift.jdbc4 \
    -Dversion=$VERSION \
    -Dpackaging=jar

