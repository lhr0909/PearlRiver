#!/bin/bash

pkg="target/scala-2.10/XYZ-Pipeline-assembly-1.0.jar"
release="$(awk -F= '/^pipeline_version/{print $2}' gradle.properties)"
build_pkg="build/libs/xyz-pipeline-$release.jar"

mkdir -p target/scala-2.10/

./gradlew shadowJar $@ || exit 1
cp -f $build_pkg $pkg

tar -czvf     xyz-pipeline-$release.tar \
    --exclude bin/submit.sh \
    --exclude conf/spark-defaults.conf \
    --exclude conf/spark-defaults-hbase2es.conf \
    --exclude conf/env.sh \
    --exclude conf/pipeline.yml \
    --exclude conf/spark-online.conf \
    --exclude conf/es-online-mapping.json \
    $pkg \
    conf/*.template \
    conf/log4j* \
    conf/patterns \
    conf/GeoLite2-City.mmdb \
    demo \
    bin/submit* \
    bin/toolbox.sh || exit 1

echo done