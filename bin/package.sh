#!/bin/bash

#pkg="target/scala-2.10/XYZ-Pipeline-assembly-1.0.jar"
#release="$(awk -F= '/^pipeline_version/{print $2}' gradle.properties)"

pkg="dist/XYZ-Processor.jar"
release="0.1"
build_pkg="build/libs/xyz-processor-$release.jar"

./gradlew shadowJar $@ || exit 1

mkdir -p dist

cp -f $build_pkg $pkg

tar -czvf xyz-processor-$release.tar \
    --exclude bin/package.sh \
    $pkg \
    conf/* \
    bin/* || exit 1

echo done