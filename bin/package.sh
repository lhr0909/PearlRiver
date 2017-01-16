#!/bin/bash

#pkg="target/scala-2.10/XYZ-Pipeline-assembly-1.0.jar"
#release="$(awk -F= '/^pipeline_version/{print $2}' gradle.properties)"

pkg="dist/XYZ-Processor.jar"
release="0.1-SNAPSHOT"
build_pkg="build/libs/xyz-processor-$release.jar"

mkdir -p dist

./gradlew copyRuntimeLibs $@ || exit 1
./gradlew assemble $@ || exit 1

cp -f $build_pkg $pkg

tar -czvf xyz-processor-$release.tar \
    --exclude bin/package.sh \
    $pkg \
    dist/**/* \
    conf/* \
    bin/* || exit 1

echo done