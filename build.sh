#!/usr/bin/env bash

docker build --tag sss-build .
docker run --rm -it --name sss-build \
 -v /var/run/docker.sock:/var/run/docker.sock \
 -v $PWD/artifacts:/artifacts \
 -v $PWD/.git:/.git \
 --network host \
 -e TRAVIS_BUILD_NUMBER=$TRAVIS_BUILD_NUMBER \
 -e FEEDZ_SSS_API_KEY=$FEEDZ_SSS_API_KEY \
 sss-build \
 dotnet run -p /repo/build/build.csproj -- "$@"