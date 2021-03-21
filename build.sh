#!/usr/bin/env bash
set -eo pipefail

docker build --tag sss-build .
docker run --rm --name sss-build \
 -v /var/run/docker.sock:/var/run/docker.sock \
 -v $PWD/artifacts:/repo/artifacts \
 -v $PWD/.git:/repo/.git \
 --network host \
 -e FEEDZ_SSS_API_KEY=${FEEDZ_SSS_API_KEY} \
 sss-build \
 dotnet run -p /repo/build/build.csproj -- "$@"