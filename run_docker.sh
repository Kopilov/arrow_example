#!/bin/bash
git submodule update --init --recursive
docker build . -t arrow_examples
mkdir -p generated/docker
docker run --mount type=bind,source=$(pwd)/generated/docker,target=/target --workdir="/target" arrow_examples:latest arrow_examples
