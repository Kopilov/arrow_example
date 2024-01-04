#!/bin/bash
git submodule update --init --recursive
#mkdir -p build
#cd build

#cmake .. && make -j5
#STATUS=($?)
#cd -

#exit $STATUS

docker build . -t arrow_example
