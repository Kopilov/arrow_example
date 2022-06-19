#!/bin/bash
git submodule update --init
mkdir -p build
cd build

cmake .. && make -j5

cd -
