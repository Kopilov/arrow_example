#!/bin/bash

set -e

git submodule update --init --recursive

mkdir -p build_arrow
pushd build_arrow

cmake ../arrow/cpp && make -j$(nproc) && sudo make install; sudo ldconfig
popd

mkdir -p build
pushd build
cmake .. && make -j$(nproc) && sudo make install
popd

arrow_examples
