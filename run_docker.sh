#!/bin/bash
git submodule update --init --recursive
docker build . -t arrow_examples
docker run arrow_examples:latest arrow_examples

