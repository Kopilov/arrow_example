#!/bin/bash
sudo xargs rm < build/install_manifest.txt
rm -rf build
sudo xargs rm < build_arrow/install_manifest.txt
rm -rf build_arrow
