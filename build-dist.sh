#!/bin/bash

rm -rf ./dist
rm -rf ./build
mkdir -p ./dist

python setup.py sdist bdist_wheel
