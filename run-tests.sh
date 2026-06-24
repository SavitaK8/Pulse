#!/usr/bin/env sh
set -eu
mkdir -p out
javac -encoding UTF-8 -d out ./*.java
java -ea -cp out PulseTests
