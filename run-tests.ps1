$ErrorActionPreference = "Stop"
New-Item -ItemType Directory -Force -Path "out" | Out-Null
javac -encoding UTF-8 -d out *.java
java -ea -cp out PulseTests
