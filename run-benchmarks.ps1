$ErrorActionPreference = "Stop"
$env:Path = "C:\Program Files\Microsoft\jdk-21.0.11.10-hotspot\bin;" + "$PWD\apache-maven-3.9.6\bin;" + $env:Path

Write-Host "Setting up pulse package for benchmarks..."
if (-not (Test-Path "benchmarks/src/main/java/pulse")) {
    New-Item -ItemType Directory -Force -Path "benchmarks/src/main/java/pulse" | Out-Null
}
Copy-Item *.java benchmarks/src/main/java/pulse/

(Get-ChildItem benchmarks/src/main/java/pulse/*.java) | Foreach-Object {
    if ($_.Name -notmatch "Benchmark|BucketedGCounterV1") {
        $content = Get-Content $_.FullName
        if (-not ($content[0] -match "package pulse;")) {
            $newContent = "package pulse;`n" + ($content -join "`n")
            Set-Content $_.FullName $newContent
        }
    }
}

Write-Host "Building Benchmarks via Maven..."
Push-Location benchmarks
mvn clean package
if ($LASTEXITCODE -ne 0) { throw "Maven build failed" }
Pop-Location

$report = "benchmark-report.md"
"# Pulse v2 Benchmark Report`n`n" | Out-File $report
"## Environment`n" | Out-File -Append $report
"- **OS**: " + (Get-CimInstance Win32_OperatingSystem).Caption | Out-File -Append $report
"- **CPU**: " + (Get-CimInstance Win32_Processor).Name | Out-File -Append $report
"- **RAM**: " + [math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory / 1GB) + " GB" | Out-File -Append $report
"- **JDK**: 21`n`n" | Out-File -Append $report

"## System Benchmarks (C, D, E)`n`n" | Out-File -Append $report
"```text`n" | Out-File -Append $report

Write-Host "Running System Benchmarks..."
java -cp "benchmarks/target/classes" pulse.PulseSystemBenchmark | Tee-Object -Variable sysBenchOutput
$sysBenchOutput | Out-File -Append $report

"`n````n`n" | Out-File -Append $report

"## JMH Concurrency Benchmarks (A, B, F)`n`n" | Out-File -Append $report
"```text`n" | Out-File -Append $report

Write-Host "Running JMH Benchmarks..."
Push-Location benchmarks
java -jar target/benchmarks.jar -rf json -rff results.json | Tee-Object -Variable jmhBenchOutput
Pop-Location
$jmhBenchOutput | Out-File -Append $report

"`n````n" | Out-File -Append $report

Write-Host "Benchmark Suite complete! Results written to $report."
