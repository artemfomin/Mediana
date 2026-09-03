# Gate: UNION branch coverage across both assets (net10 + ns2.1) must be >= 95% per core package
$ErrorActionPreference = "Stop"

$packages = @("Mediana", "Mediana.Abstractions", "Mediana.Transport.Abstractions", "Mediana.Outbox")
$union = @{}

foreach ($proj in @("tests/Mediana.UnitTests", "tests/Mediana.UnitTests.Ns21")) {
 $files = Get-ChildItem -Path "$proj/TestResults" -Filter "coverage.cobertura.xml" -Recurse -ErrorAction SilentlyContinue | Sort-Object LastWriteTime
 if ($files.Count -eq 0) { Write-Host "No coverage for $proj"; exit 1 }

 [xml]$coverage = Get-Content $files[-1].FullName
 foreach ($pkg in $coverage.coverage.packages.package) {
 $name = $pkg.name
 if ($packages -notcontains $name) { continue }
 $rate = [double]$pkg."branch-rate"
 if (-not $union.ContainsKey($name) -or $union[$name] -lt $rate) {
 $union[$name] = $rate
 }
 }
}

if ($union.Count -eq 0) { Write-Host "No core packages in coverage"; exit 1 }

$failed = $false
foreach ($name in $packages) {
 if (-not $union.ContainsKey($name)) { Write-Host "MISSING: $name"; $failed = $true; continue }
 $percent = [math]::Round($union[$name] * 100, 1)
 Write-Host "$name : $percent%"
 if ($union[$name] -lt 0.95) {
 Write-Host "FAIL: $name below 95%"
 $failed = $true
 }
}

if ($failed) { exit 1 }
Write-Host "Union coverage gate (95%): OK"
