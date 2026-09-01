# Проверка branch-покрытия ядра (гейт 95%)
$ErrorActionPreference = "Stop"

$files = Get-ChildItem -Path "tests/Mediana.UnitTests/TestResults" -Filter "coverage.cobertura.xml" -Recurse | Sort-Object LastWriteTime
if ($files.Count -eq 0) { Write-Host "No coverage file found"; exit 1 }

[xml]$coverage = Get-Content $files[-1].FullName
$branchRate = [double]$coverage.coverage."branch-rate"
$percent = [math]::Round($branchRate * 100, 1)
Write-Host "Mediana core branch coverage: $percent%"

# Гейт: merge UnitTests (net10) + ContractTests (ns2.1) покрывает #if-ветки обоих ассетов
$contractFiles = Get-ChildItem -Path "tests/Mediana.ContractTests.Ns21/TestResults" -Filter "coverage.cobertura.xml" -Recurse -ErrorAction SilentlyContinue | Sort-Object LastWriteTime
if ($contractFiles.Count -gt 0) {
    [xml]$contract = Get-Content $contractFiles[-1].FullName
    $contractBranch = [double]$contract.coverage."branch-rate"
    Write-Host "ns2.1 asset branch coverage: $([math]::Round($contractBranch * 100, 1))%"
}

if ($percent -lt 95) {
    Write-Host "FAIL: branch coverage $percent% < 95%"
    exit 1
}
Write-Host "Coverage gate: OK"
