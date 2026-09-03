# Mediana

 ! [README](README.md) ( — north star ) .

## North star 

** ** (-Microsoft) . PR, , , . : — [ CI](.github/workflows/ci.yml) .

## 

- .NET SDK 10.0.302+ (`global.json` )
- : Docker (Testcontainers) — Docker 
- NativeAOT- : VS C++ workload ( CI )

## PR ( CI)

```bash
dotnet build -c Release # 0 , 0 (warnings as errors)
dotnet test tests/Mediana.UnitTests # + 
dotnet test tests/Mediana.UnitTests.Ns21 # ns2.1-
dotnet test tests/Mediana.GeneratorTests # source generator
dotnet test tests/Mediana.ContractTests.Ns21 # API TFM
dotnet test tests/Mediana.InteropTests # MediatR- + 

# 
powershell -File scripts/check-coverage.ps1 # union branch coverage >= 95%
dotnet tool restore
dotnet tool run dotnet-stryker # mutation score >= 90%
dotnet run --project benchmarks/Mediana.Benchmarks -c Release -- alloc-check # 0 B/

# ( )
dotnet run --project benchmarks/Mediana.Benchmarks -c Release -- load-check all
dotnet run --project benchmarks/Mediana.Benchmarks -c Release -- ram-check all
```

 `benchmarks/RESULTS.md` ( + ).

## 

- C# latest, `Nullable` + `TreatWarningsAsErrors` — 
- : ; canon-generic (. `RequestCallSiteCompositor` `AllocationBisectTests`) — invoke generic- canon- ~24–32 
- API — `required`- (ns2.1), ; `[RequiresDynamicCode]`/`[RequiresUnreferencedCode]`
- `// Stryker disable` (. )
- : behavioral- , ; 95% union

## PR

- : `type(scope): message` — `feat|fix|perf|refactor|test|docs|chore|bench`; breaking changes — `!` BREAKING CHANGE 
- PR — ; 
- PR-: [.github/PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md)
- `CHANGELOG.md` — 

## 

 : `alloc-check`/`load-check`/`ram-check` + `.NET`- . — [benchmarks/RESULTS.md](benchmarks/RESULTS.md).
