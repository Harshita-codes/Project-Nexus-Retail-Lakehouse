[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$repositoryRoot = Split-Path -Parent $PSScriptRoot

function Invoke-SbtTest {
    param([Parameter(Mandatory = $true)][string]$ProjectPath)

    $workingDirectory = Join-Path $repositoryRoot $ProjectPath
    if (-not (Test-Path -LiteralPath $workingDirectory -PathType Container)) {
        throw "Test project directory does not exist: $workingDirectory"
    }

    Write-Output "Running sbt test in $ProjectPath"
    Push-Location -LiteralPath $workingDirectory
    try {
        & sbt test
        if ($LASTEXITCODE -ne 0) {
            throw "sbt test failed in $ProjectPath with exit code $LASTEXITCODE"
        }
    }
    finally {
        Pop-Location
    }
}

Invoke-SbtTest 'Data Ingestion\Batch\test-harness'

$isWindows = $env:OS -eq 'Windows_NT'
$winutilsPath = if ($env:HADOOP_HOME) {
    Join-Path $env:HADOOP_HOME 'bin\winutils.exe'
} else {
    $null
}
$hasWinutils = $winutilsPath -and (Test-Path -LiteralPath $winutilsPath -PathType Leaf)

if ($isWindows -and -not $hasWinutils) {
    Write-Warning 'Skipping the local Spark batch integration tests: Hadoop winutils.exe is unavailable. GitHub Actions runs this suite on Linux.'
} else {
    Invoke-SbtTest 'Data Ingestion\Batch'
}

Invoke-SbtTest 'Data Ingestion\Producer'
Invoke-SbtTest 'Data Ingestion\Streaming'
Write-Output 'All runnable local test suites passed.'
