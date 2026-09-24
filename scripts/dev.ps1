param(
    [ValidateSet("install", "lint", "test", "run", "hooks")]
    [string]$Task = "test"
)
$ErrorActionPreference = "Stop"
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    switch ($Task) {
        "install" { & uv sync --frozen --extra dev }
        "lint" {
            & uv run --frozen ruff check .
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
            & uv run --frozen ruff format --check .
        }
        "test" { & uv run --frozen pytest }
        "run" { & uv run --frozen python -m ai_decision_platform }
        "hooks" { & uv run --frozen pre-commit install }
    }
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} finally { Pop-Location }
