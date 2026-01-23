Param(
  [string]$ComposeFile = "$(Split-Path -Parent $PSScriptRoot)\compose\docker-compose.yml"
)

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\\..")
$composePath = Resolve-Path $ComposeFile

Write-Host "Building and starting services..."
docker compose -f $composePath up -d --build

Write-Host "Services are running:"
docker compose -f $composePath ps
