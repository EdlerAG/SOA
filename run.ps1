param(
    [Parameter(Position = 0)]
    [string]$Lab = "4"
)

$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path

foreach ($n in @("2", "3", "4", "5", "6")) {
    $dir = Join-Path $root "$n\soa-lab$n"
    if (Test-Path "$dir\docker-compose.yml") {
        Push-Location $dir
        cmd /c "docker compose down --remove-orphans >nul 2>&1"
        Pop-Location
    }
}

if ($Lab -eq "stop") {
    Write-Host "All lab stacks stopped."
    exit 0
}

if ($Lab -notin @("2", "3", "4", "5", "6")) {
    Write-Error "Specify lab number: 2, 3, 4, 5, 6 or stop"
    exit 1
}

$target = Join-Path $root "$Lab\soa-lab$Lab"
Push-Location $target
try {
    if ($Lab -in @("2", "6")) {
        docker compose up --build -d --scale nlp-service=3
    } else {
        docker compose up --build -d
    }
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
} finally {
    Pop-Location
}

Write-Host ""
Write-Host "Lab $Lab started:"
Write-Host "  Gateway:  http://localhost:8080"
Write-Host "  Consul:   http://localhost:8500"
Write-Host "  RabbitMQ: http://localhost:15672 (guest/guest)"
if ($Lab -eq "4") {
    Write-Host "  Function Calling: http://localhost:8093/run-demo-suite"
    Write-Host "  MCP Server HTTP:  http://localhost:8090/mcp"
    Write-Host "  MCP Inspector UI: http://localhost:6274"
    Write-Host "  A2A Producer:     http://localhost:8091/.well-known/agent.json"
    Write-Host "  A2A Consumer:     http://localhost:8092/.well-known/agent.json"
} elseif ($Lab -eq "5") {
    Write-Host "  Demo Auth Page:   http://localhost:8080/"
    Write-Host "  Keycloak UI:      http://localhost:8180 (admin/admin, user yaroslav/kpi2026)"
} elseif ($Lab -eq "6") {
    Write-Host "  Grafana UI:       http://localhost:3000 (admin/admin)"
    Write-Host "  Prometheus:       http://localhost:9090"
    Write-Host "  Alertmanager:     http://localhost:9093"
    Write-Host "  n8n:              http://localhost:5678"
}
