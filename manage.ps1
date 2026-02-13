#    Copyright 2025 FAO
# 
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
# 
#        http://www.apache.org/licenses/LICENSE-2.0
# 
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
# 
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/
# 
#    manage.ps1

param (
    [string]$Mode = "prod" # Default is now PROD
)

$ErrorActionPreference = "Stop"
$DockerDir = "docker"
$RootEnv = ".env"
$DockerEnv = Join-Path $DockerDir ".env"

# 1. Check for .env file location
if (Test-Path $DockerEnv) {
    $EnvPath = ".env" 
} elseif (Test-Path $RootEnv) {
    Write-Warning ".env found in Root but scripts run inside /docker. Please move .env to /docker/ folder."
    exit 1
} else {
    Write-Error "No .env file found! Please create one in the docker/ directory."
    exit 1
}

# 2. Ensure Logs Exist
if (-not (Test-Path "logs\api")) { New-Item -Path "logs\api" -ItemType Directory -Force | Out-Null }
if (-not (Test-Path "logs\worker")) { New-Item -Path "logs\worker" -ItemType Directory -Force | Out-Null }

# 3. Execution
Write-Host "Entering $DockerDir directory..."
Push-Location $DockerDir

try {
    switch ($Mode) {
        "dev" {
            Write-Host "------------------------------------------------" -ForegroundColor Cyan
            Write-Host "   STARTING DYNASTORE (DEV MODE)                " -ForegroundColor Cyan
            Write-Host "------------------------------------------------" -ForegroundColor Cyan
            
            # Start Dev (Base + Dev Override)
            docker compose --env-file .env -f docker-compose.yml -f docker-compose.dev.yml -p dynastore up -d --build

            Write-Host ""
            Write-Host "✅ Dev Services started." -ForegroundColor Green
            Write-Host "👉 PRESS [ENTER] TO STOP SERVICES AND EXIT" -ForegroundColor Yellow
            $null = Read-Host
            
            Write-Host "Stopping services..." -ForegroundColor Cyan
            docker compose --env-file .env -f docker-compose.yml -f docker-compose.dev.yml -p dynastore stop
        }
        "prod" {
            Write-Host "------------------------------------------------" -ForegroundColor Magenta
            Write-Host "   STARTING DYNASTORE (PRODUCTION MODE)         " -ForegroundColor Magenta
            Write-Host "------------------------------------------------" -ForegroundColor Magenta
            
            # Start Prod (Base Only)
            docker compose --env-file .env -f docker-compose.yml -p dynastore up -d --build

            Write-Host ""
            Write-Host "✅ Production Services started (Optimized)." -ForegroundColor Green
            Write-Host "   Data is safe. Database is running."
            Write-Host ""
            Write-Host "👉 PRESS [ENTER] TO STOP SERVICES AND EXIT" -ForegroundColor Yellow
            Write-Host "   (Do not close this window with 'X' or services will keep running)" -ForegroundColor Gray
            
            # Wait for user input
            $null = Read-Host

            # Graceful Stop
            Write-Host "Stopping services (preserving data)..." -ForegroundColor Cyan
            docker compose --env-file .env -f docker-compose.yml -p dynastore stop
            Write-Host "Services stopped. Bye!" -ForegroundColor Green
            Start-Sleep -Seconds 1
        }
        "down" {
            Write-Host "Removing services..." -ForegroundColor Yellow
            docker compose --env-file .env -f docker-compose.yml -p dynastore down
        }
    }
}
catch {
    Write-Error "An error occurred: $_"
    Write-Host "Press Enter to exit..."
    $null = Read-Host
}
finally {
    Write-Host "Returning to original directory..."
    Pop-Location
}