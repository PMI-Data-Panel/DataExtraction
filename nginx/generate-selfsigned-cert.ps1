# PowerShell 스크립트: 자체 서명 SSL 인증서 생성

$sslDir = ".\nginx\ssl"
if (-not (Test-Path $sslDir)) {
    New-Item -ItemType Directory -Path $sslDir -Force | Out-Null
}

Write-Host "자체 서명 SSL 인증서 생성 중..." -ForegroundColor Yellow

# OpenSSL이 설치되어 있는지 확인
$opensslPath = Get-Command openssl -ErrorAction SilentlyContinue
if (-not $opensslPath) {
    Write-Host "오류: OpenSSL이 설치되어 있지 않습니다." -ForegroundColor Red
    Write-Host "Windows에서 OpenSSL 설치 방법:" -ForegroundColor Yellow
    Write-Host "1. Chocolatey: choco install openssl" -ForegroundColor Cyan
    Write-Host "2. 또는 Git for Windows에 포함된 OpenSSL 사용" -ForegroundColor Cyan
    exit 1
}

# 인증서 생성
openssl req -x509 -nodes -days 365 -newkey rsa:2048 `
  -keyout "$sslDir\nginx-selfsigned.key" `
  -out "$sslDir\nginx-selfsigned.crt" `
  -subj "/C=KR/ST=Seoul/L=Seoul/O=DataExtraction/CN=localhost"

if ($LASTEXITCODE -eq 0) {
    Write-Host "인증서 생성 완료!" -ForegroundColor Green
    Write-Host "인증서 위치: $sslDir\nginx-selfsigned.crt" -ForegroundColor Cyan
    Write-Host "키 위치: $sslDir\nginx-selfsigned.key" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "주의: 자체 서명 인증서는 브라우저에서 보안 경고가 표시됩니다." -ForegroundColor Yellow
    Write-Host "프로덕션 환경에서는 Let's Encrypt 인증서를 사용하세요." -ForegroundColor Yellow
} else {
    Write-Host "인증서 생성 실패!" -ForegroundColor Red
    exit 1
}

