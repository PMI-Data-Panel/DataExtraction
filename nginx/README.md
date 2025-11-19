# Nginx HTTPS 설정 가이드

이 디렉토리에는 Nginx 리버스 프록시를 통한 HTTPS 설정이 포함되어 있습니다.

## 디렉토리 구조

```
nginx/
├── nginx.conf              # Nginx 메인 설정 파일
├── conf.d/
│   └── default.conf        # 서버 블록 설정 (FastAPI, Grafana, Prometheus)
├── ssl/                    # SSL 인증서 디렉토리
│   ├── nginx-selfsigned.crt
│   └── nginx-selfsigned.key
├── certbot/                # Let's Encrypt 인증서 (프로덕션)
│   ├── conf/
│   └── www/
├── generate-selfsigned-cert.sh    # Linux/Mac용 인증서 생성 스크립트
└── generate-selfsigned-cert.ps1    # Windows용 인증서 생성 스크립트
```

## 빠른 시작 (자체 서명 인증서 - 테스트용)

### Linux/Mac
```bash
chmod +x nginx/generate-selfsigned-cert.sh
./nginx/generate-selfsigned-cert.sh
```

### Windows (PowerShell)
```powershell
.\nginx\generate-selfsigned-cert.ps1
```

### 수동 생성
```bash
mkdir -p nginx/ssl
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout nginx/ssl/nginx-selfsigned.key \
  -out nginx/ssl/nginx-selfsigned.crt \
  -subj "/C=KR/ST=Seoul/L=Seoul/O=DataExtraction/CN=localhost"
```

## 프로덕션 환경 (Let's Encrypt 인증서)

### 1. 도메인 준비
- 도메인을 서버 IP에 연결 (A 레코드)
- DNS 전파 대기 (보통 몇 분~몇 시간)

### 2. Certbot으로 인증서 발급

```bash
# Certbot 컨테이너 실행
docker run -it --rm \
  -v $(pwd)/nginx/certbot/conf:/etc/letsencrypt \
  -v $(pwd)/nginx/certbot/www:/var/www/certbot \
  certbot/certbot certonly \
  --webroot \
  --webroot-path=/var/www/certbot \
  --email your-email@example.com \
  --agree-tos \
  --no-eff-email \
  -d your-domain.com \
  -d api.your-domain.com \
  -d grafana.your-domain.com \
  -d prometheus.your-domain.com
```

### 3. Nginx 설정 업데이트

`nginx/conf.d/default.conf`에서 자체 서명 인증서 주석 처리하고 Let's Encrypt 인증서 주석 해제:

```nginx
# Let's Encrypt 인증서 사용
ssl_certificate /etc/letsencrypt/live/your-domain.com/fullchain.pem;
ssl_certificate_key /etc/letsencrypt/live/your-domain.com/privkey.pem;

# 자체 서명 인증서 주석 처리
# ssl_certificate /etc/nginx/ssl/nginx-selfsigned.crt;
# ssl_certificate_key /etc/nginx/ssl/nginx-selfsigned.key;
```

### 4. 인증서 자동 갱신

Let's Encrypt 인증서는 90일마다 갱신이 필요합니다. Cron 작업 또는 systemd timer 설정:

```bash
# Cron 작업 추가
0 0 * * * docker run --rm -v $(pwd)/nginx/certbot/conf:/etc/letsencrypt -v $(pwd)/nginx/certbot/www:/var/www/certbot certbot/certbot renew --quiet && docker restart nginx
```

## 접근 방법

### 자체 서명 인증서 사용 시
- **FastAPI**: `https://your-server-ip/` 또는 `https://your-domain.com/`
- **Grafana**: `https://grafana.your-domain.com/` 또는 `https://your-server-ip/` (서브도메인 라우팅)
- **Prometheus**: `https://prometheus.your-domain.com/` 또는 `https://your-server-ip/` (서브도메인 라우팅)

### 브라우저 보안 경고
자체 서명 인증서를 사용하면 브라우저에서 보안 경고가 표시됩니다. "고급" → "안전하지 않음으로 이동"을 클릭하여 접근할 수 있습니다.

## 방화벽 설정

다음 포트를 열어야 합니다:
- **80**: HTTP (HTTPS로 리다이렉트)
- **443**: HTTPS

Google Cloud Platform의 경우:
```bash
gcloud compute firewall-rules create allow-https \
  --allow tcp:443 \
  --source-ranges 0.0.0.0/0 \
  --description "Allow HTTPS traffic"
```

## 문제 해결

### Nginx 컨테이너가 시작되지 않음
```bash
# Nginx 로그 확인
docker logs nginx

# 설정 파일 문법 검사
docker exec nginx nginx -t
```

### SSL 인증서 오류
- 인증서 파일이 올바른 경로에 있는지 확인
- 파일 권한 확인 (읽기 가능해야 함)
- 인증서 만료 확인

### 502 Bad Gateway
- 백엔드 서비스(FastAPI, Grafana, Prometheus)가 실행 중인지 확인
- Docker 네트워크 연결 확인: `docker network inspect rag_net`

## 보안 권장사항

1. **프로덕션 환경에서는 반드시 Let's Encrypt 인증서 사용**
2. **자체 서명 인증서는 테스트/개발 환경에서만 사용**
3. **불필요한 포트 노출 제거** (docker-compose.yml에서 포트 매핑 주석 처리)
4. **정기적인 인증서 갱신** (Let's Encrypt)
5. **방화벽 규칙 최소화** (필요한 IP만 허용)

