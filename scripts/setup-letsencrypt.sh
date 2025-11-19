#!/bin/bash
# Let's Encrypt 인증서 발급 스크립트

set -e

# 색상 출력
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Let's Encrypt 인증서 발급 스크립트${NC}"
echo ""

# 입력 받기
read -p "도메인 이름을 입력하세요 (예: example.com): " DOMAIN
read -p "이메일 주소를 입력하세요: " EMAIL

if [ -z "$DOMAIN" ] || [ -z "$EMAIL" ]; then
    echo -e "${RED}도메인과 이메일은 필수입니다.${NC}"
    exit 1
fi

# 프로젝트 디렉토리 확인
if [ ! -f "docker-compose.yml" ]; then
    echo -e "${RED}docker-compose.yml 파일을 찾을 수 없습니다. 프로젝트 루트 디렉토리에서 실행하세요.${NC}"
    exit 1
fi

echo -e "${YELLOW}도메인: $DOMAIN${NC}"
echo -e "${YELLOW}이메일: $EMAIL${NC}"
echo ""

# 디렉토리 생성
mkdir -p nginx/certbot/conf
mkdir -p nginx/certbot/www

# 1단계: Nginx 설정에서 HTTPS 리다이렉트 임시 비활성화
echo -e "${YELLOW}1단계: Nginx 설정 업데이트 중...${NC}"
sed -i.bak 's/return 301/# return 301/' nginx/conf.d/default.conf || true
echo -e "${GREEN}✓ 완료${NC}"

# 2단계: Nginx 재시작
echo -e "${YELLOW}2단계: Nginx 재시작 중...${NC}"
docker compose restart nginx
sleep 5
echo -e "${GREEN}✓ 완료${NC}"

# 3단계: Let's Encrypt 인증서 발급
echo -e "${YELLOW}3단계: Let's Encrypt 인증서 발급 중...${NC}"
docker run -it --rm \
  -v "$(pwd)/nginx/certbot/conf:/etc/letsencrypt" \
  -v "$(pwd)/nginx/certbot/www:/var/www/certbot" \
  certbot/certbot certonly \
  --webroot \
  --webroot-path=/var/www/certbot \
  --email "$EMAIL" \
  --agree-tos \
  --no-eff-email \
  -d "$DOMAIN"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ 인증서 발급 완료!${NC}"
else
    echo -e "${RED}✗ 인증서 발급 실패${NC}"
    exit 1
fi

# 4단계: Nginx 설정 업데이트 (Let's Encrypt 인증서 사용)
echo -e "${YELLOW}4단계: Nginx 설정 업데이트 중...${NC}"
sed -i.bak "s|# ssl_certificate /etc/letsencrypt/live/YOUR_DOMAIN/fullchain.pem;|ssl_certificate /etc/letsencrypt/live/$DOMAIN/fullchain.pem;|" nginx/conf.d/default.conf
sed -i.bak "s|# ssl_certificate_key /etc/letsencrypt/live/YOUR_DOMAIN/privkey.pem;|ssl_certificate_key /etc/letsencrypt/live/$DOMAIN/privkey.pem;|" nginx/conf.d/default.conf
sed -i.bak "s|ssl_certificate /etc/nginx/ssl/nginx-selfsigned.crt;|# ssl_certificate /etc/nginx/ssl/nginx-selfsigned.crt;|" nginx/conf.d/default.conf
sed -i.bak "s|ssl_certificate_key /etc/nginx/ssl/nginx-selfsigned.key;|# ssl_certificate_key /etc/nginx/ssl/nginx-selfsigned.key;|" nginx/conf.d/default.conf
sed -i.bak 's/# return 301/return 301/' nginx/conf.d/default.conf
echo -e "${GREEN}✓ 완료${NC}"

# 5단계: Nginx 재시작
echo -e "${YELLOW}5단계: Nginx 재시작 중...${NC}"
docker compose restart nginx
echo -e "${GREEN}✓ 완료${NC}"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Let's Encrypt 인증서 설정 완료!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "다음 URL로 접속하세요:"
echo -e "${YELLOW}https://$DOMAIN${NC}"
echo ""
echo "인증서 자동 갱신 설정:"
echo "crontab -e 를 실행하고 다음 라인을 추가하세요:"
echo "0 0 * * * cd $(pwd) && docker run --rm -v $(pwd)/nginx/certbot/conf:/etc/letsencrypt -v $(pwd)/nginx/certbot/www:/var/www/certbot certbot/certbot renew --quiet && docker compose restart nginx"

