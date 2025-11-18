#!/bin/bash
# 자체 서명 SSL 인증서 생성 스크립트

SSL_DIR="./nginx/ssl"
mkdir -p "$SSL_DIR"

echo "자체 서명 SSL 인증서 생성 중..."

openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout "$SSL_DIR/nginx-selfsigned.key" \
  -out "$SSL_DIR/nginx-selfsigned.crt" \
  -subj "/C=KR/ST=Seoul/L=Seoul/O=DataExtraction/CN=localhost"

echo "인증서 생성 완료!"
echo "인증서 위치: $SSL_DIR/nginx-selfsigned.crt"
echo "키 위치: $SSL_DIR/nginx-selfsigned.key"
echo ""
echo "주의: 자체 서명 인증서는 브라우저에서 보안 경고가 표시됩니다."
echo "프로덕션 환경에서는 Let's Encrypt 인증서를 사용하세요."

