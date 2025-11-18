# 보안 대응 가이드: SSL 비밀 키 유출

## 발견된 문제
- SSL 인증서 비밀 키(`nginx/ssl/nginx-selfsigned.key`)가 git 저장소에 커밋되어 원격 저장소에 노출됨

## 즉시 조치 사항

### 1. Git 히스토리에서 완전히 제거
```bash
# 방법 1: git filter-branch 사용 (권장)
git filter-branch --force --index-filter \
  "git rm --cached --ignore-unmatch nginx/ssl/nginx-selfsigned.key nginx/ssl/nginx-selfsigned.crt" \
  --prune-empty --tag-name-filter cat -- --all

# 방법 2: BFG Repo-Cleaner 사용 (더 빠름)
# https://rtyley.github.io/bfg-repo-cleaner/
# java -jar bfg.jar --delete-files nginx-selfsigned.key
# java -jar bfg.jar --delete-files nginx-selfsigned.crt
```

### 2. 강제 푸시 (주의: 팀원과 협의 필요)
```bash
git push origin --force --all
git push origin --force --tags
```

### 3. 새로운 SSL 인증서 생성
```bash
# 로컬에서
.\nginx\generate-selfsigned-cert.ps1

# 서버에서 (CI/CD가 자동 생성하지만 수동으로도 가능)
cd ~/DataExtraction
mkdir -p nginx/ssl
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout nginx/ssl/nginx-selfsigned.key \
  -out nginx/ssl/nginx-selfsigned.crt \
  -subj "/C=KR/ST=Seoul/L=Seoul/O=DataExtraction/CN=localhost"
```

### 4. 서버에서 인증서 교체
```bash
# 서버에 SSH 접속 후
cd ~/DataExtraction
# 기존 인증서 삭제
rm -f nginx/ssl/nginx-selfsigned.*
# 새 인증서 생성 (위 명령어 실행)
# Nginx 재시작
docker compose restart nginx
```

## 보안 모범 사항

### ✅ 이미 적용된 사항
- `.gitignore`에 SSL 인증서 파일 추가
- CI/CD에서 자동 인증서 생성

### ⚠️ 추가 권장 사항
1. **프로덕션 환경에서는 Let's Encrypt 사용**
   - 자체 서명 인증서는 테스트용
   - Let's Encrypt는 무료이고 브라우저에서 신뢰됨

2. **GitHub Secrets 활용**
   - 민감한 정보는 절대 코드에 포함하지 않음
   - 환경 변수는 GitHub Secrets 사용

3. **정기적인 보안 감사**
   - `git-secrets` 도구 설치
   - pre-commit hook으로 비밀 키 커밋 방지

## 참고
- 자체 서명 인증서는 테스트용이므로 큰 보안 위험은 아니지만, git 히스토리에서 제거하는 것이 좋습니다.
- 프로덕션 환경에서는 반드시 Let's Encrypt 또는 상용 인증서를 사용하세요.

