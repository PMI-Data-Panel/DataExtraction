# 🚨 비밀 키 유출 즉시 대응 가이드

## 현재 상황
- SSL 인증서 비밀 키(`nginx/ssl/nginx-selfsigned.key`)가 git에 커밋되어 GitHub에 노출됨
- 커밋: `266ad2a4 https 접근 허용`

## ⚡ 즉시 조치 (5분 내)

### 1단계: Git에서 파일 제거 (이미 완료)
```bash
git rm --cached nginx/ssl/nginx-selfsigned.key nginx/ssl/nginx-selfsigned.crt
git add .gitignore
git commit -m "Remove SSL certificates from git tracking"
git push
```

### 2단계: Git 히스토리에서 완전히 제거

**Windows (PowerShell):**
```powershell
.\scripts\remove-ssl-from-git-history.ps1
```

**Linux/Mac:**
```bash
chmod +x scripts/remove-ssl-from-git-history.sh
./scripts/remove-ssl-from-git-history.sh
```

**또는 수동 실행:**
```bash
git filter-branch --force --index-filter \
  "git rm --cached --ignore-unmatch nginx/ssl/nginx-selfsigned.key nginx/ssl/nginx-selfsigned.crt" \
  --prune-empty --tag-name-filter cat -- --all

git for-each-ref --format="%(refname)" refs/original/ | xargs -n 1 git update-ref -d
git reflog expire --expire=now --all
git gc --prune=now --aggressive
```

### 3단계: 강제 푸시 (⚠️ 팀원과 협의 필수)
```bash
git push origin --force --all
git push origin --force --tags
```

### 4단계: 서버에서 새 인증서 생성
```bash
# 서버에 SSH 접속 후
cd ~/DataExtraction
rm -f nginx/ssl/nginx-selfsigned.*
mkdir -p nginx/ssl
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout nginx/ssl/nginx-selfsigned.key \
  -out nginx/ssl/nginx-selfsigned.crt \
  -subj "/C=KR/ST=Seoul/L=Seoul/O=DataExtraction/CN=localhost"

docker compose restart nginx
```

## ✅ 완료 확인

1. GitHub에서 파일이 제거되었는지 확인
2. `git log --all -- nginx/ssl/nginx-selfsigned.key` 실행 시 결과 없음 확인
3. HTTPS 접속 테스트

## 📝 참고사항

- **자체 서명 인증서는 테스트용**이므로 큰 보안 위험은 아니지만, 제거하는 것이 좋습니다.
- **프로덕션 환경**에서는 반드시 **Let's Encrypt** 사용을 권장합니다.
- 향후 비밀 키 커밋 방지를 위해 `git-secrets` 도구 설치를 고려하세요.

## 🔒 향후 예방

1. `.gitignore`에 이미 추가됨 ✅
2. CI/CD에서 자동 생성 ✅
3. `git-secrets` 설치 권장 (선택사항)

