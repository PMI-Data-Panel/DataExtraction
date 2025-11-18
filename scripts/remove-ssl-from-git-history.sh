#!/bin/bash
# Git 히스토리에서 SSL 인증서 파일 완전히 제거 스크립트

echo "⚠️  경고: 이 스크립트는 git 히스토리를 영구적으로 수정합니다."
echo "모든 팀원과 협의한 후 실행하세요."
echo ""
read -p "계속하시겠습니까? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "취소되었습니다."
    exit 1
fi

echo "Git 히스토리에서 SSL 인증서 파일 제거 중..."

# 방법 1: git filter-branch 사용
git filter-branch --force --index-filter \
  "git rm --cached --ignore-unmatch nginx/ssl/nginx-selfsigned.key nginx/ssl/nginx-selfsigned.crt" \
  --prune-empty --tag-name-filter cat -- --all

# 참조 정리
git for-each-ref --format="%(refname)" refs/original/ | xargs -n 1 git update-ref -d

# 재압축
git reflog expire --expire=now --all
git gc --prune=now --aggressive

echo "✅ 완료! 이제 강제 푸시를 실행하세요:"
echo "   git push origin --force --all"
echo "   git push origin --force --tags"

