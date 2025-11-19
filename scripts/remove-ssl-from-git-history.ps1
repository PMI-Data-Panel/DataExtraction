# PowerShell 스크립트: Git 히스토리에서 SSL 인증서 파일 완전히 제거

Write-Host "⚠️  경고: 이 스크립트는 git 히스토리를 영구적으로 수정합니다." -ForegroundColor Yellow
Write-Host "모든 팀원과 협의한 후 실행하세요." -ForegroundColor Yellow
Write-Host ""

$confirm = Read-Host "계속하시겠습니까? (yes/no)"

if ($confirm -ne "yes") {
    Write-Host "취소되었습니다." -ForegroundColor Red
    exit 1
}

Write-Host "Git 히스토리에서 SSL 인증서 파일 제거 중..." -ForegroundColor Cyan

# git filter-branch 실행
git filter-branch --force --index-filter `
  "git rm --cached --ignore-unmatch nginx/ssl/nginx-selfsigned.key nginx/ssl/nginx-selfsigned.crt" `
  --prune-empty --tag-name-filter cat -- --all

if ($LASTEXITCODE -eq 0) {
    Write-Host "참조 정리 중..." -ForegroundColor Cyan
    
    # 참조 정리
    git for-each-ref --format="%(refname)" refs/original/ | ForEach-Object {
        git update-ref -d $_
    }
    
    # 재압축
    Write-Host "재압축 중..." -ForegroundColor Cyan
    git reflog expire --expire=now --all
    git gc --prune=now --aggressive
    
    Write-Host "✅ 완료!" -ForegroundColor Green
    Write-Host "이제 강제 푸시를 실행하세요:" -ForegroundColor Yellow
    Write-Host "   git push origin --force --all" -ForegroundColor Cyan
    Write-Host "   git push origin --force --tags" -ForegroundColor Cyan
} else {
    Write-Host "❌ 오류 발생!" -ForegroundColor Red
    Write-Host "BFG Repo-Cleaner 사용을 고려하세요: https://rtyley.github.io/bfg-repo-cleaner/" -ForegroundColor Yellow
}

