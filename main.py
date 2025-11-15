"""
메인 실행 파일

이 파일은 리팩토링된 API 모듈을 실행합니다.
실제 애플리케이션 로직은 api/main_api.py에 있습니다.
"""

if __name__ == "__main__":
    import uvicorn
    # reload=False로 명시적으로 설정하여 파일 변경 감지 비활성화
    uvicorn.run("api.main_api:app", host="0.0.0.0", port=8000, reload=False)
