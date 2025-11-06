"""
메인 실행 파일

이 파일은 리팩토링된 API 모듈을 실행합니다.
실제 애플리케이션 로직은 api/main_api.py에 있습니다.
"""

from api.main_api import app

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
