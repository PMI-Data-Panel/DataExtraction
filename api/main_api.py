"""메인 FastAPI 애플리케이션"""
import os
import logging
import torch
from fastapi import FastAPI
from opensearchpy import OpenSearch
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient

# 설정
from rag_query_analyzer.config import get_config, Config

# 라우터
from indexer.router import router as indexer_router
from .search_api import router as search_router
from .visualization_api import router as visualization_router

# --- 로깅 설정 ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- 전역 변수 ---
config: Config = None
os_client: OpenSearch = None
qdrant_client: QdrantClient = None
embedding_model: SentenceTransformer = None


def create_app() -> FastAPI:
    """
    FastAPI 애플리케이션 생성 및 초기화

    Returns:
        초기화된 FastAPI 앱 인스턴스
    """
    global config, os_client, qdrant_client, embedding_model

    try:
        # 설정 로드
        config = get_config()

        # FastAPI 앱 생성
        app = FastAPI(
            title="RAG Query Analyzer API",
            description="OpenSearch 기반 설문조사 데이터 색인 및 검색 API",
            version="4.0.0 (Refactored)"
        )

        # OpenSearch 클라이언트 초기화
        logger.info("OpenSearch 클라이언트 초기화 중...")
        os_client = OpenSearch(
            hosts=[{'host': config.OPENSEARCH_HOST, 'port': config.OPENSEARCH_PORT}],
            http_auth=(config.OPENSEARCH_USER, config.OPENSEARCH_PASSWORD),
            use_ssl=config.OPENSEARCH_USE_SSL,
            verify_certs=False,
            ssl_show_warn=False,
            request_timeout=30
        )
        logger.info("[OK] OpenSearch 클라이언트 초기화 완료")

        # Qdrant 클라이언트 초기화
        logger.info("Qdrant 클라이언트 초기화 중...")
        qdrant_host = os.getenv("QDRANT_HOST", "104.248.144.17")
        qdrant_port = int(os.getenv("QDRANT_PORT", "6333"))
        qdrant_client = QdrantClient(host=qdrant_host, port=qdrant_port, timeout=30)
        logger.info(f"[OK] Qdrant 클라이언트 초기화 완료: {qdrant_host}:{qdrant_port}")

        # 임베딩 모델 로딩 (KURE-v1)
        logger.info(f"임베딩 모델 로딩 중: {config.EMBEDDING_MODEL}")
        embedding_model = SentenceTransformer(
            config.EMBEDDING_MODEL,
            device='cuda' if torch.cuda.is_available() else 'cpu'
        )
        embedding_model.max_seq_length = 512
        logger.info(f"[OK] KURE-v1 모델 로드 완료 (차원: {config.EMBEDDING_DIM}, 장치: {embedding_model.device})")

        # 라우터에 의존성 주입
        indexer_router.os_client = os_client
        indexer_router.embedding_model = embedding_model
        search_router.os_client = os_client
        search_router.qdrant_client = qdrant_client
        search_router.embedding_model = embedding_model
        search_router.config = config

        # 시작 이벤트 등록
        @app.on_event("startup")
        async def startup_event():
            """애플리케이션 시작 시 연결 상태 확인"""
            logger.info("=" * 60)
            logger.info("RAG Query Analyzer API 시작")
            logger.info("=" * 60)

            # OpenSearch 연결 확인
            try:
                if os_client.ping():
                    logger.info("[OK] OpenSearch 연결 성공")
                    info = os_client.info()
                    logger.info(f"   - 버전: {info['version']['number']}")
                    logger.info(f"   - 클러스터: {info['cluster_name']}")
                else:
                    logger.warning("[WARNING] OpenSearch 연결 실패")
            except Exception as e:
                logger.warning(f"[WARNING] OpenSearch 연결 실패: {e}")

            logger.info("\n사용 가능한 엔드포인트:")
            logger.info("   - GET  /                          : API 환영 메시지")
            logger.info("   - GET  /health                    : 헬스 체크")
            logger.info("   - GET  /system-status             : 시스템 상태 확인")
            logger.info("   - POST /indexer/index-survey-data : 설문 데이터 색인")
            logger.info("   - DELETE /indexer/index/{name}    : 인덱스 삭제")
            logger.info("   - POST /search/query              : 검색 쿼리 실행")
            logger.info("   - GET  /docs                      : API 문서 (Swagger UI)")
            logger.info("   - GET  /redoc                     : API 문서 (ReDoc)")
            logger.info("=" * 60 + "\n")

        # 기본 엔드포인트
        @app.get("/", summary="API 환영 메시지")
        def read_root():
            """API 기본 정보"""
            return {
                "message": "RAG Query Analyzer API에 오신 것을 환영합니다!",
                "version": "4.0.0",
                "description": "설문조사 데이터를 OpenSearch에 색인하고 검색합니다.",
                "endpoints": {
                    "docs": "/docs",
                    "redoc": "/redoc",
                    "health": "/health",
                    "system_status": "/system-status",
                    "indexer": "/indexer",
                    "search": "/search",
                    "visualization": "/visualization"
                }
            }

        @app.get("/health", summary="헬스 체크")
        def health_check():
            """간단한 헬스 체크"""
            return {"status": "ok"}

        @app.get("/system-status", summary="시스템 상태 확인")
        def system_status():
            """시스템의 주요 구성 요소 상태를 확인합니다."""
            # OpenSearch 연결 상태
            opensearch_status = "disconnected"
            opensearch_info = None
            try:
                if os_client.ping():
                    opensearch_status = "connected"
                    info = os_client.info()
                    opensearch_info = {
                        "version": info['version']['number'],
                        "cluster_name": info['cluster_name']
                    }
            except Exception as e:
                opensearch_status = f"error: {str(e)}"

            return {
                "status": "operational",
                "components": {
                    "opensearch": {
                        "status": opensearch_status,
                        "info": opensearch_info
                    },
                    "embedding_model": {
                        "status": "loaded",
                        "model": config.EMBEDDING_MODEL,
                        "dimension": config.EMBEDDING_DIM,
                        "device": str(embedding_model.device)
                    }
                },
                "version": "4.0.0"
            }

        # 라우터 등록
        app.include_router(indexer_router)
        app.include_router(search_router)
        app.include_router(visualization_router)

        return app

    except Exception as e:
        logger.critical(f"[ERROR] 애플리케이션 초기화 실패: {e}", exc_info=True)
        raise


# 앱 인스턴스 생성
app = create_app()


if __name__ == "__main__":
    import uvicorn
    logger.info("FastAPI 서버를 시작합니다...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
