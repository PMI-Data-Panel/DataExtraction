"""메인 FastAPI 애플리케이션"""
import os
import logging
import torch
from typing import Optional
from fastapi import FastAPI
from opensearchpy import OpenSearch, AsyncOpenSearch
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
import redis

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
async_os_client: Optional[AsyncOpenSearch] = None
qdrant_client: QdrantClient = None
embedding_model: SentenceTransformer = None


def create_app() -> FastAPI:
    """
    FastAPI 애플리케이션 생성 및 초기화

    Returns:
        초기화된 FastAPI 앱 인스턴스
    """
    global config, os_client, async_os_client, qdrant_client, embedding_model

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
        common_os_kwargs = dict(
            hosts=[{'host': config.OPENSEARCH_HOST, 'port': config.OPENSEARCH_PORT}],
            http_auth=(config.OPENSEARCH_USER, config.OPENSEARCH_PASSWORD),
            use_ssl=config.OPENSEARCH_USE_SSL,
            verify_certs=config.OPENSEARCH_VERIFY_CERTS,
            ssl_assert_hostname=config.OPENSEARCH_SSL_ASSERT_HOSTNAME,
            ssl_show_warn=False,
            request_timeout=60  # ⭐ 타임아웃 증가: 배치 조회 대응 (30초 → 60초)
        )
        os_client = OpenSearch(**common_os_kwargs)
        logger.info("OpenSearch client initialized with settings: %s", common_os_kwargs)
        async_os_client = AsyncOpenSearch(**common_os_kwargs)
        logger.info("AsyncOpenSearch client initialized with settings: %s", common_os_kwargs)
        logger.info("[OK] OpenSearch 클라이언트 초기화 완료 (sync/async)")

        # Qdrant 클라이언트 초기화
        logger.info("Qdrant 클라이언트 초기화 중...")
        qdrant_host = os.getenv("QDRANT_HOST", "104.248.144.17")
        qdrant_port = int(os.getenv("QDRANT_PORT", "6333"))
        qdrant_client = QdrantClient(host=qdrant_host, port=qdrant_port, timeout=30)
        logger.info(f"[OK] Qdrant 클라이언트 초기화 완료: {qdrant_host}:{qdrant_port}")

        # Redis 클라이언트 (검색 결과 캐시)
        redis_client = None
        if config.REDIS_URL:
            try:
                redis_client = redis.Redis.from_url(config.REDIS_URL, decode_responses=True)
                redis_client.ping()
                logger.info(f"[OK] Redis 연결 성공: {config.REDIS_URL}")
            except Exception as e:
                redis_client = None
                logger.warning(f"⚠️ Redis 연결 실패 ({config.REDIS_URL}): {e}")

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
        search_router.async_os_client = async_os_client
        search_router.qdrant_client = qdrant_client
        search_router.embedding_model = embedding_model
        search_router.embedding_model_factory = lambda: embedding_model
        search_router.config = config
        search_router.redis_client = redis_client
        search_router.cache_ttl_seconds = config.SEARCH_CACHE_TTL_SECONDS
        search_router.cache_max_results = config.SEARCH_CACHE_MAX_RESULTS
        search_router.cache_prefix = "search:results"

        # 시작 이벤트 등록
        @app.on_event("startup")
        async def startup_event():
            """애플리케이션 시작 시 연결 상태 확인"""
            logger.info("=" * 60)
            logger.info("RAG Query Analyzer API 시작")
            logger.info("=" * 60)
            
            # 동의어 확장기 초기화 (정적 사전 + Qdrant 동적 확장)
            try:
                from rag_query_analyzer.utils.synonym_expander import get_synonym_expander
                expander = get_synonym_expander(
                    qdrant_client=qdrant_client,
                    embedding_model=embedding_model
                )
                stats = expander.get_stats()
                logger.info("📚 동의어 확장기 정보:")
                logger.info(f"   - Terms: {stats['total_terms']}개")
                logger.info(f"   - 동의어: {stats['total_synonyms']}개")
                logger.info(f"   - 평균: {stats['avg_synonyms']:.1f}개/term")
                logger.info(f"   - 파일: {stats['loaded_from']}")
                logger.info(f"   - Qdrant 동적 확장: {'활성화' if stats['dynamic_enabled'] else '비활성화'}")
                if stats['dynamic_enabled']:
                    logger.info(f"   - 동적 캐시 크기: {stats['dynamic_cache_size']}/{stats['cache_size_limit']}")
            except Exception as e:
                logger.warning(f"⚠️  동의어 확장기 초기화 실패: {e}")
                logger.info(f"   생성 방법: python scripts/generate_synonyms.py")
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

            # Async OpenSearch 연결 확인
            try:
                if async_os_client and await async_os_client.ping():
                    logger.info("[OK] Async OpenSearch 연결 성공")
            except Exception as e:
                logger.warning(f"[WARNING] Async OpenSearch 연결 실패: {e}")

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

        @app.on_event("shutdown")
        async def shutdown_event():
            """리소스 정리"""
            logger.info("🛑 애플리케이션 종료: 리소스 정리 중...")
            try:
                if async_os_client:
                    await async_os_client.close()
                    logger.info("[OK] Async OpenSearch 클라이언트 종료")
            except Exception as e:
                logger.warning(f"⚠️ Async OpenSearch 종료 실패: {e}")

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
