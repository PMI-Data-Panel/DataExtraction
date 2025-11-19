"""메인 FastAPI 애플리케이션"""
import os
import logging
import torch
from typing import Optional
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from opensearchpy import OpenSearch, AsyncOpenSearch
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
import redis
import anthropic
# 설정
from rag_query_analyzer.config import get_config, Config

# 라우터
from indexer.router import router as indexer_router
from .search_api import router as search_router
from .visualization_api import router as visualization_router
from .visualization_qa_api import router as visualization_qa_router
from .search.refine_api import router as refine_router

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

        # CORS 미들웨어 추가
        # 개발 환경에서는 모든 오리진 허용, 프로덕션에서는 특정 도메인만 지정
        is_production = os.getenv("APP_ENV", "development").lower() == "production"
        
        if is_production:
            # 프로덕션: 명시적으로 허용할 오리진 지정
            allowed_origins = [
                "http://localhost:5173",
                "http://localhost:5174",
                "http://localhost:3000",
                "http://localhost:8080",
                "http://127.0.0.1:5173",
                "http://127.0.0.1:5174",
                "http://127.0.0.1:3000",
                "http://127.0.0.1:8080",

                # 프론트엔드 프로덕션 도메인
                "https://data-panel-fe-six.vercel.app",
            ]
            # 환경변수에서 추가 오리진을 가져올 수 있음
            extra_origins = os.getenv("CORS_ORIGINS", "").split(",")
            allowed_origins.extend([origin.strip() for origin in extra_origins if origin.strip()])
            allow_creds = True
        else:
            # 개발 환경: 모든 오리진 허용 (와일드카드 사용 시 credentials는 False)
            allowed_origins = ["*"]
            allow_creds = False
        
        app.add_middleware(
            CORSMiddleware,
            allow_origins=allowed_origins,
            allow_credentials=True,
            allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
            allow_headers=["*"],  # 모든 헤더 허용
            expose_headers=["*"],  # 응답 헤더 노출
        )

        # Prometheus 메트릭 수집 설정
        try:
            from prometheus_fastapi_instrumentator import Instrumentator
            instrumentator = Instrumentator()
            instrumentator.instrument(app).expose(app, endpoint="/metrics")
            logger.info("[OK] Prometheus 메트릭 수집 활성화: /metrics")
        except ImportError:
            logger.warning("⚠️ prometheus-fastapi-instrumentator가 설치되지 않았습니다. 모니터링이 비활성화됩니다.")

        # OpenSearch 클라이언트 초기화
        logger.info("OpenSearch 클라이언트 초기화 중...")
        common_os_kwargs = dict(
            hosts=[{'host': config.OPENSEARCH_HOST, 'port': config.OPENSEARCH_PORT}],
            http_auth=(config.OPENSEARCH_USER, config.OPENSEARCH_PASSWORD),
            use_ssl=config.OPENSEARCH_USE_SSL,
            verify_certs=config.OPENSEARCH_VERIFY_CERTS,
            ssl_assert_hostname=config.OPENSEARCH_SSL_ASSERT_HOSTNAME,
            ssl_show_warn=False,
            request_timeout=180  # ⭐ 타임아웃 증가: 대량 데이터 조회 대응 (60초 → 180초, 전체 데이터 약 35000개)
        )
        os_client = OpenSearch(**common_os_kwargs)
        logger.info("OpenSearch client initialized with settings: %s", common_os_kwargs)
        async_os_client = AsyncOpenSearch(**common_os_kwargs)
        logger.info("AsyncOpenSearch client initialized with settings: %s", common_os_kwargs)
        logger.info("[OK] OpenSearch 클라이언트 초기화 완료 (sync/async)")
        
        # ⭐ 인덱스 max_result_window 설정 확인 및 업데이트 (전체 데이터 약 35000개 대응)
        try:
            from rag_query_analyzer.utils.opensearch_utils import ensure_max_result_window
            default_index = config.OPENSEARCH_INDEX if hasattr(config, 'OPENSEARCH_INDEX') else "survey_responses_merged"
            if os_client.indices.exists(index=default_index):
                ensure_max_result_window(os_client, default_index, max_result_window=50000)
            else:
                logger.warning(f"⚠️ 인덱스 {default_index}가 존재하지 않아 max_result_window 설정을 건너뜁니다.")
        except Exception as e:
            logger.warning(f"⚠️ max_result_window 설정 중 오류 발생 (계속 진행): {e}")

        # Qdrant 클라이언트 초기화
        logger.info("Qdrant 클라이언트 초기화 중...")
        qdrant_host = os.getenv("QDRANT_HOST", "104.248.144.17")
        qdrant_port = int(os.getenv("QDRANT_PORT", "6333"))
        qdrant_client = QdrantClient(host=qdrant_host, port=qdrant_port, timeout=30)
        logger.info(f"[OK] Qdrant 클라이언트 초기화 완료: {qdrant_host}:{qdrant_port}")

        # Redis 클라이언트 (검색 결과 캐시 및 로그)
        redis_client = None
        if config.REDIS_URL:
            try:
                redis_client = redis.Redis.from_url(config.REDIS_URL, decode_responses=True)
                redis_client.ping()
                logger.info(f"[OK] Redis 연결 성공: {config.REDIS_URL}")
            except Exception as e:
                redis_client = None
                logger.warning(f"⚠️ Redis 연결 실패 ({config.REDIS_URL}): {e}")

        # Claude/Anthropic 클라이언트 (재사용)
        anthropic_client = None
        if config.CLAUDE_API_KEY:
            try:
                anthropic_client = anthropic.Anthropic(api_key=config.CLAUDE_API_KEY)
                logger.info("[OK] Anthropic 클라이언트 초기화 완료")
            except Exception as e:
                anthropic_client = None
                logger.warning(f"⚠️ Anthropic 클라이언트 초기화 실패: {e}")

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
        search_router.conversation_history_prefix = config.CONVERSATION_HISTORY_PREFIX
        search_router.conversation_history_ttl_seconds = config.CONVERSATION_HISTORY_TTL_SECONDS
        search_router.conversation_history_max_messages = config.CONVERSATION_HISTORY_MAX_MESSAGES
        search_router.search_history_prefix = config.SEARCH_HISTORY_PREFIX
        search_router.search_history_ttl_seconds = config.SEARCH_HISTORY_TTL_SECONDS
        search_router.search_history_max_entries = config.SEARCH_HISTORY_MAX_ENTRIES
        search_router.enable_search_summary = config.ENABLE_SEARCH_SUMMARY
        search_router.search_summary_max_results = config.SEARCH_SUMMARY_MAX_RESULTS
        search_router.search_summary_max_chars = config.SEARCH_SUMMARY_MAX_CHARS
        search_router.search_summary_model = (
            config.SEARCH_SUMMARY_MODEL or config.CLAUDE_MODEL
        )
        search_router.anthropic_client = anthropic_client

        # Visualization 라우터에 의존성 주입
        visualization_router.os_client = os_client
        visualization_qa_router.os_client = os_client


        # Refine 라우터에 의존성 주입 (search_router와 동일한 의존성 공유)
        refine_router.os_client = os_client
        refine_router.anthropic_client = anthropic_client
        refine_router.config = config
        refine_router.redis_client = redis_client
        refine_router.conversation_history_prefix = config.CONVERSATION_HISTORY_PREFIX

        # 시작 이벤트 등록
        @app.on_event("startup")
        async def startup_event():
            """애플리케이션 시작 시 연결 상태 확인"""
            import asyncio
            try:
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

                # ⭐ Panel 데이터 메모리 프리로드 (초고속 검색을 위한 최적화)
                try:
                    logger.info("=" * 60)
                    logger.info("⚡ Panel 데이터 메모리 프리로드 시작...")
                    logger.info("=" * 60)

                    from connectors.data_fetcher import DataFetcher
                    from .search_api import panel_cache

                    data_fetcher = DataFetcher(
                        opensearch_client=os_client,
                        qdrant_client=qdrant_client,
                        async_opensearch_client=async_os_client
                    )

                    await panel_cache.initialize(data_fetcher, index_name="survey_responses_merged")

                    logger.info("=" * 60)
                    logger.info("✅ Panel 데이터 프리로드 완료!")
                    logger.info(f"   - 전체 패널: {panel_cache.total_count:,}명")
                    logger.info(f"   - 로딩 시간: {panel_cache.load_time:.2f}초")
                    logger.info(f"   - 이후 검색은 0.05-0.2초 이내 응답 예상")
                    logger.info("=" * 60)
                except Exception as e:
                    logger.error(f"❌ Panel 데이터 프리로드 실패: {e}")
                    logger.warning("   → 기존 Scroll API 방식으로 작동합니다 (느림)")

                # ⭐ RAG Query Analyzer 사전 로딩 (첫 검색 응답 속도 개선)
                try:
                    logger.info("=" * 60)
                    logger.info("🧠 RAG Query Analyzer 모델 사전 로딩 시작...")
                    logger.info("=" * 60)

                    from rag_query_analyzer.config import get_config
                    from rag_query_analyzer.analyzers.main_analyzer import AdvancedRAGQueryAnalyzer
                    from .search_api import router as search_router

                    # Config 초기화
                    config = get_config()
                    search_router.config = config

                    # Analyzer 초기화 (모든 모델 로딩)
                    analyzer = AdvancedRAGQueryAnalyzer(config)
                    search_router.analyzer = analyzer

                    logger.info("=" * 60)
                    logger.info("✅ RAG Query Analyzer 모델 사전 로딩 완료!")
                    logger.info("   - SemanticModel, QueryRewriter, Reranker 등 모두 로드됨")
                    logger.info("   - 첫 검색 요청부터 빠른 응답 가능")
                    logger.info("=" * 60)
                except Exception as e:
                    logger.error(f"❌ RAG Query Analyzer 사전 로딩 실패: {e}")
                    logger.warning("   → 첫 검색 요청 시 초기화됩니다 (약간 느림)")

                logger.info("\n사용 가능한 엔드포인트:")
                logger.info("   - GET  /                          : API 환영 메시지")
                logger.info("   - GET  /health                    : 헬스 체크")
                logger.info("   - GET  /system-status             : 시스템 상태 확인")
                logger.info("   - POST /indexer/index-survey-data : 설문 데이터 색인")
                logger.info("   - DELETE /indexer/index/{name}    : 인덱스 삭제")
                logger.info("   - POST /search/nl                : 자연어 검색 쿼리 실행")
                logger.info("   - GET  /docs                      : API 문서 (Swagger UI)")
                logger.info("   - GET  /redoc                     : API 문서 (ReDoc)")
                logger.info("=" * 60 + "\n")
            except asyncio.CancelledError:
                # 정상적인 종료 과정에서 발생할 수 있는 취소 에러는 무시
                pass
            except Exception as e:
                logger.error(f"⚠️ Startup 이벤트 처리 중 오류: {e}")

        @app.on_event("shutdown")
        async def shutdown_event():
            """리소스 정리"""
            import asyncio
            logger.info("🛑 애플리케이션 종료: 리소스 정리 중...")
            try:
                if async_os_client:
                    try:
                        await asyncio.wait_for(async_os_client.close(), timeout=2.0)
                        logger.info("[OK] Async OpenSearch 클라이언트 종료")
                    except asyncio.CancelledError:
                        logger.info("[INFO] Async OpenSearch 클라이언트 종료 취소됨")
                    except asyncio.TimeoutError:
                        logger.warning("⚠️ Async OpenSearch 클라이언트 종료 타임아웃")
            except asyncio.CancelledError:
                # 정상적인 종료 과정에서 발생할 수 있는 취소 에러는 무시
                pass
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

        
        # 라우터 등록
        app.include_router(indexer_router)
        app.include_router(search_router)
        app.include_router(visualization_router)
        app.include_router(visualization_qa_router)
        app.include_router(refine_router)

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
