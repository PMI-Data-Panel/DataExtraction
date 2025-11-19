import os
import logging
from dataclasses import dataclass, field
from typing import List
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

@dataclass
class Config:
    """RAG Query Analyzer 설정"""
    # --- 환경 ---
    APP_ENV: str = os.getenv("APP_ENV", "development")

    # --- OpenSearch ---
    OPENSEARCH_HOST: str = os.getenv("OPENSEARCH_HOST", "localhost")
    OPENSEARCH_PORT: int = int(os.getenv("OPENSEARCH_PORT", "9200"))
    OPENSEARCH_USER: str = os.getenv("OPENSEARCH_USER", "admin")
    OPENSEARCH_USERNAME: str = os.getenv("OPENSEARCH_USERNAME", os.getenv("OPENSEARCH_USER", "admin"))  # OPENSEARCH_USER와 동일하게 사용
    OPENSEARCH_PASSWORD: str = os.getenv("OPENSEARCH_PASSWORD", "admin")
    OPENSEARCH_USE_SSL: bool = os.getenv("OPENSEARCH_USE_SSL", "true").lower() == "true"
    OPENSEARCH_VERIFY_CERTS: bool = os.getenv("OPENSEARCH_VERIFY_CERTS", "false").lower() == "true"
    OPENSEARCH_SSL_ASSERT_HOSTNAME: bool = os.getenv("OPENSEARCH_SSL_ASSERT_HOSTNAME", "false").lower() == "true"
    OPENSEARCH_VERSION: float = float(os.getenv("OPENSEARCH_VERSION", "2.11"))
    
    # --- OpenSearch Dashboards ---
    # OpenSearch와 동일한 호스트 사용, 포트만 다름 (기본값: 5601)
    OPENSEARCH_DASHBOARDS_HOST: str = os.getenv("OPENSEARCH_DASHBOARDS_HOST", os.getenv("OPENSEARCH_HOST", "localhost"))
    OPENSEARCH_DASHBOARDS_PORT: int = int(os.getenv("OPENSEARCH_DASHBOARDS_PORT", "5601"))
    OPENSEARCH_DASHBOARDS_USE_SSL: bool = os.getenv("OPENSEARCH_DASHBOARDS_USE_SSL", os.getenv("OPENSEARCH_USE_SSL", "false")).lower() == "true"

    # --- Elasticsearch (레거시) ---
    ES_HOST: str = os.getenv("ES_HOST", "http://localhost:9200")

    # --- Embedding & Reranking ---
    EMBEDDING_MODEL: str = os.getenv("EMBEDDING_MODEL", "nlpai-lab/KURE-v1")
    EMBEDDING_DIM: int = int(os.getenv("EMBEDDING_DIM", 1024))
    ENABLE_RERANKING: bool = os.getenv("ENABLE_RERANKING", "true").lower() == "true"
    RERANKER_MODELS: List[str] = field(default_factory=lambda: os.getenv("RERANKER_MODELS", "BAAI/bge-reranker-base,cross-encoder/ms-marco-MiniLM-L-12-v2").split(","))
    RERANK_TOP_K: int = int(os.getenv("RERANK_TOP_K", 5))

    # --- 벡터 검색 엔진 ---
    VECTOR_ENGINE: str = os.getenv("VECTOR_ENGINE", "nmslib")  # nmslib 또는 faiss
    HNSW_EF_CONSTRUCTION: int = int(os.getenv("HNSW_EF_CONSTRUCTION", "128"))
    HNSW_M: int = int(os.getenv("HNSW_M", "16"))

    # --- Claude API ---
    CLAUDE_API_KEY: str = os.getenv("CLAUDE_API_KEY")
    CLAUDE_MODEL: str = os.getenv("CLAUDE_MODEL", "claude-3-7-sonnet-latest")
    CLAUDE_MODEL_FAST: str = os.getenv("CLAUDE_MODEL_FAST", "claude-3-7-sonnet-latest")
    CLAUDE_MAX_TOKENS: int = int(os.getenv("CLAUDE_MAX_TOKENS", 1500))
    CLAUDE_TEMPERATURE: float = float(os.getenv("CLAUDE_TEMPERATURE", 0.1))
    ENABLE_CLAUDE_ANALYZER: bool = os.getenv("ENABLE_CLAUDE_ANALYZER", "true").lower() == "true"
    CLAUDE_ANALYZER_TIMEOUT: int = int(os.getenv("CLAUDE_ANALYZER_TIMEOUT", "3"))

    # --- 시스템 동작 ---
    ENABLE_CACHE: bool = os.getenv("ENABLE_CACHE", "true").lower() == "true"
    CACHE_SIZE: int = int(os.getenv("CACHE_SIZE", 100))
    CACHE_SIMILARITY_THRESHOLD: float = float(os.getenv("CACHE_SIMILARITY_THRESHOLD", 0.95))
    ENABLE_ASYNC: bool = os.getenv("ENABLE_ASYNC", "true").lower() == "true"
    MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", 4))
    MAX_REWRITTEN_QUERIES: int = int(os.getenv("MAX_REWRITTEN_QUERIES", 2))
    QUERY_LOG_FILE: str = os.getenv("QUERY_LOG_FILE", "query_performance.json")
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    SEARCH_CACHE_TTL_SECONDS: int = int(os.getenv("SEARCH_CACHE_TTL_SECONDS", "300"))
    SEARCH_CACHE_MAX_RESULTS: int = int(os.getenv("SEARCH_CACHE_MAX_RESULTS", "12000"))
    CONVERSATION_HISTORY_PREFIX: str = os.getenv("CONVERSATION_HISTORY_PREFIX", "chat:session")
    CONVERSATION_HISTORY_TTL_SECONDS: int = int(os.getenv("CONVERSATION_HISTORY_TTL_SECONDS", "604800"))
    CONVERSATION_HISTORY_MAX_MESSAGES: int = int(os.getenv("CONVERSATION_HISTORY_MAX_MESSAGES", "200"))
    SEARCH_HISTORY_PREFIX: str = os.getenv("SEARCH_HISTORY_PREFIX", "search:history")
    SEARCH_HISTORY_TTL_SECONDS: int = int(os.getenv("SEARCH_HISTORY_TTL_SECONDS", "2592000"))
    SEARCH_HISTORY_MAX_ENTRIES: int = int(os.getenv("SEARCH_HISTORY_MAX_ENTRIES", "500"))
    ENABLE_SEARCH_SUMMARY: bool = os.getenv("ENABLE_SEARCH_SUMMARY", "true").lower() == "true"
    SEARCH_SUMMARY_MODEL: str = os.getenv("SEARCH_SUMMARY_MODEL", "")
    SEARCH_SUMMARY_MAX_RESULTS: int = int(os.getenv("SEARCH_SUMMARY_MAX_RESULTS", "10"))
    SEARCH_SUMMARY_MAX_CHARS: int = int(os.getenv("SEARCH_SUMMARY_MAX_CHARS", "16000"))
    
    # --- 검색 파라미터 ---
    INITIAL_SEARCH_SIZE: int = int(os.getenv("INITIAL_SEARCH_SIZE", 2000))
    FINAL_RESULT_SIZE: int = int(os.getenv("FINAL_RESULT_SIZE", 6000))

    # --- 인덱스 이름 ---
    WELCOME_INDEX: str = os.getenv("WELCOME_INDEX", "welcome_all")  # 기본값: welcome_all (통합 인덱스)

    def validate(self):
        if self.ENABLE_CLAUDE_ANALYZER and not self.CLAUDE_API_KEY:
            logger.warning("ENABLE_CLAUDE_ANALYZER가 활성화되었지만 CLAUDE_API_KEY가 설정되지 않아 자동으로 비활성화합니다.")
            self.ENABLE_CLAUDE_ANALYZER = False
        if self.CLAUDE_ANALYZER_TIMEOUT <= 0:
            raise ValueError("CLAUDE_ANALYZER_TIMEOUT 값은 1 이상이어야 합니다.")
        if self.SEARCH_CACHE_TTL_SECONDS < 0:
            raise ValueError("SEARCH_CACHE_TTL_SECONDS 값은 0 이상이어야 합니다.")
        if self.SEARCH_CACHE_MAX_RESULTS <= 0:
            raise ValueError("SEARCH_CACHE_MAX_RESULTS 값은 1 이상이어야 합니다.")
        if self.FINAL_RESULT_SIZE <= 0:
            raise ValueError("FINAL_RESULT_SIZE 값은 1 이상이어야 합니다.")
        if self.SEARCH_CACHE_MAX_RESULTS < self.FINAL_RESULT_SIZE:
            logger.warning(
                "SEARCH_CACHE_MAX_RESULTS(%s)이 FINAL_RESULT_SIZE(%s)보다 작아 FINAL_RESULT_SIZE를 조정합니다.",
                self.SEARCH_CACHE_MAX_RESULTS,
                self.FINAL_RESULT_SIZE,
            )
            self.FINAL_RESULT_SIZE = self.SEARCH_CACHE_MAX_RESULTS
        if self.CONVERSATION_HISTORY_TTL_SECONDS < 0:
            raise ValueError("CONVERSATION_HISTORY_TTL_SECONDS 값은 0 이상이어야 합니다.")
        if self.CONVERSATION_HISTORY_MAX_MESSAGES < 0:
            raise ValueError("CONVERSATION_HISTORY_MAX_MESSAGES 값은 0 이상이어야 합니다.")
        if self.SEARCH_HISTORY_TTL_SECONDS < 0:
            raise ValueError("SEARCH_HISTORY_TTL_SECONDS 값은 0 이상이어야 합니다.")
        if self.SEARCH_HISTORY_MAX_ENTRIES < 0:
            raise ValueError("SEARCH_HISTORY_MAX_ENTRIES 값은 0 이상이어야 합니다.")
        if self.SEARCH_SUMMARY_MAX_RESULTS <= 0:
            raise ValueError("SEARCH_SUMMARY_MAX_RESULTS 값은 1 이상이어야 합니다.")
        if self.SEARCH_SUMMARY_MAX_CHARS <= 0:
            raise ValueError("SEARCH_SUMMARY_MAX_CHARS 값은 1 이상이어야 합니다.")

# 싱글턴 인스턴스
_config_instance: Config = None

def get_config() -> Config:
    """설정 객체 반환"""
    global _config_instance
    if _config_instance is None:
        _config_instance = Config()
    return _config_instance