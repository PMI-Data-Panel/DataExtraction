import os
from dataclasses import dataclass, field
from typing import List
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Config:
    """RAG Query Analyzer 설정"""
    # --- 환경 ---
    APP_ENV: str = os.getenv("APP_ENV", "development")

    # --- OpenSearch ---
    OPENSEARCH_HOST: str = os.getenv("OPENSEARCH_HOST", "localhost")
    OPENSEARCH_PORT: int = int(os.getenv("OPENSEARCH_PORT", "9200"))
    OPENSEARCH_USER: str = os.getenv("OPENSEARCH_USER", "admin")
    OPENSEARCH_PASSWORD: str = os.getenv("OPENSEARCH_PASSWORD", "admin")
    OPENSEARCH_USE_SSL: bool = os.getenv("OPENSEARCH_USE_SSL", "true").lower() == "true"
    OPENSEARCH_VERIFY_CERTS: bool = os.getenv("OPENSEARCH_VERIFY_CERTS", "false").lower() == "true"
    OPENSEARCH_SSL_ASSERT_HOSTNAME: bool = os.getenv("OPENSEARCH_SSL_ASSERT_HOSTNAME", "false").lower() == "true"
    OPENSEARCH_VERSION: float = float(os.getenv("OPENSEARCH_VERSION", "2.11"))

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

    # --- 시스템 동작 ---
    ENABLE_CACHE: bool = os.getenv("ENABLE_CACHE", "true").lower() == "true"
    CACHE_SIZE: int = int(os.getenv("CACHE_SIZE", 100))
    CACHE_SIMILARITY_THRESHOLD: float = float(os.getenv("CACHE_SIMILARITY_THRESHOLD", 0.95))
    ENABLE_ASYNC: bool = os.getenv("ENABLE_ASYNC", "true").lower() == "true"
    MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", 4))
    MAX_REWRITTEN_QUERIES: int = int(os.getenv("MAX_REWRITTEN_QUERIES", 2))
    QUERY_LOG_FILE: str = os.getenv("QUERY_LOG_FILE", "query_performance.json")
    
    # --- 검색 파라미터 ---
    INITIAL_SEARCH_SIZE: int = int(os.getenv("INITIAL_SEARCH_SIZE", 50))
    FINAL_RESULT_SIZE: int = int(os.getenv("FINAL_RESULT_SIZE", 10))

    def validate(self):
        if not self.CLAUDE_API_KEY:
            raise ValueError("CLAUDE_API_KEY가 설정되지 않았습니다.")

# 싱글턴 인스턴스
_config_instance: Config = None

def get_config() -> Config:
    """설정 객체 반환"""
    global _config_instance
    if _config_instance is None:
        _config_instance = Config()
    return _config_instance