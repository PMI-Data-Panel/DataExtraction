"""
Connectors Module - 클라우드 데이터베이스 연결 관리

이 모듈은 OpenSearch, Qdrant 등 외부 클라우드 서비스와의 연결을 담당합니다.
"""

from .opensearch_cloud import (
    OpenSearchCloudConnector,
    OpenSearchConfig,
    create_opensearch_client,
)

from .qdrant_cloud import (
    QdrantCloudConnector,
    QdrantConfig,
    create_qdrant_client,
)

from .hybrid_searcher import (
    OpenSearchHybridQueryBuilder,
    calculate_rrf_score
)

from .data_fetcher import DataFetcher

__all__ = [
    # OpenSearch 클래스
    'OpenSearchCloudConnector',
    'OpenSearchConfig',
    'create_opensearch_client',

    # Qdrant 클래스
    'QdrantCloudConnector',
    'QdrantConfig',
    'create_qdrant_client',

    # 하이브리드 검색
    'OpenSearchHybridQueryBuilder',
    'calculate_rrf_score',

    # 데이터 페처
    'DataFetcher',
]
