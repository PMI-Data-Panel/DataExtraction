"""OpenSearch 유틸리티 및 인덱스 관리"""
import logging
from typing import List, Dict, Any
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
from ..config import Config

logger = logging.getLogger(__name__)


def create_opensearch_client(config: Config) -> OpenSearch:
    """OpenSearch 클라이언트 생성"""
    client = OpenSearch(
        hosts=[{
            "host": config.OPENSEARCH_HOST,
            "port": config.OPENSEARCH_PORT
        }],
        http_auth=(config.OPENSEARCH_USER, config.OPENSEARCH_PASSWORD),
        use_ssl=config.OPENSEARCH_USE_SSL,
        verify_certs=False,
        ssl_show_warn=False,
        timeout=30,
        max_retries=3,
        retry_on_timeout=True
    )

    # 연결 테스트
    try:
        info = client.info()
        logger.info(f"✅ OpenSearch 연결 성공: v{info['version']['number']}")
        return client
    except Exception as e:
        logger.error(f"❌ OpenSearch 연결 실패: {e}")
        raise


def create_hybrid_index(client: OpenSearch, index_name: str, config: Config):
    """하이브리드 구조의 OpenSearch 인덱스 생성"""

    # kNN 플러그인 설정
    settings = {
        "index": {
            "knn": True,  # kNN 활성화
            "knn.algo_param.ef_search": 512,  # 검색 시 탐색 범위
            "number_of_shards": 2,
            "number_of_replicas": 1,
            "refresh_interval": "5s"
        },
        "analysis": {
            "analyzer": {
                "korean_analyzer": {
                    "type": "custom",
                    "tokenizer": "nori_tokenizer",
                    "filter": ["nori_part_of_speech", "lowercase", "nori_readingform"]
                }
            }
        }
    }

    # 매핑 정의
    mappings = {
        "properties": {
            "user_id": {
                "type": "keyword"
            },

            # 인구통계 필터 (정확한 term 매칭)
            "demographics": {
                "properties": {
                    "age_group": {"type": "keyword"},
                    "gender": {"type": "keyword"},
                    "region": {"type": "keyword"},
                    "occupation": {"type": "keyword"},
                    "income": {"type": "keyword"},
                    "education": {"type": "keyword"},
                    "marital_status": {"type": "keyword"},
                    "household": {"type": "keyword"}
                }
            },

            # 기타 객관식 (인구통계 아님)
            "other_objectives": {
                "type": "object",
                "enabled": True
            },

            # 주관식 응답 (nested + kNN vector)
            "subjective_responses": {
                "type": "nested",
                "properties": {
                    "q_text": {
                        "type": "text",
                        "analyzer": "korean_analyzer",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "q_code": {"type": "keyword"},
                    "q_category": {"type": "keyword"},
                    "answer_text": {
                        "type": "text",
                        "analyzer": "korean_analyzer",
                        "term_vector": "with_positions_offsets"  # 하이라이팅용
                    },
                    "answer_vector": {
                        "type": "knn_vector",
                        "dimension": config.EMBEDDING_DIM,
                        "method": {
                            "name": "hnsw",
                            "space_type": "cosinesimil",
                            "engine": config.VECTOR_ENGINE,
                            "parameters": {
                                "ef_construction": config.HNSW_EF_CONSTRUCTION,
                                "m": config.HNSW_M
                            }
                        }
                    },
                    "answer_length": {"type": "integer"}
                }
            },

            # 전체 주관식 통합 텍스트 (키워드 검색용)
            "all_subjective_text": {
                "type": "text",
                "analyzer": "korean_analyzer"
            },

            # 메타데이터
            "metadata": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "total_questions": {"type": "integer"},
                    "demographic_count": {"type": "integer"},
                    "objective_count": {"type": "integer"},
                    "subjective_count": {"type": "integer"},
                    "avg_answer_length": {"type": "float"}
                }
            }
        }
    }

    # 인덱스 생성
    if client.indices.exists(index=index_name):
        logger.info(f"👍 인덱스 '{index_name}' 이미 존재")
        return

    try:
        client.indices.create(
            index=index_name,
            body={"settings": settings, "mappings": mappings}
        )
        logger.info(f"✅ OpenSearch 하이브리드 인덱스 '{index_name}' 생성 완료")

    except Exception as e:
        logger.error(f"❌ 인덱스 생성 실패: {e}")
        raise


def bulk_index_documents(client: OpenSearch, actions: List[Dict[str, Any]]) -> tuple:
    """대량 문서 색인"""
    if not actions:
        logger.warning("⚠️ 색인할 문서가 없습니다")
        return 0, []

    try:
        success, failed = bulk(
            client,
            actions,
            raise_on_error=False,
            refresh=True,
            request_timeout=60
        )

        if failed:
            logger.warning(f"⚠️ {len(failed)}개 문서 색인 실패")
            for fail in failed[:5]:  # 처음 5개만 로깅
                logger.warning(f"   실패 예시: {fail}")

        logger.info(f"✅ 벌크 색인 완료: 성공 {success}개, 실패 {len(failed)}개")
        return success, failed

    except Exception as e:
        logger.error(f"❌ 벌크 색인 오류: {e}")
        raise


def warmup_knn_index(client: OpenSearch, index_name: str, dimension: int = 1024):
    """kNN 인덱스 워밍업 (성능 향상)"""
    try:
        client.indices.refresh(index=index_name)

        # 더미 벡터 검색으로 캐시 워밍
        dummy_vector = [0.0] * dimension

        client.search(
            index=index_name,
            body={
                "size": 1,
                "query": {
                    "nested": {
                        "path": "subjective_responses",
                        "query": {
                            "knn": {
                                "subjective_responses.answer_vector": {
                                    "vector": dummy_vector,
                                    "k": 1
                                }
                            }
                        }
                    }
                }
            }
        )

        logger.info(f"✅ kNN 인덱스 워밍업 완료: {index_name}")

    except Exception as e:
        logger.warning(f"⚠️ kNN 워밍업 실패 (무시 가능): {e}")


def delete_index(client: OpenSearch, index_name: str):
    """인덱스 삭제 (재생성 시 사용)"""
    if client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)
        logger.info(f"🗑️ 인덱스 '{index_name}' 삭제 완료")
    else:
        logger.info(f"👍 인덱스 '{index_name}' 존재하지 않음")


def get_index_stats(client: OpenSearch, index_name: str) -> Dict[str, Any]:
    """인덱스 통계 조회"""
    try:
        stats = client.indices.stats(index=index_name)
        count = client.count(index=index_name)

        return {
            "index_name": index_name,
            "doc_count": count['count'],
            "store_size": stats['_all']['total']['store']['size_in_bytes'],
            "store_size_mb": round(stats['_all']['total']['store']['size_in_bytes'] / 1024 / 1024, 2),
            "search_total": stats['_all']['total']['search']['query_total'],
            "search_time_ms": stats['_all']['total']['search']['query_time_in_millis']
        }

    except Exception as e:
        logger.error(f"❌ 통계 조회 실패: {e}")
        return {}
