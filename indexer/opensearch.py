"""
OpenSearch 인덱스 관리 모듈
"""

from opensearchpy import OpenSearch
from fastapi import HTTPException
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def create_survey_index(
    os_client: OpenSearch,
    index_name: str,
    force_recreate: bool = False,
    number_of_shards: int = 3,
    number_of_replicas: int = 1
) -> bool:
    """
    OpenSearch에 설문조사 데이터용 인덱스를 생성합니다.

    Args:
        os_client: OpenSearch 클라이언트
        index_name: 생성할 인덱스 이름
        force_recreate: 기존 인덱스 강제 삭제 여부
        number_of_shards: 샤드 개수 (데이터 규모에 따라 조정)
        number_of_replicas: 복제본 개수

    Returns:
        인덱스 생성/유지 성공 여부
    """

    # 기존 인덱스 존재 확인
    if os_client.indices.exists(index=index_name):
        if force_recreate:
            logger.warning(f"⚠️ '{index_name}' 인덱스를 강제 삭제합니다.")
            try:
                os_client.indices.delete(index=index_name)
                logger.info(f"🗑️ 기존 '{index_name}' 인덱스를 삭제했습니다.")
            except Exception as e:
                logger.error(f"🚨 기존 '{index_name}' 인덱스 삭제 실패: {e}")
                raise HTTPException(status_code=500, detail=f"기존 인덱스 삭제 실패: {e}")
        else:
            logger.info(f"ℹ️ '{index_name}' 인덱스가 이미 존재합니다. 기존 인덱스를 사용합니다.")
            return True

    logger.info(f"✨ '{index_name}' 인덱스를 새로 생성합니다.")

    # 매핑 정의
    mappings = {
        "properties": {
            "user_id": {
                "type": "keyword"
            },
            "timestamp": {
                "type": "date"
            },
            "qa_pairs": {
                "type": "nested",
                "properties": {
                    "q_code": {
                        "type": "keyword"
                    },
                    "q_text": {
                        "type": "text",
                        "analyzer": "nori_analyzer",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "q_type": {
                        "type": "keyword"
                    },
                    "answer_text": {
                        "type": "text",
                        "analyzer": "nori_analyzer",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "embedding_text": {
                        "type": "text",
                        "analyzer": "nori_analyzer"
                    },
                    "answer_vector": {
                        "type": "knn_vector",
                        "dimension": 1024,  # KURE-v1 차원
                        "method": {
                            "name": "hnsw",
                            "engine": "lucene",
                            "space_type": "cosinesimil",
                            "parameters": {
                                "ef_construction": 128,
                                "m": 24
                            }
                        }
                    }
                }
            },
        }
    }

    # 인덱스 설정
    settings = {
        "number_of_shards": number_of_shards,
        "number_of_replicas": number_of_replicas,
        "refresh_interval": "30s",  # 색인 중 성능 향상 (완료 후 변경)
        "analysis": {
            "analyzer": {
                "nori_analyzer": {
                    "type": "custom",
                    "tokenizer": "nori_tokenizer",
                    "filter": ["nori_posfilter", "lowercase", "nori_readingform"]
                }
            },
            "tokenizer": {
                "nori_tokenizer": {
                    "type": "nori_tokenizer",
                    "decompound_mode": "mixed"  # 복합어 분해 모드
                }
            },
            "filter": {
                "nori_posfilter": {
                    "type": "nori_part_of_speech",
                    # 불필요한 품사 제거 (조사, 어미, 접미사 등)
                    "stoptags": [
                        "E", "IC", "J", "MAG", "MM", "SP", "SSC",
                        "SSO", "SC", "SE", "XPN", "XSA", "XSN",
                        "XSV", "UNA", "NA", "VSV"
                    ]
                }
            }
        }
    }

    try:
        body = {
            "settings": settings,
            "mappings": mappings
        }

        response = os_client.indices.create(index=index_name, body=body)
        logger.info(f"👍 '{index_name}' 인덱스 생성 완료")
        logger.debug(f"   응답: {response}")

        return True

    except Exception as e:
        logger.error(f"🚨 '{index_name}' 인덱스 생성 실패: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"'{index_name}' 인덱스 생성 실패: {str(e)}"
        )


def update_index_refresh_interval(
    os_client: OpenSearch,
    index_name: str,
    interval: str = "1s"
) -> None:
    """
    인덱스의 refresh_interval 설정을 변경합니다.
    대량 색인 후 검색 성능을 위해 기본값으로 복구할 때 사용.

    Args:
        os_client: OpenSearch 클라이언트
        index_name: 인덱스 이름
        interval: refresh 간격 (예: "1s", "30s", "-1" for 비활성화)
    """
    try:
        os_client.indices.put_settings(
            index=index_name,
            body={"index": {"refresh_interval": interval}}
        )
        logger.info(f"✅ '{index_name}' 인덱스의 refresh_interval을 '{interval}'로 변경했습니다.")
    except Exception as e:
        logger.error(f"⚠️ refresh_interval 변경 실패: {e}")


def get_index_stats(os_client: OpenSearch, index_name: str) -> Optional[dict]:
    """
    인덱스의 통계 정보를 조회합니다.

    Args:
        os_client: OpenSearch 클라이언트
        index_name: 인덱스 이름

    Returns:
        인덱스 통계 정보 또는 None
    """
    try:
        stats = os_client.indices.stats(index=index_name)
        doc_count = stats['indices'][index_name]['total']['docs']['count']
        size_in_bytes = stats['indices'][index_name]['total']['store']['size_in_bytes']
        size_in_mb = size_in_bytes / (1024 * 1024)

        logger.info(f"📊 '{index_name}' 인덱스 통계:")
        logger.info(f"   문서 수: {doc_count:,}")
        logger.info(f"   크기: {size_in_mb:.2f} MB")

        return {
            'doc_count': doc_count,
            'size_bytes': size_in_bytes,
            'size_mb': size_in_mb
        }
    except Exception as e:
        logger.error(f"⚠️ 인덱스 통계 조회 실패: {e}")
        return None


def force_merge_index(os_client: OpenSearch, index_name: str, max_num_segments: int = 1) -> None:
    """
    인덱스를 force merge하여 검색 성능을 최적화합니다.
    대량 색인 완료 후 한 번만 실행 권장.

    Args:
        os_client: OpenSearch 클라이언트
        index_name: 인덱스 이름
        max_num_segments: 최대 세그먼트 수
    """
    try:
        logger.info(f"🔧 '{index_name}' 인덱스 force merge 시작...")
        os_client.indices.forcemerge(
            index=index_name,
            max_num_segments=max_num_segments,
            wait_for_completion=True
        )
        logger.info(f"✅ force merge 완료")
    except Exception as e:
        logger.error(f"⚠️ force merge 실패: {e}")
