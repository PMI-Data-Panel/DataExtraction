"""
welcome_1st와 welcome_2nd를 조인하여 통합 인덱스 생성

사전 조인의 장점:
1. RRF 결합 정확도 상승 - 같은 user_id 기반으로 쉽게 병합
2. 필터링 효율 향상 - must 필터를 한 번에 적용 가능
3. 검색 속도 개선 - 조인 비용이 사라짐
4. 결과 일관성 확보 - 필터 누락/불일치 문제 해결
"""

import logging
from opensearchpy import OpenSearch
from typing import Dict, Any, Optional
import sys
import os
from opensearchpy.helpers import bulk

# 프로젝트 루트를 Python 경로에 추가
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, PROJECT_ROOT)

from rag_query_analyzer.config import get_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_joined_index_mapping() -> Dict[str, Any]:
    """조인된 인덱스의 매핑 정의"""
    return {
        "mappings": {
            "properties": {
                "user_id": {
                    "type": "keyword"
                },
                "metadata": {
                    "type": "object",
                    "properties": {
                        # welcome_1st에서 가져온 필드
                        "age_group": {
                            "type": "keyword"
                        },
                        "gender": {
                            "type": "keyword"
                        },
                        "birth_year": {
                            "type": "keyword"
                        },
                        "region": {
                            "type": "keyword"
                        },
                        # welcome_2nd에서 가져온 필드
                        "occupation": {
                            "type": "keyword"
                        },
                        "job_category": {
                            "type": "keyword"
                        }
                    }
                },
                "qa_pairs": {
                    "type": "nested",
                    "properties": {
                        "q_text": {
                            "type": "text",
                            "analyzer": "nori"
                        },
                        "q_code": {
                            "type": "keyword"
                        },
                        "answer": {
                            "type": "text",
                            "analyzer": "nori"
                        },
                        "answer_text": {
                            "type": "text",
                            "analyzer": "nori"
                        },
                        "answer_vector": {
                            "type": "dense_vector",
                            "dims": 1024,
                            "index": True,
                            "similarity": "cosine"
                        }
                    }
                },
                "timestamp": {
                    "type": "date"
                },
                # 원본 인덱스 정보 (디버깅용)
                "source_indices": {
                    "type": "keyword"
                }
            }
        },
        "settings": {
            "number_of_shards": 3,
            "number_of_replicas": 1,
            "refresh_interval": "30s",
            "analysis": {
                "analyzer": {
                    "nori": {
                        "type": "nori",
                        "decompound_mode": "mixed"
                    }
                }
            }
        }
    }


def join_documents(
    os_client: OpenSearch,
    source_index_1st: str = "s_welcome_1st",
    source_index_2nd: str = "s_welcome_2nd",
    target_index: str = "s_users_joined",
    batch_size: int = 1000
) -> Dict[str, Any]:
    """
    welcome_1st와 welcome_2nd를 조인하여 통합 인덱스 생성
    
    Args:
        os_client: OpenSearch 클라이언트
        source_index_1st: welcome_1st 인덱스 이름
        source_index_2nd: welcome_2nd 인덱스 이름
        target_index: 생성할 조인된 인덱스 이름
        batch_size: 배치 처리 크기
    
    Returns:
        조인 통계 정보
    """
    stats = {
        "total_processed": 0,
        "successful_joins": 0,
        "missing_1st": 0,
        "missing_2nd": 0,
        "errors": 0
    }
    
    # 1. 인덱스 존재 확인
    if not os_client.indices.exists(index=source_index_1st):
        raise ValueError(f"소스 인덱스 '{source_index_1st}'가 존재하지 않습니다.")
    if not os_client.indices.exists(index=source_index_2nd):
        raise ValueError(f"소스 인덱스 '{source_index_2nd}'가 존재하지 않습니다.")
    
    # 2. 타겟 인덱스 생성 (이미 존재하면 삭제 후 재생성)
    if os_client.indices.exists(index=target_index):
        logger.warning(f"⚠️ 기존 인덱스 '{target_index}' 삭제 중...")
        os_client.indices.delete(index=target_index)
    
    logger.info(f"✨ 조인된 인덱스 '{target_index}' 생성 중...")
    mapping = create_joined_index_mapping()
    os_client.indices.create(index=target_index, body=mapping)
    logger.info(f"✅ 인덱스 생성 완료")
    
    # 3. welcome_1st의 모든 user_id 수집
    logger.info(f"📊 '{source_index_1st}'에서 user_id 수집 중...")
    user_ids_1st = set()
    
    query = {
        "size": 0,
        "aggs": {
            "user_ids": {
                "terms": {
                    "field": "user_id",
                    "size": 10000  # 최대 10000개 (필요시 증가)
                }
            }
        }
    }
    
    response = os_client.search(index=source_index_1st, body=query)
    for bucket in response['aggregations']['user_ids']['buckets']:
        user_ids_1st.add(bucket['key'])
    
    logger.info(f"✅ '{source_index_1st}'에서 {len(user_ids_1st)}개의 user_id 발견")
    
    # 4. welcome_2nd의 모든 user_id 수집
    logger.info(f"📊 '{source_index_2nd}'에서 user_id 수집 중...")
    user_ids_2nd = set()
    
    response = os_client.search(index=source_index_2nd, body=query)
    for bucket in response['aggregations']['user_ids']['buckets']:
        user_ids_2nd.add(bucket['key'])
    
    logger.info(f"✅ '{source_index_2nd}'에서 {len(user_ids_2nd)}개의 user_id 발견")
    
    # 5. 모든 user_id의 합집합 (양쪽 인덱스에 있는 모든 사용자)
    all_user_ids = user_ids_1st.union(user_ids_2nd)
    logger.info(f"📊 총 {len(all_user_ids)}개의 고유 user_id 발견")
    
    # 6. 배치 단위로 조인 및 인덱싱
    logger.info(f"🔄 조인 및 인덱싱 시작 (배치 크기: {batch_size})...")
    
    user_id_list = list(all_user_ids)
    for i in range(0, len(user_id_list), batch_size):
        batch = user_id_list[i:i + batch_size]
        batch_docs = []
        
        for user_id in batch:
            try:
                # welcome_1st에서 문서 조회
                doc_1st = None
                try:
                    response = os_client.get(index=source_index_1st, id=user_id, ignore=[404])
                    if response.get('found'):
                        doc_1st = response['_source']
                except Exception as e:
                    logger.debug(f"⚠️ '{source_index_1st}'에서 {user_id} 조회 실패: {e}")
                
                # welcome_2nd에서 문서 조회
                doc_2nd = None
                try:
                    response = os_client.get(index=source_index_2nd, id=user_id, ignore=[404])
                    if response.get('found'):
                        doc_2nd = response['_source']
                except Exception as e:
                    logger.debug(f"⚠️ '{source_index_2nd}'에서 {user_id} 조회 실패: {e}")
                
                # 조인된 문서 생성
                joined_doc = {
                    "user_id": user_id,
                    "metadata": {},
                    "qa_pairs": [],
                    "source_indices": []
                }
                
                # welcome_1st 데이터 병합
                if doc_1st:
                    metadata_1st = doc_1st.get('metadata', {})
                    if metadata_1st:
                        joined_doc["metadata"].update({
                            "age_group": metadata_1st.get("age_group"),
                            "gender": metadata_1st.get("gender"),
                            "birth_year": metadata_1st.get("birth_year"),
                            "region": metadata_1st.get("region")
                        })
                    
                    # qa_pairs 병합
                    qa_pairs_1st = doc_1st.get('qa_pairs', [])
                    if qa_pairs_1st:
                        joined_doc["qa_pairs"].extend(qa_pairs_1st)
                    
                    joined_doc["source_indices"].append(source_index_1st)
                    
                    # timestamp는 welcome_1st 것을 사용 (없으면 welcome_2nd)
                    if 'timestamp' in doc_1st:
                        joined_doc["timestamp"] = doc_1st.get("timestamp")
                else:
                    stats["missing_1st"] += 1
                
                # welcome_2nd 데이터 병합
                if doc_2nd:
                    metadata_2nd = doc_2nd.get('metadata', {})
                    if metadata_2nd:
                        joined_doc["metadata"].update({
                            "occupation": metadata_2nd.get("occupation"),
                            "job_category": metadata_2nd.get("job_category")
                        })
                    
                    # qa_pairs 병합
                    qa_pairs_2nd = doc_2nd.get('qa_pairs', [])
                    if qa_pairs_2nd:
                        joined_doc["qa_pairs"].extend(qa_pairs_2nd)
                    
                    joined_doc["source_indices"].append(source_index_2nd)
                    
                    # timestamp가 없으면 welcome_2nd 것을 사용
                    if 'timestamp' not in joined_doc and 'timestamp' in doc_2nd:
                        joined_doc["timestamp"] = doc_2nd.get("timestamp")
                else:
                    stats["missing_2nd"] += 1
                
                # 최소한 하나의 인덱스에서 데이터를 가져왔으면 인덱싱
                if doc_1st or doc_2nd:
                    batch_docs.append({
                        "_index": target_index,
                        "_id": user_id,
                        "_source": joined_doc
                    })
                    stats["successful_joins"] += 1
                else:
                    stats["errors"] += 1
                
                stats["total_processed"] += 1
                
            except Exception as e:
                logger.error(f"❌ user_id={user_id} 처리 중 오류: {e}")
                stats["errors"] += 1
                stats["total_processed"] += 1
        
        # 배치 인덱싱
        if batch_docs:
            success, failed = bulk(os_client, batch_docs, raise_on_error=False)
            logger.info(f"✅ 배치 {i//batch_size + 1}: {success}건 성공, {len(failed)}건 실패")
            
            if failed:
                for item in failed:
                    logger.error(f"❌ 인덱싱 실패: {item}")
    
    # 7. 인덱스 새로고침
    logger.info("🔄 인덱스 새로고침 중...")
    os_client.indices.refresh(index=target_index)
    
    # 8. 통계 출력
    logger.info("\n" + "="*60)
    logger.info("📊 조인 통계")
    logger.info("="*60)
    logger.info(f"총 처리: {stats['total_processed']}건")
    logger.info(f"성공적 조인: {stats['successful_joins']}건")
    logger.info(f"welcome_1st 누락: {stats['missing_1st']}건")
    logger.info(f"welcome_2nd 누락: {stats['missing_2nd']}건")
    logger.info(f"오류: {stats['errors']}건")
    logger.info("="*60)
    
    return stats


def main():
    """메인 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description="welcome_1st와 welcome_2nd를 조인하여 통합 인덱스 생성")
    parser.add_argument(
        "--source-1st",
        default="s_welcome_1st",
        help="welcome_1st 인덱스 이름 (기본값: s_welcome_1st)"
    )
    parser.add_argument(
        "--source-2nd",
        default="s_welcome_2nd",
        help="welcome_2nd 인덱스 이름 (기본값: s_welcome_2nd)"
    )
    parser.add_argument(
        "--target",
        default="s_users_joined",
        help="생성할 조인된 인덱스 이름 (기본값: s_users_joined)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="배치 처리 크기 (기본값: 1000)"
    )
    
    args = parser.parse_args()
    
    # 설정 로드 및 OpenSearch 클라이언트 생성
    config = get_config()
    os_client = OpenSearch(
        hosts=[{'host': config.OPENSEARCH_HOST, 'port': config.OPENSEARCH_PORT}],
        http_auth=(config.OPENSEARCH_USER, config.OPENSEARCH_PASSWORD),
        use_ssl=config.OPENSEARCH_USE_SSL,
        verify_certs=False,
        ssl_show_warn=False,
        request_timeout=30
    )
    
    if not os_client or not os_client.ping():
        logger.error("❌ OpenSearch 서버에 연결할 수 없습니다.")
        return
    
    logger.info("="*60)
    logger.info("🚀 조인된 인덱스 생성 시작")
    logger.info("="*60)
    logger.info(f"소스 인덱스 1: {args.source_1st}")
    logger.info(f"소스 인덱스 2: {args.source_2nd}")
    logger.info(f"타겟 인덱스: {args.target}")
    logger.info(f"배치 크기: {args.batch_size}")
    logger.info("="*60)
    
    try:
        stats = join_documents(
            os_client=os_client,
            source_index_1st=args.source_1st,
            source_index_2nd=args.source_2nd,
            target_index=args.target,
            batch_size=args.batch_size
        )
        
        logger.info("\n✅ 조인된 인덱스 생성 완료!")
        logger.info(f"📁 인덱스 이름: {args.target}")
        logger.info(f"📊 총 문서 수: {stats['successful_joins']}건")
        
    except Exception as e:
        logger.error(f"❌ 오류 발생: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()

