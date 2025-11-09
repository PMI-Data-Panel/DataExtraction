import logging
from elasticsearch.helpers import bulk
from ..config import get_config, Config
from ..models.query import QueryAnalysis
from typing import List, Dict, Optional, Any

logger = logging.getLogger(__name__)

def create_index_if_not_exists(es_client, index_name: str):
    """
    Elasticsearch에 특정 인덱스가 존재하지 않으면,
    '사용자' 단위의 nested 구조가 적용된 매핑으로 새 인덱스를 생성합니다.
    """
    if not es_client.indices.exists(index=index_name):
        logger.info(f"✨ '{index_name}' 인덱스가 없어 새로 생성합니다.")
        config = get_config()

        mappings = {
            "properties": {
                "user_id": {"type": "keyword"},
                "demographics": {
                    "type": "object",
                    "enabled": True
                },
                "other_objectives": {
                    "type": "object",
                    "enabled": True
                },
                "subjective_responses": {
                    "type": "nested",
                    "properties": {
                        "q_text": {"type": "text", "analyzer": "nori"},
                        "q_code": {"type": "keyword"},
                        "q_category": {"type": "keyword"},
                        "answer_text": {"type": "text", "analyzer": "nori"},
                        "answer_vector": {
                            "type": "dense_vector",
                            "dims": config.EMBEDDING_DIM,
                        },
                        "answer_length": {"type": "integer"}
                    },
                },
                "all_subjective_text": {
                    "type": "text",
                    "analyzer": "nori"
                },
                "metadata": {
                    "type": "object",
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

        try:
            es_client.indices.create(index=index_name, mappings=mappings)
            logger.info(f"👍 '{index_name}' 인덱스 생성 완료 (사용자 단위 Nested 구조 적용).")
        except Exception as e:
            logger.error(f"🚨 '{index_name}' 인덱스 생성 실패: {e}")
            raise

def bulk_index_data(es_client, actions: list):
    """Elasticsearch에 데이터를 대량으로 색인하는 함수"""
    if not actions:
        return 0, []
    try:
        success, failed = bulk(es_client, actions, raise_on_error=False, refresh=True)
        if failed:
            logger.warning(f"🚨 {len(failed)}개의 문서 색인 실패.")
        return success, failed
    except Exception as e:
        logger.error(f"🚨 벌크 색인 중 오류 발생: {e}")
        raise

class ElasticsearchQueryBuilder:
    """쿼리 분석 결과를 바탕으로 Elasticsearch 쿼리를 생성합니다."""
    def __init__(self, config: Config):
        self.config = config

    def _get_keyword_query(self, term: str) -> Dict:
        """단일 키워드에 대한 멀티-매치 쿼리를 생성합니다."""
        return {
            "multi_match": {
                "query": term,
                "fields": ["qa_pairs.q_text", "qa_pairs.answer_text"]
            }
        }

    def build_complete_request(self, analysis: QueryAnalysis, query_vector: Optional[List[float]] = None, size: int = 10, filters: Optional[List[Dict]] = None) -> Dict[str, Any]:
        """분석 결과를 바탕으로 전체 ES 검색 요청 본문을 생성합니다."""
        
        # 각 키워드 조건을 별개의 nested 쿼리로 생성
        must_clauses = [
            {"nested": {"path": "qa_pairs", "query": self._get_keyword_query(term), "inner_hits": {"name": f"hit_{term}"}}}
            for term in analysis.must_terms
        ]
        should_clauses = [
            {"nested": {"path": "qa_pairs", "query": self._get_keyword_query(term), "inner_hits": {"name": f"hit_{term}"}}}
            for term in analysis.should_terms
        ]
        must_not_clauses = [
            {"nested": {"path": "qa_pairs", "query": self._get_keyword_query(term)}}
            for term in analysis.must_not_terms
        ]

        if filters:
            must_clauses.extend(filters)

        # 최종 bool 쿼리
        final_query = {
            "bool": {
                "must": must_clauses,
                "should": should_clauses,
                "must_not": must_not_clauses
            }
        }
        
        if not any(final_query["bool"].values()):
            final_query = {"match_all": {}}

        # k-NN 벡터 검색 쿼리 추가 (Hybrid 검색)
        if query_vector and analysis.intent in ["hybrid", "semantic_search"]:
            knn_query = {
                "field": "qa_pairs.answer_vector",
                "query_vector": query_vector,
                "k": size,
                "num_candidates": self.config.INITIAL_SEARCH_SIZE,
                "inner_hits": {
                    "_source": ["qa_pairs.embedding_text"]
                }
            }
            
            if final_query != {"match_all": {}}:
                knn_query["filter"] = final_query

            request_body = {
                "knn": knn_query,
                "size": size
            }
            
            if analysis.intent == "hybrid":
                request_body["query"] = final_query

            return request_body
        else:
            # 키워드 검색만 사용
            return {
                "query": final_query,
                "size": size
            }