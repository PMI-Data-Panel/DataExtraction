"""OpenSearch 하이브리드 쿼리 빌더 (RRF 기반)"""
import logging
from typing import List, Dict, Optional, Any
from ..models.query import QueryAnalysis

logger = logging.getLogger(__name__)


def build_dynamic_demographics_filters(demographics) -> List[Dict[str, Any]]:
    """
    DemographicEntity 리스트를 OpenSearch 필터로 변환
    
    Args:
        demographics: ExtractedEntities 객체, DemographicEntity 리스트, 또는 dict 형태의 demographics
        
    Returns:
        OpenSearch 필터 리스트
    """
    filters = []
    
    # ExtractedEntities 객체인 경우
    if hasattr(demographics, 'demographics'):
        demographics_list = demographics.demographics
    # demographics가 dict 형태인 경우 (extracted entities dict)
    elif isinstance(demographics, dict):
        demographics_list = demographics.get("demographics", [])
    elif isinstance(demographics, list):
        demographics_list = demographics
    else:
        return filters
    
    for demo in demographics_list:
        # DemographicEntity 객체인 경우
        if hasattr(demo, 'to_opensearch_filter'):
            filter_clause = demo.to_opensearch_filter(
                metadata_only=False,  # survey_responses_merged는 metadata와 qa_pairs 모두 사용
                include_qa_fallback=True,  # qa_pairs fallback 활성화
            )
            if filter_clause and filter_clause != {"match_all": {}}:
                filters.append(filter_clause)
        # dict 형태인 경우
        elif isinstance(demo, dict):
            # dict에서 DemographicEntity로 변환 시도
            try:
                from ..models.entities import DemographicEntity, DemographicType
                demo_type = DemographicType(demo.get("demographic_type"))
                demo_entity = DemographicEntity(
                    demographic_type=demo_type,
                    value=demo.get("value")
                )
                filter_clause = demo_entity.to_opensearch_filter(
                    metadata_only=False,
                    include_qa_fallback=True,
                )
                if filter_clause and filter_clause != {"match_all": {}}:
                    filters.append(filter_clause)
            except Exception as e:
                logger.warning(f"Failed to convert dict to DemographicEntity: {e}")
                continue
    
    return filters


class OpenSearchHybridQueryBuilder:
    """
    RRF (Reciprocal Rank Fusion) 기반 하이브리드 검색

    키워드 검색과 벡터 검색 결과를 RRF 알고리즘으로 결합하여
    진정한 하이브리드 점수를 생성합니다.
    """

    def __init__(self, config):
        self.config = config

        # RRF 파라미터
        self.rrf_k = 60  # RRF 상수 (일반적으로 60)

        # 인구통계 패턴 (정규화 매핑)
        self.demographic_patterns = {
            "age_group": {
                "values": ["10대", "20대", "30대", "40대", "50대", "60대", "70대"],
                "synonyms": {}
            },
            "gender": {
                "values": ["남성", "여성"],
                "synonyms": {"남자": "남성", "여자": "여성", "남": "남성", "여": "여성"}
            },
            "region": {
                "values": ["서울", "부산", "대구", "인천", "광주", "대전", "울산", "세종"],
                "synonyms": {"서울시": "서울", "부산시": "부산"}
            }
        }

    def build_query(
        self,
        analysis: QueryAnalysis,
        query_vector: Optional[List[float]] = None,
        size: int = 10
    ) -> Dict[str, Any]:
        """하이브리드 쿼리 생성"""

        # 1. 인구통계 필터 추출
        demographic_filters = self._extract_demographic_filters(analysis)

        # 2. 의미적 키워드 추출 (인구통계 제외)
        semantic_terms = self._extract_semantic_terms(analysis)

        # 3. 쿼리 전략 선택
        if query_vector and analysis.intent in ["hybrid", "semantic_search"]:
            # 하이브리드: 키워드 + 벡터
            return self._build_hybrid_query(
                semantic_terms,
                query_vector,
                demographic_filters,
                size,
                analysis.alpha
            )
        else:
            # 키워드만
            return self._build_keyword_only_query(
                semantic_terms,
                demographic_filters,
                size
            )

    def _extract_demographic_filters(self, analysis: QueryAnalysis) -> List[Dict]:
        """must_terms에서 인구통계 필터 추출 및 정규화"""
        filters = []

        for term in analysis.must_terms:
            matched = False

            for field, config in self.demographic_patterns.items():
                # 직접 매칭
                if term in config["values"]:
                    filters.append({"term": {f"demographics.{field}": term}})
                    matched = True
                    break

                # 동의어 매칭
                if term in config["synonyms"]:
                    normalized = config["synonyms"][term]
                    filters.append({"term": {f"demographics.{field}": normalized}})
                    matched = True
                    break

            if not matched:
                logger.debug(f"'{term}'은(는) 인구통계 키워드가 아님")

        return filters

    def _extract_semantic_terms(self, analysis: QueryAnalysis) -> Dict[str, List[str]]:
        """인구통계가 아닌 의미적 키워드 추출"""
        all_demographic_values = set()
        for field_config in self.demographic_patterns.values():
            all_demographic_values.update(field_config["values"])
            all_demographic_values.update(field_config["synonyms"].keys())

        must_semantic = [
            term for term in analysis.must_terms
            if term not in all_demographic_values
        ]

        should_semantic = [
            term for term in analysis.should_terms
            if term not in all_demographic_values
        ]

        return {
            "must": must_semantic,
            "should": should_semantic,
            "must_not": analysis.must_not_terms
        }

    def _build_hybrid_query(
        self,
        semantic_terms: Dict[str, List[str]],
        query_vector: List[float],
        demographic_filters: List[Dict],
        size: int,
        alpha: float
    ) -> Dict[str, Any]:
        """
        RRF 기반 하이브리드 쿼리

        전략:
        1. 키워드 검색 결과 얻기 (nested bool)
        2. 벡터 검색 결과 얻기 (kNN)
        3. OpenSearch의 RRF로 결합 (v2.10+)

        OpenSearch 2.10 미만에서는 수동 RRF 필요
        """

        # OpenSearch 2.10+ : 내장 RRF 지원
        # https://opensearch.org/docs/latest/search-plugins/searching-data/rrf/

        # 키워드 검색 쿼리
        keyword_query = self._build_keyword_search(
            semantic_terms,
            demographic_filters
        )

        # 벡터 검색 쿼리
        vector_query = self._build_vector_search(
            query_vector,
            demographic_filters,
            size
        )

        # ⭐ None 쿼리 제거: hybrid 쿼리의 queries 배열에 null이 들어가면 안 됨
        valid_queries = []
        if keyword_query is not None:
            valid_queries.append(keyword_query)
        if vector_query is not None:
            valid_queries.append(vector_query)

        # 유효한 쿼리가 없으면 match_all 반환
        if not valid_queries:
            logger.warning("⚠️ 유효한 쿼리가 없어 match_all을 반환합니다.")
            return {
                "query": {"match_all": {}},
                "size": size
            }

        # 쿼리가 하나만 있으면 hybrid 없이 직접 반환
        if len(valid_queries) == 1:
            logger.info("⚠️ 쿼리가 하나만 있어 hybrid 없이 직접 반환합니다.")
            return {
                "query": valid_queries[0],
                "size": size
            }

        # RRF 결합 (OpenSearch 2.10+)
        # 이전 버전에서는 두 쿼리를 따로 실행하고 Python에서 RRF 계산
        if hasattr(self.config, 'OPENSEARCH_VERSION') and self.config.OPENSEARCH_VERSION >= 2.10:
            # 내장 RRF 사용
            query = {
                "query": {
                    "hybrid": {
                        "queries": valid_queries
                    }
                },
                "size": size,
                "ext": {
                    "rrf": {
                        "rank_constant": self.rrf_k,
                        "window_size": size * 2
                    }
                }
            }
        else:
            # Fallback: 가중 평균 방식
            query = self._build_weighted_hybrid(
                valid_queries[0],  # keyword_query
                valid_queries[1] if len(valid_queries) > 1 else None,  # vector_query
                alpha,
                size
            )

        return query

    def _build_keyword_search(
        self,
        semantic_terms: Dict[str, List[str]],
        demographic_filters: List[Dict]
    ) -> Dict:
        """키워드 검색 쿼리 (nested 내부 bool)"""

        if not semantic_terms["must"] and not semantic_terms["should"]:
            nested_query = {"match_all": {}}
        else:
            bool_clauses = {}

            # must 조건
            if semantic_terms["must"]:
                bool_clauses["must"] = [
                    {"match": {"qa_pairs.answer_text": term}}
                    for term in semantic_terms["must"]
                ]

            # should 조건
            if semantic_terms["should"]:
                bool_clauses["should"] = [
                    {"match": {"qa_pairs.answer_text": term}}
                    for term in semantic_terms["should"]
                ]
                bool_clauses["minimum_should_match"] = 1

            # must_not 조건
            if semantic_terms["must_not"]:
                bool_clauses["must_not"] = [
                    {"match": {"qa_pairs.answer_text": term}}
                    for term in semantic_terms["must_not"]
                ]

            nested_query = {"bool": bool_clauses}

        # Nested 래핑 (qa_pairs 경로로 통일)
        query = {
            "nested": {
                "path": "qa_pairs",
                "query": nested_query,
                "score_mode": "max",
                "inner_hits": {
                    "size": 3,
                    "_source": ["q_text", "answer_text", "q_category"],
                    "highlight": {
                        "fields": {
                            "qa_pairs.answer_text": {
                                "fragment_size": 150,
                                "number_of_fragments": 2
                            }
                        }
                    }
                }
            }
        }

        # 인구통계 필터 결합
        if demographic_filters:
            query = {
                "bool": {
                    "must": [query],
                    "filter": demographic_filters
                }
            }

        return query

    def _build_vector_search(
        self,
        query_vector: List[float],
        demographic_filters: List[Dict],
        k: int
    ) -> Dict:
        """벡터 검색 쿼리 (qa_pairs 경로로 통일)"""

        knn_query = {
            "nested": {
                "path": "qa_pairs",
                "query": {
                    "knn": {
                        "qa_pairs.answer_vector": {
                            "vector": query_vector,
                            "k": k * 2
                        }
                    }
                },
                "score_mode": "max",
                "inner_hits": {
                    "size": 3,
                    "_source": ["q_text", "answer_text", "q_category"]
                }
            }
        }

        # 인구통계 필터 적용
        if demographic_filters:
            knn_query = {
                "bool": {
                    "must": [knn_query],
                    "filter": demographic_filters
                }
            }

        return knn_query

    def _build_weighted_hybrid(
        self,
        keyword_query: Dict,
        vector_query: Optional[Dict],
        alpha: float,
        size: int
    ) -> Dict:
        """가중 평균 방식 하이브리드 (Fallback)"""

        # ⭐ None 쿼리 제거
        should_clauses = []
        
        if keyword_query is not None:
            keyword_weight = 1.0 - alpha
            should_clauses.append({
                "constant_score": {
                    "filter": keyword_query,
                    "boost": keyword_weight
                }
            })
        
        if vector_query is not None:
            vector_weight = alpha
            should_clauses.append({
                "constant_score": {
                    "filter": vector_query,
                    "boost": vector_weight
                }
            })

        # 유효한 쿼리가 없으면 match_all 반환
        if not should_clauses:
            logger.warning("⚠️ 유효한 쿼리가 없어 match_all을 반환합니다.")
            return {
                "query": {"match_all": {}},
                "size": size
            }

        # 쿼리가 하나만 있으면 should 없이 직접 반환
        if len(should_clauses) == 1:
            return {
                "query": should_clauses[0]["constant_score"]["filter"],
                "size": size
            }

        query = {
            "query": {
                "bool": {
                    "should": should_clauses
                }
            },
            "size": size
        }

        return query

    def _build_keyword_only_query(
        self,
        semantic_terms: Dict[str, List[str]],
        demographic_filters: List[Dict],
        size: int
    ) -> Dict:
        """키워드 검색만 (벡터 없음)"""

        keyword_query = self._build_keyword_search(semantic_terms, demographic_filters)

        return {
            "query": keyword_query,
            "size": size,
            "_source": {
                "excludes": ["*.answer_vector"]  # 벡터 제외 (응답 크기 절감)
            }
        }


def calculate_rrf_score(
    keyword_results: List[Dict],
    vector_results: List[Dict],
    k: int = 60
) -> List[Dict]:
    """
    수동 RRF 점수 계산 (OpenSearch 2.10 미만용)

    RRF 공식: score = Σ (1 / (k + rank_i))
    """

    # 문서별 점수 누적
    doc_scores = {}

    # 키워드 결과
    for rank, result in enumerate(keyword_results, start=1):
        doc_id = result['_id']
        score = 1.0 / (k + rank)

        if doc_id not in doc_scores:
            doc_scores[doc_id] = {
                'doc': result,
                'keyword_rank': rank,
                'keyword_score': score,
                'vector_rank': None,
                'vector_score': 0.0,
                'rrf_score': score
            }
        else:
            doc_scores[doc_id]['keyword_rank'] = rank
            doc_scores[doc_id]['keyword_score'] = score
            doc_scores[doc_id]['rrf_score'] += score

    # 벡터 결과
    for rank, result in enumerate(vector_results, start=1):
        doc_id = result['_id']
        score = 1.0 / (k + rank)

        if doc_id not in doc_scores:
            doc_scores[doc_id] = {
                'doc': result,
                'keyword_rank': None,
                'keyword_score': 0.0,
                'vector_rank': rank,
                'vector_score': score,
                'rrf_score': score
            }
        else:
            doc_scores[doc_id]['vector_rank'] = rank
            doc_scores[doc_id]['vector_score'] = score
            doc_scores[doc_id]['rrf_score'] += score

    # RRF 점수로 정렬
    ranked = sorted(doc_scores.values(), key=lambda x: x['rrf_score'], reverse=True)

    # 결과 구성
    results = []
    for item in ranked:
        doc = item['doc'].copy()
        doc['_score'] = item['rrf_score']
        doc['_rrf_details'] = {
            'keyword_rank': item['keyword_rank'],
            'vector_rank': item['vector_rank'],
            'keyword_score': item['keyword_score'],
            'vector_score': item['vector_score']
        }
        results.append(doc)

    return results
