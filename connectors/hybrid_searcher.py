"""OpenSearch 하이브리드 검색 쿼리 빌더 (RRF 기반)"""
import logging
from typing import List, Dict, Optional, Any

logger = logging.getLogger(__name__)


class OpenSearchHybridQueryBuilder:
    """
    RRF (Reciprocal Rank Fusion) 기반 하이브리드 검색

    키워드 검색과 벡터 검색 결과를 RRF 알고리즘으로 결합하여
    진정한 하이브리드 점수를 생성합니다.
    """

    def __init__(self, config=None):
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
        analysis,  # QueryAnalysis 객체
        query_vector: Optional[List[float]] = None,
        size: int = 10
    ) -> Dict[str, Any]:
        """
        하이브리드 쿼리 생성

        Args:
            analysis: QueryAnalysis 객체 (must_terms, should_terms, intent 등 포함)
            query_vector: 쿼리 임베딩 벡터
            size: 반환할 문서 개수

        Returns:
            OpenSearch 쿼리 DSL
        """

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

    def _extract_demographic_filters(self, analysis) -> List[str]:
        """must_terms에서 인구통계 키워드 추출 (qa_pairs.answer에서 검색용)"""
        demographic_terms = []

        for term in analysis.must_terms:
            matched = False

            for field, config in self.demographic_patterns.items():
                # 직접 매칭
                if term in config["values"]:
                    demographic_terms.append(term)
                    matched = True
                    break

                # 동의어 매칭
                if term in config["synonyms"]:
                    normalized = config["synonyms"][term]
                    demographic_terms.append(normalized)
                    matched = True
                    break

            if not matched:
                logger.debug(f"'{term}'은(는) 인구통계 키워드가 아님")

        return demographic_terms

    def _extract_semantic_terms(self, analysis) -> Dict[str, List[str]]:
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
        """

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

        # RRF 결합 (OpenSearch 2.10+)
        if hasattr(self.config, 'OPENSEARCH_VERSION') and self.config.OPENSEARCH_VERSION >= 2.10:
            # 내장 RRF 사용
            query = {
                "query": {
                    "hybrid": {
                        "queries": [
                            keyword_query,
                            vector_query
                        ]
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
                keyword_query,
                vector_query,
                alpha,
                size
            )

        return query

    def _build_keyword_search(
        self,
        semantic_terms: Dict[str, List[str]],
        demographic_filters: List[str]
    ) -> Dict:
        """키워드 검색 쿼리 (qa_pairs nested에서 검색, 각 키워드는 서로 다른 qa_pair에서 검색)"""

        must_queries = []

        # 인구통계 조건 (각각 별도의 nested 쿼리)
        if demographic_filters:
            for term in demographic_filters:
                must_queries.append({
                    "nested": {
                        "path": "qa_pairs",
                        "query": {"match": {"qa_pairs.answer": term}},
                        "score_mode": "max"
                    }
                })

        # 의미적 키워드 must 조건 (각각 별도의 nested 쿼리)
        if semantic_terms["must"]:
            for term in semantic_terms["must"]:
                must_queries.append({
                    "nested": {
                        "path": "qa_pairs",
                        "query": {"match": {"qa_pairs.answer": term}},
                        "score_mode": "max"
                    }
                })

        # should 조건 (하나의 nested 쿼리로 묶음)
        should_query = None
        if semantic_terms["should"]:
            should_query = {
                "nested": {
                    "path": "qa_pairs",
                    "query": {
                        "bool": {
                            "should": [
                                {"match": {"qa_pairs.answer": term}}
                                for term in semantic_terms["should"]
                            ],
                            "minimum_should_match": 1
                        }
                    },
                    "score_mode": "max"
                }
            }

        # 최종 쿼리 구성
        if must_queries or should_query:
            bool_parts = {}

            if must_queries:
                bool_parts["must"] = must_queries

            if should_query:
                bool_parts["should"] = [should_query]

            # must_not 조건
            if semantic_terms["must_not"]:
                bool_parts["must_not"] = [
                    {
                        "nested": {
                            "path": "qa_pairs",
                            "query": {"match": {"qa_pairs.answer": term}}
                        }
                    }
                    for term in semantic_terms["must_not"]
                ]

            query = {
                "bool": bool_parts
            }
        else:
            query = {"match_all": {}}

        return query

    def _build_vector_search(
        self,
        query_vector: List[float],
        demographic_filters: List[str],
        k: int
    ) -> Dict:
        """벡터 검색 쿼리 (qa_pairs에서 검색)"""

        # 참고: qa_pairs에 answer_vector가 없으면 벡터 검색 불가
        # 현재는 키워드 검색으로 fallback
        logger.warning("벡터 검색은 qa_pairs에 answer_vector 필드가 필요합니다. 키워드 검색만 사용합니다.")

        # 임시로 키워드 검색 반환
        return self._build_keyword_search(
            {"must": [], "should": [], "must_not": []},
            demographic_filters
        )

    def _build_weighted_hybrid(
        self,
        keyword_query: Dict,
        vector_query: Dict,
        alpha: float,
        size: int
    ) -> Dict:
        """가중 평균 방식 하이브리드 (Fallback)"""

        # alpha = 0: 키워드만, alpha = 1: 벡터만
        keyword_weight = 1.0 - alpha
        vector_weight = alpha

        query = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "constant_score": {
                                "filter": keyword_query,
                                "boost": keyword_weight
                            }
                        },
                        {
                            "constant_score": {
                                "filter": vector_query,
                                "boost": vector_weight
                            }
                        }
                    ]
                }
            },
            "size": size
        }

        return query

    def _build_keyword_only_query(
        self,
        semantic_terms: Dict[str, List[str]],
        demographic_filters: List[str],
        size: int
    ) -> Dict:
        """키워드 검색만 (벡터 없음)"""

        keyword_query = self._build_keyword_search(semantic_terms, demographic_filters)

        # 디버깅: 생성된 쿼리 로깅
        logger.info(f"Generated keyword query: {keyword_query}")

        return {
            "query": keyword_query,
            "size": size
        }


def calculate_rrf_score(
    keyword_results: List[Dict],
    vector_results: List[Dict],
    k: int = 60
) -> List[Dict]:
    """
    수동 RRF 점수 계산 (OpenSearch 2.10 미만용)

    RRF 공식: score = Σ (1 / (k + rank_i))

    Args:
        keyword_results: 키워드 검색 결과 리스트
        vector_results: 벡터 검색 결과 리스트
        k: RRF 상수 (기본값 60)

    Returns:
        RRF 점수로 정렬된 결과 리스트
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
