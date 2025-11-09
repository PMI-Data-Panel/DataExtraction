"""
원격 OpenSearch의 qa_pairs 구조에 맞는 쿼리 빌더
"""
import logging
from typing import List, Dict, Optional, Any
from ..models.query import QueryAnalysis

logger = logging.getLogger(__name__)


class RemoteOpenSearchQueryBuilder:
    """
    원격 OpenSearch (qa_pairs 구조)용 쿼리 빌더

    데이터 구조:
    {
      "user_id": "...",
      "timestamp": "...",
      "qa_pairs": [
        {"q_code": "Q1", "q_type": "SINGLE", "q_text": "...", "answer": "..."},
        {"q_code": "Q8", "q_type": "MULTI", "q_text": "...", "answer": ["...", "..."]}
      ]
    }
    """

    def __init__(self):
        """초기화"""
        # 인구통계 질문 코드 매핑 (question_list.csv 기반)
        self.demographic_mapping = {
            # 나이 관련
            "20대": {"q_codes": ["Q2", "Q3"], "field": "age"},
            "30대": {"q_codes": ["Q2", "Q3"], "field": "age"},
            "40대": {"q_codes": ["Q2", "Q3"], "field": "age"},
            "50대": {"q_codes": ["Q2", "Q3"], "field": "age"},

            # 성별 관련
            "남성": {"q_codes": ["Q5", "Q5_1"], "field": "gender"},
            "여성": {"q_codes": ["Q5", "Q5_1"], "field": "gender"},
            "남자": {"q_codes": ["Q5", "Q5_1"], "field": "gender", "synonym": "남성"},
            "여자": {"q_codes": ["Q5", "Q5_1"], "field": "gender", "synonym": "여성"},

            # 지역 관련
            "서울": {"q_codes": ["Q6"], "field": "region"},
            "부산": {"q_codes": ["Q6"], "field": "region"},
            "경기": {"q_codes": ["Q6"], "field": "region"},

            # 결혼 여부
            "기혼": {"q_codes": ["Q1"], "field": "marital"},
            "미혼": {"q_codes": ["Q1"], "field": "marital"},

            # 학력
            "대졸": {"q_codes": ["Q4"], "field": "education"},
            "고졸": {"q_codes": ["Q4"], "field": "education"},
        }

    def build_query(
        self,
        analysis: QueryAnalysis,
        size: int = 10,
        include_source: bool = True
    ) -> Dict[str, Any]:
        """
        QueryAnalysis 결과를 원격 OpenSearch 쿼리로 변환

        Args:
            analysis: query_analyzer의 분석 결과
            size: 반환할 문서 개수
            include_source: _source 포함 여부

        Returns:
            OpenSearch 쿼리
        """
        logger.info(f"[RemoteQueryBuilder] 쿼리 생성 중...")
        logger.info(f"  - must_terms: {analysis.must_terms}")
        logger.info(f"  - should_terms: {analysis.should_terms}")
        logger.info(f"  - intent: {analysis.intent}")

        # must, should, must_not 조건 생성
        must_queries = self._build_must_queries(analysis.must_terms)
        should_queries = self._build_should_queries(analysis.should_terms)
        must_not_queries = self._build_must_not_queries(analysis.must_not_terms)

        # 전체 쿼리 구성
        bool_query = {}

        if must_queries:
            bool_query["must"] = must_queries

        if should_queries:
            bool_query["should"] = should_queries
            bool_query["minimum_should_match"] = 1

        if must_not_queries:
            bool_query["must_not"] = must_not_queries

        # 최종 쿼리
        if not bool_query:
            # 조건이 없으면 전체 검색
            query = {"match_all": {}}
        else:
            query = {"bool": bool_query}

        # 결과 구성
        result = {
            "query": query,
            "size": size
        }

        if include_source:
            result["_source"] = {
                "includes": ["user_id", "timestamp", "qa_pairs"]
            }

        # 하이라이트 추가
        result["highlight"] = {
            "fields": {
                "qa_pairs.answer": {
                    "fragment_size": 150,
                    "number_of_fragments": 3
                }
            }
        }

        logger.info(f"[RemoteQueryBuilder] 쿼리 생성 완료")
        return result

    def _build_must_queries(self, must_terms: List[str]) -> List[Dict]:
        """필수 조건 (AND) 쿼리 생성"""
        queries = []

        for term in must_terms:
            # 인구통계 키워드인지 확인
            if term in self.demographic_mapping:
                mapping = self.demographic_mapping[term]

                # 동의어 처리
                search_term = mapping.get("synonym", term)
                q_codes = mapping["q_codes"]

                # 특정 질문 코드에서 검색
                queries.append({
                    "nested": {
                        "path": "qa_pairs",
                        "query": {
                            "bool": {
                                "must": [
                                    {"terms": {"qa_pairs.q_code": q_codes}},
                                    {"match": {"qa_pairs.answer": search_term}}
                                ]
                            }
                        },
                        "inner_hits": {
                            "size": 3,
                            "_source": ["q_code", "q_text", "answer"]
                        }
                    }
                })
            else:
                # 일반 키워드 - 모든 qa_pairs에서 검색
                queries.append({
                    "nested": {
                        "path": "qa_pairs",
                        "query": {
                            "match": {"qa_pairs.answer": term}
                        },
                        "inner_hits": {
                            "size": 3,
                            "_source": ["q_code", "q_text", "answer"]
                        }
                    }
                })

        return queries

    def _build_should_queries(self, should_terms: List[str]) -> List[Dict]:
        """선택 조건 (OR) 쿼리 생성"""
        queries = []

        for term in should_terms:
            queries.append({
                "nested": {
                    "path": "qa_pairs",
                    "query": {
                        "match": {"qa_pairs.answer": term}
                    }
                }
            })

        return queries

    def _build_must_not_queries(self, must_not_terms: List[str]) -> List[Dict]:
        """제외 조건 (NOT) 쿼리 생성"""
        queries = []

        for term in must_not_terms:
            queries.append({
                "nested": {
                    "path": "qa_pairs",
                    "query": {
                        "match": {"qa_pairs.answer": term}
                    }
                }
            })

        return queries

    def build_aggregation_query(
        self,
        analysis: QueryAnalysis,
        agg_field: str = "qa_pairs.answer"
    ) -> Dict[str, Any]:
        """
        통계/집계 쿼리 생성

        예: "20대 남성이 가장 많이 보유한 가전제품은?"
        """
        base_query = self.build_query(analysis, size=0, include_source=False)

        # 집계 추가
        base_query["aggs"] = {
            "qa_nested": {
                "nested": {
                    "path": "qa_pairs"
                },
                "aggs": {
                    "answer_distribution": {
                        "terms": {
                            "field": f"{agg_field}.keyword",
                            "size": 20
                        }
                    }
                }
            }
        }

        return base_query
