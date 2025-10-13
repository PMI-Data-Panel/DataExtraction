from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional
from datetime import datetime
from enum import Enum


# 필터의 종류(AND, OR, NOT)를 명시하기 위한 Enum
class FilterClause(Enum):
    MUST = "must"          # AND 조건
    SHOULD = "should"        # OR 조건
    MUST_NOT = "must_not"    # NOT 조건


# 필터의 연산자(term, range 등)를 명시하기 위한 Enum
class FilterOperator(Enum):
    TERM = "term"          # 정확히 일치 (Categorical 데이터용)
    RANGE = "range"        # 범위 (Numerical, Date 데이터용)
    MATCH = "match"        # 전문 검색 (Text 데이터용)


# 구조화된 검색 필터를 정의하기 위한 클래스
@dataclass
class FilterCondition:
    """
    Elasticsearch 필터 조건을 명확하게 정의하는 데이터 클래스.
    "나이가 30대 이상"과 같은 조건을 기계가 이해할 수 있도록 구조화합니다.
    """
    field: str                      # 필터링할 대상 필드 (예: "age", "region.keyword")
    operator: FilterOperator        # 사용할 연산자 (term, range 등)
    value: Any                      # 필터링할 값 (예: "서울", {"gte": 30, "lt": 40})
    clause: FilterClause = FilterClause.MUST # 기본값은 AND 조건


@dataclass
class QueryAnalysis:
    """쿼리 분석 결과를 담는 데이터 클래스"""
    
    # 기본 분석 결과 필터는 구조화된 객체로 관리
    intent: str                              # 검색 의도: exact_match, semantic_search, hybrid
    must_terms: List[str]                    # AND 조건 키워드
    should_terms: List[str]                  # OR 조건 키워드
    must_not_terms: List[str]                # 제외 키워드
    expanded_keywords: Dict[str, List[str]]  # 키워드별 확장어
    
    filters: List[FilterCondition] = field(default_factory=list) # 구조화된 필터 조건
    keywords: List[str] = field(default_factory=list)           # 전문 검색용 키워드
    # 검색 파라미터
    alpha: float = 0.5                            # 하이브리드 검색 가중치 (0: 키워드, 1: 벡터)
    # 확장 정보
    confidence: float = 1.0                    # 분석 신뢰도 (0-1)
    explanation: str = ""                        # 분석 설명
    # 추가 메타데이터
    reasoning_steps: List[str] = field(default_factory=list)    # CoT 추론 과정
    rewritten_queries: List[str] = field(default_factory=list)  # 재작성된 쿼리들
    semantic_intent: str = "unknown"         # 의미론적 의도
    execution_time: float = 0.0              # 실행 시간 (초)
    # 분석 메타데이터
    analyzer_used: str = ""                  # 사용된 분석기
    fallback_used: bool = False              # 폴백 사용 여부
    cache_hit: bool = False                  # 캐시 히트 여부
    target_metric: Optional[str] = None # 계산해야 할 Metric의 이름
    
    def to_dict(self) -> dict:
        """딕셔너리로 변환"""
        return {
            "intent": self.intent,
            "must_terms": self.must_terms,
            "should_terms": self.should_terms,
            "must_not_terms": self.must_not_terms,
            "alpha": self.alpha,
            "expanded_keywords": self.expanded_keywords,
            "confidence": self.confidence,
            "explanation": self.explanation,
            "semantic_intent": self.semantic_intent,
            "execution_time": self.execution_time
        }
    
    def merge_with(self, other: 'QueryAnalysis'):
        """다른 분석 결과와 병합"""
        # 키워드 병합 (중복 제거)
        self.must_terms = list(set(self.must_terms + other.must_terms))
        self.should_terms = list(set(self.should_terms + other.should_terms))
        self.must_not_terms = list(set(self.must_not_terms + other.must_not_terms))
        
        # 확장 키워드 병합
        for key, values in other.expanded_keywords.items():
            if key in self.expanded_keywords:
                self.expanded_keywords[key] = list(set(self.expanded_keywords[key] + values))
            else:
                self.expanded_keywords[key] = values
        
        # 신뢰도는 평균
        self.confidence = (self.confidence + other.confidence) / 2
        
        # 재작성 쿼리 추가
        self.rewritten_queries.extend(other.rewritten_queries)
@dataclass
class SearchResult:
    """검색 결과를 담는 데이터 클래스"""
    
    # 기본 정보
    doc_id: str                              # 문서 ID
    score: float                             # 검색 점수
    
    # 리랭킹 정보
    rerank_score: Optional[float] = None     # 리랭킹 점수
    
    # 내용
    summary: str = ""                        # 요약
    answers: Dict = field(default_factory=dict)     # 응답 내용
    metadata: Dict = field(default_factory=dict)    # 메타데이터
    
    # 추가 정보
    highlights: List[str] = field(default_factory=list)  # 하이라이트
    matched_terms: List[str] = field(default_factory=list)  # 매칭된 키워드
    
    def get_final_score(self) -> float:
        """최종 점수 반환 (리랭킹 점수 우선)"""
        return self.rerank_score if self.rerank_score is not None else self.score
    
    def to_dict(self) -> dict:
        """딕셔너리로 변환"""
        return {
            "doc_id": self.doc_id,
            "score": self.score,
            "rerank_score": self.rerank_score,
            "summary": self.summary,
            "answers": self.answers,
            "metadata": self.metadata
        }
        


# 집계(Aggregation) 결과를 담기 위한 클래스
@dataclass
class AggregationResult:
    """
    집계 분석 결과를 담는 데이터 클래스.
    "평균 나이: 35.2세", "상위 활동: 달리기(500명)"와 같은 결과를 담습니다.
    """
    name: str              # 집계의 이름 (예: "average_age", "top_activities")
    value: Any             # 집계 결과 값 (예: 35.2, [{"key": "달리기", "doc_count": 500}])


# 모든 종류의 시스템 응답을 통합 관리하는 클래스
@dataclass
class SystemResponse:
    """
    RAG 시스템의 최종 응답을 통합적으로 담는 클래스.
    문서 검색 결과와 집계 분석 결과를 모두 포함할 수 있습니다.
    """
    analysis: QueryAnalysis
    documents: List[SearchResult] = field(default_factory=list)
    aggregations: Dict[str, AggregationResult] = field(default_factory=dict)
    final_answer: str = "결과를 찾았습니다." # LLM이 생성한 최종 자연어 답변
