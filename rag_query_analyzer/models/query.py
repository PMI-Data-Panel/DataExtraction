from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime


@dataclass
class QueryAnalysis:
    """쿼리 분석 결과를 담는 데이터 클래스"""
    
    # 기본 분석 결과
    intent: str                              # 검색 의도: exact_match, semantic_search, hybrid
    must_terms: List[str]                    # AND 조건 키워드  고민중!!
    should_terms: List[str]                  # OR 조건 키워드
    must_not_terms: List[str]                # 제외 키워드
    
    # 검색 파라미터
    alpha: float                              # 하이브리드 검색 가중치 (0: 키워드, 1: 벡터)
    
    # 확장 정보
    expanded_keywords: Dict[str, List[str]]  # 키워드별 확장어
    confidence: float                         # 분석 신뢰도 (0-1)
    explanation: str                          # 분석 설명
    
    # 추가 메타데이터
    reasoning_steps: List[str] = field(default_factory=list)    # CoT 추론 과정
    rewritten_queries: List[str] = field(default_factory=list)  # 재작성된 쿼리들
    semantic_intent: str = "unknown"         # 의미론적 의도
    execution_time: float = 0.0              # 실행 시간 (초)
    
    # 분석 메타데이터
    analyzer_used: str = ""                  # 사용된 분석기
    fallback_used: bool = False              # 폴백 사용 여부
    cache_hit: bool = False                  # 캐시 히트 여부
    
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