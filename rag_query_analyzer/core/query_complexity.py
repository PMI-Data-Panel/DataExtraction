"""쿼리 복잡도 분석 - LLM 사용 여부 결정"""
import re
import logging
from typing import Tuple, Dict
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ComplexityScore:
    """쿼리 복잡도 점수"""
    total_score: float  # 0-100
    level: str  # simple, moderate, complex
    use_llm: bool  # LLM 사용 여부
    reasons: list  # 복잡도 요인
    details: dict  # 세부 점수


class QueryComplexityAnalyzer:
    """
    쿼리 복잡도 분석기

    규칙:
    - 0-30점: simple → Rule-based만 사용
    - 31-60점: moderate → Semantic analyzer 사용
    - 61-100점: complex → Claude LLM 사용
    """

    def __init__(self):
        # 단순 패턴 (낮은 복잡도)
        self.simple_patterns = {
            "age": ["10대", "20대", "30대", "40대", "50대", "60대", "70대"],
            "gender": ["남성", "여성", "남자", "여자"],
            "region": ["서울", "부산", "대구", "인천"],
            "yes_no": ["만족", "불만족", "좋", "나쁨"]
        }

        # 복잡한 패턴 (높은 복잡도)
        self.complex_indicators = {
            "comparison": ["비교", "차이", "대조", "vs", "versus"],
            "aggregation": ["평균", "합계", "비율", "분포", "통계"],
            "temporal": ["변화", "추세", "증가", "감소", "트렌드"],
            "causality": ["때문", "원인", "이유", "why", "왜"],
            "negation": ["제외", "아닌", "없는", "빼고"],
            "multi_condition": ["동시", "그리고", "또한", "뿐만 아니라"]
        }

    def analyze(self, query: str) -> ComplexityScore:
        """쿼리 복잡도 분석"""

        details = {}
        reasons = []
        total_score = 0

        # 1. 길이 점수 (0-15점)
        length_score = self._score_length(query)
        details['length'] = length_score
        total_score += length_score
        if length_score > 10:
            reasons.append(f"긴 쿼리 (길이: {len(query)})")

        # 2. 단순 패턴 매칭 (0-20점, 역점수)
        simple_score = self._score_simple_patterns(query)
        details['simplicity'] = simple_score
        # 단순하면 점수 감소
        if simple_score > 10:
            total_score -= simple_score
            reasons.append("단순한 인구통계 필터")

        # 3. 복잡한 패턴 존재 (0-30점)
        complex_score = self._score_complex_patterns(query)
        details['complexity'] = complex_score
        total_score += complex_score
        if complex_score > 15:
            reasons.append(f"복잡한 패턴 감지 ({complex_score}점)")

        # 4. 조건 개수 (0-20점)
        condition_score = self._score_conditions(query)
        details['conditions'] = condition_score
        total_score += condition_score
        if condition_score > 10:
            reasons.append(f"다중 조건 ({condition_score/5:.0f}개)")

        # 5. 문장 구조 (0-15점)
        structure_score = self._score_structure(query)
        details['structure'] = structure_score
        total_score += structure_score
        if structure_score > 10:
            reasons.append("복잡한 문장 구조")

        # 점수 정규화 (0-100)
        total_score = max(0, min(100, total_score))

        # 레벨 결정
        if total_score <= 30:
            level = "simple"
            use_llm = False
        elif total_score <= 60:
            level = "moderate"
            use_llm = False
        else:
            level = "complex"
            use_llm = True

        logger.info(f"🎯 쿼리 복잡도: {total_score:.1f}점 ({level}) - LLM 사용: {use_llm}")

        return ComplexityScore(
            total_score=total_score,
            level=level,
            use_llm=use_llm,
            reasons=reasons,
            details=details
        )

    def _score_length(self, query: str) -> float:
        """길이 점수"""
        length = len(query)

        if length < 20:
            return 0
        elif length < 50:
            return 5
        elif length < 100:
            return 10
        else:
            return 15

    def _score_simple_patterns(self, query: str) -> float:
        """단순 패턴 점수 (많을수록 단순함)"""
        score = 0
        query_lower = query.lower()

        for category, patterns in self.simple_patterns.items():
            for pattern in patterns:
                if pattern in query_lower:
                    score += 5

        return min(20, score)

    def _score_complex_patterns(self, query: str) -> float:
        """복잡한 패턴 점수"""
        score = 0
        query_lower = query.lower()
        matched_categories = []

        for category, patterns in self.complex_indicators.items():
            for pattern in patterns:
                if pattern in query_lower:
                    score += 10
                    matched_categories.append(category)
                    break  # 카테고리당 1번만

        if matched_categories:
            logger.debug(f"복잡한 패턴: {matched_categories}")

        return min(30, score)

    def _score_conditions(self, query: str) -> float:
        """조건 개수 점수"""
        # 조건 구분자
        separators = ["그리고", "또한", "또는", "및", ",", "、"]

        count = 1  # 기본 1개
        for sep in separators:
            count += query.count(sep)

        # 조건당 5점
        score = min(20, count * 5)

        return score

    def _score_structure(self, query: str) -> float:
        """문장 구조 복잡도"""
        score = 0

        # 종속절 존재
        subordinate_markers = ["때", "면", "어서", "니까", "지만", "는데"]
        for marker in subordinate_markers:
            if marker in query:
                score += 5
                break

        # 의문문 존재
        if "?" in query or any(q in query for q in ["무엇", "어떤", "어느", "어디", "왜"]):
            score += 5

        # 인용문 존재
        if '"' in query or "'" in query or "「" in query:
            score += 5

        return min(15, score)


# 전역 인스턴스
_complexity_analyzer = None


def get_complexity_analyzer() -> QueryComplexityAnalyzer:
    """싱글톤 인스턴스 반환"""
    global _complexity_analyzer
    if _complexity_analyzer is None:
        _complexity_analyzer = QueryComplexityAnalyzer()
    return _complexity_analyzer
