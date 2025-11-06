import re
import logging
from typing import List, Dict
from .base import BaseAnalyzer
from ..models.query import QueryAnalysis

logger = logging.getLogger(__name__)


class RuleBasedAnalyzer(BaseAnalyzer):
    """강화된 규칙 기반 쿼리 분석기

    정규 표현식과 패턴 매칭을 사용하여 쿼리를 분석합니다.
    단순한 쿼리는 이것만으로도 충분히 처리 가능합니다.
    """

    def __init__(self):
        """초기화"""
        self.patterns = self._init_patterns()
        self.keyword_expansions = self._init_keyword_expansions()
        logger.info("RuleBasedAnalyzer 초기화 완료 (강화됨)")
    
    def get_name(self) -> str:
        """분석기 이름 반환"""
        return "RuleBasedAnalyzer"
    
    def _init_patterns(self) -> Dict:
        """패턴 정의"""
        return {
            "age": {
                "pattern": r'(\d+대|\d+세|[이삼사오육칠팔구]십대)',
                "type": "demographic"
            },
            "gender": {
                "pattern": r'(남성|여성|남자|여자|남|여)',
                "type": "demographic"
            },
            "region": {
                "pattern": r'(서울|부산|대구|인천|광주|대전|울산|경기|강원|충북|충남|전북|전남|경북|경남|제주|세종)',
                "type": "demographic"
            },
            "job": {
                "pattern": r'(학생|직장인|주부|자영업|전문직|사무직|서비스직|생산직|무직|프리랜서)',
                "type": "demographic"
            },
            "marital": {
                "pattern": r'(미혼|기혼|싱글|결혼)',
                "type": "demographic"
            },
            "emotion": {
                "pattern": r'(만족|불만|행복|스트레스|긍정|부정|좋|싫|편안|불편)',
                "type": "sentiment"
            },
            "frequency": {
                "pattern": r'(자주|가끔|매일|매주|매월|항상|전혀|거의)',
                "type": "behavioral"
            },
            "comparison": {
                "pattern": r'(비교|차이|대비|versus|vs|보다|더)',
                "type": "comparison"
            }
        }

    def _init_keyword_expansions(self) -> Dict[str, List[str]]:
        """키워드 확장 규칙"""
        return {
            # 나이
            "20대": ["20-29", "이십대", "twenties"],
            "30대": ["30-39", "삼십대", "thirties"],
            "40대": ["40-49", "사십대", "forties"],
            "50대": ["50-59", "오십대", "fifties"],

            # 성별
            "남성": ["남자", "남"],
            "여성": ["여자", "여"],

            # 만족도
            "만족": ["만족함", "만족스러움", "satisfied"],
            "불만족": ["불만", "불만족스러움", "dissatisfied"],

            # 빈도
            "자주": ["빈번히", "많이", "often"],
            "가끔": ["때때로", "종종", "sometimes"]
        }
    
    def analyze(self, query: str, context: str = "") -> QueryAnalysis:
        """강화된 규칙 기반 쿼리 분석

        Args:
            query: 분석할 쿼리
            context: 추가 맥락

        Returns:
            분석 결과
        """
        if not self.validate_query(query):
            return self._create_empty_analysis()

        query = self.preprocess_query(query)

        # 키워드 추출
        must_terms = []
        should_terms = []
        intent_hints = []
        expanded_keywords = {}

        for pattern_name, pattern_info in self.patterns.items():
            matches = re.findall(pattern_info["pattern"], query)
            if matches:
                for match in matches:
                    # 중복 제거
                    if match not in must_terms:
                        must_terms.append(match)

                        # 키워드 확장
                        if match in self.keyword_expansions:
                            expanded_keywords[match] = self.keyword_expansions[match]
                            should_terms.extend(self.keyword_expansions[match])

                intent_hints.append(pattern_info["type"])

        # 추가 키워드 추출 (패턴 외)
        additional_terms = self._extract_additional_keywords(query, must_terms)
        must_terms.extend(additional_terms)

        # 의도 결정
        intent = self._determine_intent(intent_hints)

        # Alpha 값 결정
        alpha = self._calculate_alpha(intent)

        # 신뢰도 계산 (키워드가 많을수록 높음)
        confidence = min(0.7, 0.3 + len(must_terms) * 0.1)

        return QueryAnalysis(
            intent=intent,
            must_terms=list(set(must_terms)),
            should_terms=list(set(should_terms)),
            must_not_terms=[],
            alpha=alpha,
            expanded_keywords=expanded_keywords,
            confidence=confidence,
            explanation=f"규칙 기반 분석 - {len(must_terms)}개 키워드 추출",
            reasoning_steps=[
                "패턴 매칭 수행",
                f"추출된 키워드: {', '.join(must_terms[:5])}",
                f"의도: {intent}"
            ],
            analyzer_used=self.get_name()
        )

    def _extract_additional_keywords(self, query: str, existing_terms: List[str]) -> List[str]:
        """패턴 외 추가 키워드 추출"""
        # 의미 있는 명사 추출 (간단한 휴리스틱)
        additional = []

        # 불용어 제거
        stop_words = ['사람', '인', '분', '중', '중에', '중에서', '의', '를', '을', '이', '가']

        words = query.split()
        for word in words:
            # 이미 추출된 키워드는 스킵
            if word in existing_terms:
                continue

            # 불용어는 스킵
            if word in stop_words:
                continue

            # 너무 짧은 단어 스킵
            if len(word) < 2:
                continue

            # 의미 있는 키워드로 판단
            additional.append(word)

        return additional
    
    def _determine_intent(self, hints: List[str]) -> str:
        """힌트 기반 의도 결정"""
        if not hints:
            return "hybrid"
        
        hint_counts = {}
        for hint in hints:
            hint_counts[hint] = hint_counts.get(hint, 0) + 1
        
        # 가장 많은 힌트 타입
        dominant_hint = max(hint_counts, key=hint_counts.get)
        
        # 힌트를 의도로 매핑
        intent_map = {
            "demographic": "exact_match",
            "sentiment": "semantic_search",
            "behavioral": "hybrid",
            "comparison": "hybrid"
        }
        
        return intent_map.get(dominant_hint, "hybrid")
    
    def _calculate_alpha(self, intent: str) -> float:
        """의도에 따른 alpha 값 계산"""
        alpha_map = {
            "exact_match": 0.2,
            "semantic_search": 0.8,
            "hybrid": 0.5
        }
        return alpha_map.get(intent, 0.5)
    
    def _create_empty_analysis(self) -> QueryAnalysis:
        """빈 분석 결과 생성"""
        return QueryAnalysis(
            intent="hybrid",
            must_terms=[],
            should_terms=[],
            must_not_terms=[],
            alpha=0.5,
            expanded_keywords={},
            confidence=0.0,
            explanation="유효하지 않은 쿼리",
            analyzer_used=self.get_name()
        )