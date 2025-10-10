import re
import logging
from typing import List, Dict
from .base import BaseAnalyzer
from ..models.query import QueryAnalysis

logger = logging.getLogger(__name__)


class RuleBasedAnalyzer(BaseAnalyzer):
    """규칙 기반 쿼리 분석기
    
    정규 표현식과 패턴 매칭을 사용하여 쿼리를 분석합니다.
    폴백 분석기로 주로 사용됩니다.
    """
    
    def __init__(self):
        """초기화"""
        self.patterns = self._init_patterns()
        logger.info("RuleBasedAnalyzer 초기화 완료")
    
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
    
    def analyze(self, query: str, context: str = "") -> QueryAnalysis:
        """규칙 기반 쿼리 분석
        
        Args:
            query: 분석할 쿼리
            context: 추가 맥락
            
        Returns:
            분석 결과
        """
        if not self.validate_query(query):
            return self._create_empty_analysis()
        
        query = self.preprocess_query(query)

        # 일반적인 단어 제거
        stop_words = ['사람', '인', '분']
        for word in stop_words:
            query = query.replace(word, '')
        
        # 키워드 추출
        must_terms = []
        intent_hints = []
        
        for pattern_name, pattern_info in self.patterns.items():
            matches = re.findall(pattern_info["pattern"], query)
            if matches:
                must_terms.extend(matches)
                intent_hints.append(pattern_info["type"])
        
        # 의도 결정
        intent = self._determine_intent(intent_hints)
        
        # Alpha 값 결정
        alpha = self._calculate_alpha(intent)
        
        return QueryAnalysis(
            intent=intent,
            must_terms=list(set(must_terms)),  # 중복 제거
            should_terms=[],
            must_not_terms=[],
            alpha=alpha,
            expanded_keywords={},
            confidence=0.3,  # 규칙 기반은 낮은 신뢰도
            explanation="규칙 기반 패턴 매칭으로 분석",
            reasoning_steps=["패턴 매칭", f"발견된 패턴: {', '.join(set(intent_hints))}"],
            analyzer_used=self.get_name(),
            fallback_used=True
        )
    
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