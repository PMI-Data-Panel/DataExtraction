import logging
from typing import Optional
from .base import BaseAnalyzer
from ..models.query import QueryAnalysis
from ..core.semantic_model import SemanticModel
from ..config import Config

logger = logging.getLogger(__name__)


class SemanticAnalyzer(BaseAnalyzer):  # 전부다 벡터값을 넣는게 아니니까 벡터값이 없는 거는 어떻게 하지?? 라는 의문 
    # 그럼 다시 제조명해야 하는게 주관식과 객관식, 주관식 벡터로만!! 좋았다!! 주관식하고 객관식을 나눌 수 있지 않을까?? 
    # 주관식은 필요하지 않아?? - 그거는 해봐야 할 것 같기한데 ㅇㅋㅇㅋ 벡터값이 없는 걸로!!한번 해보고 
    # 그럼 코드방향성을 테스트할 수 있게끔 여려 가지로 만들어야 겠네 (벡터 없이도, 벡터넣어서도 해보고 )
    """의미론적 모델 기반 분석기
    
    도메인 지식을 활용하여 쿼리를 분석합니다.
    """
    
    def __init__(self, config: Config = None):
        """초기화
        
        Args:
            config: 설정 객체
        """
        self.config = config or Config()
        self.semantic_model = SemanticModel()
        logger.info("SemanticAnalyzer 초기화 완료")
    
    def get_name(self) -> str:
        """분석기 이름 반환"""
        return "SemanticAnalyzer"
    
    def analyze(self, query: str, context: str = "") -> QueryAnalysis:
        """의미론적 분석 수행
        
        Args:
            query: 분석할 쿼리
            context: 추가 맥락
            
        Returns:
            분석 결과
        """
        if not self.validate_query(query):
            return self._create_empty_analysis()
        
        query = self.preprocess_query(query)
        
        # 의도 파악
        semantic_intent, confidence = self.semantic_model.detect_intent(query)
        search_intent = self.semantic_model.map_to_search_intent(semantic_intent)
        
        # 엔티티 추출
        entities = self.semantic_model.extract_entities(query)
        
        # 키워드 구성
        must_terms = []
        expanded_keywords = {}
        
        for entity_type, attributes in entities.items():
            for attr in attributes:
                # 원본 속성 추가
                clean_attr = attr.split('(')[0] if '(' in attr else attr
                must_terms.append(clean_attr)
                
                # 관련 키워드 확장
                related = self.semantic_model.get_related_keywords(entity_type, clean_attr)
                if len(related) > 1:
                    expanded_keywords[clean_attr] = [r for r in related if r != clean_attr]
        
        # Alpha 값 계산
        alpha = self._calculate_alpha(search_intent, confidence)
        
        # 추론 단계 구성
        reasoning_steps = [
            f"의도 감지: {semantic_intent} (신뢰도: {confidence:.2f})",
            f"추출된 엔티티: {', '.join(entities.keys())}",
            f"검색 전략: {search_intent}"
        ]
        
        return QueryAnalysis(
            intent=search_intent,
            must_terms=list(set(must_terms)),
            should_terms=[],
            must_not_terms=[],
            alpha=alpha,
            expanded_keywords=expanded_keywords,
            confidence=confidence,
            explanation=f"의미론적 분석 완료 (의도: {semantic_intent})",
            reasoning_steps=reasoning_steps,
            semantic_intent=semantic_intent,
            analyzer_used=self.get_name(),
            behavioral_conditions={},
        )
    
    def _calculate_alpha(self, intent: str, confidence: float) -> float:
        """Alpha 값 계산
        
        Args:
            intent: 검색 의도
            confidence: 신뢰도
            
        Returns:
            Alpha 값
        """
        base_alpha = {
            "exact_match": 0.2,
            "semantic_search": 0.8,
            "hybrid": 0.5
        }.get(intent, 0.5)
        
        # 신뢰도가 낮으면 중간값으로 조정
        if confidence < 0.5:
            base_alpha = 0.5 + (base_alpha - 0.5) * confidence
        
        return base_alpha
    
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
            analyzer_used=self.get_name(),
            behavioral_conditions={},
        )

