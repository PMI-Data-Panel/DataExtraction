from dataclasses import dataclass
from typing import List, Optional
from enum import Enum

# 속성의 데이터 타입을 정의하기 위한 Enum 클래스
class AttributeType(Enum):
    """
    엔티티 속성의 데이터 타입을 정의합니다.
    이 정보는 Elasticsearch 쿼리 생성 시 매우 중요하게 사용됩니다.
    - CATEGORICAL: '서울', '여성' 처럼 정확히 일치하는 값을 필터링 (term 쿼리용)
    - NUMERICAL: '나이', '점수' 처럼 범위로 필터링 (range 쿼리용)
    - DATE: 날짜/시간을 기준으로 필터링 (date_range 쿼리용)
    """
    CATEGORICAL = "categorical"
    NUMERICAL = "numerical"
    DATE = "date"
    
# 속성을 더 구체적으로 정의하기 위한 클래스
@dataclass
class Attribute:
    """엔티티의 개별 속성을 구체적으로 정의합니다."""
    name: str                   # 속성의 이름 (예: "나이", "지역")
    attribute_type: AttributeType # 속성의 데이터 타입 (쿼리 생성의 핵심)
    field_name: str             # Elasticsearch에 저장된 실제 필드 이름 (예: "age", "region.keyword")
    description: str = ""       # 속성에 대한 설명
    
    is_nested: bool = False             # 이 속성이 nested 필드 안에 있는지 여부
    nested_path: Optional[str] = None   # nested 필드의 경로 (예: "qa_pairs")
    identifier_field: Optional[str] = None # 속성을 식별하는 필드 (예: "qa_pairs.q_text")
    value_field: Optional[str] = None      # 실제 값이 담긴 필드 (예: "qa_pairs.answer_text")
@dataclass
class Entity:
    """의미론적 모델의 엔티티
    
    엔티티는 설문조사 도메인의 핵심 개념을 표현합니다.
    예: '응답자', '응답', '행동' 등
    """
    name: str                    # 엔티티 이름
    attributes: List[Attribute]      # 엔티티의 속성들
    synonyms: List[str]          # 동의어/유사어
    weight: float = 1.0          # 중요도 가중치 (0-1)
    description: str = ""        # 설명
    
    def has_attribute(self, attr: str) -> bool:
        """특정 속성을 가지고 있는지 확인"""
        return attr.lower() in [a.lower() for a in self.attributes]
    
    def matches(self, text: str) -> bool:
        """텍스트가 엔티티와 매칭되는지 확인"""
        text_lower = text.lower()
        
        # 이름 매칭
        if self.name.lower() in text_lower:
            return True
        
        # 동의어 매칭
        for synonym in self.synonyms:
            if synonym.lower() in text_lower:
                return True
        
        # 속성 '이름'으로 매칭 확인
        for attr in self.attributes:
            if attr.name.lower() in text_lower:
                return True
        
        return False


@dataclass
class Relationship:
    """엔티티 간의 관계
    
    두 엔티티가 어떻게 연결되어 있는지를 표현합니다.
    예: '응답자' --provides--> '응답'
    """
    source: str              # 시작 엔티티
    target: str              # 대상 엔티티
    relation_type: str       # 관계 유형
    strength: float = 1.0    # 관계 강도 (0-1)
    bidirectional: bool = False  # 양방향 관계 여부
    
    def involves(self, entity: str) -> bool:
        """특정 엔티티가 관계에 포함되는지 확인"""
        return entity in [self.source, self.target]
    
    def get_related(self, entity: str) -> Optional[str]:
        """주어진 엔티티와 연결된 엔티티 반환"""
        if entity == self.source:
            return self.target
        elif entity == self.target and self.bidirectional:
            return self.source
        return None


@dataclass
class Metric:
    """비즈니스 메트릭
    
    설문 분석에서 계산할 수 있는 지표들을 정의합니다.
    예: '만족도 평균', '응답률' 등
    """
    name: str                    # 메트릭 이름
    calculation: str             # 계산 방법 설명
    aggregation_type:str        # 집계 유형 (avg, sum, count, etc.)
    related_entities: List[str]  # 관련 엔티티들
    unit: str = ""              # 단위 (%, 점 등)
    
    def requires_entity(self, entity: str) -> bool:
        """특정 엔티티가 필요한지 확인"""
        return entity in self.related_entities