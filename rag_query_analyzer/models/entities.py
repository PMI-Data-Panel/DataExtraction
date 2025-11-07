"""엔티티 모델 - 기존 + 개선된 버전"""
from dataclasses import dataclass
from typing import List, Optional, Dict, Set
from pydantic import BaseModel, Field
from enum import Enum


# ============================================
# 기존 엔티티 (하위 호환성 유지)
# ============================================

@dataclass
class Entity:
    """의미론적 모델의 엔티티

    엔티티는 설문조사 도메인의 핵심 개념을 표현합니다.
    예: '응답자', '응답', '행동' 등
    """
    name: str                    # 엔티티 이름
    attributes: List[str]        # 엔티티의 속성들
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

        # 속성 매칭
        for attr in self.attributes:
            if attr.lower() in text_lower:
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
    related_entities: List[str]  # 관련 엔티티들
    unit: str = ""              # 단위 (%, 점 등)
    aggregation_type: str = "avg"  # 집계 유형 (avg, sum, count, etc.)

    def requires_entity(self, entity: str) -> bool:
        """특정 엔티티가 필요한지 확인"""
        return entity in self.related_entities


# ============================================
# 개선된 엔티티 시스템 (Pydantic 기반)
# ============================================

class EntityType(str, Enum):
    """엔티티 타입"""
    DEMOGRAPHIC = "demographic"      # 인구통계
    TOPIC = "topic"                  # 주제
    QUESTION = "question"            # 질문
    ANSWER = "answer"                # 답변


class DemographicType(str, Enum):
    """인구통계 타입"""
    AGE = "age"
    GENDER = "gender"
    REGION = "region"
    OCCUPATION = "occupation"
    EDUCATION = "education"
    INCOME = "income"
    MARITAL_STATUS = "marital_status"


class BaseEntity(BaseModel):
    """모든 엔티티의 기본 클래스"""
    entity_type: EntityType
    name: str
    canonical_form: str  # 정규화된 형태
    synonyms: Set[str] = Field(default_factory=set)
    confidence: float = 1.0
    metadata: Dict = Field(default_factory=dict)


class DemographicEntity(BaseEntity):
    """인구통계 엔티티

    예시:
        "20대" → DemographicEntity(
            demographic_type=AGE,
            value="20s",
            raw_value="20대",
            synonyms={"20대", "20-29", "이십대"}
        )
    """
    entity_type: EntityType = EntityType.DEMOGRAPHIC
    demographic_type: DemographicType
    value: str  # 정규화된 값 (예: "20s", "M")
    raw_value: str  # 원본 값 (예: "20대", "남성")

    # 범위 지원 (연령, 소득 등)
    range_min: Optional[int] = None
    range_max: Optional[int] = None

    def matches(self, query_value: str) -> bool:
        """쿼리 값과 매칭되는지 확인"""
        return (
            query_value in self.synonyms or
            query_value == self.value or
            query_value == self.raw_value
        )
    
    def _build_answer_match_query(self) -> Dict:
        """qa_pairs 답변 매칭 쿼리 생성 (최적화: 중복 제거)
        
        ⚠️ 중요: 실제 답변이 "사무직 (기업체 차장 이하 사무직 종사자, 공무원 등)" 같은 긴 형식이므로
        부분 매칭을 허용하기 위해 match 쿼리 사용 (기본적으로 단어 단위 매칭)
        
        최적화:
        - answer_text와 answer를 하나의 multi_match로 통합
        - 중복 제거
        - 동의어 확장기 사용 (정적 사전 기반)
        """
        # ⭐ 동의어 확장기 사용
        try:
            from ..utils.synonym_expander import get_synonym_expander
            expander = get_synonym_expander()
            # 동의어 확장 (정적 사전 기반)
            expanded_synonyms = expander.expand(self.raw_value)
        except Exception as e:
            # 동의어 확장기 로드 실패 시 기존 방식 사용
            import logging
            logging.getLogger(__name__).debug(f"동의어 확장기 로드 실패, 기본 synonyms 사용: {e}")
            expanded_synonyms = [self.raw_value]
            expanded_synonyms.extend([syn for syn in self.synonyms if syn])
        
        # 모든 매칭할 값 수집 (raw_value + 확장된 동의어)
        all_values = expanded_synonyms if expanded_synonyms else [self.raw_value]
        
        # 기존 synonyms도 추가 (하위 호환성)
        all_values.extend([syn for syn in self.synonyms if syn and syn not in all_values])
        
        # 중복 제거
        unique_values = list(dict.fromkeys(all_values))  # 순서 유지하면서 중복 제거
        
        # answer_text와 answer를 하나의 multi_match로 통합 (중복 제거)
        answer_should = []
        
        # 각 값에 대해 multi_match 사용 (answer_text와 answer 동시 검색)
        for value in unique_values:
            if value:
                # multi_match: answer_text와 answer를 동시에 검색 (중복 제거)
                answer_should.append({
                    "multi_match": {
                        "query": value,
                        "fields": ["qa_pairs.answer_text^1.0", "qa_pairs.answer^0.8"],  # answer_text에 약간 더 가중치
                        "type": "best_fields",  # 가장 높은 점수 사용
                        "operator": "or"  # 단어 단위 매칭
                    }
                })
                # match_phrase: 정확한 구문 매칭 (answer_text만, answer는 생략하여 중복 제거)
                answer_should.append({
                    "match_phrase": {
                        "qa_pairs.answer_text": value
                    }
                })
        
        # 연령대의 경우: 출생년도 범위도 추가 (예: "30대" → 1985-1994)
        # 최적화: 출생년도를 하나의 terms 쿼리로 통합
        if self.demographic_type == DemographicType.AGE:
            from datetime import datetime
            current_year = datetime.now().year
            birth_years = []
            
            if self.raw_value == "30대":
                birth_years = [str(year) for year in range(current_year - 39, current_year - 29)]  # 1985-1994
            elif self.raw_value == "20대":
                birth_years = [str(year) for year in range(current_year - 29, current_year - 19)]  # 1995-2004
            elif self.raw_value == "40대":
                birth_years = [str(year) for year in range(current_year - 49, current_year - 39)]  # 1975-1984
            elif self.raw_value == "50대":
                birth_years = [str(year) for year in range(current_year - 59, current_year - 49)]  # 1965-1974
            
            if birth_years:
                # 출생년도는 정확한 매칭이 필요하므로 terms 쿼리 사용 (중복 제거)
                # answer_text와 answer 모두에 대해 terms 쿼리 적용
                answer_should.append({
                    "terms": {
                        "qa_pairs.answer_text": birth_years
                    }
                })
                answer_should.append({
                    "terms": {
                        "qa_pairs.answer": birth_years
                    }
                })
        
        return {
            "bool": {
                "should": answer_should,
                "minimum_should_match": 1
            }
        }

    def to_opensearch_filter(self) -> Dict:
        """OpenSearch 필터로 변환 (Metadata 우선, qa_pairs fallback)
        
        인덱스별 구조:
        - welcome_1st: 성별, 출생정보(연령) → metadata 필드
        - welcome_2nd: 직업 → metadata 필드
        - 나머지 설문조사: qa_pairs에서 찾기
        
        Returns:
            {
                "bool": {
                    "should": [
                        {"term": {"metadata.age_group.keyword": "30s"}},
                        {"nested": {...}}  # qa_pairs fallback
                    ],
                    "minimum_should_match": 1
                }
            }
        """
        # 1️⃣ Metadata 필드 매핑
        # 인덱스별 실제 질문 텍스트 반영:
        # - welcome_1st: "귀하의 성별은", "귀하의 출생년도는 어떻게 되십니까?", "회원님께서 현재 살고 계신 지역은 어디인가요?"
        # - welcome_2nd: "직업", "직무"
        metadata_mapping = {
            DemographicType.AGE: {
                "field": "metadata.age_group.keyword",
                "qa_questions": [
                    "출생년도",  # welcome_1st: "귀하의 출생년도는 어떻게 되십니까?"
                    "출생", "연령", "나이", "연령대", "age", "생년월일"
                ]
            },
            DemographicType.GENDER: {
                "field": "metadata.gender.keyword",
                "qa_questions": [
                    "성별",  # welcome_1st: "귀하의 성별은"
                    "gender"
                ]
            },
            DemographicType.OCCUPATION: {
                "field": "metadata.occupation.keyword",
                "qa_questions": [
                    "직업",  # welcome_2nd: "직업"
                    "직무",  # welcome_2nd: "직무"
                    "occupation", "직종"
                ]
            },
            DemographicType.REGION: {
                "field": "metadata.region.keyword",
                "qa_questions": [
                    "지역",  # welcome_1st: "회원님께서 현재 살고 계신 지역은 어디인가요?"
                    "거주지", "주소", "region"
                ]
            },
            DemographicType.EDUCATION: {
                "field": "metadata.education.keyword",
                "qa_questions": [
                    "최종학력",  # welcome_2nd: "최종학력"
                    "학력", "education", "학위"
                ]
            },
        }
        
        config = metadata_mapping.get(self.demographic_type)
        if not config:
            # 알 수 없는 타입: 단순 term 필터
            field_name = f"metadata.{self.demographic_type.value}.keyword"
            return {
                "term": {
                    field_name: self.value
                }
            }
        
        # 2️⃣ Metadata 필터 (1순위) - welcome_1st, welcome_2nd 등에서 사용
        # ⚠️ 중요: welcome_1st의 metadata.age_group은 이미 "30대" 형식이므로
        # value가 "30s"가 아니라 raw_value인 "30대"를 사용해야 함!
        metadata_value = self.value
        
        # welcome_1st의 경우: age_group과 gender는 raw_value 사용
        if self.demographic_type == DemographicType.AGE:
            # metadata.age_group은 "30대" 형식이므로 raw_value 사용
            metadata_value = self.raw_value
        elif self.demographic_type == DemographicType.GENDER:
            # metadata.gender는 "여성", "남성" 형식이므로 raw_value 사용
            metadata_value = self.raw_value
        
        metadata_filter = {
            "term": {
                config["field"]: metadata_value
            }
        }
        
        # 3️⃣ QA Pairs 필터 (2순위, fallback) - 나머지 설문조사 데이터에서 사용
        qa_filter = {
            "nested": {
                "path": "qa_pairs",
                "query": {
                    "bool": {
                        "must": [
                            # 질문 매칭 (여러 키워드 OR)
                            {
                                "bool": {
                                    "should": [
                                        {"match": {"qa_pairs.q_text": q}}
                                        for q in config["qa_questions"]
                                    ],
                                    "minimum_should_match": 1
                                }
                            },
                            # 답변 매칭 (raw_value와 synonyms 모두 고려)
                            # ⚠️ 출생년도는 숫자("2001")로 저장되므로, 연령대를 출생년도 범위로 변환 필요
                            self._build_answer_match_query()
                        ]
                    }
                },
                "inner_hits": {
                    "size": 3,
                    "_source": {
                        "includes": ["qa_pairs.q_text", "qa_pairs.answer_text", "qa_pairs.answer"]
                    }
                }
            }
        }
        
        # 4️⃣ Bool 쿼리: metadata OR qa_pairs
        # ⚠️ OCCUPATION은 metadata.occupation 필드가 없거나 "미정"인 경우가 많으므로 qa_pairs만 사용
        if self.demographic_type == DemographicType.OCCUPATION:
            # qa_pairs만 사용 (metadata 제거)
            return qa_filter
        
        # 인덱스별로 자동으로 적절한 필드를 찾음:
        # - welcome_1st, welcome_2nd: metadata 필드 매칭
        # - 나머지: qa_pairs에서 매칭
        return {
            "bool": {
                "should": [
                    metadata_filter,
                    qa_filter
                ],
                "minimum_should_match": 1
            }
        }

    def to_nested_qa_filter(self, question_keyword: str) -> Dict:
        """qa_pairs nested 쿼리 필터로 변환 (하위 호환성 유지)

        Args:
            question_keyword: 질문 키워드 (예: "직업", "연령")

        Returns:
            nested 쿼리
        """
        return {
            "nested": {
                "path": "qa_pairs",
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"qa_pairs.q_text": question_keyword}},
                            {"match": {"qa_pairs.answer_text": self.raw_value}}
                        ]
                    }
                },
                "inner_hits": {
                    "size": 3,
                    "_source": {
                        "includes": ["qa_pairs.q_text", "qa_pairs.answer_text", "qa_pairs.answer"]
                    }
                }
            }
        }


class TopicEntity(BaseEntity):
    """주제 엔티티

    예시:
        "제품 만족도" → TopicEntity(
            keywords={"제품", "만족도", "평가"},
            related_topics={"구매", "품질"}
        )
    """
    entity_type: EntityType = EntityType.TOPIC
    keywords: Set[str] = Field(default_factory=set)
    related_topics: Set[str] = Field(default_factory=set)
    parent_topic: Optional[str] = None
    sub_topics: List[str] = Field(default_factory=list)
    frequency: int = 0  # 이 주제가 나타난 횟수


class QuestionEntity(BaseEntity):
    """질문 엔티티"""
    entity_type: EntityType = EntityType.QUESTION
    question_id: str
    question_text: str
    question_type: str  # "satisfaction", "preference", "awareness" 등
    topics: List[str] = Field(default_factory=list)
    keywords: Set[str] = Field(default_factory=set)


class ExtractedEntities(BaseModel):
    """쿼리에서 추출된 모든 엔티티

    예시:
        "20대 남성의 제품 만족도" →
        ExtractedEntities(
            demographics=[
                DemographicEntity(type=AGE, value="20s"),
                DemographicEntity(type=GENDER, value="M")
            ],
            topics=[TopicEntity(name="제품 만족도")]
        )
    """
    demographics: List[DemographicEntity] = Field(default_factory=list)
    topics: List[TopicEntity] = Field(default_factory=list)
    questions: List[QuestionEntity] = Field(default_factory=list)

    # 원본 쿼리
    original_query: str
    confidence: float = 0.0

    def to_filters(self) -> List[Dict]:
        """OpenSearch 필터로 변환

        Returns:
            [{"bool": {"should": [metadata_filter, qa_filter]}}, ...]
            
        Note:
            각 DemographicEntity.to_opensearch_filter()는 이미 metadata OR qa_pairs를 포함하므로
            중복으로 to_nested_qa_filter()를 추가할 필요 없음
        """
        filters = []

        # Demographics → 필터 (metadata 우선, qa_pairs fallback)
        for demo in self.demographics:
            # to_opensearch_filter()가 이미 metadata OR qa_pairs를 포함
            filters.append(demo.to_opensearch_filter())

        return filters

    def to_keywords(self) -> List[str]:
        """검색 키워드로 변환

        Returns:
            ["20대", "남성", "제품", "만족도"]
        """
        keywords = []

        # Demographics
        for demo in self.demographics:
            keywords.append(demo.raw_value)
            keywords.extend(list(demo.synonyms))

        # Topics
        for topic in self.topics:
            keywords.append(topic.name)
            keywords.extend(list(topic.keywords))

        # 중복 제거
        return list(set(keywords))

    def to_dict(self) -> Dict:
        """딕셔너리로 변환 (로깅/디버깅용)"""
        return {
            "original_query": self.original_query,
            "confidence": self.confidence,
            "demographics": [
                {
                    "type": demo.demographic_type.value,
                    "value": demo.value,
                    "raw_value": demo.raw_value
                }
                for demo in self.demographics
            ],
            "topics": [
                {
                    "name": topic.name,
                    "keywords": list(topic.keywords)
                }
                for topic in self.topics
            ]
        }
