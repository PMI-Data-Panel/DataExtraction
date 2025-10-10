import logging
from typing import Dict, List, Tuple, Optional
from ..models.entities import Entity, Relationship, Metric

logger = logging.getLogger(__name__)


class SemanticModel:
    """설문 데이터의 의미론적 모델
    
    설문조사 도메인의 개념, 관계, 의도를 모델링합니다.
    """
    
    def __init__(self):
        """의미론적 모델 초기화"""
        self._init_entities()
        self._init_relationships()
        self._init_intent_patterns()
        self._init_metrics()
        logger.info("SemanticModel 초기화 완료")
    
    def _init_entities(self):
        """엔티티 정의"""
        self.entities = {
            "respondent": Entity(
                name="응답자",
                attributes=["나이", "성별", "지역", "직업", "소득", "학력", "결혼상태", "가구구성"],
                synonyms=["참가자", "설문자", "응답인", "피조사자", "참여자", "조사대상자"],
                weight=1.0,
                description="설문조사에 참여한 사람"
            ),
            
            "response": Entity(
                name="응답",
                attributes=["만족도", "선호도", "의견", "평가", "점수", "등급", "순위", "척도"],
                synonyms=["답변", "대답", "피드백", "응답내용", "회답", "답변내용"],
                weight=0.9,
                description="설문 문항에 대한 응답"
            ),
            
            "behavior": Entity(
                name="행동",
                attributes=["구매", "사용", "방문", "참여", "활동", "이용", "경험", "빈도"],
                synonyms=["행위", "활동", "패턴", "습관", "행태", "이용행태"],
                weight=0.8,
                description="응답자의 행동 패턴"
            ),
            
            "emotion": Entity(
                name="감정",
                attributes=["행복", "불안", "스트레스", "만족", "기쁨", "걱정", "우울", "분노"],
                synonyms=["정서", "기분", "느낌", "감성", "심리", "마음"],
                weight=0.7,
                description="응답자의 감정 상태"
            ),
            
            "product": Entity(
                name="제품",
                attributes=["품질", "가격", "디자인", "기능", "브랜드", "서비스", "AS"],
                synonyms=["상품", "제품", "서비스", "브랜드", "아이템"],
                weight=0.8,
                description="평가 대상 제품/서비스"
            )
        }
    
    def _init_relationships(self):
        """엔티티 간 관계 정의"""
        self.relationships = [
            Relationship("respondent", "response", "provides", 1.0, False),
            Relationship("respondent", "behavior", "exhibits", 0.9, False),
            Relationship("behavior", "emotion", "influences", 0.8, True),
            Relationship("response", "emotion", "reflects", 0.7, False),
            Relationship("product", "response", "receives", 0.9, False),
            Relationship("respondent", "product", "uses", 0.8, False)
        ]
    
    def _init_intent_patterns(self):
        """검색 의도 패턴 정의"""
        self.intent_patterns = {
            "demographic_filter": {
                "patterns": ["나이", "성별", "지역", "직업", "소득", "학력", "세대", "연령", "거주", "사는"],
                "weight": 1.0,
                "description": "인구통계학적 필터링"
            },
            
            "sentiment_analysis": {
                "patterns": ["만족", "불만", "행복", "스트레스", "긍정", "부정", "좋", "나쁘", "싫", "감정"],
                "weight": 0.9,
                "description": "감정/태도 분석"
            },
            
            "comparison": {
                "patterns": ["비교", "차이", "대비", "vs", "versus", "상대적", "더", "보다", "대조"],
                "weight": 0.8,
                "description": "그룹 간 비교"
            },
            
            "trend": {
                "patterns": ["변화", "추세", "경향", "패턴", "흐름", "추이", "동향", "증가", "감소"],
                "weight": 0.7,
                "description": "시계열 트렌드 분석"
            },
            
            "aggregation": {
                "patterns": ["평균", "총", "합계", "비율", "분포", "통계", "집계", "빈도", "몇"],
                "weight": 0.8,
                "description": "통계 집계"
            },
            
            "segmentation": {
                "patterns": ["그룹", "세그먼트", "클러스터", "유형", "분류", "타입", "종류"],
                "weight": 0.7,
                "description": "응답자 세분화"
            }
        }
    
    def _init_metrics(self):
        """비즈니스 메트릭 정의"""
        self.metrics = {
            "satisfaction_score": Metric(
                name="만족도 점수",
                calculation="만족도 응답의 평균값",
                related_entities=["response", "respondent"],
                unit="점",
                aggregation_type="avg"
            ),
            
            "response_rate": Metric(
                name="응답률",
                calculation="응답자 수 / 전체 대상자 수",
                related_entities=["respondent"],
                unit="%",
                aggregation_type="rate"
            ),
            
            "nps": Metric(
                name="NPS",
                calculation="추천자% - 비추천자%",
                related_entities=["response", "respondent"],
                unit="점",
                aggregation_type="custom"
            )
        }
    
    def detect_intent(self, query: str) -> Tuple[str, float]:
        """쿼리에서 의미론적 의도 파악
        
        Args:
            query: 분석할 쿼리
            
        Returns:
            (의도, 신뢰도) 튜플
        """
        query_lower = query.lower()
        intent_scores = {}
        
        # 각 의도별 점수 계산
        for intent, data in self.intent_patterns.items():
            patterns = data["patterns"]
            weight = data["weight"]
            
            # 패턴 매칭 점수
            matches = sum(1 for pattern in patterns if pattern in query_lower)
            score = matches * weight
            
            intent_scores[intent] = score
        
        # 최고 점수 의도 찾기
        if not intent_scores or max(intent_scores.values()) == 0:
            return "unknown", 0.0
        
        best_intent = max(intent_scores, key=intent_scores.get)
        total_score = sum(intent_scores.values())
        confidence = intent_scores[best_intent] / total_score if total_score > 0 else 0
        
        logger.debug(f"의도 감지: {best_intent} (신뢰도: {confidence:.2f})")
        return best_intent, confidence
    
    def extract_entities(self, query: str) -> Dict[str, List[str]]:
        """쿼리에서 엔티티 추출
        
        Args:
            query: 분석할 쿼리
            
        Returns:
            {엔티티_타입: [추출된_속성들]} 딕셔너리
        """
        extracted = {}
        query_lower = query.lower()
        
        for entity_key, entity in self.entities.items():
            found_attributes = []
            
            # 엔티티 이름 체크
            if entity.name.lower() in query_lower:
                found_attributes.append(entity.name)
            
            # 속성 체크
            for attr in entity.attributes:
                if attr.lower() in query_lower:
                    found_attributes.append(attr)
            
            # 동의어 체크
            for synonym in entity.synonyms:
                if synonym.lower() in query_lower:
                    found_attributes.append(f"{entity.name}({synonym})")
                    break
            
            if found_attributes:
                extracted[entity_key] = list(set(found_attributes))  # 중복 제거
        
        logger.debug(f"추출된 엔티티: {extracted}")
        return extracted
    
    def map_to_search_intent(self, semantic_intent: str) -> str:
        """의미론적 의도를 검색 전략으로 매핑
        
        Args:
            semantic_intent: 의미론적 의도
            
        Returns:
            검색 전략 (exact_match, semantic_search, hybrid)
        """
        mapping = {
            "demographic_filter": "exact_match",
            "sentiment_analysis": "semantic_search",
            "comparison": "hybrid",
            "trend": "hybrid",
            "aggregation": "exact_match",
            "segmentation": "hybrid",
            "unknown": "hybrid"
        }
        
        return mapping.get(semantic_intent, "hybrid")
    
    def get_related_keywords(self, entity_type: str, attribute: str) -> List[str]:
        """엔티티와 속성에 관련된 키워드 반환
        
        Args:
            entity_type: 엔티티 타입
            attribute: 속성
            
        Returns:
            관련 키워드 리스트
        """
        keywords = [attribute]
        
        if entity_type in self.entities:
            entity = self.entities[entity_type]
            
            # 동의어 추가
            if attribute == entity.name:
                keywords.extend(entity.synonyms)
            
            # 관련 속성 추가
            if attribute in entity.attributes:
                # 특정 속성에 대한 확장 (도메인 특화)
                expansions = {
                    "나이": ["연령", "세대", "연령대", "나이대"],
                    "성별": ["남성", "여성", "남자", "여자", "남", "여"],
                    "지역": ["거주지", "사는곳", "거주", "지역"],
                    "직업": ["직장", "일", "근무", "직종"],
                    "만족도": ["만족", "불만족", "만족감", "만족수준"],
                    "선호도": ["선호", "좋아함", "preference", "선택"]
                }
                
                if attribute in expansions:
                    keywords.extend(expansions[attribute])
        
        return list(set(keywords))  # 중복 제거
    
    def calculate_entity_relevance(self, query: str, entity_type: str) -> float:
        """쿼리에 대한 엔티티 관련성 점수 계산
        
        Args:
            query: 분석할 쿼리
            entity_type: 엔티티 타입
            
        Returns:
            관련성 점수 (0-1)
        """
        if entity_type not in self.entities:
            return 0.0
        
        entity = self.entities[entity_type]
        query_lower = query.lower()
        score = 0.0
        
        # 엔티티 이름 매칭
        if entity.name.lower() in query_lower:
            score += 0.5
        
        # 동의어 매칭
        for synonym in entity.synonyms:
            if synonym.lower() in query_lower:
                score += 0.3
                break
        
        # 속성 매칭
        matched_attrs = sum(1 for attr in entity.attributes if attr.lower() in query_lower)
        score += matched_attrs * 0.1
        
        # 가중치 적용
        score *= entity.weight
        
        return min(score, 1.0)

