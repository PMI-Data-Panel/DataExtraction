import logging
from typing import Dict, List, Optional
from .semantic_model import SemanticModel

logger = logging.getLogger(__name__)


class QueryExpander:
    """쿼리 확장 및 보강
    
    쿼리에 맥락 정보를 추가하고 동의어를 확장합니다.
    """
    
    def __init__(self, semantic_model: SemanticModel = None):
        """초기화
        
        Args:
            semantic_model: 의미론적 모델 (선택)
        """
        self.semantic_model = semantic_model or SemanticModel()
        logger.info("QueryExpander 초기화 완료")
    
    def expand_with_context(self, 
                           query: str, 
                           survey_metadata: Dict = None) -> str:
        """설문 메타데이터를 활용한 쿼리 확장
        
        Args:
            query: 원본 쿼리
            survey_metadata: 설문 메타데이터
            
        Returns:
            확장된 쿼리
        """
        if not survey_metadata:
            return query
        
        expanded_parts = [query]
        
        # 시간적 맥락 추가
        if "시기" not in query.lower() and survey_metadata.get("period"):
            expanded_parts.append(f"({survey_metadata['period']} 기준)")
        
        # 지역적 맥락 추가
        if survey_metadata.get("region_scope"):
            if survey_metadata["region_scope"] not in query:
                expanded_parts.append(f"({survey_metadata['region_scope']} 지역)")
        
        # 설문 유형별 맥락
        if survey_metadata.get("survey_type"):
            context_info = self._get_survey_context(survey_metadata["survey_type"])
            if context_info:
                logger.info(f"🔄 설문 유형 '{survey_metadata['survey_type']}' 맥락 추가")
        
        # 표본 크기 정보
        if survey_metadata.get("sample_size"):
            expanded_parts.append(f"(n={survey_metadata['sample_size']})")
        
        # 타겟 그룹 정보
        if survey_metadata.get("target_group"):
            expanded_parts.append(f"(대상: {survey_metadata['target_group']})")
        
        return " ".join(expanded_parts)
    
    def expand_with_synonyms(self, query: str) -> List[str]:
        """동의어를 활용한 쿼리 확장
        
        Args:
            query: 원본 쿼리
            
        Returns:
            확장된 쿼리 리스트
        """
        expanded_queries = [query]
        
        # 엔티티별 동의어 치환
        for entity_key, entity in self.semantic_model.entities.items():
            # 엔티티 이름이 쿼리에 있는지 확인
            if entity.name in query:
                for synonym in entity.synonyms:
                    expanded = query.replace(entity.name, synonym)
                    if expanded not in expanded_queries:
                        expanded_queries.append(expanded)
            
            # 속성이 쿼리에 있는지 확인
            for attr in entity.attributes:
                if attr in query:
                    related_keywords = self.semantic_model.get_related_keywords(
                        entity_key, attr
                    )
                    for keyword in related_keywords:
                        if keyword != attr:
                            expanded = query.replace(attr, keyword)
                            if expanded not in expanded_queries:
                                expanded_queries.append(expanded)
        
        return expanded_queries[:10]  # 최대 10개로 제한
    
    def expand_keywords(self, keywords: List[str]) -> Dict[str, List[str]]:
        """키워드 리스트를 확장
        
        Args:
            keywords: 원본 키워드 리스트
            
        Returns:
            {원본_키워드: [확장된_키워드들]} 딕셔너리
        """
        expanded = {}
        
        for keyword in keywords:
            expansions = [keyword]
            
            # 표준 확장 패턴
            standard_expansions = {
                #question_list csv 파일 -  저도주 관련, 직업(서비스,직업)
                # 나이 관련
                "20대": ["20-29", "이십대", "20세~29세", "twenties"],
                "30대": ["30-39", "삼십대", "30세~39세", "thirties"],
                "40대": ["40-49", "사십대", "40세~49세", "forties"],
                "50대": ["50-59", "오십대", "50세~59세", "fifties"],
                
                # 성별 관련
                "남성": ["남자", "male", "남"],
                "여성": ["여자", "female", "여"],
                
                # 지역 관련
                "서울": ["서울시", "서울특별시", "수도권"],
                "부산": ["부산시", "부산광역시"],
                
                # 직업 관련
                "직장인": ["회사원", "사무직", "직장", "근로자"],
                "학생": ["학생", "대학생", "중고생", "student"],
                "주부": ["전업주부", "가정주부", "housewife"],
                
                # 감정 관련
                "만족": ["만족함", "만족스러움", "satisfied"],
                "불만": ["불만족", "불만스러움", "dissatisfied"],
                
                # 빈도 관련
                "자주": ["빈번히", "많이", "often", "frequently"],
                "가끔": ["때때로", "종종", "sometimes"],
                "거의": ["대부분", "almost", "nearly"]
            }
            
            # 표준 확장 적용
            for pattern, expansion_list in standard_expansions.items():
                if keyword.lower() == pattern.lower():
                    expansions.extend(expansion_list)
                    break
            
            # 의미론적 모델 기반 확장
            entities = self.semantic_model.extract_entities(keyword)
            for entity_type, attributes in entities.items():
                for attr in attributes:
                    related = self.semantic_model.get_related_keywords(entity_type, attr)
                    expansions.extend(related)
            
            # 중복 제거하고 저장
            expanded[keyword] = list(set(expansions))[:5]  # 최대 5개
        
        return expanded
    
    def _get_survey_context(self, survey_type: str) -> Optional[Dict]:
        """설문 유형별 컨텍스트 정보 반환"""
        context_map = {
            "만족도": {
                "scales": ["매우만족", "만족", "보통", "불만", "매우불만"],
                "keywords": ["만족감", "만족수준", "satisfaction"],
                "focus": "긍정/부정 평가"
            },
            "선호도": {
                "scales": ["매우선호", "선호", "보통", "비선호", "매우비선호"],
                "keywords": ["좋아함", "선호함", "preference"],
                "focus": "선택과 기호"
            },
            "인식": {
                "scales": ["잘알고있음", "조금알고있음", "모름", "전혀모름"],
                "keywords": ["인지", "알고있음", "awareness"],
                "focus": "인지도와 이해도"
            },
            "구매의향": {
                "scales": ["반드시구매", "구매고려", "미정", "구매안함", "절대안함"],
                "keywords": ["구매", "구입", "purchase intention"],
                "focus": "구매 가능성"
            },
            "추천의향": {
                "scales": ["적극추천", "추천", "보통", "비추천", "절대비추천"],
                "keywords": ["추천", "권유", "recommendation", "NPS"],
                "focus": "타인 추천 의향"
            }
        }
        
        return context_map.get(survey_type)