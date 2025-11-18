import re
import json
import time
import logging
from typing import Optional, Dict
import anthropic
from .base import BaseAnalyzer
from ..models.query import QueryAnalysis
from ..config import Config

logger = logging.getLogger(__name__)


class ClaudeAnalyzer(BaseAnalyzer):
    """Claude API 기반 쿼리 분석기
    
    Claude를 사용하여 자연어 쿼리를 심층 분석합니다.
    """
    
    def __init__(self, config: Config = None):
        """초기화
        
        Args:
            config: 설정 객체
        """
        self.config = config or Config()
        self.client = anthropic.Anthropic(api_key=self.config.CLAUDE_API_KEY)
        logger.info("ClaudeAnalyzer 초기화 완료")
    
    def get_name(self) -> str:
        """분석기 이름 반환"""
        return "ClaudeAnalyzer"
    
    def analyze(self, query: str, context: str = "") -> QueryAnalysis:
        """Claude를 사용한 쿼리 분석
        
        Args:
            query: 분석할 쿼리
            context: 추가 맥락
            
        Returns:
            분석 결과
        """
        start_time = time.time()
        
        if not self.validate_query(query):
            return self._create_empty_analysis()
        
        query = self.preprocess_query(query)
        
        # 프롬프트 구성
        prompt = self._build_prompt(query, context)
        
        try:
            # Claude API 호출
            response = self.client.messages.create(
                model=self.config.CLAUDE_MODEL,
                max_tokens=self.config.CLAUDE_MAX_TOKENS,
                temperature=self.config.CLAUDE_TEMPERATURE,
                messages=[{"role": "user", "content": prompt}]
            )
            
            # 응답 파싱
            analysis = self._parse_response(response.content[0].text)
            analysis.execution_time = time.time() - start_time
            analysis.analyzer_used = self.get_name()
            
            return analysis
            
        except Exception as e:
            logger.error(f"Claude API 오류: {e}")
            return self._create_fallback_analysis(query, str(e))
    
    def _build_prompt(self, query: str, context: str) -> str:
        """Claude용 프롬프트 구성"""
        return f"""
설문조사 데이터 검색을 위한 쿼리를 분석해주세요.

쿼리: "{query}"
맥락: {context if context else "일반 설문조사"}

다음 단계별로 분석하고 JSON으로 출력하세요:

1. 검색 의도 파악
   - exact_match: 정확한 조건 매칭이 필요
   - semantic_search: 의미적 유사성 검색이 필요
   - hybrid: 둘 다 필요

2. **인구통계 조건 추출 (Demographics)**
   - 나이/연령: "20대", "젊은층"(20대/30대), "중년"(40대/50대), "시니어"(60대 이상)
   - 성별: "남성", "여성", "남자", "여자"
   - 지역: "서울", "경기", "부산" 등
   - 직업: "학생", "직장인", "주부", "전문직" 등
   - 결혼여부: "미혼", "기혼"

   예시:
   - "젊은층" → age_groups: ["20대", "30대"]
   - "서울, 경기" → regions: ["서울", "경기"]
   - "중년 남성" → age_groups: ["40대", "50대"], gender: "남성"

3. **행동 조건 추출 (Behavioral)** - 35개 이상의 설문 질문 기반 조건 지원

   기본 행동:
   - 흡연: "흡연자", "비흡연자" → smoker
   - 음주: "술 마시는", "음주자" → drinker
   - 차량: "차량 보유", "자동차 있는" → has_vehicle
   - OTT: "OTT 이용", "넷플릭스 보는" → ott_user
   - 반려동물: "반려동물 키우는", "펫" → has_pet
   - AI: "AI 사용", "챗GPT 쓰는" → ai_user
   - 운동: "헬스하는", "운동하는" → exercises

   생활/소비 패턴:
   - 빠른배송: "당일배송", "새벽배송" → uses_fast_delivery
   - 전통시장: "재래시장 가는" → visits_traditional_market
   - 배달음식: "배달 자주 시키는" → uses_food_delivery
   - 커피: "커피 좋아하는", "카페 자주 가는" → drinks_coffee
   - 외식: "외식 자주 하는" → dines_out
   - 택배: "온라인쇼핑 하는" → uses_parcel_delivery

   디지털/미디어:
   - 구독서비스: "구독 중인" → has_subscription
   - SNS: "인스타 하는", "소셜미디어" → uses_social_media
   - 게임: "게임 하는" → plays_games
   - 독서: "책 읽는" → reads_books
   - 영화/드라마: "드라마 보는" → watches_movies_dramas
   - 음악: "음악 듣는" → streams_music
   - 온라인교육: "인강 듣는" → takes_online_courses

   금융/자산:
   - 금융서비스: "투자하는", "주식" → uses_financial_services
   - 보험: "보험 가입한" → has_insurance
   - 신용카드: "카드 사용하는" → uses_credit_card

   건강/뷰티:
   - 건강검진: "정기검진 받는" → gets_health_checkups
   - 뷰티/화장품: "화장품 쓰는", "스킨케어" → uses_beauty_products
   - 패션: "옷 자주 사는" → shops_fashion

   기술/환경:
   - 가전제품: "가전 관심있는" → interested_in_home_appliances
   - 스마트기기: "아이폰 쓰는" → uses_smart_devices
   - 환경: "친환경 실천하는" → cares_about_environment
   - 기부/봉사: "기부하는" → does_charity
   - 자동차관심: "차 관심있는" → interested_in_cars

   기타:
   - 주거형태: "아파트 거주" → housing_type
   - 대중교통: "지하철 이용" → uses_public_transport
   - 스트레스: "스트레스 받는" → has_stress
   - 여행: "여행 자주 가는" → travels
   - 술자리: "회식 많은" → attends_drinking_gatherings
   - 야근: "야근 많은" → works_overtime
   - 재택근무: "재택근무 하는" → works_remotely

   예시:
   - "OTT 이용하는" → behavioral_conditions: {{"ott_user": true}}
   - "흡연자" → behavioral_conditions: {{"smoker": true}}
   - "술 마시는" → behavioral_conditions: {{"drinker": true}}
   - "반려동물 키우는" → behavioral_conditions: {{"has_pet": true}}
   - "게임 하는" → behavioral_conditions: {{"plays_games": true}}
   - "카페 자주 가는" → behavioral_conditions: {{"drinks_coffee": true}}

4. 키워드 추출 (Demographics/Behavioral 제외)
   - must_terms: 인구통계/행동 조건이 아닌 검색 키워드만
   - should_terms: 포함되면 좋은 키워드
   - must_not_terms: 제외해야 할 키워드

5. 키워드 확장
   - 각 주요 키워드의 동의어, 유사어, 관련어

6. 검색 파라미터
   - alpha: 0.0~1.0 (키워드 중심이면 낮게, 의미 중심이면 높게)

JSON 형식:
{{
  "reasoning_steps": ["분석 과정 설명"],
  "intent": "exact_match|semantic_search|hybrid",
  "demographics": {{
    "age_groups": ["20대", "30대"],
    "gender": "남성|여성|null",
    "regions": ["서울", "경기"],
    "occupation": "직장인|학생|null",
    "marital_status": "미혼|기혼|null"
  }},
  "behavioral_conditions": {{
    "smoker": true|false|null,
    "drinker": true|false|null,
    "has_vehicle": true|false|null,
    "ott_user": true|false|null,
    "has_pet": true|false|null,
    "ai_user": true|false|null,
    "exercises": true|false|null,
    "uses_fast_delivery": true|false|null,
    "visits_traditional_market": true|false|null,
    "uses_food_delivery": true|false|null,
    "drinks_coffee": true|false|null,
    "has_subscription": true|false|null,
    "uses_social_media": true|false|null,
    "plays_games": true|false|null,
    "reads_books": true|false|null,
    "watches_movies_dramas": true|false|null,
    "streams_music": true|false|null,
    "takes_online_courses": true|false|null,
    "uses_financial_services": true|false|null,
    "gets_health_checkups": true|false|null,
    "uses_beauty_products": true|false|null,
    "shops_fashion": true|false|null,
    "interested_in_home_appliances": true|false|null,
    "uses_smart_devices": true|false|null,
    "cares_about_environment": true|false|null,
    "does_charity": true|false|null,
    "interested_in_cars": true|false|null,
    "housing_type": true|false|null,
    "has_insurance": true|false|null,
    "uses_credit_card": true|false|null,
    "uses_public_transport": true|false|null,
    "uses_parcel_delivery": true|false|null,
    "dines_out": true|false|null,
    "attends_drinking_gatherings": true|false|null,
    "works_overtime": true|false|null,
    "works_remotely": true|false|null,
    "has_stress": true|false|null,
    "travels": true|false|null
    // ⭐ null = 이 조건을 체크하지 않음 (쿼리에서 언급 안됨)
    // ⭐ true/false = 명시적으로 체크해야 함
  }},
  "must_terms": ["필수키워드"],
  "should_terms": ["선택키워드"],
  "must_not_terms": ["제외키워드"],
  "alpha": 0.5,
  "expanded_keywords": {{"키워드": ["확장어"]}},
  "confidence": 0.85,
  "explanation": "최종 설명"
}}

⚠️ 중요: Demographics/Behavioral 조건은 must_terms에 넣지 마세요!

예시 1: "서울, 경기 OTT 이용하는 젊은층 30명"
{{
  "reasoning_steps": ["젊은층=20대/30대 추출", "지역=서울,경기 추출", "OTT 이용=behavioral 추출"],
  "intent": "exact_match",
  "demographics": {{
    "age_groups": ["20대", "30대"],
    "gender": null,
    "regions": ["서울", "경기"],
    "occupation": null,
    "marital_status": null
  }},
  "behavioral_conditions": {{
    "smoker": null,
    "drinker": null,
    "has_vehicle": null,
    "ott_user": true
  }},
  "must_terms": [],
  "should_terms": [],
  "must_not_terms": [],
  "alpha": 0.3,
  "expanded_keywords": {{}},
  "confidence": 0.95,
  "explanation": "젊은층(20대/30대), 서울/경기, OTT 사용자를 필터로 검색"
}}

예시 2: "스킨케어 관심있는 20대 여성"
{{
  "reasoning_steps": ["20대 추출", "여성 추출", "스킨케어=검색 키워드"],
  "intent": "hybrid",
  "demographics": {{
    "age_groups": ["20대"],
    "gender": "여성",
    "regions": [],
    "occupation": null,
    "marital_status": null
  }},
  "behavioral_conditions": {{
    "smoker": null,
    "drinker": null,
    "has_vehicle": null,
    "ott_user": null
  }},
  "must_terms": ["스킨케어"],
  "should_terms": ["관심", "화장품", "피부"],
  "must_not_terms": [],
  "alpha": 0.6,
  "expanded_keywords": {{"스킨케어": ["피부관리", "화장품", "미용"]}},
  "confidence": 0.9,
  "explanation": "20대 여성 필터 + 스킨케어 키워드 검색"
}}
"""
    
    def _parse_response(self, response_text: str) -> QueryAnalysis:
        """Claude 응답 파싱"""
        try:
            # JSON 추출
            json_match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
            if json_match:
                json_text = json_match.group(1)
            else:
                # JSON 마커가 없으면 중괄호 찾기
                json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
                json_text = json_match.group(0) if json_match else response_text

            # JSON 파싱
            data = json.loads(json_text)

            # ⭐ 디버깅: Claude 응답 로깅
            logger.warning(f"[Claude 응답] intent={data.get('intent')}")
            logger.warning(f"[Claude 응답] demographics={data.get('demographics')}")
            logger.warning(f"[Claude 응답] behavioral_conditions={data.get('behavioral_conditions')}")
            logger.warning(f"[Claude 응답] must_terms={data.get('must_terms')}")

            # ⭐ Demographics 파싱 및 DemographicEntity 생성
            demographic_entities = []
            demographics_data = data.get("demographics", {})

            if demographics_data:
                from ..models.entities import DemographicEntity, DemographicType

                # Age groups
                age_groups = demographics_data.get("age_groups", [])
                if isinstance(age_groups, list):
                    for age_group in age_groups:
                        if age_group:
                            demographic_entities.append(DemographicEntity(
                                name=age_group,  # ⭐ BaseEntity 필수 필드
                                canonical_form=age_group,  # ⭐ BaseEntity 필수 필드
                                demographic_type=DemographicType.AGE,
                                value=age_group,
                                raw_value=age_group,
                                synonyms={age_group}
                            ))

                # Gender
                gender = demographics_data.get("gender")
                if gender and gender not in ["null", None]:
                    demographic_entities.append(DemographicEntity(
                        name=gender,  # ⭐ BaseEntity 필수 필드
                        canonical_form=gender,  # ⭐ BaseEntity 필수 필드
                        demographic_type=DemographicType.GENDER,
                        value=gender,
                        raw_value=gender,
                        synonyms={gender}
                    ))

                # Regions
                regions = demographics_data.get("regions", [])
                if isinstance(regions, list):
                    for region in regions:
                        if region:
                            demographic_entities.append(DemographicEntity(
                                name=region,  # ⭐ BaseEntity 필수 필드
                                canonical_form=region,  # ⭐ BaseEntity 필수 필드
                                demographic_type=DemographicType.REGION,
                                value=region,
                                raw_value=region,
                                synonyms={region}
                            ))

                # Occupation
                occupation = demographics_data.get("occupation")
                if occupation and occupation not in ["null", None]:
                    demographic_entities.append(DemographicEntity(
                        name=occupation,  # ⭐ BaseEntity 필수 필드
                        canonical_form=occupation,  # ⭐ BaseEntity 필수 필드
                        demographic_type=DemographicType.OCCUPATION,
                        value=occupation,
                        raw_value=occupation,
                        synonyms={occupation}
                    ))

                # Marital status
                marital_status = demographics_data.get("marital_status")
                if marital_status and marital_status not in ["null", None]:
                    demographic_entities.append(DemographicEntity(
                        name=marital_status,  # ⭐ BaseEntity 필수 필드
                        canonical_form=marital_status,  # ⭐ BaseEntity 필수 필드
                        demographic_type=DemographicType.MARITAL_STATUS,
                        value=marital_status,
                        raw_value=marital_status,
                        synonyms={marital_status}
                    ))

            logger.warning(f"[Claude 파싱] demographic_entities count: {len(demographic_entities)}")

            return QueryAnalysis(
                intent=data.get("intent", "hybrid"),
                must_terms=data.get("must_terms", []),
                should_terms=data.get("should_terms", []),
                must_not_terms=data.get("must_not_terms", []),
                alpha=float(data.get("alpha", 0.5)),
                expanded_keywords=data.get("expanded_keywords", {}),
                confidence=float(data.get("confidence", 0.5)),
                explanation=data.get("explanation", ""),
                reasoning_steps=data.get("reasoning_steps", []),
                behavioral_conditions=data.get("behavioral_conditions", {}),
                demographic_entities=demographic_entities,
            )
            
        except Exception as e:
            logger.error(f"응답 파싱 실패: {e}")
            raise
    
    def _create_fallback_analysis(self, query: str, error: str) -> QueryAnalysis:
        """폴백 분석 결과"""
        return QueryAnalysis(
            intent="hybrid",
            must_terms=[query],
            should_terms=[],
            must_not_terms=[],
            alpha=0.5,
            expanded_keywords={},
            confidence=0.1,
            explanation=f"Claude API 오류로 기본값 사용: {error}",
            analyzer_used=self.get_name(),
            fallback_used=True,
            behavioral_conditions={},
        )
    
    def _create_empty_analysis(self) -> QueryAnalysis:
        """빈 분석 결과"""
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