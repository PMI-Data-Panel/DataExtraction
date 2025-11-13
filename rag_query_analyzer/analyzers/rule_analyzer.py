import re
import logging
from typing import List, Dict, Tuple, Set
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
        self.meta_keywords = self._init_meta_keywords()
        self.demographic_keywords = self._init_demographic_keywords()
        self.behavior_keywords = self._init_behavior_keywords()
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
    
    def _init_meta_keywords(self) -> set:
        """메타 키워드 (검색 조건에서 제외)"""
        return {
            '설문조사', '설문', '데이터', '자료', '정보',
            '보여줘', '보여주세요', '알려줘', '알려주세요',
            '검색', '찾아줘', '찾아주세요', '조회',
            '을', '를', '이', '가', '의', '에', '에서',
            '와', '과', '에게', '한테', '명', '개', '건',
            '사람', '인', '분', '중', '중에', '중에서',
            '응답자', '참여자', '참여자들', '설문응답자', '설문참여자'
        }
    
    def _init_demographic_keywords(self) -> set:
        """Demographics 키워드 (must 조건으로만 가야 함)"""
        return {
            # 연령
            '10대', '20대', '30대', '40대', '50대', '60대', '70대',
            '10-19', '20-29', '30-39', '40-49', '50-59', '60-69', '70-79',
            '십대', '이십대', '삼십대', '사십대', '오십대', '육십대', '칠십대',
            # 성별
            '남성', '여성', '남자', '여자', '남', '여',
            # 직업
            '사무직', '전문직', '서비스직', '학생', '주부', '자영업',
            '직장인', '생산직', '무직', '프리랜서', '사무원', '화이트칼라',
            '대학생', '고등학생', '소상공인', '의사', '변호사', '회계사',
            '서비스업', '판매직', '영업', '블루칼라',
            '공무원', '공직자', '공무', '공직', '가정주부', '실직', '미취업', '자유직',
            '중/고등학생', '대학생/대학원생', '간호사', '엔지니어', '프로그래머',
            '생산/노무직', '경영/관리직', '경영관리직', '교직', '교사', '교수', '강사',
            # 지역
            '서울', '부산', '대구', '인천', '광주', '대전', '울산',
            '경기', '강원', '충북', '충남', '전북', '전남', '경북', '경남', '제주', '세종',
            # 결혼여부
            '미혼', '기혼', '싱글', '결혼', '이혼', '사별', '별거',
            # 학력
            '고졸', '대졸', '대학원', '석사', '박사', '대학 재학', '대학교 재학',
            # 소득
            '100만원', '200만원', '300만원', '400만원', '500만원',
            '소득', '급여', '연봉',
            # 가족수, 자녀수
            '1명', '2명', '3명', '4명', '5명', '가족수', '자녀수', '혼자', '독거',
            # 직무
            'IT', '아이티', '개발', '프로그래밍', '코딩',
            '경영', '인사', '총무', '사무',
            '생산', '정비', '기능', '노무',
            '서비스', '여행', '숙박', '음식', '미용', '보안',
            '의료', '간호', '보건', '복지',
            '건설', '건축', '토목', '환경',
            '교육', '교사', '강사', '교직원',
            '유통', '물류', '운송', '운전',
            '무역', '영업', '판매', '매장관리',
            '전자', '기계', '기술', '화학', '연구개발', 'R&D',
            '재무', '회계', '경리',
            '마케팅', '광고', '홍보', '조사',
            '금융', '보험', '증권',
            '고객상담', 'TM', '텔레마케팅',
            '법률', '인문사회',
            '디자인',
            '문화', '스포츠',
            '인터넷', '통신',
            '방송', '언론',
            '게임'
        }

    def _init_behavior_keywords(self) -> set:
        """행동/습관 관련 키워드"""
        return {
            '흡연', '흡연자', '흡연하는', '흡연량', '흡연률', '흡연율',
            '비흡연', '금연', '금연자', '담배', '담배피는', '담배피움', '담배피우는',
            '담배피고', '담배피며', '담배피면서', '담배피거나',  # ✅ 추가: 담배 피고 등 연결형
            '차량', '자동차', '차량여부', '보유차량', '차', '차량보유', '차량 보유',
            '소유', '소유하는', '소유한', '가진', '보유', '보유한', '보유하는',
            '맥주', '와인', '소주', '술', '음주', '비음주', '금주', '음용', '음용경험'
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

        # Demographics 키워드 분리 (must_terms에서 제거, 필터로만 처리)
        demographic_terms = [t for t in must_terms if t in self.demographic_keywords]
        must_terms = [t for t in must_terms if t not in self.demographic_keywords]

        if demographic_terms:
            logger.info(f"🔍 [RuleAnalyzer] Demographics 키워드 분리: {demographic_terms}")

        # 추가 키워드 추출 (패턴 외, Demographics 제외)
        additional_terms = self._extract_additional_keywords(query, must_terms + demographic_terms)

        # 행동 조건 추출 (예: 흡연자, 비흡연자 등)
        behavioral_conditions, behavior_tokens = self._extract_behavioral_conditions(query)
        if behavior_tokens:
            behavior_keywords_lower = {kw.lower() for kw in self.behavior_keywords}
            behavior_tokens_lower = {token.lower() for token in behavior_tokens}

            def is_behavior_term(term: str) -> bool:
                term_lower = term.lower()
                if term_lower in behavior_keywords_lower:
                    return True
                for token_lower in behavior_tokens_lower:
                    if token_lower and token_lower in term_lower:
                        return True
                for kw_lower in behavior_keywords_lower:
                    if kw_lower and kw_lower in term_lower:
                        return True
                return False

            removed_behavior = [t for t in must_terms + should_terms + demographic_terms + additional_terms if is_behavior_term(t)]
            must_terms = [t for t in must_terms if not is_behavior_term(t)]
            should_terms = [t for t in should_terms if not is_behavior_term(t)]
            demographic_terms = [t for t in demographic_terms if not is_behavior_term(t)]
            additional_terms = [t for t in additional_terms if not is_behavior_term(t)]

            if removed_behavior:
                logger.info(f"🔍 [RuleAnalyzer] 행동 키워드 제거: {list(set(removed_behavior))}")
            if behavioral_conditions:
                logger.info(f"   ✅ 행동 조건 추출: {behavioral_conditions}")

        must_terms.extend(additional_terms)

        # 최종 용어 정리: 행동/메타 키워드 제거
        def _is_behavior_term(term: str) -> bool:
            term_lower = term.lower()
            if term_lower in {kw.lower() for kw in self.behavior_keywords}:
                return True
            for keyword in self.behavior_keywords:
                if keyword.lower() in term_lower:
                    return True
            return False

        def _is_meta_term(term: str) -> bool:
            return term in self.meta_keywords or term.lower() in {kw.lower() for kw in self.meta_keywords}

        original_must_count = len(must_terms)
        original_should_count = len(should_terms)
        removed_meta_must = [t for t in must_terms if _is_meta_term(t)]
        removed_meta_should = [t for t in should_terms if _is_meta_term(t)]

        must_terms = [t for t in must_terms if t and not _is_behavior_term(t) and not _is_meta_term(t)]
        should_terms = [t for t in should_terms if t and not _is_behavior_term(t) and not _is_meta_term(t)]

        if removed_meta_must or removed_meta_should:
            logger.info(f"🔍 [RuleAnalyzer] 메타 키워드 제거: must={removed_meta_must}, should={removed_meta_should}")

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
            analyzer_used=self.get_name(),
            behavioral_conditions=behavioral_conditions,
        )

    def _extract_additional_keywords(self, query: str, existing_terms: List[str]) -> List[str]:
        """패턴 외 추가 키워드 추출
        
        Demographics와 메타 키워드 제외
        """
        additional = []
        
        # 토큰화 (한글 단어 단위로 분리)
        tokens = re.findall(r'\w+', query)
        
        for token in tokens:
            # 제외 조건
            if (token in existing_terms or 
                token in self.meta_keywords or 
                token in self.demographic_keywords or
                token in self.behavior_keywords or
                len(token) <= 1):
                continue
            
            additional.append(token)
        
        logger.info(f"🔑 실제 검색 키워드 (Demographics 제외): {additional}")
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
            analyzer_used=self.get_name(),
            behavioral_conditions={},
        )

    # ---------------------------------------------------------
    # 행동 조건 추출
    # ---------------------------------------------------------
    def _extract_behavioral_conditions(self, query: str) -> Tuple[Dict[str, bool], Set[str]]:
        """쿼리에서 행동 조건(예: 흡연 여부, 차량 보유)을 추출"""
        lowered = query.lower()
        conditions: Dict[str, bool] = {}
        tokens_to_remove: Set[str] = set()
        tokens = re.findall(r'\w+', query)

        def mark_tokens(keyword_list: Tuple[str, ...]) -> None:
            for token in tokens:
                token_lower = token.lower()
                for keyword in keyword_list:
                    if keyword in token_lower:
                        tokens_to_remove.add(token)
                        break

        specs = {
            "smoker": {
                "negative": [
                    r'비\s*흡연자?',
                    r'흡연\s*(안|않|하지|안함|않음)',
                    r'담배\s*(안|않)\s*피',
                    r'담배를\s*피워본\s*적\s*이\s*없',
                    r'금연자?',
                    r'non[-\s]?smoker',
                    r'담배\s*안\s*피',
                ],
                "positive": [
                    r'흡연\s*자',
                    r'흡연\s*중',
                    r'흡연\s*하는',
                    r'흡연\s*하고',      # "smoking and" - conjunction form
                    r'흡연\s*하며',      # "while smoking"
                    r'흡연\s*하면서',    # "while smoking"
                    r'흡연\s*하거나',    # "smoking or"
                    r'담배\s*(피우|피는|피운|피움|피고|피며|피면서|피거나)',  # ✅ 추가: 피고, 피며, 피면서, 피거나
                    r'smoker',
                ],
                "token_keywords": ("흡연", "담배"),
            },
            "has_vehicle": {
                "negative": [
                    r'차량\s*(없|미보유|미소유)',
                    r'자동차\s*(없|미보유|미소유)',
                    r'차\s*없',
                    r'차가\s*없',
                    r'무\s*차량',
                    r'차량\s*보유\s*(안|않)',
                ],
                "positive": [
                    r'차량\s*(있|보유|소유)',
                    r'자동차\s*(있|보유|소유)',
                    r'차\s*있',
                    r'차가\s*있',
                    r'차량\s*보유',
                    r'차량\s*소유',
                    r'차를\s*소유하는',   # "owning a car" - descriptive form
                    r'차를\s*소유하고',   # "owning a car and"
                    r'차를\s*가진',       # "having a car"
                    r'차를\s*보유한',     # "possessing a car"
                    r'차량을\s*소유하는',
                    r'차량을\s*소유하고',
                    r'차량을\s*가진',
                    r'차량을\s*보유한',
                ],
                "token_keywords": ("차량", "자동차", "차", "보유차량", "차량여부"),
            },
            "drinks_beer": {
                "positive": [
                    r'맥주\s*(마시|음용|선호|좋아|즐기)',
                    r'맥주',
                    r'beer',
                ],
                "token_keywords": ("맥주", "beer"),
            },
            "drinks_wine": {
                "positive": [
                    r'와인\s*(마시|음용|선호|좋아|즐기)',
                    r'와인',
                    r'wine',
                ],
                "token_keywords": ("와인", "wine"),
            },
            "drinks_soju": {
                "positive": [
                    r'소주\s*(마시|음용|선호|좋아|즐기)',
                    r'소주',
                    r'soju',
                ],
                "token_keywords": ("소주", "soju"),
            },
            "non_drinker": {
                "positive": [
                    r'술\s*(안|않)\s*마',
                    r'술\s*못\s*마',
                    r'비음주',
                    r'금주',
                    r'논\s*드링커',
                    r'non[-\s]?drinker',
                    r'술을\s*마시지\s*않',
                ],
                "token_keywords": ("술", "음주", "비음주", "금주"),
            },
        }

        for key, spec in specs.items():
            found = False
            # negative 패턴 체크 (있는 경우만)
            for pattern in spec.get("negative", []):
                if re.search(pattern, lowered):
                    conditions[key] = False
                    mark_tokens(spec["token_keywords"])
                    found = True
                    break
            if found:
                continue
            # positive 패턴 체크
            for pattern in spec.get("positive", []):
                if re.search(pattern, lowered):
                    conditions[key] = True
                    mark_tokens(spec["token_keywords"])
                    break

        return conditions, tokens_to_remove