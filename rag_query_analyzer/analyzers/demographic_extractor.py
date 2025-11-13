import re
from typing import List, Set

from ..models.entities import (
    DemographicEntity,
    DemographicType,
    EntityType,
    ExtractedEntities,
)


class DemographicExtractor:
    """간단한 규칙 기반 인구통계 추출기 (연령/성별)

    - 연령: '10대'..'70대', '20-29', '이십대' 등
    - 성별: 남성/남자/남, 여성/여자/여
    """

    AGE_PATTERNS = {
        "10대": {"value": "10s", "synonyms": {"10대", "10-19", "십대"}},
        "20대": {"value": "20s", "synonyms": {"20대", "20-29", "이십대"}},
        "30대": {"value": "30s", "synonyms": {"30대", "30-39", "삼십대"}},
        "40대": {"value": "40s", "synonyms": {"40대", "40-49", "사십대"}},
        "50대": {"value": "50s", "synonyms": {"50대", "50-59", "오십대"}},
        "60대": {"value": "60s", "synonyms": {"60대", "60-69", "육십대"}},
        "70대": {"value": "70s", "synonyms": {"70대", "70-79", "칠십대"}},
    }

    GENDER_MAP = {
        "남성": {"value": "M", "synonyms": {"남성", "남자", "남"}},
        "여성": {"value": "F", "synonyms": {"여성", "여자", "여"}},
    }

    OCCUPATION_MAP = {
        # canonical: value normalized for metadata.occupation
        "사무직": {"value": "office", "synonyms": {"사무직", "사무원", "화이트칼라", "직장인"}},
        "학생": {"value": "student", "synonyms": {"학생", "대학생", "고등학생", "중/고등학생", "대학생/대학원생"}},
        "자영업": {"value": "self_employed", "synonyms": {"자영업", "소상공인"}},
        "전문직": {"value": "professional", "synonyms": {"전문직", "의사", "변호사", "회계사", "간호사", "엔지니어", "프로그래머"}},
        "서비스직": {"value": "service", "synonyms": {"서비스직", "서비스업"}},
        "판매직": {"value": "sales", "synonyms": {"판매직", "영업"}},
        "생산직": {"value": "manufacturing", "synonyms": {"생산직", "블루칼라", "생산/노무직"}},
        "공무원": {"value": "public_servant", "synonyms": {"공무원", "공직자", "공무", "공직"}},
        "주부": {"value": "homemaker", "synonyms": {"주부", "가정주부", "전업주부"}},
        "무직": {"value": "unemployed", "synonyms": {"무직", "실직", "미취업"}},
        "프리랜서": {"value": "freelancer", "synonyms": {"프리랜서", "자유직", "freelance"}},
        "경영관리직": {"value": "management", "synonyms": {"경영/관리직", "경영관리직", "사장", "임원"}},
        "교직": {"value": "education", "synonyms": {"교직", "교사", "교수", "강사"}},
    }

    MARITAL_STATUS_MAP = {
        "기혼": {"value": "married", "synonyms": {"기혼", "결혼"}},
        "미혼": {"value": "single", "synonyms": {"미혼", "싱글", "미결혼"}},
        "기타": {"value": "other", "synonyms": {"이혼", "사별", "별거"}},
    }

    EDUCATION_MAP = {
        "고졸": {"value": "high_school", "synonyms": {"고졸", "고등학교", "고등학교 졸업"}},
        "대졸": {"value": "bachelor", "synonyms": {"대졸", "대학교 졸업", "대학 졸업"}},
        "대학원": {"value": "graduate", "synonyms": {"대학원", "대학원 졸업", "석사", "박사"}},
        "대학재학": {"value": "university_student", "synonyms": {"대학 재학", "대학교 재학"}},
    }

    INCOME_MAP = {
        "100만원미만": {"value": "under_100", "synonyms": {"100만원 미만", "100만원미만", "월 100만원 미만"}},
        "100~199만원": {"value": "100_199", "synonyms": {"100~199만원", "월 100~199만원"}},
        "200~299만원": {"value": "200_299", "synonyms": {"200~299만원", "월 200~299만원"}},
        "300~399만원": {"value": "300_399", "synonyms": {"300~399만원", "월 300~399만원"}},
        "400~499만원": {"value": "400_499", "synonyms": {"400~499만원", "월 400~499만원"}},
        "500만원이상": {"value": "over_500", "synonyms": {"500만원 이상", "500만원이상", "월 500만원 이상"}},
    }

    FAMILY_SIZE_MAP = {
        "1명": {"value": "1", "synonyms": {"1명", "혼자", "독거", "1명(혼자 거주)"}},
        "2명": {"value": "2", "synonyms": {"2명"}},
        "3명": {"value": "3", "synonyms": {"3명"}},
        "4명": {"value": "4", "synonyms": {"4명"}},
        "5명이상": {"value": "5+", "synonyms": {"5명 이상", "5명이상"}},
    }

    JOB_FUNCTION_MAP = {
        # welcome_2nd 직무 분류 (20,978명)
        "IT": {"value": "it", "synonyms": {"IT", "아이티", "개발", "프로그래밍", "코딩"}},
        "경영사무": {"value": "management_office", "synonyms": {"경영", "인사", "총무", "사무", "경영•인사•총무•사무"}},
        "생산기능": {"value": "production", "synonyms": {"생산", "정비", "기능", "노무", "생산•정비•기능•노무"}},
        "서비스": {"value": "service", "synonyms": {"서비스", "여행", "숙박", "음식", "미용", "보안", "서비스•여행•숙박•음식•미용•보안"}},
        "의료간호": {"value": "medical", "synonyms": {"의료", "간호", "보건", "복지", "의료•간호•보건•복지"}},
        "건설": {"value": "construction", "synonyms": {"건설", "건축", "토목", "환경", "건설•건축•토목•환경"}},
        "교육": {"value": "education", "synonyms": {"교육", "교사", "강사", "교직원", "교육•교사•강사•교직원"}},
        "유통물류": {"value": "logistics", "synonyms": {"유통", "물류", "운송", "운전", "유통•물류•운송•운전"}},
        "영업판매": {"value": "sales", "synonyms": {"무역", "영업", "판매", "매장관리", "무역•영업•판매•매장관리"}},
        "연구개발": {"value": "rd", "synonyms": {"전자", "기계", "기술", "화학", "연구개발", "전자•기계•기술•화학•연구개발", "R&D"}},
        "재무회계": {"value": "finance", "synonyms": {"재무", "회계", "경리", "재무•회계•경리"}},
        "마케팅": {"value": "marketing", "synonyms": {"마케팅", "광고", "홍보", "조사", "마케팅•광고•홍보•조사"}},
        "금융": {"value": "financial", "synonyms": {"금융", "보험", "증권", "금융•보험•증권"}},
        "고객상담": {"value": "customer_service", "synonyms": {"고객상담", "TM", "고객상담•TM", "텔레마케팅"}},
        "전문직": {"value": "professional", "synonyms": {"전문직", "법률", "인문사회", "임원", "전문직•법률•인문사회•임원"}},
        "디자인": {"value": "design", "synonyms": {"디자인"}},
        "문화스포츠": {"value": "culture_sports", "synonyms": {"문화", "스포츠", "문화•스포츠"}},
        "인터넷통신": {"value": "internet_telecom", "synonyms": {"인터넷", "통신", "인터넷•통신"}},
        "방송언론": {"value": "media", "synonyms": {"방송", "언론", "방송•언론"}},
        "게임": {"value": "game", "synonyms": {"게임"}},
    }

    def extract(self, query: str) -> ExtractedEntities:
        text = query.strip()

        demographics: List[DemographicEntity] = []

        # Gender detection
        gender_found = self._match_gender(text)
        if gender_found is not None:
            gender_key, value, synonyms = gender_found
            demographics.append(
                DemographicEntity(
                    entity_type=EntityType.DEMOGRAPHIC,
                    name="성별",
                    canonical_form="gender",
                    demographic_type=DemographicType.GENDER,
                    value=value,
                    raw_value=gender_key,
                    synonyms=set(synonyms),
                    confidence=1.0,
                )
            )

        # Age detection (match any known age band)
        age_found = self._match_age(text)
        if age_found is not None:
            raw_age, value, synonyms = age_found
            demographics.append(
                DemographicEntity(
                    entity_type=EntityType.DEMOGRAPHIC,
                    name="연령",
                    canonical_form="age_group",
                    demographic_type=DemographicType.AGE,
                    value=value,
                    raw_value=raw_age,
                    synonyms=set(synonyms),
                    confidence=1.0,
                )
            )

        # Occupation detection
        occ_found = self._match_occupation(text)
        if occ_found is not None:
            canon, value, synonyms = occ_found
            demographics.append(
                DemographicEntity(
                    entity_type=EntityType.DEMOGRAPHIC,
                    name="직업",
                    canonical_form="occupation",
                    demographic_type=DemographicType.OCCUPATION,
                    value=value,
                    raw_value=canon,
                    synonyms=set(synonyms),
                    confidence=1.0,
                )
            )

        # Marital status detection
        marital_found = self._match_marital_status(text)
        if marital_found is not None:
            canon, value, synonyms = marital_found
            demographics.append(
                DemographicEntity(
                    entity_type=EntityType.DEMOGRAPHIC,
                    name="결혼여부",
                    canonical_form="marital_status",
                    demographic_type=DemographicType.MARITAL_STATUS,
                    value=value,
                    raw_value=canon,
                    synonyms=set(synonyms),
                    confidence=1.0,
                )
            )

        # Education detection
        edu_found = self._match_education(text)
        if edu_found is not None:
            canon, value, synonyms = edu_found
            demographics.append(
                DemographicEntity(
                    entity_type=EntityType.DEMOGRAPHIC,
                    name="학력",
                    canonical_form="education",
                    demographic_type=DemographicType.EDUCATION,
                    value=value,
                    raw_value=canon,
                    synonyms=set(synonyms),
                    confidence=1.0,
                )
            )

        # Income detection
        income_found = self._match_income(text)
        if income_found is not None:
            canon, value, synonyms = income_found
            demographics.append(
                DemographicEntity(
                    entity_type=EntityType.DEMOGRAPHIC,
                    name="소득",
                    canonical_form="income",
                    demographic_type=DemographicType.INCOME,
                    value=value,
                    raw_value=canon,
                    synonyms=set(synonyms),
                    confidence=1.0,
                )
            )

        # Family size detection
        family_found = self._match_family_size(text)
        if family_found is not None:
            canon, value, synonyms = family_found
            demographics.append(
                DemographicEntity(
                    entity_type=EntityType.DEMOGRAPHIC,
                    name="가족수",
                    canonical_form="family_size",
                    demographic_type=DemographicType.FAMILY_SIZE,
                    value=value,
                    raw_value=canon,
                    synonyms=set(synonyms),
                    confidence=1.0,
                )
            )

        # Job function detection
        job_func_found = self._match_job_function(text)
        if job_func_found is not None:
            canon, value, synonyms = job_func_found
            demographics.append(
                DemographicEntity(
                    entity_type=EntityType.DEMOGRAPHIC,
                    name="직무",
                    canonical_form="job_function",
                    demographic_type=DemographicType.JOB_FUNCTION,
                    value=value,
                    raw_value=canon,
                    synonyms=set(synonyms),
                    confidence=1.0,
                )
            )

        return ExtractedEntities(
            demographics=demographics,
            topics=[],
            questions=[],
            original_query=text,
            confidence=1.0 if demographics else 0.0,
        )

    def _match_gender(self, text: str):
        """성별 매칭 (단어 경계 확인)"""
        # 단어 경계를 고려한 매칭 (예: "보여줘"의 "여"는 매칭하지 않음)
        for canon, info in self.GENDER_MAP.items():
            for syn in info["synonyms"]:
                # 단어 경계를 고려한 정규식 패턴
                # \b는 단어 경계, 하지만 한글은 \b가 제대로 작동하지 않으므로
                # 공백이나 문장 시작/끝을 확인
                pattern = r'(^|\s)' + re.escape(syn) + r'(\s|$)'
                if re.search(pattern, text):
                    return canon, info["value"], info["synonyms"]
        return None

    def _match_age(self, text: str):
        # direct label first (10대..70대)
        for label, data in self.AGE_PATTERNS.items():
            for syn in data["synonyms"]:
                if syn in text:
                    return label, data["value"], data["synonyms"]

        # numeric range like 20-29 or 20~29
        m = re.search(r"(\d{2})\s*[-~]\s*(\d{2})", text)
        if m:
            start = int(m.group(1))
            # Map start decade to label
            decade = (start // 10) * 10
            label = f"{decade}대"
            if label in self.AGE_PATTERNS:
                data = self.AGE_PATTERNS[label]
                return label, data["value"], data["synonyms"]
        return None

    def _match_occupation(self, text: str):
        """직업 매칭 (단어 경계 확인, 조사 허용)"""
        # 단어 경계를 고려한 매칭 (조사 허용: "학생인", "학생이" 등)
        for canon, info in self.OCCUPATION_MAP.items():
            for syn in info["synonyms"]:
                # 단어 경계를 고려한 정규식 패턴
                # 조사 허용: "학생인", "학생이", "학생을" 등
                pattern = r'(^|\s)' + re.escape(syn) + r'(\s|인|이|을|를|은|는|의|에|에서|와|과|$)'
                if re.search(pattern, text):
                    return canon, info["value"], info["synonyms"]
        return None

    def parse_requested_size(self, text: str, default_size: int = 10, max_size: int = 1000) -> int:
        """문장에서 '300명', '200건' 등 수량 요청을 파싱.
        - 단위(명|건)가 붙은 경우만 size로 인정해 '30대' 같은 숫자와 구분
        반환값은 1..max_size 범위로 클램핑.
        """
        m = re.search(
            r"(\d{1,4})\s*(?:명|건)(?:[을를이가은는도만의께]*|(?:만큼|정도|가량|쯤))?",
            text,
        )
        if not m:
            return default_size
        try:
            val = int(m.group(1))
            if val <= 0:
                return default_size
            return min(val, max_size)
        except Exception:
            return default_size

    def extract_with_size(self, query: str, default_size: int = 10, max_size: int = 1000):
        entities = self.extract(query)
        size = self.parse_requested_size(query, default_size=default_size, max_size=max_size)
        return entities, size

    def _match_marital_status(self, text: str):
        """결혼여부 매칭"""
        for canon, info in self.MARITAL_STATUS_MAP.items():
            for syn in info["synonyms"]:
                pattern = r'(^|\s)' + re.escape(syn) + r'(\s|인|이|을|를|은|는|의|자|$)'
                if re.search(pattern, text):
                    return canon, info["value"], info["synonyms"]
        return None

    def _match_education(self, text: str):
        """학력 매칭"""
        for canon, info in self.EDUCATION_MAP.items():
            for syn in info["synonyms"]:
                if syn in text:
                    return canon, info["value"], info["synonyms"]
        return None

    def _match_income(self, text: str):
        """소득 매칭"""
        for canon, info in self.INCOME_MAP.items():
            for syn in info["synonyms"]:
                if syn in text:
                    return canon, info["value"], info["synonyms"]
        return None

    def _match_family_size(self, text: str):
        """가족수 매칭"""
        for canon, info in self.FAMILY_SIZE_MAP.items():
            for syn in info["synonyms"]:
                if syn in text:
                    return canon, info["value"], info["synonyms"]
        return None

    def _match_job_function(self, text: str):
        """직무 매칭"""
        for canon, info in self.JOB_FUNCTION_MAP.items():
            for syn in info["synonyms"]:
                # 직무는 여러 단어 조합 가능 (예: "경영•인사•총무•사무")
                if syn in text:
                    return canon, info["value"], info["synonyms"]
        return None


