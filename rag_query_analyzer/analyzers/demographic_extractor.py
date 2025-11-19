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
        # ⭐ 학생 세분화 (더 구체적인 것을 먼저 배치)
        "대학생/대학원생": {"value": "university_graduate_student", "synonyms": {"대학생/대학원생"}},
        "대학원생": {"value": "graduate_student", "synonyms": {"대학원생", "석사과정", "박사과정"}},
        "대학생": {"value": "university_student", "synonyms": {"대학생", "대학교 학생", "대학교생"}},
        "중/고등학생": {"value": "middle_high_student", "synonyms": {"중/고등학생", "중고등학생", "중학생", "고등학생"}},
        "학생": {"value": "student", "synonyms": {"학생"}},  # 일반적인 "학생"
        "자영업": {"value": "self_employed", "synonyms": {"자영업", "소상공인"}},
        "전문직": {"value": "professional", "synonyms": {"전문직", "의사", "변호사", "회계사", "간호사", "엔지니어", "프로그래머"}},
        "서비스직": {"value": "service", "synonyms": {"서비스직", "서비스업", "미용", "통신", "안내", "요식업", "요식"}},
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

    REGION_MAP = {
        # 17개 시도 (정규화된 약칭을 value로 사용)
        "서울": {
            "value": "서울",
            "synonyms": {
                "서울", "서울시", "서울특별시", "서울특별시", "서울 도심", "서울 지역"
            }
        },
        "부산": {
            "value": "부산",
            "synonyms": {
                "부산", "부산시", "부산광역시", "부산 지역"
            }
        },
        "인천": {
            "value": "인천",
            "synonyms": {
                "인천", "인천시", "인천광역시", "인천 지역"
            }
        },
        "대구": {
            "value": "대구",
            "synonyms": {
                "대구", "대구시", "대구광역시", "대구 지역"
            }
        },
        "광주": {
            "value": "광주",
            "synonyms": {
                "광주", "광주시", "광주광역시", "광주 지역"
            }
        },
        "대전": {
            "value": "대전",
            "synonyms": {
                "대전", "대전시", "대전광역시", "대전 지역"
            }
        },
        "울산": {
            "value": "울산",
            "synonyms": {
                "울산", "울산시", "울산광역시", "울산 지역"
            }
        },
        "세종": {
            "value": "세종",
            "synonyms": {
                "세종", "세종시", "세종특별자치시", "세종 지역"
            }
        },
        "경기": {
            "value": "경기",
            "synonyms": {
                "경기", "경기도", "경기 지역", "경기도 지역"
            }
        },
        "강원": {
            "value": "강원",
            "synonyms": {
                "강원", "강원도", "강원특별자치도", "강원 지역"
            }
        },
        "충북": {
            "value": "충북",
            "synonyms": {
                "충북", "충청북도", "충북 지역"
            }
        },
        "충남": {
            "value": "충남",
            "synonyms": {
                "충남", "충청남도", "충남 지역"
            }
        },
        "전북": {
            "value": "전북",
            "synonyms": {
                "전북", "전라북도", "전북 지역"
            }
        },
        "전남": {
            "value": "전남",
            "synonyms": {
                "전남", "전라남도", "전남 지역"
            }
        },
        "경북": {
            "value": "경북",
            "synonyms": {
                "경북", "경상북도", "경북 지역"
            }
        },
        "경남": {
            "value": "경남",
            "synonyms": {
                "경남", "경상남도", "경남 지역"
            }
        },
        "제주": {
            "value": "제주",
            "synonyms": {
                "제주", "제주도", "제주특별자치도", "제주 지역", "제주시", "서귀포시"
            }
        },
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

        # Income detection (범위 조건 지원)
        income_found = self._match_income(text)
        if income_found is not None:
            matched_ranges, synonyms = income_found
            # ⭐ "200만원 이상" 같은 범위 조건은 여러 구간을 매칭할 수 있음
            for canon, value in matched_ranges:
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

        # Region detection
        region_found = self._match_region(text)
        if region_found is not None:
            canon, value, synonyms = region_found
            demographics.append(
                DemographicEntity(
                    entity_type=EntityType.DEMOGRAPHIC,
                    name="지역",
                    canonical_form="region",
                    demographic_type=DemographicType.REGION,
                    value=value,
                    raw_value=canon,
                    synonyms=set(synonyms),
                    confidence=1.0,
                )
            )

        # ⭐ Sub-region detection (구/군/시 단위)
        sub_region_found = self._match_sub_region(text)
        if sub_region_found is not None:
            sub_region_value = sub_region_found
            demographics.append(
                DemographicEntity(
                    entity_type=EntityType.DEMOGRAPHIC,
                    name="세부지역",
                    canonical_form="sub_region",
                    demographic_type=DemographicType.SUB_REGION,
                    value=sub_region_value,
                    raw_value=sub_region_value,
                    synonyms=set(),
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
        """직업 매칭 (단어 경계 확인, 조사 허용, 긴 매칭 우선)"""
        # ⭐ 더 긴 매칭을 우선하기 위해 모든 매칭을 수집한 후 정렬
        matches = []

        for canon, info in self.OCCUPATION_MAP.items():
            for syn in info["synonyms"]:
                # 단어 경계를 고려한 정규식 패턴
                # 조사 허용: "학생인", "학생이", "학생을" 등
                pattern = r'(^|\s)' + re.escape(syn) + r'(\s|인|이|을|를|은|는|의|에|에서|와|과|쪽|계열|관련|분야|계통|$)'
                if re.search(pattern, text):
                    # (매칭된 synonym 길이, canonical, info) 저장
                    matches.append((len(syn), canon, info))
                    break  # 같은 canonical에서 여러 synonym 중복 방지

        if matches:
            # 가장 긴 매칭 우선 (예: "대학생"이 "학생"보다 우선)
            matches.sort(key=lambda x: x[0], reverse=True)
            _, canon, info = matches[0]
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

    def extract_with_size(self, query: str, default_size: int = 1000, max_size: int = 5000):
        """
        쿼리에서 demographics와 요청 size 추출

        Args:
            query: 검색 쿼리
            default_size: 수량 미지정 시 기본값 (기본 1000 - 전체 결과 확인 가능하도록)
            max_size: 최대 허용 size (기본 5000)
        """
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
        """소득 매칭 (범위 조건 지원)"""
        import re

        # ⭐ 범위 조건 패턴: "200만원 이상", "300만원 이하" 등
        range_pattern = r'(\d+)\s*만원\s*(이상|이하|초과|미만)'
        range_match = re.search(range_pattern, text)

        if range_match:
            amount = int(range_match.group(1))
            condition = range_match.group(2)

            # 각 구간의 범위 정의 (하한값 기준)
            income_ranges = [
                ("100만원미만", 0, "under_100"),
                ("100~199만원", 100, "100_199"),
                ("200~299만원", 200, "200_299"),
                ("300~399만원", 300, "300_399"),
                ("400~499만원", 400, "400_499"),
                ("500만원이상", 500, "over_500"),
            ]

            matched_ranges = []
            for canon, lower_bound, value in income_ranges:
                if condition == "이상":
                    if lower_bound >= amount:
                        matched_ranges.append((canon, value))
                elif condition == "이하":
                    if lower_bound < amount or lower_bound == 0:  # 100만원미만 포함
                        matched_ranges.append((canon, value))
                elif condition == "초과":
                    if lower_bound > amount:
                        matched_ranges.append((canon, value))
                elif condition == "미만":
                    if lower_bound < amount:
                        matched_ranges.append((canon, value))

            # ⭐ 복수 구간 반환 (리스트로 반환)
            if matched_ranges:
                # 모든 매칭된 구간의 synonyms 결합
                all_synonyms = set()
                for canon, _ in matched_ranges:
                    all_synonyms.update(self.INCOME_MAP[canon]["synonyms"])

                # 첫 번째 매칭 구간을 대표로 반환하되, synonyms는 모두 포함
                return matched_ranges, all_synonyms

        # ⭐ 정확한 구간 매칭 (기존 로직)
        for canon, info in self.INCOME_MAP.items():
            for syn in info["synonyms"]:
                if syn in text:
                    return [(canon, info["value"])], info["synonyms"]

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

    def _normalize_region_text(self, text: str) -> str:
        """지역 텍스트 정규화
        - "서울특별시" → "서울"
        - "경기도" → "경기"
        - "인천광역시" → "인천"
        - "충청북도" → "충북"
        - "제주특별자치도" → "제주"
        """
        # 공백 제거
        normalized = text.strip()
        
        # 시/도/특별시/광역시/특별자치시/특별자치도 제거
        normalized = re.sub(r'(특별자치시|광역시|특별시|특별자치도|도|시)\s*$', '', normalized)
        
        # 약칭 변환 (전체 이름 → 약칭)
        full_to_short = {
            "서울특별시": "서울",
            "부산광역시": "부산",
            "인천광역시": "인천",
            "대구광역시": "대구",
            "광주광역시": "광주",
            "대전광역시": "대전",
            "울산광역시": "울산",
            "세종특별자치시": "세종",
            "경기도": "경기",
            "강원특별자치도": "강원",
            "강원도": "강원",
            "충청북도": "충북",
            "충청남도": "충남",
            "전라북도": "전북",
            "전라남도": "전남",
            "경상북도": "경북",
            "경상남도": "경남",
            "제주특별자치도": "제주",
            "제주도": "제주",
        }
        
        for full, short in full_to_short.items():
            if full in normalized:
                normalized = normalized.replace(full, short)
        
        return normalized.strip()

    def _match_region(self, text: str):
        """지역 매칭 (정규화 포함)
        
        정규화 처리:
        - "서울특별시" → "서울"
        - "경기도" → "경기"
        - "인천광역시" → "인천"
        - "충청북도" → "충북"
        등
        
        매칭 패턴:
        - "인천 거주자", "서울 지역", "경기도 사람" 등 다양한 표현 지원
        - 단어 경계와 조사를 고려한 매칭
        """
        # 텍스트 정규화
        normalized_text = self._normalize_region_text(text)
        
        # REGION_MAP에서 매칭 시도 (긴 매칭 우선)
        # synonyms를 길이 순으로 정렬하여 더 긴 매칭을 우선시
        matches = []
        for canon, info in self.REGION_MAP.items():
            for syn in info["synonyms"]:
                # 정규화된 동의어도 확인
                normalized_syn = self._normalize_region_text(syn)
                
                # 단어 경계를 고려한 정규식 패턴
                # "인천 거주자", "서울 지역", "경기도 사람" 등 다양한 표현 지원
                pattern = r'(^|\s)' + re.escape(syn) + r'(\s|지역|거주|사람|인|이|을|를|은|는|의|에|에서|와|과|$|시|도)'
                if re.search(pattern, text, re.IGNORECASE):
                    matches.append((len(syn), canon, info))
                    break
                
                # 정규화된 텍스트에서도 매칭 시도
                if normalized_syn in normalized_text or syn in normalized_text:
                    matches.append((len(syn), canon, info))
                    break
        
        if matches:
            # 가장 긴 매칭을 우선 선택 (예: "서울특별시"가 "서울"보다 우선)
            matches.sort(key=lambda x: x[0], reverse=True)
            _, canon, info = matches[0]
            return canon, info["value"], info["synonyms"]

        return None

    def _match_sub_region(self, text: str):
        """세부 지역 매칭 (구/군/시 단위)"""
        # "연수구", "남동구", "강남구" 등의 패턴 찾기
        # 2-5글자 + (구|군|시) 패턴
        import re
        pattern = r'([가-힣]{2,5})(구|군|시)(?:\s|지역|거주|사람|인|이|을|를|은|는|의|에|에서|와|과|$)'
        match = re.search(pattern, text)
        if match:
            sub_region = match.group(1) + match.group(2)  # "연수" + "구" = "연수구"
            return sub_region
        return None


