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
        "학생": {"value": "student", "synonyms": {"학생", "대학생", "고등학생"}},
        "자영업": {"value": "self_employed", "synonyms": {"자영업", "소상공인"}},
        "전문직": {"value": "professional", "synonyms": {"전문직", "의사", "변호사", "회계사"}},
        "서비스직": {"value": "service", "synonyms": {"서비스직", "서비스업"}},
        "판매직": {"value": "sales", "synonyms": {"판매직", "영업"}},
        "생산직": {"value": "manufacturing", "synonyms": {"생산직", "블루칼라"}},
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
        m = re.search(r"(\d{1,4})\s*(명|건)", text)
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


