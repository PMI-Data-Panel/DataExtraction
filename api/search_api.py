"""검색 API 라우터"""
import asyncio
import json
import logging
import hashlib
import re
import gzip
import pickle
from collections import defaultdict, OrderedDict
from time import perf_counter
from datetime import datetime, timezone
from uuid import uuid4
from typing import List, Dict, Any, Optional, Set, Tuple, Literal, Union
from fastapi import APIRouter, HTTPException, Depends, Query, BackgroundTasks
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from opensearchpy import OpenSearch
import pandas as pd
import numpy as np
from cachetools import TTLCache

# 분석기 및 쿼리 빌더
from rag_query_analyzer.analyzers.main_analyzer import AdvancedRAGQueryAnalyzer
from rag_query_analyzer.analyzers.demographic_extractor import DemographicExtractor
from rag_query_analyzer.models.entities import DemographicType, DemographicEntity
from connectors.hybrid_searcher import OpenSearchHybridQueryBuilder, calculate_rrf_score
from connectors.data_fetcher import DataFetcher
from connectors.qdrant_helper import search_qdrant_async, search_qdrant_collections_async

logger = logging.getLogger(__name__)
# main_api.py에서 이미 basicConfig로 루트 로거가 설정되어 있으므로
# propagate만 True로 설정하여 루트 로거로 전파되도록 함
logger.propagate = True


def _mask_user_ids_in_query(query: Dict[str, Any]) -> Dict[str, Any]:
    """쿼리에서 user_id 리스트를 마스킹하여 로깅용 복사본 생성"""
    import copy
    masked = copy.deepcopy(query)
    
    def mask_recursive(obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key in ("_id", "user_id") and isinstance(value, list):
                    # user_id 리스트를 개수만 표시하도록 마스킹
                    obj[key] = f"[{len(value)} user_ids masked]"
                elif isinstance(value, (dict, list)):
                    mask_recursive(value)
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, (dict, list)):
                    mask_recursive(item)
    
    mask_recursive(masked)
    return masked

class PanelDataCache:
    """Survey panel 데이터를 메모리에 캐싱 - 초고속 검색을 위한 프리로드"""

    def __init__(self):
        self.df: pd.DataFrame = None  # Pandas DataFrame (벡터화 필터링용)
        self.user_map: Dict[str, Dict] = {}  # user_id → full document 매핑
        self.loaded = False
        self.load_time = None
        self.total_count = 0

    async def initialize(self, data_fetcher, index_name="survey_responses_merged"):
        """서버 시작 시 전체 데이터 로드 (1번만 실행)"""
        if self.loaded:
            logger.info("✅ Panel data already loaded")
            return

        start = perf_counter()
        logger.info("🔄 Loading all panel data into memory... (메모리 프리로드)")

        # ⭐ Scroll API로 전체 데이터 조회 (필요한 필드만)
        query = {
            "query": {"match_all": {}},
            "_source": {
                "includes": [
                    "user_id",
                    "metadata",   # demographics
                    "qa_pairs",   # occupation, marital, behavioral
                    "timestamp",
                    "text"
                ],
                "excludes": []
            }
        }

        try:
            all_docs = await data_fetcher.scroll_search_async(
                index_name=index_name,
                query=query,
                batch_size=2000,
                scroll_time="5m",
                num_slices=8,  # 병렬 8개 (빠른 로딩)
                request_timeout=300,
            )

            # ⭐⭐⭐ Pandas DataFrame으로 변환 + 모든 조건 사전 추출 (초고속!)
            extract_start = perf_counter()

            # ⭐ 모든 behavioral 키 (BEHAVIORAL_KEYWORD_MAP에서 자동 생성!)
            all_behavioral_keys = list(BEHAVIORAL_KEYWORD_MAP.keys())

            records = []
            for idx, doc in enumerate(all_docs):
                if idx % 5000 == 0 and idx > 0:
                    logger.info(f"    진행: {idx}/{len(all_docs)}...")

                source = doc.get('_source', {})
                metadata = source.get('metadata', {}) if isinstance(source.get('metadata'), dict) else {}

                # ⭐ qa_pairs 처리: dict → list 변환 (OpenSearch 구조 대응!)
                qa_pairs_raw = source.get('qa_pairs', {})
                if isinstance(qa_pairs_raw, dict):
                    # dict → list of values
                    qa_pairs = list(qa_pairs_raw.values())
                elif isinstance(qa_pairs_raw, list):
                    # 이미 list면 그대로
                    qa_pairs = qa_pairs_raw
                else:
                    # 기타 타입은 빈 리스트
                    qa_pairs = []

                # ⭐ Occupation 사전 추출
                occupation_value = None

                # 🔍 디버그: 첫 3개 문서의 qa_pairs 구조 확인
                if idx < 3:
                    logger.info(f"\n🔍 [DEBUG] Document #{idx}: user_id={source.get('user_id')}")
                    logger.info(f"   - qa_pairs type: {type(qa_pairs)}")
                    logger.info(f"   - qa_pairs count: {len(qa_pairs)}")
                    if qa_pairs and len(qa_pairs) > 0:
                        logger.info(f"   - First QA pair structure: {qa_pairs[0]}")
                        logger.info(f"   - QA pair keys: {qa_pairs[0].keys() if isinstance(qa_pairs[0], dict) else 'NOT A DICT'}")
                        # 처음 5개 질문 출력
                        for i, qa in enumerate(qa_pairs[:5]):
                            if isinstance(qa, dict):
                                # ⭐ 수정: 실제 키는 'q_text'
                                q_text_raw = qa.get("q_text", "") or qa.get("question", "") or qa.get("question_text", "") or ""
                                logger.info(f"   - Q{i+1}: '{q_text_raw[:100]}'")  # 처음 100자만

                if qa_pairs:
                    for qa in qa_pairs:
                        if not isinstance(qa, dict):
                            continue
                        # ⭐ 수정: 실제 키는 'q_text'
                        q_text = str(qa.get("q_text", "") or qa.get("question", "") or qa.get("question_text", "")).lower()
                        if any(keyword in q_text for keyword in ("직업", "직무", "occupation", "직종")):
                            answer = qa.get("answer") or qa.get("answer_text")
                            if answer:
                                occupation_value = str(answer).strip()
                                if idx < 3:
                                    logger.info(f"   ✅ Found occupation: '{occupation_value}' from q_text: '{q_text[:50]}'")
                                break

                    # 디버그: 첫 3개 문서에서 occupation 찾지 못한 경우
                    if idx < 3 and occupation_value is None:
                        logger.info(f"   ❌ No occupation found in {len(qa_pairs)} QA pairs")

                # ⭐ Marital status 사전 추출
                marital_value = metadata.get("marital_status")
                if not marital_value and qa_pairs:
                    for qa in qa_pairs:
                        if not isinstance(qa, dict):
                            continue
                        # ⭐ 수정: 실제 키는 'q_text'
                        q_text = str(qa.get("q_text", "") or qa.get("question", "") or qa.get("question_text", "")).lower()
                        if any(keyword in q_text for keyword in ("결혼", "혼인", "marital")):
                            answer = qa.get("answer") or qa.get("answer_text")
                            if answer:
                                marital_value = str(answer).strip()
                                break

                # ⭐⭐⭐ 모든 Behavioral 조건 사전 추출 (77개) - 배치 최적화!
                # qa_pairs를 한 번만 순회하여 모든 패턴을 동시에 추출 (77배 속도 향상!)
                behavioral_values = extract_all_behaviors_batch(qa_pairs)

                # DataFrame 레코드 생성
                record = {
                    'user_id': source.get('user_id'),
                    'gender': metadata.get('gender'),
                    'age_group': metadata.get('age_group'),
                    'birth_year': metadata.get('birth_year'),
                    'region': metadata.get('region'),
                    'sub_region': metadata.get('sub_region'),
                    'occupation': occupation_value,  # ⭐ 사전 추출!
                    'marital_status': marital_value,  # ⭐ 사전 추출!
                    'timestamp': source.get('timestamp'),
                    # ⭐ 전체 데이터 보관 (필요 시 접근)
                    '_full_source': source,
                    '_doc': doc,
                }

                # ⭐ 모든 behavioral 컬럼 기본값으로 초기화 (None)
                for key in all_behavioral_keys:
                    record[key] = None

                # ⭐ 실제 추출된 값으로 업데이트
                record.update(behavioral_values)

                records.append(record)

                # user_id → full document 매핑 (빠른 조회용)
                if source.get('user_id'):
                    self.user_map[source['user_id']] = doc

            extract_duration = perf_counter() - extract_start

            self.df = pd.DataFrame(records)
            self.total_count = len(self.df)
            self.loaded = True
            self.load_time = perf_counter() - start

            memory_mb = self.df.memory_usage(deep=True).sum() / 1024**2
            logger.info(f"✅ Panel data loaded: {self.total_count}건, {self.load_time:.2f}초")
            logger.info(f"   메모리 사용: {memory_mb:.2f} MB")
            logger.info(f"   컬럼: {list(self.df.columns)}")

            # ⭐ Occupation 통계 (디버깅용)
            occupation_stats = self.df['occupation'].value_counts()
            logger.info(f"\n📊 Occupation 통계:")
            logger.info(f"   - Total: {self.total_count}건")
            logger.info(f"   - None: {self.df['occupation'].isna().sum()}건")
            logger.info(f"   - 고유값: {occupation_stats.nunique()}개")
            logger.info(f"   - 상위 20개:")
            for occ, count in occupation_stats.head(20).items():
                if occ:
                    logger.info(f"      * {occ}: {count}건")

            # ⭐ "학생" 관련 occupation
            student_mask = self.df['occupation'].str.contains('학생', na=False, case=False)
            student_count = student_mask.sum()
            logger.info(f"\n   - '학생' 포함: {student_count}건")
            if student_count > 0:
                student_occupations = self.df[student_mask]['occupation'].value_counts()
                for occ, count in student_occupations.items():
                    logger.info(f"      * {occ}: {count}건")

            # ⭐⭐⭐ Behavioral 패턴 통계 (디버깅용)
            logger.info(f"\n📊 Behavioral 패턴 통계:")

            # late_night_snack_method 통계
            if 'late_night_snack_method' in self.df.columns:
                lns_stats = self.df['late_night_snack_method'].value_counts()
                lns_count = self.df['late_night_snack_method'].notna().sum()
                logger.info(f"\n   [late_night_snack_method]")
                logger.info(f"   - Total: {self.total_count}건")
                logger.info(f"   - None: {self.df['late_night_snack_method'].isna().sum()}건")
                logger.info(f"   - 값 있음: {lns_count}건")
                logger.info(f"   - 고유값: {lns_stats.nunique()}개")
                for value, count in lns_stats.items():
                    logger.info(f"      * '{value}': {count}건")
            else:
                logger.warning(f"   ⚠️ 'late_night_snack_method' 컬럼이 없습니다!")

            # ⭐ skin_satisfaction 통계 (디버깅!)
            if 'skin_satisfaction' in self.df.columns:
                skin_stats = self.df['skin_satisfaction'].value_counts()
                skin_count = self.df['skin_satisfaction'].notna().sum()
                logger.info(f"\n   [skin_satisfaction] ⭐ 중요!")
                logger.info(f"   - Total: {self.total_count}건")
                logger.info(f"   - None: {self.df['skin_satisfaction'].isna().sum()}건")
                logger.info(f"   - 값 있음: {skin_count}건")
                logger.info(f"   - 고유값: {skin_stats.nunique() if len(skin_stats) > 0 else 0}개")
                if len(skin_stats) > 0:
                    logger.info(f"   - 상위 값:")
                    for value, count in skin_stats.head(10).items():
                        logger.info(f"      * '{value}': {count}건")
            else:
                logger.warning(f"   ⚠️ 'skin_satisfaction' 컬럼이 없습니다!")

            # uses_food_delivery 통계
            if 'uses_food_delivery' in self.df.columns:
                fd_stats = self.df['uses_food_delivery'].value_counts()
                logger.info(f"\n   [uses_food_delivery]")
                logger.info(f"   - True: {(self.df['uses_food_delivery'] == True).sum()}건")
                logger.info(f"   - False: {(self.df['uses_food_delivery'] == False).sum()}건")
                logger.info(f"   - None: {self.df['uses_food_delivery'].isna().sum()}건")

        except Exception as e:
            logger.error(f"❌ Panel data 로드 실패: {e}")
            raise

    def filter_all(
        self,
        gender: Optional[str] = None,
        age_group: Optional[str] = None,
        region: Optional[str] = None,
        sub_region: Optional[str] = None,
        occupation: Optional[str] = None,
        marital_status: Optional[str] = None,
        behavioral_conditions: Optional[Dict[str, Union[bool, str]]] = None,
    ) -> pd.DataFrame:
        """⚡ Pandas 벡터화 필터링 (초고속 - metadata + occupation + marital + behavioral 전부!)

        Args:
            gender, age_group, region, sub_region: Demographics 필터
            occupation: 직업 필터 (부분 매칭)
            marital_status: 결혼 여부 필터
            behavioral_conditions: Behavioral 필터
                - bool: {'smoker': True, 'has_vehicle': False}
                - str: {'winter_vacation_memory': '친구들과 보낸 즐거운 시간'}

        Returns:
            필터링된 DataFrame
        """
        if not self.loaded:
            raise RuntimeError("Panel data not loaded")

        mask = pd.Series([True] * len(self.df))

        # ⭐ Demographics 필터 (metadata)
        if gender:
            mask &= (self.df['gender'] == gender)

        if age_group:
            mask &= (self.df['age_group'] == age_group)

        if region:
            mask &= (self.df['region'] == region)

        if sub_region:
            mask &= (self.df['sub_region'] == sub_region)

        # ⭐ Occupation 필터 (부분 매칭)
        if occupation:
            # "영업직" → "영업" 포함 확인
            keyword = occupation.replace('직', '')
            if keyword:
                mask &= self.df['occupation'].notna() & self.df['occupation'].str.contains(
                    keyword, case=False, na=False, regex=False
                )

        # ⭐ Marital status 필터
        if marital_status:
            mask &= (self.df['marital_status'] == marital_status)

        # ⭐⭐⭐ Behavioral 필터 (39개 조건)
        if behavioral_conditions:
            for behavior_key, expected_value in behavioral_conditions.items():
                if expected_value is None:
                    continue  # None은 체크 안함

                if behavior_key in self.df.columns:
                    if isinstance(expected_value, bool):
                        # ⭐ Boolean 체크 (벡터화!)
                        mask &= (self.df[behavior_key] == expected_value)
                    elif isinstance(expected_value, list):
                        # ⭐⭐⭐ 다중 값 (리스트): OR 조건 (isin 사용!)
                        mask &= self.df[behavior_key].isin(expected_value)
                        logger.info(f"  🔍 다중 값 필터링 (Panel cache): {behavior_key} IN {expected_value}")
                    elif isinstance(expected_value, str):
                        # ⭐ 정확 매칭 먼저 시도
                        exact_match = (self.df[behavior_key] == expected_value)

                        # ⭐ 부분 매칭 (Fallback - 정확 매칭 실패 시)
                        partial_match = self.df[behavior_key].notna() & \
                                       self.df[behavior_key].str.contains(
                                           expected_value, case=False, na=False, regex=False
                                       )

                        # 둘 중 하나라도 매칭
                        mask &= (exact_match | partial_match)

        return self.df[mask]

    def get_user_docs(self, user_ids: List[str]) -> List[Dict]:
        """user_id 리스트로 전체 문서 가져오기"""
        return [self.user_map[uid] for uid in user_ids if uid in self.user_map]

    def get_all_user_ids(self) -> List[str]:
        """모든 user_id 리스트 반환"""
        return self.df['user_id'].tolist() if self.loaded else []


# ⭐ 전역 캐시 인스턴스
panel_cache = PanelDataCache()


router = APIRouter(
    prefix="/search",
    tags=["Search"]
)

# ⭐ 1차 메모리 캐시 (초고속!)
# - maxsize: 최대 100개 검색 결과 캐싱
# - ttl: 300초 (5분) 후 자동 만료
memory_cache = TTLCache(maxsize=100, ttl=300)

# ⭐ LLM 쿼리 결과 캐시 (행동 패턴 추출)
llm_query_cache = TTLCache(maxsize=1000, ttl=300)

# ⭐ 패턴 우선순위 정의 (구체적 > 일반적)
PATTERN_HIERARCHY = {
    "overseas_travel_preference": ["travels"],
    "travel_style": ["travels"],
    "happy_consumption": [],
    "winter_vacation_memory": ["travels"],
    "skin_satisfaction": ["uses_beauty_products"],
    "skincare_spending": ["uses_beauty_products"],
    "skincare_priority": ["uses_beauty_products"],
    "plastic_bag_reduction": ["cares_about_environment"],
    "rewards_attention": ["cares_about_rewards"],
    "privacy_protection_habit": ["privacy_conscious"],
    "summer_fashion_essential": ["shops_fashion"],
    "pet_experience": ["has_pet"],
    "traditional_market_frequency": ["visits_traditional_market"],
    "stress_source": ["has_stress"],
    "stress_relief_method": ["has_stress"],
    "exercise_type": ["exercises"],
    "fast_delivery_product": ["uses_fast_delivery"],
    "late_night_snack_method": ["uses_food_delivery"],  # ⭐ 야식 방법 > 배달 여부
    "ott_count": ["ott_user"],  # ⭐ OTT 개수 > OTT 사용 여부
    "solo_dining_frequency": ["dines_out"],  # ⭐ 혼밥 빈도 > 외식 여부
}

# OpenSearch 요청 타임아웃 (복잡한 쿼리나 대용량 검색을 위해 30초로 설정)
DEFAULT_OS_TIMEOUT = 180  # 대량 데이터 조회 대응 (전체 데이터 약 35000개)

# 런타임 공유 객체 (한 번만 초기화 후 재사용)
router.analyzer = None  # type: ignore[attr-defined]
router.embedding_model = None  # type: ignore[attr-defined]
router.config = None  # type: ignore[attr-defined]

# ⭐ survey_responses_merged만 사용하므로 welcome 인덱스 캐시 제거

_SUMMARY_RESPONSE_TEMPLATE = (
    "{\n"
    '  "highlights": ["요약1", "요약2", "요약3"],\n'
    '  "demographic_summary": "주요 인구통계 인사이트",\n'
    '  "behavioral_summary": "주요 행동/습관 인사이트",\n'
    '  "data_signals": ["주요 수치나 패턴"],\n'
    '  "follow_up_questions": ["후속으로 탐색하면 좋을 질문"]\n'
    "}"
)

_DEFAULT_SUMMARY_INSTRUCTIONS = (
    "검색 결과를 분석하여 사용자에게 도움이 되는 핵심 인사이트를 한국어로 제공하세요. "
    "정량적 지표(응답자 수, 비율 등)가 있을 경우 명시하고, 데이터의 편향이나 한계도 언급하세요. "
    "⚠️ 중요: 모든 요약 필드(highlights, demographic_summary, behavioral_summary 등)는 각각 최대 2줄로 간결하게 작성하세요."
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=timezone.utc).isoformat()


def _truncate_text(value: Any, max_length: int = 4000) -> str:
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    if len(text) <= max_length:
        return text
    return text[: max_length - 3] + "..."


def _redis_list_append(
    client,
    key: str,
    payload: Dict[str, Any],
    max_length: Optional[int],
    ttl_seconds: Optional[int],
) -> None:
    if not client or not key:
        return
    try:
        serialized = json.dumps(payload, ensure_ascii=False, default=str)
        pipeline = client.pipeline()
        pipeline.rpush(key, serialized)
        if max_length and max_length > 0:
            pipeline.ltrim(key, -max_length, -1)
        if ttl_seconds and ttl_seconds > 0:
            pipeline.expire(key, ttl_seconds)
        pipeline.execute()
    except Exception as exc:
        logger.warning(f"⚠️ Redis 리스트 업데이트 실패: key={key}, error={exc}")


def _make_conversation_key(prefix: Optional[str], session_id: Optional[str]) -> Optional[str]:
    if not prefix or not session_id:
        return None
    return f"{prefix}:{session_id}"


def _make_history_key(prefix: Optional[str], owner_id: Optional[str]) -> Optional[str]:
    if not prefix or not owner_id:
        return None
    return f"{prefix}:{owner_id}"


def _extract_json_from_text(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    try:
        fenced = re.search(r"```json\s*(.*?)\s*```", text, re.DOTALL)
        if fenced:
            return json.loads(fenced.group(1))
        fallback = re.search(r"\{.*\}", text, re.DOTALL)
        if fallback:
            return json.loads(fallback.group(0))
        return json.loads(text)
    except Exception:
        return None


def _ensure_request_defaults(request: Any) -> None:
    """요청에 필수 기본값을 채워 사용자가 query만 보내도 동작하도록 보정."""
    session_id = getattr(request, "session_id", None)
    if not session_id:
        session_id = str(uuid4())
        request.session_id = session_id

    user_id = getattr(request, "user_id", None)
    if not user_id:
        request.user_id = session_id

    # 모든 보조 기능 기본 활성화
    request.log_conversation = True
    request.log_search_history = True
    request.request_llm_summary = True

def _prepare_summary_results(
    results: List["SearchResult"],
    max_results: int,
    max_chars: int,
) -> List[Dict[str, Any]]:
    trimmed: List[Dict[str, Any]] = []
    total_chars = 0
    for idx, result in enumerate(results[:max_results], start=1):
        if hasattr(result, "model_dump"):
            item = result.model_dump()
        elif isinstance(result, dict):
            item = result
        else:
            continue

        item_copy = {
            "rank": idx,
            "user_id": item.get("user_id"),
            "score": item.get("score"),
            "timestamp": item.get("timestamp"),
            "demographic_info": item.get("demographic_info"),
            "behavioral_info": item.get("behavioral_info"),
            "qa_pairs": (item.get("qa_pairs") or [])[:3],
            "matched_qa_pairs": (item.get("matched_qa_pairs") or [])[:3],
            "highlights": item.get("highlights"),
        }

        serialized = json.dumps(item_copy, ensure_ascii=False)
        prospective_total = total_chars + len(serialized)
        if prospective_total > max_chars and trimmed:
            break
        trimmed.append(item_copy)
        total_chars = prospective_total
    return trimmed


def _maybe_generate_llm_summary(
    *,
    request,
    response: "SearchResponse",
    analysis,
) -> Optional[Dict[str, Any]]:
    if not getattr(request, "request_llm_summary", False):
        return None

    if not getattr(router, "enable_search_summary", False):
        logger.info("LLM 요약 비활성화 설정으로 인해 요약을 건너뜁니다.")
        return None

    client = getattr(router, "anthropic_client", None)
    if client is None:
        logger.warning("Anthropic 클라이언트가 설정되지 않아 LLM 요약을 건너뜁니다.")
        return None

    config = getattr(router, "config", None)
    if config is None:
        logger.warning("Config 설정이 없어 LLM 요약을 건너뜁니다.")
        return None

    model_name = getattr(router, "search_summary_model", None) or config.CLAUDE_MODEL
    if not model_name:
        logger.warning("요약에 사용할 모델명이 설정되지 않아 LLM 요약을 건너뜁니다.")
        return None

    max_results = getattr(router, "search_summary_max_results", 10)
    max_chars = getattr(router, "search_summary_max_chars", 16000)
    prepared_results = _prepare_summary_results(response.results, max_results, max_chars)
    if not prepared_results:
        logger.info("LLM 요약을 위한 결과가 없어 요약을 건너뜁니다.")
        return None

    instructions = getattr(request, "llm_summary_instructions", None) or _DEFAULT_SUMMARY_INSTRUCTIONS

    # ⭐⭐⭐ 활성화된 behavioral 필터 추출
    active_behavioral = {
        k: v for k, v in getattr(analysis, 'behavioral_conditions', {}).items()
        if v is not None and v is not False  # None과 False 제외
    }

    # 한글 설명으로 변환
    behavioral_text = ""
    if active_behavioral:
        items = []
        for k, v in active_behavioral.items():
            config = BEHAVIORAL_KEYWORD_MAP.get(k, {})
            label = config.get('question_text', k)
            # 간결하게 표시
            if isinstance(v, bool):
                items.append(f"- {label}: {'예' if v else '아니오'}")
            else:
                items.append(f"- {label}: {v}")
        behavioral_text = "\n".join(items)

    prompt = (
        "당신은 설문조사 데이터 분석 전문가입니다. "
        "주어진 검색 결과를 바탕으로 사용자의 질문에 대한 인사이트를 제공하세요.\n\n"
        f"사용자 질의: {request.query}\n"
        f"예상 검색 의도: {getattr(analysis, 'intent', 'N/A')}\n"
        f"추출된 must_terms: {getattr(analysis, 'must_terms', [])}\n"
        f"추출된 should_terms: {getattr(analysis, 'should_terms', [])}\n\n"
        # ⭐⭐⭐ 적용된 행동 필터 정보 추가!
        f"📋 적용된 행동 필터:\n{behavioral_text or '없음'}\n\n"
        f"⚠️ 매우 중요: 위 행동 필터가 적용되어 모든 검색 결과는 이미 필터링된 상태입니다.\n"
        f"검색된 모든 응답자는 위 조건을 만족합니다. behavioral_summary 작성 시 반드시 이를 반영하세요.\n"
        f"예: '흡연 여부: 예' 필터 적용 시 → '모든 응답자는 흡연자입니다'\n"
        f"예: 'OTT 서비스 개수: 2개' 필터 적용 시 → '모든 응답자는 OTT 서비스 2개를 이용합니다'\n\n"
        f"총 검색 결과 수: {response.total_hits}\n"
        f"현재 반환된 결과 수: {len(response.results)}\n\n"
        f"요약 지침: {instructions}\n\n"
        "⚠️ 중요: 모든 요약 필드는 각각 최대 2줄로 간결하게 작성하세요.\n"
        "- highlights: 각 항목은 1줄로, 최대 2개 항목\n"
        "- demographic_summary: 최대 2줄\n"
        "- behavioral_summary: 최대 2줄 (⭐ 적용된 행동 필터를 반드시 반영하세요!)\n"
        "- data_signals: 각 항목은 1줄로, 최대 2개 항목\n"
        "- follow_up_questions: 각 항목은 1줄로, 최대 2개 항목\n\n"
        "검색 결과(최대 일부) JSON:\n"
        f"{json.dumps(prepared_results, ensure_ascii=False, indent=2)}\n\n"
        "응답은 반드시 JSON 형식으로 작성하세요. 형식 예시는 다음과 같습니다:\n"
        f"{_SUMMARY_RESPONSE_TEMPLATE}\n"
    )

    max_tokens = min(1200, getattr(config, "CLAUDE_MAX_TOKENS", 1500))
    temperature = getattr(config, "CLAUDE_TEMPERATURE", 0.1)

    try:
        message = client.messages.create(
            model=model_name,
            max_tokens=max_tokens,
            temperature=temperature,
            messages=[{"role": "user", "content": prompt}],
        )
        content = ""
        if message and getattr(message, "content", None):
            parts = getattr(message, "content", [])
            if parts:
                # Anthropics SDK returns list of blocks with .text
                first = parts[0]
                content = getattr(first, "text", "") or ""
        summary_json = _extract_json_from_text(content)
        if summary_json is None:
            logger.warning("LLM 요약 응답에서 JSON을 추출하지 못했습니다.")
            return {
                "model": model_name,
                "generated_at": _utc_now_iso(),
                "raw_text": content,
            }
        return {
            "model": model_name,
            "generated_at": _utc_now_iso(),
            "summary": summary_json,
        }
    except Exception as exc:
        logger.warning(f"⚠️ LLM 요약 생성 실패: {exc}")
        return None


def _extract_response_timings(response: "SearchResponse", fallback: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if response.query_analysis and isinstance(response.query_analysis, dict):
        timings = response.query_analysis.get("timings_ms")
        if isinstance(timings, dict):
            return timings
    return fallback or {}


def _persist_search_logs(
    *,
    request,
    response: "SearchResponse",
    analysis,
    cache_hit: bool,
    timings: Dict[str, Any],
) -> None:
    client = getattr(router, "redis_client", None)
    if client is None:
        return

    timestamp = _utc_now_iso()
    session_id = getattr(request, "session_id", None)
    user_id = getattr(request, "user_id", None)
    request_id = getattr(request, "request_id", None)
    request_metadata = getattr(request, "metadata", None)
    conversation_prefix = getattr(router, "conversation_history_prefix", None)
    conversation_ttl = getattr(router, "conversation_history_ttl_seconds", None)
    conversation_max = getattr(router, "conversation_history_max_messages", None)
    search_history_prefix = getattr(router, "search_history_prefix", None)
    search_history_ttl = getattr(router, "search_history_ttl_seconds", None)
    search_history_max = getattr(router, "search_history_max_entries", None)

    top_user_ids = [
        getattr(result, "user_id", None) for result in (response.results or [])[:50]
        if getattr(result, "user_id", None)
    ]

    if getattr(request, "log_conversation", True):
        conversation_key = _make_conversation_key(conversation_prefix, session_id)
        if conversation_key:
            user_entry = {
                "role": "user",
                "timestamp": timestamp,
                "content": _truncate_text(request.query, 4000),
                "session_id": session_id,
                "user_id": user_id,
                "request_id": request_id,
                "metadata": request_metadata,
            }
            _redis_list_append(client, conversation_key, user_entry, conversation_max, conversation_ttl)

            # 전체 검색 결과를 직렬화하여 포함
            serialized_results = None
            if response.results:
                try:
                    serialized_results = [_serialize_result(result) for result in response.results]
                except Exception as exc:
                    logger.warning(f"⚠️ 대화 로그용 검색 결과 직렬화 실패: {exc}")
                    serialized_results = None
            
            assistant_payload: Dict[str, Any] = {
                "requested_count": getattr(response, "requested_count", None),
                "query": request.query,
                "total_hits": response.total_hits,
                "max_score": getattr(response, "max_score", None),
                "results": serialized_results,  # 전체 검색 결과 포함
                "returned_count": len(response.results or []),
                "cache_hit": cache_hit,
                "top_user_ids": top_user_ids,
                "took_ms": getattr(response, "took_ms", None),
                "page": response.page,
                "page_size": response.page_size,
                "has_more": getattr(response, "has_more", False),
            }
            if response.llm_summary:
                assistant_payload["llm_summary"] = response.llm_summary

            assistant_entry = {
                "role": "assistant",
                "timestamp": timestamp,
                "content": assistant_payload,  # 전체 딕셔너리 저장 (truncate 제거)
                "session_id": session_id,
                "user_id": user_id,
                "request_id": request_id,
            }
            _redis_list_append(client, conversation_key, assistant_entry, conversation_max, conversation_ttl)

    if getattr(request, "log_search_history", True):
        owner_id = user_id or session_id or "default"
        history_key = _make_history_key(search_history_prefix, owner_id)
        if history_key:
            # 전체 검색 결과를 직렬화하여 포함
            serialized_results = None
            if response.results:
                try:
                    serialized_results = [_serialize_result(result) for result in response.results]
                except Exception as exc:
                    logger.warning(f"⚠️ 검색 결과 직렬화 실패: {exc}")
                    serialized_results = None
            
            history_entry = {
                "timestamp": timestamp,
                "user_id": user_id,
                "session_id": session_id,
                "request_id": request_id,
                "query": request.query,
                "intent": getattr(analysis, "intent", None),
                "must_terms": getattr(analysis, "must_terms", []),
                "should_terms": getattr(analysis, "should_terms", []),
                "page": response.page,
                "page_size": response.page_size,
                "total_hits": response.total_hits,
                "returned_count": len(response.results or []),
                "cache_hit": cache_hit,
                "timings": timings,
                "top_user_ids": top_user_ids,
                "llm_summary": response.llm_summary,
                "metadata": request_metadata,
                "results": serialized_results,  # 전체 검색 결과 포함
            }
            _redis_list_append(client, history_key, history_entry, search_history_max, search_history_ttl)


class ConversationMessage(BaseModel):
    role: str
    timestamp: str
    session_id: Optional[str] = None
    user_id: Optional[str] = None
    request_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    content: Any


class SearchHistoryEntry(BaseModel):
    timestamp: str
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    request_id: Optional[str] = None
    query: str
    intent: Optional[str] = None
    must_terms: List[str] = Field(default_factory=list)
    should_terms: List[str] = Field(default_factory=list)
    page: int
    page_size: int
    total_hits: int
    returned_count: int
    cache_hit: bool
    timings: Dict[str, Any] = Field(default_factory=dict)
    top_user_ids: List[str] = Field(default_factory=list)
    llm_summary: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None
    results: Optional[List[Dict[str, Any]]] = Field(default=None, description="전체 검색 결과 (모든 사용자 데이터 포함)")


def _parse_conversation_record(item: str) -> Optional[ConversationMessage]:
    if not item:
        return None
    try:
        payload = json.loads(item)
    except Exception as exc:
        logger.warning(f"⚠️ 대화 로그 JSON 파싱 실패: {exc}")
        return None

    content = payload.get("content")
    # assistant 메시지의 content가 문자열이면 JSON으로 파싱 시도
    # (이전 버전 호환성: _truncate_text로 저장된 경우)
    if payload.get("role") == "assistant" and isinstance(content, str):
        try:
            content = json.loads(content)
        except Exception:
            # JSON 파싱 실패 시 문자열 그대로 유지
            pass
    # content가 이미 딕셔너리인 경우 그대로 사용 (새 버전)
    
    return ConversationMessage(
        role=payload.get("role"),
        timestamp=payload.get("timestamp"),
        session_id=payload.get("session_id"),
        user_id=payload.get("user_id"),
        request_id=payload.get("request_id"),
        metadata=payload.get("metadata"),
        content=content,
    )


def _parse_search_history_record(item: str) -> Optional[SearchHistoryEntry]:
    if not item:
        return None
    try:
        payload = json.loads(item)
    except Exception as exc:
        logger.warning(f"⚠️ 검색 이력 JSON 파싱 실패: {exc}")
        return None

    llm_summary = payload.get("llm_summary")
    if isinstance(llm_summary, str):
        try:
            llm_summary = json.loads(llm_summary)
        except Exception:
            pass

    return SearchHistoryEntry(
        timestamp=payload.get("timestamp"),
        user_id=payload.get("user_id"),
        session_id=payload.get("session_id"),
        request_id=payload.get("request_id"),
        query=payload.get("query", ""),
        intent=payload.get("intent"),
        must_terms=payload.get("must_terms") or [],
        should_terms=payload.get("should_terms") or [],
        page=payload.get("page", 1),
        page_size=payload.get("page_size", 10),
        total_hits=payload.get("total_hits", 0),
        returned_count=payload.get("returned_count", 0),
        cache_hit=bool(payload.get("cache_hit")),
        timings=payload.get("timings") or {},
        top_user_ids=payload.get("top_user_ids") or [],
        llm_summary=llm_summary,
        metadata=payload.get("metadata"),
        results=payload.get("results"),  # 전체 검색 결과 포함
    )


def _finalize_search_response(
    *,
    request,
    response: "SearchResponse",
    analysis,
    cache_hit: bool,
    timings: Optional[Dict[str, Any]] = None,
) -> "SearchResponse":
    summary_payload = _maybe_generate_llm_summary(
        request=request,
        response=response,
        analysis=analysis,
    )
    if summary_payload:
        response = response.model_copy(update={"llm_summary": summary_payload})

    effective_timings = _extract_response_timings(response, timings)
    _persist_search_logs(
        request=request,
        response=response,
        analysis=analysis,
        cache_hit=cache_hit,
        timings=effective_timings,
    )
    return response


# ⭐ survey_responses_merged만 사용하므로 welcome 인덱스 캐시 함수 제거


def calculate_rrf_score_adaptive(
    keyword_results: List[Dict[str, Any]],
    vector_results: List[Dict[str, Any]],
    query_intent: Optional[str],
    has_filters: bool,
    use_vector_search: bool,
) -> Tuple[List[Dict[str, Any]], int, str]:
    """쿼리 특성에 따라 RRF k 값과 alpha 가중치를 조정"""
    k = 60
    alpha = 0.6  # ⭐⭐⭐ 벡터 검색 가중치: 60% (keyword는 40%)
    reason = "균형 유지 (k=60, alpha=0.6 → vector 60%)"

    if has_filters:
        k = 40
        alpha = 0.6  # 필터가 있어도 벡터 검색 중시
        reason = "필터 적용 → 벡터 중심 (k=40, alpha=0.6 → vector 60%)"
    elif use_vector_search and query_intent and query_intent.lower() in {"semantic", "semantic_search"}:
        k = 80
        alpha = 0.7  # 시맨틱 검색이면 벡터 가중치 더 높임
        reason = f"의도={query_intent} → 벡터 강화 (k=80, alpha=0.7 → vector 70%)"

    combined = calculate_rrf_score(
        keyword_results=keyword_results,
        vector_results=vector_results,
        k=k,
        alpha=alpha,
    )
    return combined, k, reason


def _sort_dict_recursive(obj: Any) -> Any:
    """딕셔너리/리스트를 재귀적으로 정렬"""
    if isinstance(obj, dict):
        return {key: _sort_dict_recursive(obj[key]) for key in sorted(obj)}
    if isinstance(obj, list):
        normalized_items = [_sort_dict_recursive(item) for item in obj]
        try:
            return sorted(
                normalized_items,
                key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True),
            )
        except TypeError:
            return normalized_items
    return obj


def _normalize_filters_for_cache(filters: List[Dict[str, Any]]) -> str:
    """필터 목록을 안정적인 문자열로 변환"""
    if not filters:
        return ""

    normalized_strings = []
    for filter_item in filters:
        normalized = _sort_dict_recursive(filter_item)
        normalized_strings.append(json.dumps(normalized, ensure_ascii=False, sort_keys=True))

    normalized_strings.sort()
    return "|".join(normalized_strings)


def _make_cache_key(
    *,
    prefix: str,
    query: str,
    index_name: str,
    page_size: int,
    use_vector: bool,
    use_claude: bool,
    must_terms: List[str],
    should_terms: List[str],
    must_not_terms: List[str],
    filters_signature: Optional[str] = None,
    behavior_signature: Optional[str] = None,
) -> str:
    """생성된 검색 결과를 재사용하기 위한 캐시 키 생성 (안정화)"""
    stable_must = sorted(must_terms) if must_terms else []
    stable_should = sorted(should_terms) if should_terms else []
    stable_must_not = sorted(must_not_terms) if must_not_terms else []

    payload = {
        "query": query.strip().lower(),
        "index": index_name,
        "page_size": page_size,
        "use_vector": use_vector,
        "use_claude": bool(use_claude),
        "must_terms": stable_must,
        "should_terms": stable_should,
        "must_not_terms": stable_must_not,
        "filters_signature": filters_signature or "",
        "behavior_signature": behavior_signature or "",
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    key = f"{prefix}:{digest}"

    logger.debug(f"🔑 Cache key generated: {key}")
    logger.debug(f"   - must_terms: {stable_must}")
    logger.debug(f"   - should_terms: {stable_should}")
    logger.debug(f"   - filters_signature: {(filters_signature or '')[:100]}...")
    logger.debug(f"   - behavior_signature: {(behavior_signature or '')[:100]}...")
    logger.debug(f"   - use_claude: {bool(use_claude)}")

    return key


def _serialize_result(result: "SearchResult") -> Dict[str, Any]:
    """SearchResult를 JSON 직렬화 가능한 dict로 변환"""
    return result.model_dump()


def _deserialize_result(payload: Dict[str, Any]) -> "SearchResult":
    """dict를 SearchResult 객체로 역직렬화"""
    return SearchResult(**payload)


def _slice_results(
    serialized_items: List[Dict[str, Any]],
    page: int,
    page_size: int,
) -> Tuple[List["SearchResult"], bool]:
    """페이지 정보를 기준으로 결과를 슬라이싱"""
    if page <= 0:
        page = 1
    start = (page - 1) * page_size
    end = start + page_size

    if start >= len(serialized_items):
        return [], False

    page_items = serialized_items[start:end]
    results = [_deserialize_result(item) for item in page_items]
    has_more = end < len(serialized_items)
    return results, has_more


def _build_cached_response(
    *,
    payload: Dict[str, Any],
    request: "SearchRequest",
    analysis,
    filters_for_response: List[Dict[str, Any]],
    overall_start: float,
    extracted_entities_dict: Optional[Dict[str, Any]] = None,
) -> "SearchResponse":
    """Redis 캐시에서 불러온 결과로 SearchResponse 구성"""
    total_hits = payload.get("total_hits", 0)
    max_score = payload.get("max_score", 0.0)
    serialized_items = payload.get("items", [])

    cached_page_size = payload.get("page_size")
    request_page_size = getattr(request, "size", None)
    if request_page_size is None:
        request_page_size = getattr(request, "page_size", None)
    page_size = cached_page_size or request_page_size or max(len(serialized_items), 1)

    page_results, has_more_local = _slice_results(serialized_items, request.page, page_size)
    has_more = has_more_local and ((request.page * page_size) < total_hits)
    total_duration_ms = (perf_counter() - overall_start) * 1000

    timings = {
        "cache_hit": 1.0,
        "total_ms": total_duration_ms,
    }

    query_analysis = {
        "intent": analysis.intent,
        "must_terms": analysis.must_terms,
        "should_terms": analysis.should_terms,
        "alpha": analysis.alpha,
        "confidence": analysis.confidence,
        "size": page_size,
        "timings_ms": timings,
        "behavioral_conditions": payload.get("behavioral_conditions", {}),
        "use_claude_analyzer": bool(payload.get("use_claude", False)),
    }
    if extracted_entities_dict is not None:
        query_analysis["extracted_entities"] = extracted_entities_dict

    # requested_count 추출 (payload에서 가져오거나 None)
    requested_count = payload.get("requested_count")
    
    return SearchResponse(
        requested_count=requested_count,
        query=request.query,
        session_id=getattr(request, "session_id", None),
        total_hits=total_hits,
        max_score=max_score,
        results=page_results,
        query_analysis=query_analysis,
        took_ms=int(total_duration_ms),
        page=request.page,
        page_size=page_size,
        has_more=has_more,
    )


def _log_final_summary(
    *,
    stage: str,
    query: str,
    analysis,
    total_hits: int,
    returned_count: int,
    page: int,
    page_size: int,
    cache_hit: bool,
    timings: Dict[str, Any],
    took_ms: Optional[float],
    filters: Optional[List[Dict[str, Any]]],
    behavioral_conditions: Optional[Dict[str, Any]],
    use_claude: Optional[bool] = None,
) -> None:
    """검색 종료 시 핵심 정보를 한 번 더 요약 출력."""
    intent = getattr(analysis, "intent", None)
    must_terms = getattr(analysis, "must_terms", [])
    should_terms = getattr(analysis, "should_terms", [])
    filter_count = len(filters or [])
    behavior_info = behavioral_conditions or {}
    important_timings = {k: round(v, 2) if isinstance(v, (int, float)) else v for k, v in (timings or {}).items()}

    lines = [
        "",
        "🔚 최종 요약 (핵심)",
        f" • stage: {stage}",
        f" • query: {query}",
        f" • intent: {intent}",
        f" • must_terms: {must_terms}",
        f" • should_terms: {should_terms}",
        f" • behavioral_conditions: {behavior_info}",
        f" • filters: {filter_count}개",
        f" • returned/total: {returned_count}/{total_hits}",
        f" • page: {page} / page_size: {page_size}",
        f" • cache_hit: {cache_hit}",
        f" • timings: {important_timings}",
        f" • total_ms: {round(took_ms, 2) if took_ms is not None else 'N/A'}",
    ]
    if use_claude is not None:
        lines.append(f" • use_claude_analyzer: {use_claude}")

    logger.info("\n".join(lines))


def build_occupation_dsl_filter(occupation_entities: List["DemographicEntity"]) -> Dict[str, Any]:
    """직업 DemographicEntity 리스트를 OpenSearch DSL 필터로 변환"""
    if not occupation_entities:
        return {"match_all": {}}

    occupation_values: Set[str] = set()
    for demo in occupation_entities:
        for candidate in (
            getattr(demo, "raw_value", None),
            getattr(demo, "value", None),
        ):
            if candidate:
                occupation_values.add(str(candidate))
        synonyms = getattr(demo, "synonyms", None)
        if synonyms:
            for syn in synonyms:
                if syn:
                    occupation_values.add(str(syn))

    occupation_values = {value.strip() for value in occupation_values if value and value.strip()}
    if not occupation_values:
        return {"match_all": {}}

    values_list = sorted(occupation_values)

    question_should = [
        {"match_phrase": {"qa_pairs.q_text": "직업"}},
        {"match_phrase": {"qa_pairs.q_text": "직무"}},
        {"match_phrase": {"qa_pairs.q_text": "occupation"}},
    ]

    answer_should = [
        {"terms": {"qa_pairs.answer.keyword": values_list}},
        {"terms": {"qa_pairs.answer_text.keyword": values_list}},
    ]
    # ⭐ match 대신 match_phrase 사용: "전문직"과 "사무직"이 "직" 토큰으로 잘못 매칭되는 것 방지
    for value in values_list:
        answer_should.append({"match_phrase": {"qa_pairs.answer": value}})
        answer_should.append({"match_phrase": {"qa_pairs.answer_text": value}})

    nested_filter = {
        "nested": {
            "path": "qa_pairs",
            "query": {
                "bool": {
                    "must": [
                        {
                            "bool": {
                                "should": question_should,
                                "minimum_should_match": 1,
                            }
                        },
                        {
                            "bool": {
                                "should": answer_should,
                                "minimum_should_match": 1,
                            }
                        },
                    ]
                }
            }
        }
    }

    metadata_terms = {
        "terms": {
            "metadata.occupation.keyword": values_list,
        }
    }
    metadata_job_terms = {
        "terms": {
            "metadata.job.keyword": values_list,
        }
    }

    return {
        "bool": {
            "should": [
                metadata_terms,
                metadata_job_terms,
                nested_filter,
            ],
            "minimum_should_match": 1,
        }
    }


def get_adaptive_score_threshold(
    query: str,
    has_filters: bool,
    must_terms_count: int,
) -> Tuple[float, str]:
    """쿼리 특성에 따라 Qdrant score_threshold 조정"""
    threshold = 0.30
    reason = "기본값 0.30"

    if has_filters:
        threshold = 0.25
        reason = "필터 적용 → 후보 확보 (threshold=0.25)"
    elif must_terms_count >= 3:
        threshold = 0.35
        reason = "키워드 다수 → 정확도 중시 (threshold=0.35)"
    elif len(query.split()) <= 3:
        threshold = 0.35
        reason = "짧은 쿼리 → 정밀도 중시 (threshold=0.35)"

    return threshold, reason


def _collect_text_from_doc(doc: Dict[str, Any]) -> str:
    text_fragments: List[str] = []

    source = doc.get("_source") or doc.get("source") or {}
    if not source and "doc" in doc:
        source = doc.get("doc", {}).get("_source", {})
    if not source and "payload" in doc:
        payload = doc["payload"]
        if isinstance(payload, dict):
            source = {
                "payload_text": payload.get("text"),
                "payload": payload,
            }

    if isinstance(source, dict):
        for key in ("qa_pairs", "qaPairs"):
            qa_pairs = source.get(key, [])
            if isinstance(qa_pairs, list):
                for qa in qa_pairs:
                    if not isinstance(qa, dict):
                        continue
                    q_text = qa.get("q_text") or qa.get("question")
                    if q_text:
                        text_fragments.append(str(q_text).lower())
                    answer = qa.get("answer") or qa.get("answer_text") or qa.get("value")
                    if answer:
                        if isinstance(answer, list):
                            text_fragments.extend(str(item).lower() for item in answer)
                        else:
                            text_fragments.append(str(answer).lower())

        for key in ("metadata", "demographic_info", "payload"):
            meta = source.get(key)
            if isinstance(meta, dict):
                for value in meta.values():
                    if value:
                        text_fragments.append(str(value).lower())

        for key in ("title", "text", "content", "payload_text"):
            value = source.get(key)
            if value:
                text_fragments.append(str(value).lower())

    if "payload" in doc and isinstance(doc["payload"], dict):
        payload = doc["payload"]
        for key in ("text", "keywords"):
            value = payload.get(key)
            if isinstance(value, list):
                text_fragments.extend(str(item).lower() for item in value)
            elif value:
                text_fragments.append(str(value).lower())

    return " ".join(text_fragments)


def contains_must_terms(doc: Dict[str, Any], must_terms: List[str]) -> bool:
    """
    ⚠️ Deprecated: OpenSearch 쿼리에서 must 조건 처리로 대체됨
    성능 향상을 위해 Python 레벨 검증은 제거됨 (하이브리드 검색 최적화)

    레거시 코드 호환성을 위해 유지, 사용 권장하지 않음
    """
    if not must_terms:
        return True

    combined_text = _collect_text_from_doc(doc)
    if not combined_text:
        return False

    for term in must_terms:
        normalized = term.lower().strip()
        if normalized and normalized not in combined_text:
            return False
    return True


def _qa_contains_terms(qa: Dict[str, Any], terms_lower: List[str]) -> bool:
    if not isinstance(qa, dict):
        return False

    text_candidates: List[str] = []
    q_text = qa.get("q_text") or qa.get("question")
    if q_text:
        text_candidates.append(str(q_text).lower())

    answer = qa.get("answer") or qa.get("answer_text") or qa.get("value")
    if answer:
        if isinstance(answer, list):
            text_candidates.extend(str(item).lower() for item in answer)
        else:
            text_candidates.append(str(answer).lower())

    if not text_candidates:
        return False

    combined = " ".join(text_candidates)
    return all(term in combined for term in terms_lower if term)


def extract_matched_qa_pairs(source: Dict[str, Any], must_terms: List[str], limit: int = 5) -> List[Dict[str, Any]]:
    if not must_terms:
        return []
    qa_pairs = source.get("qa_pairs")
    if not isinstance(qa_pairs, list):
        return []

    terms_lower = [term.lower().strip() for term in must_terms if term]
    matched: List[Dict[str, Any]] = []
    for qa in qa_pairs:
        if _qa_contains_terms(qa, terms_lower):
            matched.append(qa)
            if len(matched) >= limit:
                break
    return matched


def get_display_qa_pairs(source: Dict[str, Any], must_terms: List[str], limit: int = 5) -> List[Dict[str, Any]]:
    qa_pairs = source.get("qa_pairs")
    if not isinstance(qa_pairs, list):
        return []

    if not must_terms:
        return qa_pairs[:limit]

    terms_lower = [term.lower().strip() for term in must_terms if term]
    matched = []
    others = []
    for qa in qa_pairs:
        if _qa_contains_terms(qa, terms_lower):
            matched.append(qa)
        else:
            others.append(qa)

    ordered = matched + others
    return ordered[:limit]


def extract_inner_hit_matches(hit: Dict[str, Any]) -> List[Dict[str, Any]]:
    inner_hits = hit.get("inner_hits")
    if not isinstance(inner_hits, dict):
        return []

    collected: List[Dict[str, Any]] = []
    for inner_name, inner_data in inner_hits.items():
        hits_obj = inner_data.get("hits", {}) if isinstance(inner_data, dict) else {}
        for inner_hit in hits_obj.get("hits", []):
            inner_source = inner_hit.get("_source", {}) or {}
            if "qa_pairs" in inner_source and isinstance(inner_source["qa_pairs"], dict):
                qa_entry = inner_source["qa_pairs"].copy()
            else:
                qa_entry = inner_source.copy()

            if not isinstance(qa_entry, dict):
                continue

            if "_score" in inner_hit and "match_score" not in qa_entry:
                qa_entry["match_score"] = inner_hit["_score"]
            if "highlight" in inner_hit and "highlights" not in qa_entry:
                qa_entry["highlights"] = inner_hit["highlight"]

            qa_entry.setdefault("inner_hit_name", inner_name)
            collected.append(qa_entry)

    return collected


def extract_behavioral_qa_pairs(
    source: Dict[str, Any],
    behavioral_conditions: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Behavioral 조건에 매칭된 qa_pairs 추출

    Args:
        source: OpenSearch 문서 _source (또는 qa_pairs만 포함된 dict)
        behavioral_conditions: {"smoker": True, "has_vehicle": True, ...}

    Returns:
        매칭된 qa_pairs 리스트
        [
            {
                "condition_type": "smoker",
                "condition_value": True,
                "q_text": "귀하는 흡연을 하십니까?",
                "answer": "흡연함",
                "confidence": 1.0
            },
            ...
        ]
    """
    qa_pairs = source.get('qa_pairs', [])
    if not qa_pairs or not behavioral_conditions:
        return []

    matched = []

    # 조건별 키워드 매핑
    CONDITION_KEYWORDS = {
        'smoker': ['흡연', '담배', '피우', '피움'],
        'has_vehicle': ['차량', '차', '자동차', '보유차량'],
        'alcohol_preference': ['주류', '음주', '술', '맥주', '소주', '와인', '막걸리'],
        'exercise_frequency': ['운동', '헬스', '체육'],
        'pet_ownership': ['반려동물', '펫', '애완동물', '강아지', '고양이'],
    }

    for condition_type, condition_value in behavioral_conditions.items():
        # 이 조건에 해당하는 키워드들
        keywords = CONDITION_KEYWORDS.get(condition_type, [])
        if not keywords:
            continue

        # qa_pairs에서 이 조건에 해당하는 질문 찾기
        for qa in qa_pairs:
            q_text = qa.get('q_text', '').lower()
            answer = qa.get('answer', '')

            # 키워드 매칭
            if any(kw in q_text for kw in keywords):
                # Boolean 조건 (smoker, has_vehicle)
                if isinstance(condition_value, bool):
                    answer_lower = answer.lower()
                    is_positive = any(pos in answer_lower for pos in BEHAVIOR_YES_TOKENS)
                    is_negative = any(neg in answer_lower for neg in BEHAVIOR_NO_TOKENS)

                    # 조건값과 답변이 일치하는지 확인
                    if condition_value and is_positive:
                        matched.append({
                            'condition_type': condition_type,
                            'condition_value': condition_value,
                            'q_text': qa.get('q_text', ''),
                            'answer': answer,
                            'confidence': 1.0
                        })
                        break  # 이 조건에 대해 하나만
                    elif not condition_value and is_negative:
                        matched.append({
                            'condition_type': condition_type,
                            'condition_value': condition_value,
                            'q_text': qa.get('q_text', ''),
                            'answer': answer,
                            'confidence': 1.0
                        })
                        break

                # String 조건 (alcohol_preference)
                else:
                    # 답변에 조건값이 포함되어 있으면 매칭
                    if str(condition_value).lower() in answer.lower():
                        matched.append({
                            'condition_type': condition_type,
                            'condition_value': condition_value,
                            'q_text': qa.get('q_text', ''),
                            'answer': answer,
                            'confidence': 0.8
                        })
                        break

    return matched


def reorder_with_matches(full_list: List[Dict[str, Any]], matched: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    if not isinstance(full_list, list):
        return []

    if not matched:
        return full_list[:limit]

    def _key(qa: Dict[str, Any]) -> tuple:
        return (
            qa.get("q_text") or qa.get("question") or "",
            str(qa.get("answer") or qa.get("answer_text") or qa.get("value") or "")
        )

    seen = set()
    ordered: List[Dict[str, Any]] = []

    for qa in matched:
        key = _key(qa)
        if key not in seen:
            ordered.append(qa)
            seen.add(key)

    for qa in full_list:
        key = _key(qa)
        if key not in seen:
            ordered.append(qa)
            seen.add(key)

    return ordered[:limit]


class SearchRequest(BaseModel):
    """검색 요청"""
    query: str = Field(..., description="검색 쿼리")
    index_name: str = Field(
        default="survey_responses_merged",
        description="검색할 인덱스 이름 (기본값: survey_responses_merged; 와일드카드 사용 가능)"
    )
    size: int = Field(default=10, ge=1, le=100, description="반환할 결과 개수")
    use_vector_search: bool = Field(default=True, description="벡터 검색 사용 여부")
    page: int = Field(default=1, ge=1, description="요청할 페이지 번호 (1부터 시작)")
    use_claude_analyzer: Optional[bool] = Field(
        default=None,
        description="Claude 분석기 사용 여부 (None이면 서버 설정값을 따름)"
    )
    session_id: Optional[str] = Field(
        default=None,
        description="대화/세션 식별자 (Redis 대화 로그 키)"
    )
    user_id: Optional[str] = Field(
        default=None,
        description="요청 사용자 식별자 (검색 이력 키)"
    )
    request_id: Optional[str] = Field(
        default=None,
        description="요청 추적을 위한 ID"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description="추가 요청 메타데이터"
    )
    log_conversation: bool = Field(
        default=True,
        description="Redis 대화 로그 저장 여부"
    )
    log_search_history: bool = Field(
        default=True,
        description="Redis 검색 이력 저장 여부"
    )
    request_llm_summary: bool = Field(
        default=False,
        description="LLM 요약/분석 생성 요청 여부"
    )
    llm_summary_instructions: Optional[str] = Field(
        default=None,
        description="LLM 요약 시 사용할 추가 지침"
    )


class SearchResult(BaseModel):
    """검색 결과 항목"""
    user_id: str
    score: float
    timestamp: Optional[str] = Field(default=None, description="인덱싱 시간")
    survey_datetime: Optional[str] = Field(default=None, description="설문조사 일시 (metadata.survey_datetime)")
    demographic_info: Optional[Dict[str, Any]] = Field(default=None, description="인구통계 정보 (survey_responses_merged에서 조회)")
    behavioral_info: Optional[Dict[str, Any]] = Field(default=None, description="행동/습관 정보 (예: 흡연 여부, 차량 보유 여부)")
    qa_pairs: Optional[List[Dict[str, Any]]] = None
    matched_qa_pairs: Optional[List[Dict[str, Any]]] = None
    highlights: Optional[Dict[str, Any]] = None


class SearchResponse(BaseModel):
    """검색 응답"""
    requested_count: Optional[int] = Field(
        default=None,
        description="쿼리에서 추출된 요청 인원 수 (예: '직장인 5명' → 5, 인원 제한 없으면 None)"
    )
    query: str
    total_hits: int
    max_score: Optional[float]
    results: List[SearchResult]
    session_id: Optional[str] = Field(
        default=None,
        description="요청에 사용된 세션 ID (자동 생성/전달된 값)",
    )
    query_analysis: Optional[Dict[str, Any]] = None
    took_ms: int
    page: int = Field(default=1, description="현재 페이지 번호")
    page_size: int = Field(default=10, description="페이지 당 결과 수")
    has_more: bool = Field(default=False, description="추가 페이지 존재 여부")
    llm_summary: Optional[Dict[str, Any]] = Field(
        default=None,
        description="LLM 기반 데이터 요약/분석 결과"
    )


# ===== 간소화된 응답 모델 (프론트엔드 친화적) =====

class MatchedCondition(BaseModel):
    """Behavioral 조건 매칭 정보"""
    condition_type: str = Field(..., description="조건 타입 (smoker, has_vehicle, alcohol_preference 등)")
    condition_value: Any = Field(..., description="조건 값 (True, False, '맥주' 등)")
    question: str = Field(..., description="실제 질문 텍스트")
    answer: str = Field(..., description="실제 답변 텍스트")
    confidence: float = Field(default=1.0, description="매칭 신뢰도 (0.0~1.0)")


class SimpleResult(BaseModel):
    """간소화된 검색 결과 (프론트엔드용)"""
    user_id: str = Field(..., description="사용자 ID")
    score: float = Field(..., description="검색 점수")
    demographics: Dict[str, str] = Field(..., description="인구통계 정보 (gender, age_group, birth_year)")
    matched_conditions: List[MatchedCondition] = Field(
        default_factory=list,
        description="매칭된 behavioral 조건들"
    )


class SimpleResponse(BaseModel):
    """프론트엔드 친화적 간소화 응답"""
    state: Literal["SUCCESS", "ERROR"] = Field(..., description="응답 상태")
    message: str = Field(..., description="응답 메시지")
    query: str = Field(..., description="검색 쿼리")
    total_hits: int = Field(..., description="총 결과 수")
    results: List[SimpleResult] = Field(..., description="검색 결과 목록")
    query_info: Optional[Dict[str, Any]] = Field(
        default=None,
        description="쿼리 분석 정보 (keywords, filters_applied, behavioral_conditions)"
    )
    took_ms: int = Field(..., description="검색 소요 시간 (밀리초)")


# ===== 초경량화 응답 모델 (무한 스크롤용) =====

class LightResult(BaseModel):
    """초경량 검색 결과 (demographics만, qa_pairs 제외)"""
    user_id: str = Field(..., description="사용자 ID")
    score: float = Field(..., description="검색 점수")
    timestamp: str = Field(..., description="응답 타임스탬프")
    survey_datetime: Optional[str] = Field(None, description="설문 응답 시간")
    demographic_info: Dict[str, Optional[str]] = Field(
        ...,
        description="인구통계 정보 (age_group, gender, birth_year, region, sub_region, occupation, marital_status, panel)"
    )


class SearchResponseLight(BaseModel):
    """초경량 검색 응답 (무한 스크롤 페이지네이션)"""
    query: str = Field(..., description="검색 쿼리")
    total_hits: int = Field(..., description="전체 결과 수 (필터링 후)")
    results: List[LightResult] = Field(..., description="현재 페이지 결과")
    page: int = Field(..., description="현재 페이지 번호 (1부터 시작)")
    page_size: int = Field(..., description="페이지 당 결과 수")
    has_more: bool = Field(..., description="다음 페이지 존재 여부")
    took_ms: int = Field(..., description="검색 소요 시간 (밀리초)")
    cache_hit: bool = Field(default=False, description="캐시 히트 여부")
    cache_type: Optional[str] = Field(None, description="캐시 타입 (memory, redis, none)")

# ⭐ Behavioral 패턴 상수 import
from constants.behavior_maps import *

def extract_all_behaviors_batch(qa_pairs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    ⚡⚡⚡ 초고속 최적화: qa_pairs를 한 번만 순회하여 모든 behavioral 패턴을 동시에 추출

    최적화 기법:
    1. SequenceMatcher 제거 (100배 개선) ⚡
    2. 해시맵 전처리 O(1) 조회 (10배 개선) ⚡
    3. Early termination (2-3배 개선) ⚡
    4. 정규화 매칭 (공백/물음표/대소문자 차이 무시) ⚡

    Args:
        qa_pairs: list of dict (각 dict는 질문/답변 쌍)

    Returns:
        Dict[behavior_key, value] - 모든 behavioral 패턴의 값
    """
    # ⭐ 정규화 함수 (강화)
    def normalize_question(text: str) -> str:
        """질문 텍스트 정규화 (공백, 물음표, 느낌표, 대소문자 정규화)"""
        import re
        if not text:
            return ""
        text = text.strip()
        text = re.sub(r'\s+', ' ', text)  # 공백 정규화 (여러 공백 → 하나)
        text = text.rstrip('?!')  # 물음표, 느낌표 제거
        return text.lower()
    
    # ⚡ 최적화 1: 해시맵 전처리 (정규화된 question_text → behavior_key)
    question_to_behavior = {}
    for behavior_key, config in BEHAVIORAL_KEYWORD_MAP.items():
        if config.get('question_text'):
            normalized = normalize_question(config['question_text'])
            question_to_behavior[normalized] = behavior_key

    # 결과 딕셔너리 초기화
    behavioral_values = {}
    total_patterns = len(BEHAVIORAL_KEYWORD_MAP)

    # qa_pairs가 비어있으면 바로 반환
    if not qa_pairs:
        return behavioral_values

    # qa_pairs 순회 (단 한 번!)
    for qa in qa_pairs:
        if not isinstance(qa, dict):
            continue

        q_text = str(qa.get("q_text", ""))
        q_text_lower = q_text.lower()
        answer = qa.get("answer") or qa.get("answer_text")

        if not answer:
            continue

        answer_text = str(answer).lower()

        # ⚡ 최적화 2: 해시맵으로 O(1) 조회 (정규화된 question_text로 매칭)
        normalized_q = normalize_question(q_text)
        behavior_key = question_to_behavior.get(normalized_q)
        if behavior_key and behavioral_values.get(behavior_key) is None:
            config = BEHAVIORAL_KEYWORD_MAP[behavior_key]
            answer_values = config.get('answer_values')

            # 답변 값 추출
            if answer_values:
                # String 패턴
                matched_value = None
                answer_text_clean = answer_text.strip()

                # ⭐⭐⭐ 개선: Dict/List 자동 감지 + 완전 일치 우선! (question_text 정확 매칭)
                if isinstance(answer_values, dict):
                    max_score = 0

                    # 1차: 완전 일치
                    for value_name, keywords in answer_values.items():
                        for kw in keywords:
                            if answer_text_clean == kw.lower().strip():
                                matched_value = value_name
                                max_score = 999999
                                break
                        if max_score == 999999:
                            break

                    # 2차: 부분 매칭 (길이 가중치)
                    if not matched_value:
                        for value_name, keywords in answer_values.items():
                            matched_kws = [kw for kw in keywords if kw.lower() in answer_text_clean]
                            if matched_kws:
                                longest_len = max(len(kw) for kw in matched_kws)
                                score = len(matched_kws) * longest_len
                                if score > max_score:
                                    max_score = score
                                    matched_value = value_name

                elif isinstance(answer_values, list):
                    # List 타입 처리
                    for value in answer_values:
                        if str(value).strip().lower() == answer_text_clean:
                            matched_value = value
                            break
                    if not matched_value:
                        best_match = None
                        max_len = 0
                        for value in answer_values:
                            value_str = str(value).strip().lower()
                            if value_str in answer_text_clean and len(value_str) > max_len:
                                max_len = len(value_str)
                                best_match = value
                        matched_value = best_match

                if matched_value:
                    behavioral_values[behavior_key] = matched_value
            else:
                # Boolean 패턴
                positive_kw = config.get('positive_keywords', set())
                negative_kw = config.get('negative_keywords', set())

                if positive_kw and any(kw.lower() in answer_text for kw in positive_kw):
                    behavioral_values[behavior_key] = True
                elif negative_kw and any(kw.lower() in answer_text for kw in negative_kw):
                    behavioral_values[behavior_key] = False

        # ⭐ Fallback: question_keywords 매칭 (정확 매칭 실패 시 Fallback)
        # question_text가 있어도 정확 매칭 실패 시 question_keywords로 시도!
        for behavior_key, config in BEHAVIORAL_KEYWORD_MAP.items():
            # 이미 값이 추출된 경우 스킵
            if behavioral_values.get(behavior_key) is not None:
                continue

            # ⭐ question_text가 있어도 Fallback 시도! (정확 매칭 실패 시)
            question_keywords = config.get('question_keywords', set())
            answer_values = config.get('answer_values')

            if not question_keywords:
                continue

            # Question keywords 매칭
            is_matched = False
            for kw in question_keywords:
                if kw.lower() in q_text_lower:
                    is_matched = True
                    break

            if not is_matched:
                continue

            # ⭐ 디버깅: skin_satisfaction 매칭 과정 추적
            if behavior_key == 'skin_satisfaction' and len(behavioral_values) < 5:  # 처음 5개만
                logger.info(f"🔍 [DEBUG] skin_satisfaction 매칭 시도:")
                logger.info(f"   - q_text: '{q_text[:100]}'")
                logger.info(f"   - answer_text: '{answer_text}'")
                logger.info(f"   - answer_values: {list(answer_values.keys())}")

            # 답변 값 추출
            if answer_values:
                # String 패턴
                matched_value = None
                answer_text_clean = answer_text.strip()

                # ⭐⭐⭐ 개선: Dict/List 자동 감지 + 완전 일치 우선!
                if isinstance(answer_values, dict):
                    # Dict 타입 처리 (예: {"불만족한다": ["불만족", ...], ...})
                    max_score = 0

                    # 1차: 완전 일치 체크 (최우선!)
                    for value_name, keywords in answer_values.items():
                        for kw in keywords:
                            if answer_text_clean == kw.lower().strip():
                                # 완전 일치 → 즉시 선택!
                                matched_value = value_name
                                max_score = 999999
                                break
                        if max_score == 999999:
                            break

                    # 2차: 부분 매칭 (완전 일치 실패 시, 길이 가중치 적용!)
                    if not matched_value:
                        for value_name, keywords in answer_values.items():
                            matched_kws = [kw for kw in keywords if kw.lower() in answer_text_clean]
                            if matched_kws:
                                # 점수 = (매칭 수) * (가장 긴 키워드 길이)
                                # 예: "불만족한다"(5자) > "만족"(2자)
                                longest_len = max(len(kw) for kw in matched_kws)
                                score = len(matched_kws) * longest_len

                                if score > max_score:
                                    max_score = score
                                    matched_value = value_name

                elif isinstance(answer_values, list):
                    # ⭐ List 타입 처리 (예: ["값1", "값2", ...])
                    # 1차: 완전 일치
                    for value in answer_values:
                        if str(value).strip().lower() == answer_text_clean:
                            matched_value = value
                            break

                    # 2차: 부분 일치 (가장 긴 매칭 선택)
                    if not matched_value:
                        best_match = None
                        max_len = 0
                        for value in answer_values:
                            value_str = str(value).strip().lower()
                            if value_str in answer_text_clean:
                                if len(value_str) > max_len:
                                    max_len = len(value_str)
                                    best_match = value
                        matched_value = best_match

                # ⭐ 디버깅: skin_satisfaction 매칭 결과
                if behavior_key == 'skin_satisfaction' and matched_value:
                    logger.info(f"✅ [DEBUG] skin_satisfaction 추출 성공!")
                    logger.info(f"   - answer_text: '{answer_text[:50]}'")
                    logger.info(f"   - matched_value: '{matched_value}'")
                    logger.info(f"   - answer_values 타입: {type(answer_values).__name__}")

                if matched_value:
                    behavioral_values[behavior_key] = matched_value
            else:
                # Boolean 패턴
                positive_kw = config.get('positive_keywords', set())
                negative_kw = config.get('negative_keywords', set())

                if positive_kw and any(kw.lower() in answer_text for kw in positive_kw):
                    behavioral_values[behavior_key] = True
                elif negative_kw and any(kw.lower() in answer_text for kw in negative_kw):
                    behavioral_values[behavior_key] = False

        # ⚡ 최적화 3: Early termination (모든 패턴 찾으면 종료)
        filled_count = sum(1 for v in behavioral_values.values() if v is not None)
        if filled_count == total_patterns:
            break  # 더 이상 순회 불필요!

    return behavioral_values


def extract_behavior_from_qa_pairs(
    qa_pairs: List[Dict[str, Any]],
    behavior_key: str,
    debug: bool = False
) -> Optional[Union[bool, str]]:
    """qa_pairs에서 특정 behavioral 조건을 추출 (범용)

    Args:
        qa_pairs: QA 쌍 리스트
        behavior_key: behavioral 조건 키
            - Boolean: 'ott_user', 'smoker', 'drinker', ...
            - String: 'winter_vacation_memory', 'skin_satisfaction', 'ai_service_field', ...
        debug: 디버깅 로그 출력 여부

    Returns:
        True/False: boolean 조건 (예: smoker=True)
        str: 문자열 값 (예: winter_vacation_memory="친구들과 보낸 즐거운 시간")
        None: 정보 없음
    """
    keyword_config = BEHAVIORAL_KEYWORD_MAP.get(behavior_key)
    if not keyword_config:
        if debug:
            logger.warning(f"[Behavioral] {behavior_key}는 BEHAVIORAL_KEYWORD_MAP에 없음")
        return None

    question_keywords = keyword_config['question_keywords']
    question_text = keyword_config.get('question_text', '')  # ⭐ Question text 가져오기

    # ⭐ 문자열 값 저장 패턴 처리 (answer_values가 있는 경우)
    answer_values = keyword_config.get('answer_values')
    if answer_values:
        for qa in qa_pairs:
            if not isinstance(qa, dict):
                continue

            q_text = str(qa.get("q_text", ""))
            q_text_lower = q_text.lower()

            # ========================================
            # ⭐⭐⭐ Step 1: Question Text 정확 매칭 (최우선!)
            # ========================================
            if question_text:
                # 완전 일치 확인
                if q_text == question_text:
                    if debug:
                        logger.warning(f"[Behavioral] {behavior_key} ✅ 정확 매칭 (question_text)")

                    # 답변 가져오기
                    answer = qa.get("answer") or qa.get("answer_text")
                    if answer:
                        answer_text = str(answer).lower()

                        # 답변 값 매칭
                        matched_value = None
                        max_match_count = 0

                        for value_name, keywords in answer_values.items():
                            match_count = sum(1 for kw in keywords if kw.lower() in answer_text)
                            if match_count > max_match_count:
                                max_match_count = match_count
                                matched_value = value_name

                        if matched_value:
                            if debug:
                                logger.warning(f"[Behavioral] {behavior_key} = '{matched_value}'")
                            return matched_value

                # 높은 유사도 (95% 이상)
                from difflib import SequenceMatcher
                similarity = SequenceMatcher(None, question_text, q_text).ratio()
                if similarity > 0.95:
                    if debug:
                        logger.warning(f"[Behavioral] {behavior_key} ✅ 유사 매칭 (유사도: {similarity:.2%})")

                    # 답변 가져오기
                    answer = qa.get("answer") or qa.get("answer_text")
                    if answer:
                        answer_text = str(answer).lower()

                        # 답변 값 매칭
                        matched_value = None
                        max_match_count = 0

                        for value_name, keywords in answer_values.items():
                            match_count = sum(1 for kw in keywords if kw.lower() in answer_text)
                            if match_count > max_match_count:
                                max_match_count = match_count
                                matched_value = value_name

                        if matched_value:
                            if debug:
                                logger.warning(f"[Behavioral] {behavior_key} = '{matched_value}'")
                            return matched_value

            # ========================================
            # ⭐ Step 2: Fallback - Question Keywords 매칭
            # ========================================
            # Question text 매칭 실패 시에만 키워드 사용
            matched_kw = None
            for kw in question_keywords:
                if kw.lower() in q_text_lower:
                    matched_kw = kw
                    break

            if not matched_kw:
                continue

            # 답변 가져오기
            answer = qa.get("answer") or qa.get("answer_text")
            if not answer:
                if debug:
                    logger.warning(f"[Behavioral] {behavior_key} 질문 발견했으나 답변 없음: q={q_text_lower[:30]}")
                continue

            answer_text = str(answer).lower()

            if debug:
                logger.warning(f"[Behavioral] {behavior_key} 검사중 (Fallback): q={q_text_lower[:30]}, a={answer_text[:50]}")

            # 답변 값 매칭 (가장 긴 매칭 우선)
            matched_value = None
            max_match_count = 0

            for value_name, keywords in answer_values.items():
                match_count = sum(1 for kw in keywords if kw.lower() in answer_text)
                if match_count > max_match_count:
                    max_match_count = match_count
                    matched_value = value_name

            if matched_value:
                if debug:
                    logger.warning(f"[Behavioral] {behavior_key} = '{matched_value}' (매칭: {max_match_count}개)")
                return matched_value

        return None

    # ⭐ 기존 boolean 조건 처리
    positive_keywords = keyword_config['positive_keywords']
    negative_keywords = keyword_config['negative_keywords']

    # ⭐ OTT 특수 처리: 답변 중심 매칭 (질문 관계없이)
    # "동영상 스트리밍 앱"처럼 명확한 답변이 있으면 바로 True 반환
    if behavior_key == 'ott_user':
        ANSWER_ONLY_POSITIVE = {"동영상 스트리밍 앱", "동영상스트리밍앱"}
        ANSWER_ONLY_NEGATIVE = {"이용하지 않는다", "이용하지않는다"}

        for qa in qa_pairs:
            if not isinstance(qa, dict):
                continue

            answer = qa.get("answer") or qa.get("answer_text")
            if not answer:
                continue

            answer_text = str(answer).lower()
            answer_compact = answer_text.replace(" ", "")

            # 부정 답변 우선 체크
            for neg_kw in ANSWER_ONLY_NEGATIVE:
                if neg_kw.replace(" ", "").lower() in answer_compact:
                    if debug:
                        logger.warning(f"[Behavioral] {behavior_key} = False (답변 키워드 '{neg_kw}' 발견)")
                    return False

            # 긍정 답변 체크
            for pos_kw in ANSWER_ONLY_POSITIVE:
                if pos_kw.replace(" ", "").lower() in answer_compact:
                    if debug:
                        logger.warning(f"[Behavioral] {behavior_key} = True (답변 키워드 '{pos_kw}' 발견)")
                    return True

    # ⭐ 기존 로직: 질문 키워드 매칭 → 답변 확인
    matched_questions = []
    for qa in qa_pairs:
        if not isinstance(qa, dict):
            continue

        q_text = str(qa.get("q_text", "")).lower()

        # 질문에 관련 키워드가 있는지 확인
        matched_kw = None
        for kw in question_keywords:
            if kw.lower() in q_text:
                matched_kw = kw
                break

        if not matched_kw:
            continue

        matched_questions.append(q_text)

        # 답변 가져오기
        answer = qa.get("answer") or qa.get("answer_text")
        if not answer:
            if debug:
                logger.warning(f"[Behavioral] {behavior_key} 질문 발견했으나 답변 없음: q={q_text}")
            continue

        answer_text = str(answer).lower()
        answer_compact = answer_text.replace(" ", "")

        if debug:
            logger.warning(f"[Behavioral] {behavior_key} 검사중: q={q_text[:30]}, a={answer_text[:50]}")

        # 부정 키워드 체크 (우선순위 높음)
        for neg_kw in negative_keywords:
            neg_kw_lower = str(neg_kw).lower()
            neg_kw_compact = neg_kw_lower.replace(" ", "")
            if neg_kw_lower in answer_text or neg_kw_compact in answer_compact:
                if debug:
                    logger.warning(f"[Behavioral] {behavior_key} = False (부정 키워드 '{neg_kw}' 발견)")
                return False

        # 긍정 키워드 체크
        for pos_kw in positive_keywords:
            pos_kw_lower = str(pos_kw).lower()
            pos_kw_compact = pos_kw_lower.replace(" ", "")
            if pos_kw_lower in answer_text or pos_kw_compact in answer_compact:
                if debug:
                    logger.warning(f"[Behavioral] {behavior_key} = True (긍정 키워드 '{pos_kw}' 발견)")
                return True

    if debug and matched_questions:
        logger.warning(f"[Behavioral] {behavior_key} 관련 질문 {len(matched_questions)}개 발견했으나 매칭 실패")

    return None


def validate_llm_extraction(
    query: str,
    conditions: Dict[str, Union[bool, str]]
) -> Dict[str, Union[bool, str]]:
    """LLM 추출 결과 검증 (환각 제거! 🚨)

    전략:
    1. Categorical (문자열): 값 키워드 확인 (엄격!)
    2. Boolean: 도메인 키워드만 확인 (느슨!)

    Args:
        query: 검색 쿼리
        conditions: LLM이 추출한 behavioral 조건

    Returns:
        검증 통과한 조건만 포함
    """
    validated = {}
    query_lower = query.lower()

    # ⭐ 환각 의심: 짧은 쿼리에 너무 많은 패턴
    if len(query) < 20 and len(conditions) > 3:
        logger.warning(
            f"🚨 환각 의심: 짧은 쿼리({len(query)}자)에 "
            f"너무 많은 패턴({len(conditions)}개) - 전체 제거"
        )
        return {}

    for behavior_key, value in conditions.items():
        keyword_config = BEHAVIORAL_KEYWORD_MAP.get(behavior_key, {})

        if not keyword_config:
            logger.warning(f"⚠️ 검증 실패: {behavior_key} (정의되지 않은 패턴)")
            continue

        answer_values = keyword_config.get('answer_values')

        # ========================================
        # ⭐ Categorical: Value 키워드 확인 (엄격!)
        # ========================================
        if answer_values and isinstance(value, str):
            value_keywords = answer_values.get(value, [])

            has_value_keyword = any(
                kw.lower() in query_lower
                for kw in value_keywords
            )

            if has_value_keyword:
                validated[behavior_key] = value
                logger.debug(f"  ✅ Categorical 통과: {behavior_key}='{value}'")
            else:
                logger.warning(
                    f"  ⚠️ Categorical 제거: {behavior_key}='{value}' "
                    f"(값 키워드 없음: {value_keywords})"
                )

        # ========================================
        # ⭐ Boolean: 도메인 키워드만 확인 (느슨!)
        # ========================================
        elif isinstance(value, bool):
            # ⭐⭐⭐ 중요: answer_values가 있는 패턴은 2단계 분류를 거칠 것이므로 검증 완화!
            if answer_values:
                # String 패턴인데 Boolean 값으로 온 경우 (1단계 → 2단계 분류 대기)
                # 2단계에서 정확하게 분류할 것이므로 일단 통과
                validated[behavior_key] = value
                logger.info(f"  ✅ String 패턴 (2단계 대기): {behavior_key}={value} → 검증 통과 (2단계 분류 예정)")
            else:
                # 순수 Boolean 패턴: question_keywords로 도메인만 확인!
                domain_keywords = keyword_config.get('question_keywords', set())

                # 도메인 키워드가 쿼리에 있는지 확인
                # 예: "ott", "스트리밍" 같은 도메인 단어
                has_domain_keyword = any(
                    kw.lower() in query_lower
                    for kw in domain_keywords
                )

                if has_domain_keyword:
                    validated[behavior_key] = value
                    logger.debug(f"  ✅ Boolean 통과: {behavior_key}={value}")
                else:
                    logger.warning(
                        f"  ⚠️ Boolean 제거: {behavior_key}={value} "
                        f"(도메인 키워드 없음: {list(domain_keywords)[:3]}...)"
                    )

    if len(validated) < len(conditions):
        removed = set(conditions.keys()) - set(validated.keys())
        logger.info(f"🔍 검증 완료: {len(removed)}개 제거 - {removed}")

    return validated


def filter_redundant_patterns(
    conditions: Dict[str, Union[bool, str]]
) -> Dict[str, Union[bool, str]]:
    """중복 패턴 제거 (구체적 > 일반적)

    Args:
        conditions: LLM이 추출한 behavioral 조건

    Returns:
        중복 제거된 조건
    """
    filtered = conditions.copy()

    for specific, generics in PATTERN_HIERARCHY.items():
        if specific in filtered:
            for generic in generics:
                if generic in filtered:
                    logger.info(f"🔧 중복 제거: {specific} 우선, {generic} 제거")
                    del filtered[generic]

    return filtered


def classify_answer_value_with_llm(
    query: str,
    behavior_key: str,
    question_text: str,
    answer_values: Dict[str, List[str]],
    anthropic_client
) -> Optional[List[str]]:
    """특정 behavior_key에 대해 사용자 쿼리가 어떤 answer_value에 해당하는지 LLM으로 분류 (다중 선택 지원!)

    이 함수는 2단계 분류의 핵심입니다:
    1단계: extract_behavioral_conditions_llm에서 어떤 behavior_key가 관련있는지 추출
    2단계: (이 함수) 각 behavior_key의 정확한 answer_value 분류 (단일 또는 다중 선택)

    Args:
        query: 사용자 검색 쿼리
        behavior_key: 행동 패턴 키 (예: 'has_pet')
        question_text: 질문 텍스트 (예: '반려동물을 키우는 중이시거나 혹은 키워보신 적이 있으신가요?')
        answer_values: 가능한 답변 값들 (예: {"반려동물을 키우는 중이다": [...], "반려동물을 키워본 적이 있다": [...], ...})
        anthropic_client: Anthropic 클라이언트

    Returns:
        분류된 answer_value 리스트 (예: ["반려동물을 키우는 중이다"] 또는 ["3개월에 1회", "6개월에 1회", "1년에 1회"]) 또는 None

    예시 1 (단일 선택):
        query = "지금 냥이 두 마리 모시고 살고 있어"
        → 반환: ["반려동물을 키우는 중이다"]

    예시 2 (다중 선택):
        query = "전통시장을 자주 방문하지 않는 사람"
        → 반환: ["3개월에 1회 이상", "6개월에 1회 이상", "1년에 1회 이상", "전혀 방문하지 않음"]
    """
    if not anthropic_client:
        return None

    # ⭐ 캐시 확인 (동일 쿼리 + behavior_key 조합 재사용)
    cache_key = f"llm_answer_classify:{behavior_key}:{query}"
    if cache_key in llm_query_cache:
        logger.info(f"🔁 LLM 답변 분류 캐시 히트: {behavior_key} - {query}")
        return llm_query_cache[cache_key]

    # 가능한 답변들을 번호로 나열
    answer_options = []
    for idx, answer_key in enumerate(answer_values.keys(), 1):
        answer_options.append(f"{idx}. {answer_key}")

    options_text = "\n".join(answer_options)

    # System prompt (스마트 다중 선택 - 빈도 기준표 포함!)
    system_prompt = f"""당신은 사용자의 입력을 정확하게 분류하는 전문가입니다.

사용자의 입력이 다음 질문에 대한 답변으로 어디에 해당하는지 정확히 분류해주세요.

**질문**: {question_text}

**가능한 답변 옵션**:
{options_text}

**🚨 분류 규칙**:
1. ⭐ **사용자 입력의 의미를 정확히 파악**하세요.
2. ⭐ **사투리, 완곡한 표현, 비유적 표현**도 올바르게 이해하세요.
3. ⭐ **단일 vs 다중 선택 판단**:
   - **정확히 하나만 해당**: "현재 ~인", "지금 ~하는", "~를 사용하는"
     → 1개만 선택
   - **범위/빈도 표현**: "자주 ~하지 않는", "가끔 ~하는", "~이하", "~이상"
     → 해당하는 모든 답변 선택

**🎯 범위 판단 가이드라인** (빈도/정도 표현):

📌 **매우 자주 / 거의 항상 / 아주 자주**
  → 가장 높은 빈도만 (일주일에 1회 이상)

📌 **자주 / 종종 / 꽤 자주**
  → 중간~높은 빈도 (2주에 1회 이상, 한달에 1회 이상)

📌 **가끔 / 때때로 / 이따금**
  → 낮은~중간 빈도 (한달에 1회, 3개월에 1회)

📌 **거의 안 함 / 별로 안 함 / 드물게 / 자주 ~하지 않는**
  → 매우 낮은 빈도 (3개월에 1회, 6개월에 1회, 1년에 1회, 전혀 안 함)

📌 **전혀 안 함 / 한 번도 안 함 / 절대 안 함**
  → "전혀 ~하지 않음" 또는 "0회"만

📌 **N개 이상 / N회 이상**
  → 해당 숫자부터 최대값까지 (예: "2개 이상" = 2개, 3개, 4개 이상)

📌 **N개 이하 / N회 이하**
  → 최소값부터 해당 숫자까지 (예: "2개 이하" = 이용 안함, 1개, 2개)

⚠️ **주의**: 정확한 표현은 1개만 선택!
  - "현재 ~인", "지금 ~하는" → 정확히 1개
  - "~를 사용하는", "~인 사람" → 정확히 1개

**출력 형식** (JSON, 항상 리스트로!):
{{
  "result": ["답변1"]
}}

또는 다중 선택:
{{
  "result": ["답변1", "답변2", "답변3"]
}}

또는 매칭이 없으면:
{{
  "result": null
}}

**예시**:

✅ 단일 선택 (정확한 표현):
- 입력: "지금 냥이 두 마리 모시고 살고 있어"
  → {{"result": ["반려동물을 키우는 중이다"]}}
  (이유: "현재" 시점 명확 → 1개만)

- 입력: "OTT 2개 쓰는 사람"
  → {{"result": ["2개"]}}
  (이유: 정확한 숫자 → 1개만)

✅ 다중 선택 (범위 표현):
- 입력: "전통시장을 자주 방문하지 않는 사람"
  → {{"result": ["3개월에 1회 이상", "6개월에 1회 이상", "1년에 1회 이상", "전혀 방문하지 않음"]}}
  (이유: "자주 ~하지 않는" = 거의 안 함 = 매우 낮은 빈도)

- 입력: "전통시장을 거의 안 가는 사람"
  → {{"result": ["3개월에 1회 이상", "6개월에 1회 이상", "1년에 1회 이상", "전혀 방문하지 않음"]}}
  (이유: "거의 안 함" = 매우 낮은 빈도, 동일한 범위)

- 입력: "OTT 2개 이상 쓰는 사람"
  → {{"result": ["2개", "3개", "4개 이상"]}}
  (이유: "이상" = 범위 표현)

- 입력: "가끔 운동하는 사람"
  → {{"result": ["요가/필라테스", "달리기/걷기", ...]}}
  (이유: "가끔" = 낮은~중간 빈도, 운동 종류는 여러 개 가능)"""

    user_prompt = f'사용자 입력: "{query}"'

    # ⭐ LLM 호출
    try:
        response = anthropic_client.messages.create(
            model="claude-3-5-haiku-20241022",
            max_tokens=300,
            temperature=0,
            system=[
                {
                    "type": "text",
                    "text": system_prompt,
                    "cache_control": {"type": "ephemeral"}
                }
            ],
            messages=[
                {"role": "user", "content": user_prompt}
            ]
        )

        response_text = response.content[0].text.strip()

        # ⭐ JSON 파싱
        if response_text.startswith("```"):
            response_text = response_text.strip("`")
            if response_text.startswith("json"):
                response_text = response_text[4:]
            response_text = response_text.strip()

        # JSON 추출
        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
        if json_match:
            response_text = json_match.group(0)

        result = json.loads(response_text)
        classified_answers = result.get("result")  # List[str] or None

        # 캐시 저장
        llm_query_cache[cache_key] = classified_answers

        # ⭐ 검증: 리스트 형태인지 확인
        if classified_answers is not None:
            if not isinstance(classified_answers, list):
                logger.warning(f"⚠️ LLM이 리스트가 아닌 값 반환: {classified_answers} → 리스트로 변환")
                classified_answers = [classified_answers]  # 단일 값을 리스트로 변환

            if len(classified_answers) == 1:
                logger.info(f"✅ LLM 답변 분류 (단일): {behavior_key} = {classified_answers[0]}")
            else:
                logger.info(f"✅ LLM 답변 분류 (다중): {behavior_key} = {classified_answers} ({len(classified_answers)}개)")

            return classified_answers
        else:
            logger.info(f"⚠️ LLM 답변 분류 실패: {behavior_key} (매칭 없음)")
            return None

    except json.JSONDecodeError as e:
        logger.error(f"❌ JSON 파싱 실패 (답변 분류): {e}\n응답: {response_text}")
        return None
    except Exception as e:
        logger.error(f"❌ LLM 답변 분류 실패: {e}")
        return None


def extract_behavioral_conditions_llm(
    query: str,
    anthropic_client
) -> Dict[str, Union[bool, str]]:
    """LLM을 사용한 행동 조건 추출 (고정확도!)

    장점:
    - 문맥 이해 (표현이 달라도 매칭)
    - 유지보수 쉬움 (프롬프트만 수정)
    - 정확도 높음 (90-95%)

    단점:
    - 비용 ($0.00006/쿼리, 캐싱 적용 시)
    - 속도 (0.3~0.5초, 캐싱 시 0.001초)

    Args:
        query: 검색 쿼리
        anthropic_client: Anthropic 클라이언트

    Returns:
        behavioral 조건 딕셔너리
    """
    if not anthropic_client:
        return {}

    # ⭐ 캐시 확인 (동일 쿼리 재사용)
    cache_key = f"llm_behavioral:{query}"
    if cache_key in llm_query_cache:
        logger.info(f"🔁 LLM 추출 캐시 히트: {query}")
        return llm_query_cache[cache_key]

    # ⭐ 프롬프트 생성 (Boolean 패턴 포함!)
    pattern_descriptions = []

    for idx, (behavior_key, keyword_config) in enumerate(BEHAVIORAL_KEYWORD_MAP.items(), 1):
        question_text = keyword_config.get('question_text', behavior_key)
        answer_values = keyword_config.get('answer_values')

        if answer_values:
            # Categorical 패턴
            values_str = ", ".join(answer_values.keys())
            pattern_descriptions.append(
                f"{idx}. {behavior_key} (질문: {question_text})\n   가능한 답변: {values_str}"
            )
        else:
            # ⭐ Boolean 패턴 (추가!)
            pattern_descriptions.append(
                f"{idx}. {behavior_key} (질문: {question_text})\n   가능한 답변: true/false"
            )

    patterns_text = "\n\n".join(pattern_descriptions)

    # System prompt (1단계: behavior_key만 추출!)
    system_prompt = f"""당신은 사용자의 검색 쿼리에서 관련된 행동 패턴 질문(behavior_key)을 찾는 전문가입니다.

⭐ **중요**: 당신의 역할은 "어떤 질문"에 관련된 쿼리인지만 찾는 것입니다.
⭐ **구체적인 답변 값은 추측하지 마세요!** (다음 단계에서 처리됩니다)

다음은 가능한 모든 행동 패턴 질문 목록입니다:

{patterns_text}

**🚨 1단계 규칙**:
1. ⭐ **쿼리가 어떤 질문과 관련있는지만** 판단하세요.
2. ⭐ **구체적인 답변 값(예: "유럽", "ChatGPT")은 출력하지 마세요!**
3. ⭐ **쿼리에 명시적으로 언급된 질문만** 추출하세요.
4. ⭐ **절대로 추측하거나 추론하지 마세요.**
5. 애매하거나 불확실한 것은 절대 포함하지 마세요.

**학습 예시** (반드시 따라야 함):

✅ 올바른 예시 (질문만 추출!):
- 쿼리: "유럽 여행 가는 사람" → {{"travels": true}}
  (travels 질문과 관련있음. "유럽"은 다음 단계에서!)
- 쿼리: "현재 고양이 집사인 사람" → {{"has_pet": true}}
  (has_pet 질문과 관련있음. 구체적 답변은 다음 단계에서!)
- 쿼리: "ChatGPT 쓰는 30대" → {{"ai_user": true}}
  (ai_user 질문과 관련있음. "ChatGPT"는 다음 단계에서!)
- 쿼리: "흡연자이면서 운동하는" → {{"smoker": true, "exercises": true}}
  (두 질문 모두 관련있음)

❌ 잘못된 예시 (절대 하지 말 것):
- 쿼리: "20대 남성" → {{}}
  (행동 패턴 질문 없음! 나이/성별은 Demographics)
- 쿼리: "직장인" → {{}}
  (행동 패턴 질문 없음!)

⚠️ 환각 예시 (절대 금지):
- 쿼리: "20대" → {{"ai_user": true}}  ← 절대 안됨!
  이유: AI 사용에 대한 언급이 쿼리에 없음
- 쿼리: "유럽 여행" → {{"travels": "유럽"}}  ← 절대 안됨!
  이유: 구체적 답변 값을 출력하면 안됨! {{"travels": true}}만!

**출력 형식** (behavior_key만, 모두 true로!):
{{
  "behavior_key1": true,
  "behavior_key2": true
}}

매칭되는 질문이 없으면 반드시: {{}}"""

    user_prompt = f'검색 쿼리: "{query}"'

    # ⭐ LLM 호출
    try:
        response = anthropic_client.messages.create(
            model="claude-3-5-haiku-20241022",
            max_tokens=500,
            temperature=0,
            system=[
                {
                    "type": "text",
                    "text": system_prompt,
                    "cache_control": {"type": "ephemeral"}
                }
            ],
            messages=[
                {"role": "user", "content": user_prompt}
            ]
        )

        response_text = response.content[0].text.strip()

        # ⭐ JSON 파싱 (더 안전하게!)
        # ``` 블록 제거
        if response_text.startswith("```"):
            response_text = response_text.strip("`")
            if response_text.startswith("json"):
                response_text = response_text[4:]
            response_text = response_text.strip()

        # JSON 추출 (정규식으로 더 안전하게)
        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
        if json_match:
            response_text = json_match.group(0)

        conditions = json.loads(response_text)

        # ⭐⭐⭐ 1단계: 검증 (환각 제거!)
        conditions = validate_llm_extraction(query, conditions)

        # ⭐⭐⭐ 2단계: 중복 패턴 제거
        conditions = filter_redundant_patterns(conditions)

        # ⭐⭐⭐ 3단계: String 패턴에 대해 LLM으로 정확한 answer_value 분류 (2단계 분류!)
        logger.info(f"🔍 2단계 분류 시작: {len(conditions)}개 패턴 - {list(conditions.keys())}")
        refined_conditions = {}

        for behavior_key, value in conditions.items():
            logger.info(f"  🔎 2단계 분류 검토: {behavior_key} = {value} (타입: {type(value).__name__})")

            keyword_config = BEHAVIORAL_KEYWORD_MAP.get(behavior_key)
            if not keyword_config:
                logger.warning(f"    ⚠️ BEHAVIORAL_KEYWORD_MAP에 없는 키: {behavior_key}")
                refined_conditions[behavior_key] = value
                continue

            answer_values = keyword_config.get('answer_values')

            # ⭐ answer_values가 없는 경우: Boolean 패턴 → 그대로 유지 (True)
            if not answer_values:
                logger.info(f"    ✓ Boolean 패턴 (answer_values 없음): {behavior_key} = True → 그대로 유지")
                refined_conditions[behavior_key] = True  # 1단계에서 true로 왔으므로
                continue

            # ⭐ answer_values가 있는 경우: String 패턴 → 무조건 2단계 LLM 분류 호출!
            logger.info(f"    🎯 String 패턴 감지 (answer_values 있음): {behavior_key}")
            logger.info(f"       가능한 답변: {list(answer_values.keys())}")

            question_text = keyword_config.get('question_text', behavior_key)
            classified_answers = classify_answer_value_with_llm(  # ⭐ List[str] or None
                query=query,
                behavior_key=behavior_key,
                question_text=question_text,
                answer_values=answer_values,
                anthropic_client=anthropic_client
            )

            if classified_answers:  # List[str]
                # ⭐ 단일 vs 다중 선택 처리
                if len(classified_answers) == 1:
                    # 단일 선택: string으로 저장
                    refined_conditions[behavior_key] = classified_answers[0]
                    logger.info(f"    ✅ 2단계 분류 성공 (단일): {behavior_key} = {classified_answers[0]}")
                else:
                    # 다중 선택: list로 저장
                    refined_conditions[behavior_key] = classified_answers
                    logger.info(f"    ✅ 2단계 분류 성공 (다중): {behavior_key} = {classified_answers} ({len(classified_answers)}개)")
            else:
                # 2단계 분류 실패 시: 제거 (True만으로는 필터링 불가)
                logger.warning(f"    ❌ 2단계 분류 실패 → 제거: {behavior_key} (String 패턴은 구체적 값 필요)")
                # refined_conditions에 추가하지 않음 (제거)

        conditions = refined_conditions
        logger.info(f"🔍 2단계 분류 완료: {len(conditions)}개 패턴 - {conditions}")

        # 캐시 저장
        llm_query_cache[cache_key] = conditions

        logger.info(f"✅ LLM 추출 (검증 + 2단계 분류 완료): {len(conditions)}개 패턴 - {conditions}")

        return conditions

    except json.JSONDecodeError as e:
        logger.error(f"❌ JSON 파싱 실패: {e}\n응답: {response_text}")
        return {}
    except Exception as e:
        logger.error(f"❌ LLM 추출 실패: {e}")
        return {}


def extract_behavioral_conditions_from_query(query: str) -> Dict[str, Union[bool, str]]:
    """쿼리 텍스트에서 behavioral 조건 자동 추출 (키워드 기반 - Fallback용)

    ⭐ BEHAVIORAL_KEYWORD_MAP을 자동으로 순회하여 모든 패턴 감지!
    새로운 패턴 추가 시 BEHAVIORAL_KEYWORD_MAP에만 추가하면 됨!

    Args:
        query: 검색 쿼리

    Returns:
        behavioral 조건 딕셔너리
        - bool: {"drinker": True, "smoker": False, ...}
        - str: {"winter_vacation_memory": "친구들과 보낸 즐거운 시간"}
    """
    query_lower = query.lower()
    query_normalized = query_lower.replace(" ", "")
    conditions = {}

    # ⭐ BEHAVIORAL_KEYWORD_MAP의 모든 키를 자동으로 순회
    for behavior_key, keyword_config in BEHAVIORAL_KEYWORD_MAP.items():
        question_keywords = keyword_config.get('question_keywords', set())
        answer_values = keyword_config.get('answer_values')

        # 질문 키워드가 쿼리에 있는지 확인
        has_question_keyword = any(
            kw.lower().replace(" ", "") in query_normalized
            for kw in question_keywords
        )

        # ⭐ 문자열 값 패턴 (answer_values가 있는 경우)
        if answer_values:
            # 질문 키워드 없어도 답변 키워드로 매칭 시도
            for value_name, value_keywords in answer_values.items():
                if any(kw.lower().replace(" ", "") in query_normalized for kw in value_keywords):
                    conditions[behavior_key] = value_name
                    break

        # ⭐ Boolean 패턴 (positive/negative keywords가 있는 경우)
        else:
            positive_keywords = keyword_config.get('positive_keywords', set())
            negative_keywords = keyword_config.get('negative_keywords', set())

            # 부정 키워드 체크 (우선순위 높음)
            has_negative = any(
                kw.lower().replace(" ", "") in query_normalized
                for kw in negative_keywords
            )

            # 긍정 키워드 체크
            has_positive = any(
                kw.lower().replace(" ", "") in query_normalized
                for kw in positive_keywords
            )

            if has_negative:
                conditions[behavior_key] = False
            elif has_positive:
                conditions[behavior_key] = True

    return conditions


def build_behavioral_filters(behavioral_conditions: Dict[str, Union[bool, str, List[str]]]) -> List[Dict[str, Any]]:
    """behavioral_conditions를 OpenSearch nested 필터로 변환 (동적 처리)

    ⭐ BEHAVIORAL_KEYWORD_MAP을 사용해서 모든 조건을 자동으로 처리합니다.

    Args:
        behavioral_conditions:
            - bool: {"smoker": True, "has_vehicle": False}
            - str: {"winter_vacation_memory": "친구들과 보낸 즐거운 시간"}

    Returns:
        OpenSearch nested 쿼리 리스트

    Example:
        {"uses_smart_devices": True} →
        {
            "nested": {
                "path": "qa_pairs",
                "query": {
                    "bool": {
                        "must": [
                            {"bool": {"should": [질문 매칭]}},
                            {"bool": {"should": [긍정 답변], "must_not": [부정 답변]}}
                        ]
                    }
                }
            }
        }
    """
    filters = []

    for key, value in behavioral_conditions.items():
        if value is None:
            continue

        # ⭐ BEHAVIORAL_KEYWORD_MAP에서 키워드 설정 가져오기
        if key not in BEHAVIORAL_KEYWORD_MAP:
            logger.warning(f"⚠️ Behavioral condition '{key}' not found in BEHAVIORAL_KEYWORD_MAP, skipping")
            continue

        keyword_config = BEHAVIORAL_KEYWORD_MAP[key]
        question_keywords = keyword_config['question_keywords']

        # 질문 매칭 쿼리 생성
        question_should = [
            {"match": {"qa_pairs.q_text": q}}
            for q in question_keywords
        ]

        # ⭐ 다중 값 (리스트) 처리: OR 조건
        if isinstance(value, list):
            # 리스트 값: 여러 답변 중 하나라도 매칭 (OR 조건)
            answer_should = []
            answer_values = keyword_config.get('answer_values', {})

            for v in value:
                # 정확한 값 매칭
                answer_should.append({"match_phrase": {"qa_pairs.answer": v}})

                # 답변 값 매핑에서 키워드 가져오기
                if v in answer_values:
                    for kw in answer_values[v]:
                        answer_should.append({"match": {"qa_pairs.answer": kw}})

            logger.info(f"  🔍 다중 값 필터 생성: {key} IN {value} → {len(answer_should)}개 조건")

        # ⭐ 단일 문자열 값 처리
        elif isinstance(value, str):
            # 문자열 값: answer에서 정확한 값 매칭
            answer_should = [
                {"match_phrase": {"qa_pairs.answer": value}}
            ]

            # 답변 값 매핑에서 키워드 가져오기
            answer_values = keyword_config.get('answer_values', {})
            if value in answer_values:
                for kw in answer_values[value]:
                    answer_should.append({"match": {"qa_pairs.answer": kw}})

        else:
            # Boolean 값: positive/negative keywords 사용
            positive_keywords = keyword_config['positive_keywords']
            negative_keywords = keyword_config['negative_keywords']

            # ⭐ 답변 매칭 쿼리 생성 (positive keywords만 사용, negative 무시)
            # 이유: negative keywords가 너무 일반적 (예: "해당 없음", "보유하지 않음")
            if value:  # True: positive keywords만 찾기
                answer_should = [
                    {"match": {"qa_pairs.answer": kw}}
                    for kw in positive_keywords
                ]
            else:  # False: negative keywords만 찾기
                answer_should = [
                    {"match": {"qa_pairs.answer": kw}}
                    for kw in negative_keywords
                ]

        # OpenSearch nested 필터 생성 (must_not 제거)
        filters.append({
            "nested": {
                "path": "qa_pairs",
                "query": {
                    "bool": {
                        "must": [
                            {
                                "bool": {
                                    "should": question_should,
                                    "minimum_should_match": 1
                                }
                            },
                            {
                                "bool": {
                                    "should": answer_should,
                                    "minimum_should_match": 1
                                }
                            }
                        ]
                    }
                }
            }
        })

    return filters


# ⭐ 아래는 legacy 하드코딩된 조건들 (참고용으로 주석 처리)
# 이제 위의 동적 처리 로직이 모든 조건을 자동으로 처리합니다.
"""
def build_behavioral_filters_OLD_HARDCODED(behavioral_conditions: Dict[str, bool]) -> List[Dict[str, Any]]:
    filters = []

    for key, value in behavioral_conditions.items():
        if value is None:
            continue

        if key == "smoker":
            # 흡연 필터
            question_should = [
                {"match": {"qa_pairs.q_text": q}}
                for q in SMOKER_QUESTION_KEYWORDS
            ]

            if value:  # 흡연자
                answer_should = [
                    {"match": {"qa_pairs.answer": kw}}  # ✅ Changed to match (answer is text type)
                    for kw in SMOKER_POSITIVE_KEYWORDS
                ]
                answer_must_not = [
                    {"match": {"qa_pairs.answer": kw}}  # ✅ Changed to match (answer is text type)
                    for kw in SMOKER_NEGATIVE_KEYWORDS
                ]
            else:  # 비흡연자
                answer_should = [
                    {"match": {"qa_pairs.answer": kw}}  # ✅ Changed to match (answer is text type)
                    for kw in SMOKER_NEGATIVE_KEYWORDS
                ]
                answer_must_not = [
                    {"match": {"qa_pairs.answer": kw}}  # ✅ Changed to match (answer is text type)
                    for kw in SMOKER_POSITIVE_KEYWORDS
                ]

            filters.append({
                "nested": {
                    "path": "qa_pairs",
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "bool": {
                                        "should": question_should,
                                        "minimum_should_match": 1
                                    }
                                },
                                {
                                    "bool": {
                                        "should": answer_should,
                                        "must_not": answer_must_not,
                                        "minimum_should_match": 1
                                    }
                                }
                            ]
                        }
                    }
                }
            })

        elif key == "has_vehicle":
            # 차량 보유 필터
            question_should = [
                {"match": {"qa_pairs.q_text": q}}
                for q in VEHICLE_QUESTION_KEYWORDS
            ]

            if value:  # 차량 있음
                answer_should = [
                    {"match": {"qa_pairs.answer": kw}}  # ✅ Changed to match (answer is text type)
                    for kw in BEHAVIOR_YES_TOKENS
                ]
                answer_must_not = [
                    {"match": {"qa_pairs.answer": kw}}  # ✅ Changed to match (answer is text type)
                    for kw in BEHAVIOR_NO_TOKENS
                ]
            else:  # 차량 없음
                answer_should = [
                    {"match": {"qa_pairs.answer": kw}}  # ✅ Changed to match (answer is text type)
                    for kw in BEHAVIOR_NO_TOKENS
                ]
                answer_must_not = [
                    {"match": {"qa_pairs.answer": kw}}  # ✅ Changed to match (answer is text type)
                    for kw in BEHAVIOR_YES_TOKENS
                ]

            filters.append({
                "nested": {
                    "path": "qa_pairs",
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "bool": {
                                        "should": question_should,
                                        "minimum_should_match": 1
                                    }
                                },
                                {
                                    "bool": {
                                        "should": answer_should,
                                        "must_not": answer_must_not,
                                        "minimum_should_match": 1
                                    }
                                }
                            ]
                        }
                    }
                }
            })

        elif key == "drinker":
            # ⭐ 음주 여부 필터
            question_should = [
                {"match": {"qa_pairs.q_text": q}}
                for q in ALCOHOL_QUESTION_KEYWORDS
            ]

            if value:  # 음주자
                answer_should = [
                    {"match": {"qa_pairs.answer": kw}}
                    for kw in DRINKER_POSITIVE_KEYWORDS
                ]
                answer_must_not = [
                    {"match": {"qa_pairs.answer": kw}}
                    for kw in NON_DRINKER_KEYWORDS
                ]
            else:  # 비음주자
                answer_should = [
                    {"match": {"qa_pairs.answer": kw}}
                    for kw in NON_DRINKER_KEYWORDS
                ]
                answer_must_not = [
                    {"match": {"qa_pairs.answer": kw}}
                    for kw in DRINKER_POSITIVE_KEYWORDS
                ]

            filters.append({
                "nested": {
                    "path": "qa_pairs",
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "bool": {
                                        "should": question_should,
                                        "minimum_should_match": 1
                                    }
                                },
                                {
                                    "bool": {
                                        "should": answer_should,
                                        "must_not": answer_must_not,
                                        "minimum_should_match": 1
                                    }
                                }
                            ]
                        }
                    }
                }
            })

        elif key == "drinks_beer":
            # 맥주 음용 필터
            if value:
                question_should = [
                    {"match": {"qa_pairs.q_text": q}}
                    for q in ALCOHOL_QUESTION_KEYWORDS
                ]
                answer_should = [
                    {"match": {"qa_pairs.answer": kw}}  # ✅ Changed to match (answer is text type)
                    for kw in BEER_KEYWORDS
                ]

                filters.append({
                    "nested": {
                        "path": "qa_pairs",
                        "query": {
                            "bool": {
                                "must": [
                                    {"bool": {"should": question_should, "minimum_should_match": 1}},
                                    {"bool": {"should": answer_should, "minimum_should_match": 1}}
                                ]
                            }
                        }
                    }
                })

        elif key == "drinks_wine":
            # 와인 음용 필터
            if value:
                question_should = [
                    {"match": {"qa_pairs.q_text": q}}
                    for q in ALCOHOL_QUESTION_KEYWORDS
                ]
                answer_should = [
                    {"match": {"qa_pairs.answer": kw}}  # ✅ Changed to match (answer is text type)
                    for kw in WINE_KEYWORDS
                ]

                filters.append({
                    "nested": {
                        "path": "qa_pairs",
                        "query": {
                            "bool": {
                                "must": [
                                    {"bool": {"should": question_should, "minimum_should_match": 1}},
                                    {"bool": {"should": answer_should, "minimum_should_match": 1}}
                                ]
                            }
                        }
                    }
                })

        elif key == "drinks_soju":
            # 소주 음용 필터
            if value:
                question_should = [
                    {"match": {"qa_pairs.q_text": q}}
                    for q in ALCOHOL_QUESTION_KEYWORDS
                ]
                answer_should = [
                    {"match": {"qa_pairs.answer": kw}}  # ✅ Changed to match (answer is text type)
                    for kw in SOJU_KEYWORDS
                ]

                filters.append({
                    "nested": {
                        "path": "qa_pairs",
                        "query": {
                            "bool": {
                                "must": [
                                    {"bool": {"should": question_should, "minimum_should_match": 1}},
                                    {"bool": {"should": answer_should, "minimum_should_match": 1}}
                                ]
                            }
                        }
                    }
                })

        elif key == "non_drinker":
            # 비음주자 필터
            if value:
                question_should = [
                    {"match": {"qa_pairs.q_text": q}}
                    for q in ALCOHOL_QUESTION_KEYWORDS
                ]
                answer_should = [
                    {"match": {"qa_pairs.answer": kw}}  # ✅ Changed to match (answer is text type)
                    for kw in NON_DRINKER_KEYWORDS
                ]

                filters.append({
                    "nested": {
                        "path": "qa_pairs",
                        "query": {
                            "bool": {
                                "must": [
                                    {"bool": {"should": question_should, "minimum_should_match": 1}},
                                    {"bool": {"should": answer_should, "minimum_should_match": 1}}
                                ]
                            }
                        }
                    }
                })

    return filters
"""


@router.get("/", summary="Search API 상태")
def search_root():
    """Search API 기본 정보"""
    return {
        "message": "Search API 실행 중",
        "version": "1.0",
        "endpoints": [
            "/search/nl"
        ]
    }


class NLSearchRequest(BaseModel):
    """자연어 기반 검색 요청 (필터/size 자동 추출)"""
    query: str = Field(..., description="자연어 쿼리 (예: '30대 사무직 300명 데이터 보여줘')")
    index_name: str = Field(
        default="survey_responses_merged",
        description="검색할 인덱스 이름 (기본값: survey_responses_merged; 와일드카드 사용 가능)"
    )
    size: int = Field(default=30000, ge=1, le=50000, description="반환할 결과 개수 (쿼리에서 추출된 인원 수가 없을 때 사용, 전체 데이터 약 35000개)")
    use_vector_search: bool = Field(default=True, description="벡터 검색 사용 여부")
    page: int = Field(default=1, ge=1, description="요청할 페이지 번호 (1부터 시작)")
    use_claude_analyzer: Optional[bool] = Field(
        default=None,
        description="Claude 분석기 사용 여부 (None이면 서버 설정값을 따름)"
    )
    session_id: Optional[str] = Field(
        default=None,
        description="대화/세션 식별자 (Redis 대화 로그 키)"
    )
    user_id: Optional[str] = Field(
        default=None,
        description="요청 사용자 식별자 (검색 이력 키)"
    )
    request_id: Optional[str] = Field(
        default=None,
        description="요청 추적을 위한 ID"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description="추가 요청 메타데이터"
    )
    log_conversation: bool = Field(
        default=True,
        description="Redis 대화 로그 저장 여부"
    )
    log_search_history: bool = Field(
        default=True,
        description="Redis 검색 이력 저장 여부"
    )
    request_llm_summary: bool = Field(
        default=False,
        description="LLM 요약/분석 생성 요청 여부"
    )
    llm_summary_instructions: Optional[str] = Field(
        default=None,
        description="LLM 요약 시 사용할 추가 지침"
    )


def convert_to_simple_response(
    search_response: SearchResponse,
    behavioral_conditions: Dict[str, Any],
    max_results: int = 100
) -> SimpleResponse:
    """
    SearchResponse를 SimpleResponse로 변환 (프론트엔드 친화적)

    Args:
        search_response: 기존 검색 응답
        behavioral_conditions: 쿼리의 behavioral 조건 {"smoker": True, "has_vehicle": True}
        max_results: 최대 결과 수 (기본 100개)

    Returns:
        SimpleResponse: 간소화된 응답
    """
    try:
        simple_results = []

        for item in search_response.results[:max_results]:
            # Behavioral 조건 매칭 QA 추출
            matched_conditions_data = extract_behavioral_qa_pairs(
                source={'qa_pairs': item.qa_pairs or []},
                behavioral_conditions=behavioral_conditions
            )

            # MatchedCondition 객체로 변환
            matched_objs = [
                MatchedCondition(
                    condition_type=mc['condition_type'],
                    condition_value=mc['condition_value'],
                    question=mc['q_text'],
                    answer=mc['answer'],
                    confidence=mc.get('confidence', 1.0)
                )
                for mc in matched_conditions_data
            ]

            # Demographics 정보 추출
            demo_info = item.demographic_info or {}
            demographics = {
                'gender': demo_info.get('gender', 'N/A'),
                'age_group': demo_info.get('age_group', 'N/A'),
                'birth_year': str(demo_info.get('birth_year', 'N/A'))
            }

            simple_results.append(SimpleResult(
                user_id=item.user_id,
                score=item.score,
                demographics=demographics,
                matched_conditions=matched_objs
            ))

        # Query 분석 정보
        query_analysis = search_response.query_analysis or {}
        query_info = {
            'keywords': [
                *(query_analysis.get('must_terms', [])),
                *(query_analysis.get('should_terms', []))
            ],
            'filters_applied': bool(query_analysis.get('filters')),
            'behavioral_conditions': behavioral_conditions,
            'extracted_entities': query_analysis.get('extracted_entities')
        }

        return SimpleResponse(
            state="SUCCESS",
            message="검색 성공",
            query=search_response.query,
            total_hits=search_response.total_hits,
            results=simple_results,
            query_info=query_info,
            took_ms=search_response.took_ms
        )

    except Exception as e:
        logger.error(f"SimpleResponse 변환 중 에러: {e}", exc_info=True)
        # 에러 시 빈 응답 반환
        return SimpleResponse(
            state="ERROR",
            message=f"응답 변환 실패: {str(e)}",
            query=search_response.query if search_response else "",
            total_hits=0,
            results=[],
            query_info=None,
            took_ms=0
        )


# ============================================
# ⭐ 압축 저장/로드 함수 (Redis 캐시 최적화)
# ============================================

def save_search_cache_compressed(
    cache_key: str,
    cache_ttl: int,
    # ⭐ cache_payload 대신 개별 파라미터로 받기 (백그라운드에서 딕셔너리 생성)
    total_hits: int,
    max_score: float,
    stored_items: List[Dict],
    page_size: int,
    filters_for_response: Dict,
    extracted_entities_dict: Dict,
    behavioral_conditions: Dict,
    use_claude: bool,
    requested_count: int,
):
    """압축해서 Redis 저장 (백그라운드 실행)"""
    try:
        cache_client = getattr(router, "redis_client", None)
        if not cache_client:
            return

        start = perf_counter()

        # ⭐ 백그라운드에서 cache_payload 생성 (메인 스레드 지연 제거!)
        logger.info(f"🔧 [Background] cache_payload 생성 시작 (items: {len(stored_items)}개)")
        payload_start = perf_counter()

        cache_payload = {
            "total_hits": total_hits,
            "max_score": max_score,
            "items": stored_items,
            "page_size": page_size,
            "filters": filters_for_response,
            "extracted_entities": extracted_entities_dict,
            "behavioral_conditions": behavioral_conditions,
            "use_claude": use_claude,
            "requested_count": requested_count,
        }

        payload_duration_ms = (perf_counter() - payload_start) * 1000
        logger.info(f"✅ [Background] cache_payload 생성 완료 ({payload_duration_ms:.2f}ms)")

        # pickle + gzip (compresslevel=6: 속도와 압축률 균형)
        serialized = pickle.dumps(cache_payload, protocol=pickle.HIGHEST_PROTOCOL)
        compressed = gzip.compress(serialized, compresslevel=6)

        cache_client.setex(cache_key, cache_ttl, compressed)

        duration_ms = (perf_counter() - start) * 1000
        original_size_mb = len(serialized) / 1024**2
        compressed_size_mb = len(compressed) / 1024**2
        ratio = (1 - compressed_size_mb / original_size_mb) * 100 if original_size_mb > 0 else 0

        logger.info(
            f"💾 [Background] Redis 압축 저장: "
            f"{compressed_size_mb:.2f}MB (원본: {original_size_mb:.2f}MB, 압축률: {ratio:.1f}%), "
            f"{duration_ms:.2f}ms"
        )

    except Exception as e:
        logger.warning(f"⚠️ [Background] Redis 저장 실패: {e}")


def load_search_cache_compressed(cache_key: str) -> Optional[Dict[str, Any]]:
    """압축된 캐시 로드"""
    try:
        cache_client = getattr(router, "redis_client", None)
        if not cache_client:
            return None

        start = perf_counter()

        compressed = cache_client.get(cache_key)
        if not compressed:
            return None

        serialized = gzip.decompress(compressed)
        payload = pickle.loads(serialized)

        duration_ms = (perf_counter() - start) * 1000
        logger.info(f"🔁 Redis 압축 캐시 히트: {len(compressed)/1024**2:.2f}MB, {duration_ms:.2f}ms")

        return payload

    except Exception as e:
        logger.warning(f"⚠️ Redis 압축 로드 실패: {e}")
        return None


@router.post("/nl", response_model=SearchResponse, summary="자연어 쿼리: 자동 추출+검색")
async def search_natural_language(
    request: NLSearchRequest,
    background_tasks: BackgroundTasks,  # ⭐ 백그라운드 작업 추가
    os_client: OpenSearch = Depends(lambda: router.os_client),
    stream_callback: Optional[Any] = None,  # ⭐ SSE 스트리밍용 콜백 (callable 타입)
):
    """
    자연어 입력에서 인구통계(연령/성별/직업)와 요청 수량을 추출하여
    검색 쿼리와 size에 반영한 뒤 결과를 반환합니다.
    
    Args:
        stream_callback: 선택적 콜백 함수. (event_type, data) 형태로 호출됨.
            - event_type: 'alpha' | 'before_filter' | 'after_filter'
            - data: 이벤트 데이터 딕셔너리
    """
    try:
        logger.info("🟢 /search/nl 요청 시작")

        if not os_client or not os_client.ping():
            raise HTTPException(status_code=503, detail="OpenSearch 서버에 연결할 수 없습니다.")

        _ensure_request_defaults(request)

        config = getattr(router, 'config', None)
        if config is None:
            from rag_query_analyzer.config import get_config
            config = get_config()
            router.config = config

        analyzer = getattr(router, 'analyzer', None)
        if analyzer is None:
            analyzer = AdvancedRAGQueryAnalyzer(config)
            router.analyzer = analyzer
        use_claude = request.use_claude_analyzer if request.use_claude_analyzer is not None else config.ENABLE_CLAUDE_ANALYZER
        analysis = analyzer.analyze_query(request.query, use_claude=use_claude)
        if analysis is None:
            raise RuntimeError("Query analysis returned None")
        query_analysis = analysis
        
        # ⭐ SSE 스트리밍: 알파값 및 쿼리 분석 정보 전달
        if stream_callback:
            try:
                stream_callback('alpha', {'alpha': analysis.alpha})
                
                # Must terms, Should terms 전달
                stream_callback('query_analysis', {
                    'must_terms': analysis.must_terms or [],
                    'should_terms': analysis.should_terms or [],
                })
            except Exception as e:
                logger.warning(f"⚠️ stream_callback 오류 (query_analysis): {e}")

        # ⭐ Claude에서 온 behavioral_conditions도 2단계 분류 거쳐야 함!
        anthropic_client = getattr(router, 'anthropic_client', None)

        # Claude 결과를 2단계 분류 대상으로 수집
        all_behavioral = {}
        if analysis.behavioral_conditions:
            for key, value in analysis.behavioral_conditions.items():
                if value is True:  # True인 것만 2단계 분류 대상
                    all_behavioral[key] = value

        # ⭐ 추가: 1단계 LLM 추출 (키워드 기반, Claude 보완용)
        auto_behavioral = extract_behavioral_conditions_llm(request.query, anthropic_client)

        # Fallback: LLM 실패 시 키워드 기반
        if not auto_behavioral and not anthropic_client:
            auto_behavioral = extract_behavioral_conditions_from_query(request.query)

        # 1단계 LLM 결과 병합
        if auto_behavioral:
            for key, value in auto_behavioral.items():
                if value is not None:
                    all_behavioral[key] = value

        # ⭐⭐⭐ 중요: all_behavioral (Claude + 1단계 LLM)을 2단계 분류에 통과!
        if all_behavioral:
            # 2단계 분류: String 패턴 → 구체적 answer_value
            refined_behavioral = {}

            logger.info(f"🔍 [통합] 2단계 분류 시작: {len(all_behavioral)}개 패턴 - {list(all_behavioral.keys())}")

            for behavior_key, value in all_behavioral.items():
                # Boolean → String 변환 필요한지 확인
                if behavior_key in BEHAVIORAL_KEYWORD_MAP:
                    pattern_info = BEHAVIORAL_KEYWORD_MAP[behavior_key]
                    answer_values = pattern_info.get('answer_values', {})

                    if answer_values and value is True:
                        # 2단계 LLM 분류 필요!
                        question_text = pattern_info.get('question_text', '')
                        classified_answers = classify_answer_value_with_llm(
                            query=request.query,
                            behavior_key=behavior_key,
                            question_text=question_text,
                            answer_values=answer_values,
                            anthropic_client=anthropic_client
                        )

                        if classified_answers:
                            if len(classified_answers) == 1:
                                refined_behavioral[behavior_key] = classified_answers[0]
                            else:
                                refined_behavioral[behavior_key] = classified_answers
                            logger.info(f"   ✅ 2단계 분류 성공: {behavior_key} = {classified_answers}")
                        else:
                            logger.warning(f"   ❌ 2단계 분류 실패 → 제거: {behavior_key}")
                    else:
                        # Boolean 패턴 (2단계 불필요)
                        refined_behavioral[behavior_key] = value
                else:
                    refined_behavioral[behavior_key] = value

            # 최종 결과로 덮어쓰기
            analysis.behavioral_conditions = refined_behavioral
            logger.info(f"✅ 자동 추출된 behavioral 조건: {refined_behavioral}")
        
        # ⭐ SSE 스트리밍: Behavioral conditions (True인 것만) 전달 (LLM 추출 후)
        if stream_callback:
            try:
                behavioral_true = {}
                if analysis.behavioral_conditions:
                    for key, value in analysis.behavioral_conditions.items():
                        if value is True:
                            behavioral_true[key] = value
                if behavioral_true:
                    stream_callback('behavioral_conditions', {'behavioral_conditions': behavioral_true})
            except Exception as e:
                logger.warning(f"⚠️ stream_callback 오류 (behavioral_conditions): {e}")

        embedding_model = getattr(router, 'embedding_model', None)
        if embedding_model is None and hasattr(router, 'embedding_model_factory'):
            embedding_model = router.embedding_model_factory()
            router.embedding_model = embedding_model
        data_fetcher = DataFetcher(
            opensearch_client=os_client,
            qdrant_client=getattr(router, 'qdrant_client', None),
            async_opensearch_client=getattr(router, 'async_os_client', None)
        )

        timings: Dict[str, float] = {}
        overall_start = perf_counter()

        # 1) 추출: filters + size
        extractor = DemographicExtractor()
       
        extracted_entities, requested_size = extractor.extract_with_size(
            request.query, 
            default_size=getattr(request, "size", 30000),  
            max_size=60000
        )

        # ⭐ Claude의 demographics를 extracted_entities에 병합
        if hasattr(analysis, 'demographic_entities') and analysis.demographic_entities:
            logger.warning(f"[MERGE] Claude demographics: {len(analysis.demographic_entities)}개")
            logger.warning(f"[MERGE] DemographicExtractor demographics: {len(extracted_entities.demographics)}개")

            # Claude의 demographics를 우선 사용 (더 정확함)
            extracted_entities.demographics = list(analysis.demographic_entities)

            logger.warning(f"[MERGE] 병합 후: {len(extracted_entities.demographics)}개")
            for demo in extracted_entities.demographics:
                logger.warning(f"  - {demo.demographic_type.value}: {demo.value}")
        
        # ⭐ SSE 스트리밍: Demographics 전달 (추출 후)
        if stream_callback:
            try:
                demographics_list = [d.raw_value for d in extracted_entities.demographics] if extracted_entities.demographics else []
                if demographics_list:
                    stream_callback('demographics', {'demographics': demographics_list})
            except Exception as e:
                logger.warning(f"⚠️ stream_callback 오류 (demographics): {e}")

        filters: List[Dict[str, Any]] = []

        # Demographics 필터
        for demo in extracted_entities.demographics:
            # ⭐ 인덱스별 필터 전략:
            # - survey_responses_merged: metadata와 qa_pairs 모두 있음
            # - survey_responses_merged: 일부는 metadata, 일부는 qa_pairs에만 있음 → qa_fallback 활성화
            # 예: region은 metadata에, marital_status는 qa_pairs에만 있을 수 있음
            is_survey_merged = request.index_name in ["survey_responses_merged", "s_survey*"]
            metadata_only = not is_survey_merged  # survey_responses_merged는 False
            include_nested_fallback = is_survey_merged  # survey_responses_merged는 True (qa_pairs도 검색)
            filter_clause = demo.to_opensearch_filter(
                metadata_only=metadata_only,
                include_qa_fallback=include_nested_fallback,
            )
            if filter_clause and filter_clause != {"match_all": {}}:
                filters.append(filter_clause)

        # ⭐ 개선: behavioral_conditions를 OpenSearch 필터로 변환
        if analysis.behavioral_conditions:
            behavioral_filters = build_behavioral_filters(analysis.behavioral_conditions)
            filters.extend(behavioral_filters)
            logger.info(f"✅ Behavioral 필터 추가: {analysis.behavioral_conditions} → {len(behavioral_filters)}개 필터")

        # ⭐ inner_hits 제거 (중복 key 에러 방지)
        def remove_inner_hits_from_filter(query_dict):
            """재귀적으로 inner_hits 제거"""
            import copy
            cleaned = copy.deepcopy(query_dict)

            if isinstance(cleaned, dict):
                if 'nested' in cleaned:
                    if 'inner_hits' in cleaned['nested']:
                        del cleaned['nested']['inner_hits']
                    if 'query' in cleaned['nested']:
                        cleaned['nested']['query'] = remove_inner_hits_from_filter(cleaned['nested']['query'])

                if 'bool' in cleaned:
                    for key in ['must', 'should', 'must_not', 'filter']:
                        if key in cleaned['bool']:
                            if isinstance(cleaned['bool'][key], list):
                                cleaned['bool'][key] = [remove_inner_hits_from_filter(item) for item in cleaned['bool'][key]]
                            else:
                                cleaned['bool'][key] = remove_inner_hits_from_filter(cleaned['bool'][key])

            return cleaned

        # filters에서 inner_hits 제거
        logger.info(f"🔍 필터 상태 before inner_hits removal: filters={len(filters) if filters else 0}개")
        if filters:
            filters = [remove_inner_hits_from_filter(f) for f in filters]
            logger.info(f"✅ inner_hits 제거 완료: {len(filters)}개 필터")
        else:
            logger.warning(f"⚠️ filters가 비어있음!")

        filters_for_response = list(filters)
        filters_signature = _normalize_filters_for_cache(filters_for_response)

        # ⭐ page_size 결정: 
        # 1. 쿼리에서 명시적으로 인원 수를 추출한 경우 (예: "300명") → 추출된 값 사용
        # 2. 쿼리에서 인원 수를 추출하지 못한 경우 → request.size 사용 (기본값 30000)
        # 
        # requested_size가 request.size와 같으면 쿼리에서 추출하지 못한 것으로 간주
        request_size = getattr(request, "size", 30000)
        if requested_size is not None and requested_size > 0:
            # 쿼리에서 명시적으로 추출한 경우 (request.size와 다름)
            if requested_size != request_size:
                page_size = max(1, min(requested_size, 50000))
            else:
                # requested_size가 request.size와 같으면 쿼리에서 추출하지 못한 것
                # request.size 사용
                page_size = max(1, min(request_size, 50000))
        else:
            # 쿼리에서 인원 수를 추출하지 못한 경우, request.size 사용 (기본값 10)
            page_size = max(1, min(request_size, 50000))
        page = max(1, request.page)
        requested_window = page_size * page
        cache_client = getattr(router, "redis_client", None)
        cache_ttl = getattr(router, "cache_ttl_seconds", 0)
        cache_limit = getattr(router, "cache_max_results", requested_window)
        cache_prefix = getattr(router, "cache_prefix", "search:results")
        cache_enabled = bool(cache_client) and cache_ttl > 0
        # ⭐ window_size: 내부 검색/필터링용 (충분한 후보 확보)
        # page_size: 사용자에게 반환할 최종 결과 개수
        min_window_size = 10000  # 전체 데이터 약 35000개를 고려하여 증가 (내부 처리용)
        window_size = max(requested_window, min_window_size)
        if cache_limit and cache_limit > 0:
            window_size = min(window_size, cache_limit)
        size = window_size  # 내부 검색 크기 (응답 크기와 분리)
        cache_key = None
        cache_hit = False

        if cache_enabled:
            try:
                behavior_signature = ""
                if getattr(analysis, "behavioral_conditions", None):
                    try:
                        behavior_signature = json.dumps(analysis.behavioral_conditions, ensure_ascii=False, sort_keys=True)
                    except Exception:
                        behavior_signature = str(analysis.behavioral_conditions)

                cache_key = _make_cache_key(
                    prefix=cache_prefix,
                    query=request.query,
                    index_name=request.index_name,
                    page_size=page_size,
                    use_vector=request.use_vector_search,
                    use_claude=use_claude,
                    must_terms=analysis.must_terms or [],
                    should_terms=analysis.should_terms or [],
                    must_not_terms=getattr(analysis, "must_not_terms", []) or [],
                    filters_signature=filters_signature,
                    behavior_signature=behavior_signature,
                )

                # ========================================
                # ⭐ 1차: 메모리 캐시 조회 (0.001초)
                # ========================================
                if cache_key in memory_cache:
                    cache_payload = memory_cache[cache_key]
                    cache_hit = True
                    logger.info(f"🔁 메모리 캐시 히트: key={cache_key[:50]}...")

                    extracted_entities_dict = cache_payload.get("extracted_entities")
                    if extracted_entities_dict is None:
                        extracted_entities_dict = extracted_entities.to_dict()

                    cached_response = _build_cached_response(
                        payload=cache_payload,
                        request=request,
                        analysis=analysis,
                        filters_for_response=filters_for_response,
                        overall_start=overall_start,
                        extracted_entities_dict=extracted_entities_dict,
                    )
                    return _finalize_search_response(
                        request=request,
                        response=cached_response,
                        analysis=analysis,
                        cache_hit=True,
                        timings=cached_response.query_analysis.get("timings_ms") if cached_response.query_analysis else None,
                    )

                # ========================================
                # ⭐ 2차: Redis 캐시 조회 (0.02초)
                # ========================================
                cache_payload = load_search_cache_compressed(cache_key)
                if cache_payload:
                    cache_hit = True
                    logger.info(f"🔁 Redis 캐시 히트: key={cache_key[:50]}...")

                    # ⭐ Redis → 메모리 캐시로 승격
                    memory_cache[cache_key] = cache_payload
                    logger.info(f"  ✅ Redis → 메모리 캐시 승격 완료")

                    extracted_entities_dict = cache_payload.get("extracted_entities")
                    if extracted_entities_dict is None:
                        extracted_entities_dict = extracted_entities.to_dict()

                    cached_response = _build_cached_response(
                        payload=cache_payload,
                        request=request,
                        analysis=analysis,
                        filters_for_response=filters_for_response,
                        overall_start=overall_start,
                        extracted_entities_dict=extracted_entities_dict,
                    )
                    return _finalize_search_response(
                        request=request,
                        response=cached_response,
                        analysis=analysis,
                        cache_hit=True,
                        timings=cached_response.query_analysis.get("timings_ms") if cached_response.query_analysis else None,
                    )
            except Exception as cache_exc:
                logger.warning(f"⚠️ 캐시 조회 실패: {cache_exc}")
                cache_key = None
                cache_enabled = False

        # ⭐ 단순화: 필터 분류 없이 모두 사용
        has_demographic_filters = bool(filters_for_response)
        has_behavioral_conditions = bool(
            analysis.behavioral_conditions and
            any(v is not None for v in analysis.behavioral_conditions.values())
        )

        logger.info(f"🔍 필터 상태: {len(filters)}개 (Demographics + Behavioral)")

        # 2) 쿼리 빌드
        # ⭐ 키워드 정제는 analyzer에서 이미 완료되었으므로 그대로 사용
        if analysis is None:
            raise RuntimeError("Query analysis not initialized")

        # 로깅: 분석기에서 정제된 최종 키워드 확인
        logger.info(f"🔍 [SearchAPI] 쿼리 분석 완료:")
        logger.info(f"  ✅ Must terms: {analysis.must_terms}")
        logger.info(f"  ✅ Should terms: {analysis.should_terms}")
        logger.info(f"  ✅ Demographics: {[d.raw_value for d in extracted_entities.demographics]}")
        if hasattr(analysis, 'removed_demographic_terms') and analysis.removed_demographic_terms:
            logger.info(f"  ℹ️ 제거된 Demographics: {analysis.removed_demographic_terms}")
        if analysis.behavioral_conditions:
            logger.info(f"  ✅ Behavioral conditions: {analysis.behavioral_conditions}")

        query_builder = OpenSearchHybridQueryBuilder(config)
        query_vector = None
        if embedding_model:
            # 완전 동적 임베딩 기반 동의어 확장 (도메인 무관, 범용)
            def _enrich_query_vector() -> Optional[list]:
                """임시: 동의어 확장 비활성화 (성능 최적화)"""
                try:
                    vec = embedding_model.encode(request.query).tolist()
                    logger.info("  ⚠️ 동의어 확장 비활성화 (성능 최적화)")
                    return vec
                except Exception:
                    return None

            query_vector = _enrich_query_vector()

        base_query = query_builder.build_query(
            analysis=analysis,
            query_vector=query_vector,
            size=size,
        )

        # 🔍 Base Query 로깅 (user_id 마스킹) - DEBUG 레벨로 변경
        logger.debug(f"🔍 [BASE QUERY] 생성 완료")
        masked_base_query = _mask_user_ids_in_query(base_query)
        logger.debug(json.dumps(masked_base_query, ensure_ascii=False, indent=2))

        # 3) 필터 적용 전략: 필터는 must로, 키워드는 should로 완화
        # - 필터(30대, 사무직)는 반드시 매칭되어야 함
        # - 키워드 검색은 should로 완화 (하나만 매칭되어도 OK)
        final_query = base_query
        
        # ⭐ match_all/match_none/None 제거: base_query에서 match_all, match_none, None이 있으면 제거
        existing_query = final_query.get('query', {"match_all": {}})
        if existing_query is None or existing_query == {"match_all": {}} or existing_query == {"match_none": {}}:
            # match_all/match_none/None 제거
            removed_type = "None" if existing_query is None else ("match_all" if existing_query == {"match_all": {}} else "match_none")
            
            # ⭐ 키워드가 있으면 키워드 쿼리 생성 (필터만 있는 경우를 위해)
            if analysis.must_terms or analysis.should_terms:
                # 키워드 쿼리 재생성
                keyword_queries = []
                if analysis.must_terms:
                    for term in analysis.must_terms:
                        keyword_queries.append({
                            "nested": {
                                "path": "qa_pairs",
                                "query": {"match": {"qa_pairs.answer_text": term}},
                                "score_mode": "max"
                            }
                        })
                
                if analysis.should_terms:
                    should_keywords = [{
                        "nested": {
                            "path": "qa_pairs",
                            "query": {"match": {"qa_pairs.answer_text": term}},
                            "score_mode": "max"
                        }
                    } for term in analysis.should_terms]
                    
                    if keyword_queries:
                        # must와 should 모두 있는 경우
                        existing_query = {
                            "bool": {
                                "must": keyword_queries,
                                "should": should_keywords,
                                "minimum_should_match": 1
                            }
                        }
                    else:
                        # should만 있는 경우
                        existing_query = {
                            "bool": {
                                "should": should_keywords,
                                "minimum_should_match": 1
                            }
                        }
                else:
                    # must만 있는 경우
                    if len(keyword_queries) == 1:
                        existing_query = keyword_queries[0]
                    else:
                        existing_query = {
                            "bool": {
                                "must": keyword_queries
                            }
                        }
                
                logger.info(f"⚠️ {removed_type} 제거, 키워드 쿼리 재생성: must={len(analysis.must_terms)}, should={len(analysis.should_terms)}")
            else:
                existing_query = None
                logger.info(f"⚠️ {removed_type} 제거: 필터만 사용 (키워드 없음)")
        
        # ⭐ inner_hits 제거 함수 (중복 방지)
        def remove_inner_hits(query_dict):
            """재귀적으로 inner_hits 제거 (필터에서는 매칭만 확인하면 되므로)"""
            import copy
            cleaned = copy.deepcopy(query_dict)
            
            if isinstance(cleaned, dict):
                # nested 쿼리에서 inner_hits 제거
                if 'nested' in cleaned:
                    if 'inner_hits' in cleaned['nested']:
                        del cleaned['nested']['inner_hits']
                    # 재귀적으로 query 내부도 정제
                    if 'query' in cleaned['nested']:
                        cleaned['nested']['query'] = remove_inner_hits(cleaned['nested']['query'])
                
                # bool 쿼리 내부도 재귀적으로 정제
                if 'bool' in cleaned:
                    for key in ['must', 'should', 'must_not', 'filter']:
                        if key in cleaned['bool']:
                            if isinstance(cleaned['bool'][key], list):
                                cleaned['bool'][key] = [remove_inner_hits(item) for item in cleaned['bool'][key]]
                            else:
                                cleaned['bool'][key] = remove_inner_hits(cleaned['bool'][key])
            
            return cleaned

        # ⭐ Behavioral 필터 존재 여부 초기화 (기본값: False)
        has_behavioral_filters = False

        if filters:
            # ⭐ inner_hits 제거 (중복 방지)
            cleaned_filters = [remove_inner_hits(f) for f in filters]
            
            filter_by_type = {}
            for f in cleaned_filters:
                # 필터 타입 추출 (새로운 bool 쿼리 형태 지원)
                filter_type = None
                
                # 1. bool 쿼리 형태 (metadata OR qa_pairs)
                if 'bool' in f and 'should' in f['bool']:
                    should_clauses = f['bool']['should']
                    for clause in should_clauses:
                        # term 필터에서 타입 추출
                        if 'term' in clause:
                            term_key = list(clause['term'].keys())[0]
                            if 'age_group' in term_key:
                                filter_type = 'age'
                                break
                            elif 'gender' in term_key:
                                filter_type = 'gender'
                                break
                            elif 'occupation' in term_key:
                                filter_type = 'occupation'
                                break
                        # nested 필터에서 타입 추출
                        elif 'nested' in clause:
                            nested_q = clause['nested'].get('query', {}).get('bool', {}).get('must', [])
                            for nq in nested_q:
                                if isinstance(nq, dict) and 'bool' in nq and 'should' in nq['bool']:
                                    # q_text 매칭 확인
                                    for sq in nq['bool']['should']:
                                        if 'match' in sq:
                                            match_key = list(sq['match'].keys())[0]
                                            if 'q_text' in match_key:
                                                q_text_val = sq['match'][match_key]
                                                if '연령' in str(q_text_val) or '나이' in str(q_text_val):
                                                    filter_type = 'age'
                                                    break
                                                elif '성별' in str(q_text_val):
                                                    filter_type = 'gender'
                                                    break
                                                elif '직업' in str(q_text_val) or '직무' in str(q_text_val):
                                                    filter_type = 'occupation'
                                                    break
                                elif 'match' in nq:
                                    match_key = list(nq['match'].keys())[0]
                                    if 'q_text' in match_key:
                                        q_text_val = nq['match'][match_key]
                                        if '연령' in str(q_text_val) or '나이' in str(q_text_val):
                                            filter_type = 'age'
                                            break
                                        elif '성별' in str(q_text_val):
                                            filter_type = 'gender'
                                            break
                                        elif '직업' in str(q_text_val) or '직무' in str(q_text_val):
                                            filter_type = 'occupation'
                                            break
                        if filter_type:
                            break
                
                # 2. 기존 형태 (하위 호환성)
                elif 'term' in f:
                    term_key = list(f['term'].keys())[0]
                    if 'age_group' in term_key:
                        filter_type = 'age'
                    elif 'gender' in term_key:
                        filter_type = 'gender'
                    elif 'occupation' in term_key:
                        filter_type = 'occupation'
                elif 'nested' in f:
                    nested_q = f['nested'].get('query', {}).get('bool', {}).get('must', [])
                    for nq in nested_q:
                        if 'match' in nq:
                            match_key = list(nq['match'].keys())[0]
                            if 'q_text' in match_key:
                                q_text_val = nq['match'][match_key]
                                if '연령' in str(q_text_val) or '나이' in str(q_text_val):
                                    filter_type = 'age'
                                elif '성별' in str(q_text_val):
                                    filter_type = 'gender'
                                elif '직업' in str(q_text_val):
                                    filter_type = 'occupation'
                
                if filter_type:
                    if filter_type not in filter_by_type:
                        filter_by_type[filter_type] = []
                    filter_by_type[filter_type].append(f)
                else:
                    # 타입을 알 수 없는 필터는 그대로 추가
                    if 'unknown' not in filter_by_type:
                        filter_by_type['unknown'] = []
                    filter_by_type['unknown'].append(f)
            
            # ⭐ 필터를 should 조건으로 전환 (점수 부스팅 포함)
            # 각 타입별로 OR, 타입 간은 AND (should로 완화)
            should_filters = []
            for filter_type, type_filters in filter_by_type.items():
                if filter_type == 'unknown':
                    # ⭐ unknown 타입은 각각 개별적으로 추가 (AND 처리)
                    # region, marital_status 등 서로 다른 필터는 모두 만족해야 함
                    should_filters.extend(type_filters)
                elif len(type_filters) == 1:
                    # 단일 필터: 필터를 그대로 사용 (이미 bool 쿼리 형태)
                    filter_item = type_filters[0]
                    should_filters.append(filter_item)
                else:
                    # 같은 타입 필터는 OR (예: 30대 OR 40대)
                    should_filters.append({
                        'bool': {
                            'should': type_filters,
                            "minimum_should_match": 1
                        }
                    })

            # ⭐⭐⭐ 필터를 두 그룹으로 분리:
            # 1) Demographics 필터 (연령, 성별, 직업): Python post-processing
            # 2) Behavioral 필터 (qa_pairs의 다른 질문): OpenSearch 쿼리에 직접 포함
            demographic_filters = []
            behavioral_filters = []

            logger.info(f"🔍 필터 분류 시작: should_filters={len(should_filters)}개")

            def is_demographic_filter(f):
                """Demographics 필터인지 확인 (연령, 성별, 지역, 직업 등 - Python post-processing 가능)

                ⭐ OCCUPATION도 포함!
                - OCCUPATION 필터는 metadata OR qa_pairs 구조로 생성됨
                - Python post-processing에서 qa_pairs를 확인하여 정확한 필터링 수행
                - extract_from_qa_pairs_once() 함수가 occupation을 qa_pairs에서 추출함
                """
                # ⭐ 모든 Demographics 체크 (OCCUPATION 포함!)
                # OCCUPATION도 Python post-processing에서 qa_pairs를 확인함
                demo_keywords = ['연령', '나이', '성별',
                               '지역', '거주', '주소', 'region',
                               '결혼', '혼인', '배우자',
                               '직업', '직무', 'occupation', '직종']  # ⭐ OCCUPATION 키워드 추가!

                # Case 1: 필터에 nested가 직접 있는 경우
                if 'nested' in f and 'path' in f['nested'] and f['nested']['path'] == 'qa_pairs':
                    nested_q = f['nested'].get('query', {}).get('bool', {})
                    must_list = nested_q.get('must', [])
                    for must_item in must_list:
                        if 'bool' in must_item and 'should' in must_item['bool']:
                            for should_item in must_item['bool']['should']:
                                if 'match' in should_item:
                                    for match_key, match_val in should_item['match'].items():
                                        if 'q_text' in match_key:
                                            if any(kw in str(match_val) for kw in demo_keywords):
                                                return True

                # Case 2: bool → should 안에 nested가 있는 경우 (REGION 필터 구조)
                # 예: {"bool": {"should": [{metadata 매칭}, {"nested": {...}}]}}
                if 'bool' in f and 'should' in f['bool']:
                    for should_item in f['bool']['should']:
                        if 'nested' in should_item and 'path' in should_item['nested']:
                            if should_item['nested']['path'] == 'qa_pairs':
                                nested_q = should_item['nested'].get('query', {}).get('bool', {})
                                must_list = nested_q.get('must', [])
                                for must_item in must_list:
                                    if 'bool' in must_item and 'should' in must_item['bool']:
                                        for nested_should in must_item['bool']['should']:
                                            if 'match' in nested_should:
                                                for match_key, match_val in nested_should['match'].items():
                                                    if 'q_text' in match_key:
                                                        if any(kw in str(match_val) for kw in demo_keywords):
                                                            return True

                return False

            for f in should_filters:
                is_demo = is_demographic_filter(f)
                if is_demo:
                    demographic_filters.append(f)
                    # ⭐ 디버깅: Demographics로 분류된 필터 로그 출력
                    logger.info(f"   ✅ Demographics로 분류: {json.dumps(f, ensure_ascii=False)[:200]}")
                else:
                    behavioral_filters.append(f)
                    # ⭐ 디버깅: Behavioral로 분류된 필터 로그 출력
                    logger.info(f"   ⚠️ Behavioral로 분류: {json.dumps(f, ensure_ascii=False)[:200]}")

            logger.info(f"🔍 필터 분리:")
            logger.info(f"   - Demographics 필터 (Python post-processing): {len(demographic_filters)}개")
            logger.info(f"   - Behavioral 필터 (OpenSearch 직접 적용): {len(behavioral_filters)}개")

            # ⭐ should_filters를 demographic_filters로 대체 (Python post-processing용)
            should_filters = demographic_filters

            # ⭐ Behavioral 필터 존재 여부 (Qdrant 비활성화 판단용)
            has_behavioral_filters = bool(behavioral_filters)

            # ⭐⭐⭐ Demographics 필터만 Python post-processing으로 이동
            # 이유: 비구조화된 설문 데이터는 벡터 검색으로만 찾을 수 있음
            logger.info(f"✅ Demographics 필터를 Python post-processing으로 이동 ({len(demographic_filters)}개 필터)")
            logger.info(f"   → OpenSearch는 키워드 + Behavioral 검색 수행, Qdrant는 벡터 검색 수행")
            logger.info(f"   → RRF 후 Python에서 Demographics 필터 적용하여 정확도 유지")

            # ⭐⭐⭐ Behavioral 필터는 OpenSearch 쿼리에 직접 포함
            # 이유: qa_pairs는 OpenSearch에만 있으므로 직접 검색해야 함
            if behavioral_filters:
                logger.info(f"✅ Behavioral 필터를 OpenSearch 쿼리에 직접 포함 ({len(behavioral_filters)}개 필터)")

                # 키워드 쿼리와 Behavioral 필터를 결합
                if existing_query is None or existing_query == {"match_all": {}} or existing_query == {"match_none": {}}:
                    # 키워드가 없으면 Behavioral 필터만 사용
                    final_query['query'] = {
                        'bool': {
                            'must': behavioral_filters
                        }
                    }
                    logger.info(f"   → 키워드 없음: Behavioral 필터만 사용")
                else:
                    # ⭐ Behavioral 필터는 must (필수), 키워드는 should (점수 부스팅)
                    # 이유: nested 쿼리 간 충돌 방지 + 키워드로 결과 랭킹 개선
                    final_query['query'] = {
                        'bool': {
                            'must': behavioral_filters,
                            'should': [existing_query],
                            'minimum_should_match': 0
                        }
                    }
                    logger.info(f"   → Behavioral 필터 (필수) + 키워드 쿼리 (점수 부스팅)")
            else:
                # Behavioral 필터가 없으면 키워드 쿼리만 사용
                if existing_query is None or existing_query == {"match_all": {}} or existing_query == {"match_none": {}}:
                    final_query['query'] = {"match_all": {}}
                    logger.info(f"   → 키워드 없음: match_all 사용")
                else:
                    final_query['query'] = existing_query
                    logger.info(f"   → 키워드 쿼리만 적용")
        
        if 'size' not in final_query:
            final_query['size'] = size

        if filters:
            logger.debug(f"🔍 적용된 필터 ({len(filters)}개):")
            for i, f in enumerate(filters, 1):
                masked_filter = _mask_user_ids_in_query(f)
                logger.debug(f"  필터 {i}: {json.dumps(masked_filter, ensure_ascii=False, indent=2)}")
            logger.debug(f"🔍 최종 쿼리 구조:")
            masked_final_query = _mask_user_ids_in_query(final_query)
            logger.debug(f"  {json.dumps(masked_final_query, ensure_ascii=False, indent=2)}")
        else:
            logger.debug(f"🔍 최종 쿼리 구조 (필터 없음):")
            masked_final_query = _mask_user_ids_in_query(final_query)
            logger.debug(f"  {json.dumps(masked_final_query, ensure_ascii=False, indent=2)}")

        # ⭐ Qdrant top-N 제한: 필터 유무에 따라 분기
        has_filters = bool(filters)
        rrf_k_used: Optional[int] = None
        rrf_reason: str = ""
        adaptive_threshold: Optional[float] = None
        threshold_reason: str = ""
        has_behavioral = bool(getattr(analysis, "behavioral_conditions", None))

        # ⭐⭐⭐ 검색 크기 증가: Python post-filtering을 위해 충분한 후보 확보
        # Demographics/Behavioral 필터를 OpenSearch 쿼리에서 제거했으므로 더 많은 후보 필요
        if has_filters or has_behavioral:
            # Behavioral/Demographics 필터가 있으면 전체 데이터 대상으로 검색
            # ⭐ 전체 데이터: 35000건 → 충분한 결과를 위해 35000건 조회
            qdrant_limit = min(max(size * 10, 5000), 10000)   # Qdrant는 10000개로 제한 (성능)
            search_size = min(max(size * 20, 35000), 50000)   # ⭐ 최소 35000건, 최대 50000건
            logger.info(f"🔍 필터 있음 (Python post-processing): OpenSearch size={search_size}, Qdrant limit={qdrant_limit}")
        else:
            # 필터가 없어도 벡터 검색을 위해 충분한 후보 확보
            qdrant_limit = min(max(size * 5, 500), 50000)     # 기본 500개, 최대 50000개 (전체 데이터 약 35000개)
            search_size = min(max(size * 10, 500), 50000)    # 기본 500개, 최대 50000개 (전체 데이터 약 35000개)
            logger.info(f"🔍 필터 없음: OpenSearch size={search_size}, Qdrant limit={qdrant_limit}")

        # 4) 실행: 하이브리드 (OpenSearch + 선택적 Qdrant) with RRF
        # ⭐ survey_responses_merged 단일 인덱스만 검색
        
        # OpenSearch _source filtering: 필요한 필드만 조회
        # ⭐ qa_pairs 포함: marital_status, region 등이 qa_pairs에 있을 수 있음
        source_filter = {
            "includes": ["user_id", "metadata", "timestamp", "qa_pairs"],
            "excludes": []  # 필요시 제외할 필드 추가
        }
        
        # ⭐⭐⭐ survey_responses_merged 단일 인덱스만 사용
        logger.info(f"🔍 인덱스 검색: {request.index_name} (survey_responses_merged만 사용)")
        
        # survey_responses_merged 인덱스 검색
        keyword_results: List[Dict[str, Any]] = []
        vector_results: List[Dict[str, Any]] = []
        
        try:
            # ⭐⭐⭐ OpenSearch Scroll API 사용 (전체 데이터 조회)
            query_body = final_query.copy()
            if not isinstance(query_body.get('query'), dict):
                logger.warning("  ⚠️ 쿼리가 비어 있어 match_all로 대체합니다")
                query_body['query'] = {"match_all": {}}

            # 🔍 OpenSearch 쿼리 로깅 (디버깅용) - DEBUG 레벨로 축소, user_id 마스킹
            logger.debug(f"🔍 OpenSearch Scroll 쿼리:")
            masked_query_body = _mask_user_ids_in_query(query_body)
            logger.debug(json.dumps(masked_query_body, ensure_ascii=False, indent=2))

            # ⭐⭐⭐ 메모리 캐시 사용 (초고속!) vs Scroll API (느림)
            opensearch_start = perf_counter()

            if panel_cache.loaded:
                # ⭐ 메모리 캐시에서 즉시 조회 (0.01초 이하!)
                logger.info("  ⚡ 메모리 캐시 사용: panel_cache에서 전체 데이터 조회")

                # 전체 데이터 가져오기 (이미 메모리에 로드됨)
                all_user_ids = panel_cache.get_all_user_ids()

                # OpenSearch 형식으로 변환 (기존 코드와 호환성 유지)
                keyword_results = []
                for user_id in all_user_ids:
                    doc = panel_cache.user_map.get(user_id)
                    if doc:
                        keyword_results.append(doc)

                opensearch_duration_ms = (perf_counter() - opensearch_start) * 1000
                timings['memory_cache_ms'] = opensearch_duration_ms

                opensearch_total_hits = len(keyword_results)
                logger.info(f"  ✅ 메모리 캐시: {len(keyword_results)}건 ({opensearch_duration_ms:.2f}ms) 🚀")
                
                # ⭐ SSE 스트리밍: OpenSearch 결과 개수 전달
                if stream_callback:
                    try:
                        stream_callback('opensearch_results', {'count': opensearch_total_hits})
                    except Exception as e:
                        logger.warning(f"⚠️ stream_callback 오류 (opensearch_results): {e}")

            else:
                # ⭐ Fallback: Scroll API (메모리 캐시 없을 때)
                logger.warning("  ⚠️ 메모리 캐시 미사용 → Scroll API 사용 (느림)")

                scroll_hits = await data_fetcher.scroll_search_async(
                    index_name=request.index_name,
                    query=query_body,
                    batch_size=1000,
                    scroll_time="5m",
                    num_slices=8,  # ⭐ 8개로 증가 (병렬성 향상)
                    source_filter=source_filter,
                    request_timeout=300,
                )
                opensearch_duration_ms = (perf_counter() - opensearch_start) * 1000
                timings['opensearch_scroll_ms'] = opensearch_duration_ms

                keyword_results = scroll_hits
                opensearch_total_hits = len(keyword_results)
                logger.info(f"  ✅ OpenSearch Scroll: {len(keyword_results)}건 ({opensearch_duration_ms:.2f}ms)")
                
                # ⭐ SSE 스트리밍: OpenSearch 결과 개수 전달
                if stream_callback:
                    try:
                        stream_callback('opensearch_results', {'count': opensearch_total_hits})
                    except Exception as e:
                        logger.warning(f"⚠️ stream_callback 오류 (opensearch_results): {e}")

            # ⭐⭐⭐ Qdrant 벡터 검색 (survey_responses_merged 통합 컬렉션)
            # Behavioral 필터가 있으면 Qdrant 비활성화 (qa_pairs는 OpenSearch에만 있음)
            if has_behavioral_filters:
                logger.info(f"  ⚠️ Behavioral 필터 감지 → Qdrant 비활성화 (OpenSearch만 사용)")
                logger.info(f"     이유: qa_pairs는 OpenSearch에만 있어서 벡터 검색으로 필터링 불가")
                # ⭐ SSE 스트리밍: Qdrant 비활성화 알림
                if stream_callback:
                    try:
                        logger.info(f"  📡 SSE: Qdrant 비활성화 → count=0 전송")
                        stream_callback('qdrant_results', {'count': 0})
                    except Exception as e:
                        logger.warning(f"⚠️ stream_callback 오류 (qdrant_results): {e}")
            elif request.use_vector_search and query_vector and hasattr(router, 'qdrant_client'):
                qdrant_client = router.qdrant_client
                try:
                    # ⭐ survey_responses_merged 통합 컬렉션 사용 - 시간 측정 시작
                    qdrant_start = perf_counter()
                    collection_name = request.index_name  # survey_responses_merged
                    logger.info(f"  🔍 Qdrant 컬렉션: {collection_name} (통합 컬렉션)")
                    try:
                        r = qdrant_client.search(
                            collection_name=collection_name,
                            query_vector=query_vector,
                            limit=qdrant_limit,
                            score_threshold=0.3,
                        )
                        for item in r:
                            vector_results.append({
                                '_id': str(item.id),
                                '_score': item.score,
                                '_source': item.payload,
                                '_index': collection_name,
                            })
                        qdrant_duration_ms = (perf_counter() - qdrant_start) * 1000
                        timings['qdrant_search_ms'] = qdrant_duration_ms
                        logger.info(f"  ✅ Qdrant ({collection_name}): {len(vector_results)}건 ({qdrant_duration_ms:.2f}ms)")
                        
                        # ⭐ SSE 스트리밍: Qdrant 결과 개수 전달
                        if stream_callback:
                            try:
                                stream_callback('qdrant_results', {'count': len(vector_results)})
                            except Exception as e:
                                logger.warning(f"⚠️ stream_callback 오류 (qdrant_results): {e}")
                    except Exception as e:
                        logger.warning(f"  ⚠️ Qdrant 컬렉션 '{collection_name}' 검색 실패: {e}")
                except Exception as e:
                    logger.warning(f"  ⚠️ Qdrant 검색 실패: {e}")
        except Exception as e:
            logger.warning(f"  ⚠️ 인덱스 검색 실패: {e}")

        # user_id 및 _id -> 원본 문서 매핑 생성 - 시간 측정 시작
        mapping_start = perf_counter()
        user_doc_map = {}
        id_doc_map = {}

        # ⭐ OpenSearch 키워드 결과 매핑
        for hit in keyword_results:
            source = hit.get('_source', {})
            user_id = source.get('user_id')
            doc_id = hit.get('_id')

            doc_info = {
                'source': source,
                'inner_hits': hit.get('inner_hits', {}),
                'highlight': hit.get('highlight'),
                'index': hit.get('_index', 'unknown')
            }

            if user_id:
                user_doc_map[user_id] = doc_info
            if doc_id:
                id_doc_map[doc_id] = doc_info

        mapping_duration_ms = (perf_counter() - mapping_start) * 1000
        timings['user_doc_mapping_ms'] = mapping_duration_ms
        logger.info(f"  ✅ user_doc_map 생성 완료: {len(user_doc_map)}개 ({mapping_duration_ms:.2f}ms)")

        # ⭐⭐⭐ Qdrant 벡터 결과에서 user_id 수집 및 metadata 보강
        # Qdrant payload에는 metadata가 없으므로 OpenSearch에서 전체 문서 조회 필요
        if vector_results:
            qdrant_user_ids = set()
            for doc in vector_results:
                payload = doc.get('_source', {})
                user_id = payload.get('user_id')
                if user_id and user_id not in user_doc_map:
                    qdrant_user_ids.add(user_id)

            if qdrant_user_ids:
                logger.info(f"  🔍 Qdrant 결과 중 metadata 없는 user_id: {len(qdrant_user_ids)}개")
                logger.info(f"     → OpenSearch에서 전체 문서 조회 중...")

                try:
                    # OpenSearch에서 user_id로 전체 문서 조회
                    # user_id는 keyword 타입이므로 terms 쿼리 사용
                    user_id_list = list(qdrant_user_ids)
                    bulk_query = {
                        "query": {
                            "terms": {
                                "user_id": user_id_list
                            }
                        },
                        "size": len(qdrant_user_ids),
                        "_source": ["user_id", "metadata", "qa_pairs", "timestamp"]
                    }

                    # 🔍 Bulk 쿼리 로깅 (user_id 리스트 마스킹)
                    logger.debug(f"     Bulk query: terms user_id (count={len(user_id_list)})")

                    bulk_response = data_fetcher.search_opensearch(
                        index_name=request.index_name,
                        query=bulk_query,
                        size=len(qdrant_user_ids),
                        source_filter=None,
                        request_timeout=DEFAULT_OS_TIMEOUT,
                    )

                    fetched_count = 0
                    for hit in bulk_response['hits']['hits']:
                        source = hit.get('_source', {})
                        user_id = source.get('user_id')
                        if user_id:
                            doc_info = {
                                'source': source,
                                'inner_hits': {},
                                'highlight': None,
                                'index': hit.get('_index', 'unknown')
                            }
                            user_doc_map[user_id] = doc_info
                            fetched_count += 1

                    logger.info(f"     ✅ {fetched_count}개 user 문서 조회 완료 (metadata 포함)")
                except Exception as e:
                    logger.warning(f"     ⚠️ Bulk 조회 실패: {e}")
                    logger.warning(f"     → Qdrant 결과 중 일부는 metadata 없이 필터링됨")

        # ⭐ RRF 결합
        logger.info(f"\n{'='*60}")
        logger.info("📊 RRF 결합")
        logger.info(f"{'='*60}")

        # 벡터 결과에 인덱스 메타데이터 보강
        for doc in vector_results:
            if '_index' not in doc:
                doc['_index'] = doc.get('collection', doc.get('_source', {}).get('index', request.index_name))

        logger.info(f"  - 키워드 결과: {len(keyword_results)}건")
        logger.info(f"  - 벡터   결과: {len(vector_results)}건")

        rrf_start = perf_counter()

        if request.use_vector_search and vector_results:
            combined_rrf, rrf_k_used, rrf_reason = calculate_rrf_score_adaptive(
                keyword_results=keyword_results,
                vector_results=vector_results,
                query_intent=getattr(analysis, "intent", None),
                has_filters=has_filters,
                use_vector_search=request.use_vector_search,
            )
        else:
            combined_rrf = keyword_results
            if request.use_vector_search:
                rrf_reason = "벡터 결과 없음 → 키워드 결과 사용"
            else:
                rrf_reason = "벡터 검색 비활성화 → 키워드 결과 사용"
            rrf_k_used = 0

        user_rrf_variants: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for doc in combined_rrf:
            user_id = get_user_id_from_doc(doc)
            if not user_id:
                continue
            user_rrf_variants[user_id].append(doc)
        
        user_rrf_map: Dict[str, List[Dict[str, Any]]] = {}
        final_rrf_results: List[Dict[str, Any]] = []
        for user_id, docs in user_rrf_variants.items():
            def _score(doc: Dict[str, Any]) -> float:
                score = doc.get('_score')
                if score is None:
                    score = doc.get('rrf_score', 0.0)
                return float(score or 0.0)

            best_doc_original = max(docs, key=_score)
            total_rrf_score = sum(_score(doc) for doc in docs)
            sources = [doc.get('_index', 'unknown') for doc in docs]

            best_doc = dict(best_doc_original)
            best_doc['_score'] = total_rrf_score
            best_doc['_rrf_details'] = {
                    'combined_score': total_rrf_score,
                'source_count': len(docs),
                'sources': sources,
                }

            # ⭐ RRF 재조합 시점에 OpenSearch metadata merge
            # best_doc이 Qdrant 문서인 경우 metadata가 없으므로 OpenSearch에서 가져옴
            if user_id and user_id in user_doc_map:
                opensearch_doc = user_doc_map[user_id]
                if opensearch_doc and isinstance(opensearch_doc, dict):
                    opensearch_source = opensearch_doc.get("source", {})
                    if isinstance(opensearch_source, dict):
                        # best_doc의 _source 가져오기
                        current_source = best_doc.get('_source', {})
                        if not isinstance(current_source, dict):
                            current_source = {}

                        # OpenSearch 데이터 우선으로 merge (metadata, qa_pairs 보존)
                        merged_source = {}
                        merged_source.update(current_source)        # Qdrant: user_id, text
                        merged_source.update(opensearch_source)     # OpenSearch: metadata, qa_pairs
                        best_doc['_source'] = merged_source

            final_rrf_results.append(best_doc)
            # best_doc를 첫 번째로 유지하고, 나머지는 참고용으로 보관
            others = [doc for doc in docs if doc is not best_doc_original]
            user_rrf_map[user_id] = [best_doc] + others
        
        final_rrf_results.sort(
            key=lambda d: d.get('_score', 0.0) or d.get('rrf_score', 0.0),
            reverse=True
        )
        
        rrf_results = final_rrf_results
        took_ms = 0  # 여러 검색의 합이므로 정확한 시간 측정은 어려움
        
        logger.info(f"  ✅ 단일 RRF 결합 완료: {len(rrf_results)}건 (고유 user_id: {len(user_rrf_map)}개)")
        timings['rrf_recombination_ms'] = (perf_counter() - rrf_start) * 1000
        
        # ⭐ SSE 스트리밍: RRF Fusion 이벤트 전달
        if stream_callback:
            try:
                stream_callback('rrf_fusion', {
                    'opensearch_count': len(keyword_results),
                    'qdrant_count': len(vector_results),
                    'combined_count': len(rrf_results),
                    'alpha': analysis.alpha,
                    'rrf_k': rrf_k_used,
                    'rrf_reason': rrf_reason
                })
            except Exception as e:
                logger.warning(f"⚠️ stream_callback 오류 (rrf_fusion): {e}")
        
        # ⭐ SSE 스트리밍: 필터링 전 개수 전달
        if stream_callback:
            try:
                stream_callback('before_filter', {'count': len(rrf_results)})
            except Exception as e:
                logger.warning(f"⚠️ stream_callback 오류 (before_filter): {e}")

        # 후보 문서 수 제한 (후처리 부담 완화)
        fetch_size = window_size
        candidate_cap = max(
            fetch_size * 20,
            cache_limit if cache_limit else 0,
            40000  # 전체 데이터 약 35000개를 고려하여 증가
        )
        if candidate_cap and len(rrf_results) > candidate_cap:
            logger.info(
                f"  - 후보 문서 제한 적용: {len(rrf_results)} → {candidate_cap} (size={fetch_size})"
            )
            rrf_results = rrf_results[:candidate_cap]
        elif len(rrf_results) < fetch_size:
            backup_cap = max(fetch_size * 6, fetch_size + 50)
            logger.info(
                f"  - 후보 수가 size보다 작아 증가 시도: {len(rrf_results)} → {min(len(final_rrf_results), backup_cap)}"
            )
            rrf_results = final_rrf_results[:backup_cap]
        
        # RRF 점수 디버깅: 상위 10개 출력
        if rrf_results:
            logger.info(f"  - RRF 점수 상위 10개:")
            for i, doc in enumerate(rrf_results[:10], 1):
                rrf_score = doc.get('_score') or doc.get('rrf_score', 0.0)
                rrf_details = doc.get('_rrf_details', {})
                doc_index = doc.get('_index', 'unknown')
                logger.info(f"    {i}. doc_id={doc.get('_id', 'N/A')}, index={doc_index}, RRF={rrf_score:.6f}, "
                          f"keyword_rank={rrf_details.get('keyword_rank')}, vector_rank={rrf_details.get('vector_rank')}")
        
        # ⭐ 디버깅: 루프 전 extracted_entities.demographics 확인
        logger.warning(f"[EXTRACTED ENTITIES] demographics count: {len(extracted_entities.demographics)}")
        for demo in extracted_entities.demographics:
            logger.warning(f"  - {demo.demographic_type.value}: {demo.value}")

        demographic_filters: Dict[DemographicType, List["DemographicEntity"]] = defaultdict(list)
        for demo in extracted_entities.demographics:
            demographic_filters[demo.demographic_type].append(demo)

        # ⭐ 디버깅: demographic_filters 내용 확인
        logger.warning(f"[DEMO FILTERS] demographic_filters keys: {[k.value for k in demographic_filters.keys()]}")
        for demo_type, demo_list in demographic_filters.items():
            logger.warning(f"  [{demo_type.value}]: {[d.value for d in demo_list]}")

        filtered_rrf_results: List[Dict[str, Any]] = rrf_results
        # ⭐ total_hits는 나중에 실제 반환된 결과 수로 설정됨 (len(results))
        # 임시로 rrf_results 길이 사용
        total_hits = len(rrf_results)

        occupation_display_map: Dict[str, str] = {}
        behavior_values_map: Dict[str, Dict[str, Optional[bool]]] = {}
        doc_user_map: Dict[int, str] = {}
        # ⭐ survey_responses_merged만 사용하므로 welcome_1st/welcome_2nd 배치 제거
        synonym_cache: Dict[str, List[str]] = {}

        PLACEHOLDER_TOKENS: Set[str] = {
            "",
            "미정",
            "없음",
            "무응답",
            "해당없음",
            "n/a",
            "na",
            "null",
            "none",
            "unknown",
            "미선택",
            "미기재",
        }
        PLACEHOLDER_TOKENS = {token.strip().lower() for token in PLACEHOLDER_TOKENS}

        def normalize_value(value: Any) -> str:
            if value is None:
                return ""
            if isinstance(value, bool):
                value_str = str(value)
            elif isinstance(value, (int, float)):
                try:
                    if value.is_integer():  # type: ignore[attr-defined]
                        value = int(value)
                except AttributeError:
                    pass
                value_str = str(value)
            else:
                value_str = str(value)

            cleaned = value_str.strip()
            lower = cleaned.lower()
            if lower in PLACEHOLDER_TOKENS:
                return ""
            return lower

        def build_expected_values(demo: "DemographicEntity") -> Set[str]:
            key = f"{demo.demographic_type.value}:{demo.raw_value}"
            expected: Set[str] = set()
            expected.add(demo.raw_value)
            expected.add(demo.value)
            expected.update(demo.synonyms or set())
            expected.update(synonym_cache.get(key, []))
            normalized_expected = {normalize_value(v) for v in expected if v}

            if demo.demographic_type == DemographicType.GENDER:
                male_aliases = {
                    normalize_value(v)
                    for v in {"m", "male", "man", "남", "남성", "남자", "남성형"}
                }
                female_aliases = {
                    normalize_value(v)
                    for v in {"f", "female", "woman", "여", "여성", "여자", "여성형"}
                }
                if normalized_expected & male_aliases:
                    normalized_expected.update(male_aliases)
                if normalized_expected & female_aliases:
                    normalized_expected.update(female_aliases)

            return normalized_expected

        def values_match(values: Set[str], expected: Set[str]) -> bool:
            """값 매칭 검증 (정확한 매칭만 허용)"""
            if not values or not expected:
                return False

            # ⭐ 정확한 매칭만 허용 (부분 문자열 매칭 제거)
            # "남성" in "여성" 같은 오매칭 방지
            for val in values:
                if not val:
                    continue
                # 정규화된 값끼리 정확히 비교
                if val in expected:
                    return True
            return False

        def expand_gender_aliases(values: Set[str]) -> None:
            male_aliases = {"m", "남", "남성", "male", "man", "남자"}
            female_aliases = {"f", "여", "여성", "female", "woman", "여자"}
            if values & male_aliases:
                values.update(male_aliases)
            if values & female_aliases:
                values.update(female_aliases)

        def add_age_decade(values: Set[str], age_value: Any) -> None:
            if age_value in (None, ""):
                return
            try:
                age_int = int(age_value)
                decade = (age_int // 10) * 10
                for candidate in (f"{decade}대", f"{decade}s", str(age_int)):
                    normalized_candidate = normalize_value(candidate)
                    if normalized_candidate:
                        values.add(normalized_candidate)
            except (ValueError, TypeError):
                pass

        filter_start = perf_counter()
        if 'has_filter_constraints' not in locals():
            has_filter_constraints = has_demographic_filters or has_behavioral_conditions
        if has_filter_constraints:

            # ⭐ survey_responses_merged만 사용하므로 welcome_1st 관련 로직 제거
            gender_dsl_handled = bool(demographic_filters.get(DemographicType.GENDER))
            age_dsl_handled = bool(demographic_filters.get(DemographicType.AGE))
            occupation_dsl_handled = bool(demographic_filters.get(DemographicType.OCCUPATION))

            # ⭐ 모든 demographic_filters를 검증 (REGION, MARITAL_STATUS, OCCUPATION 포함!)
            # OCCUPATION은 demographic_filters로 분류되어 Python post-processing에서 처리됨
            filters_to_validate: List[DemographicType] = list(demographic_filters.keys())
            logger.info(f"  ✅ 후처리 검증 대상: {[f.value for f in filters_to_validate]}")

            for demo in extracted_entities.demographics:
                cache_key = f"{demo.demographic_type.value}:{demo.raw_value}"
                if demo.demographic_type in {DemographicType.GENDER, DemographicType.OCCUPATION}:
                    try:
                        from rag_query_analyzer.utils.synonym_expander import get_synonym_expander
                        expander = get_synonym_expander()
                        synonym_cache[cache_key] = expander.expand(demo.raw_value)
                    except Exception:
                        synonyms = [demo.raw_value]
                        synonyms.extend([syn for syn in demo.synonyms if syn])
                        synonym_cache[cache_key] = synonyms
                else:
                    synonym_cache[cache_key] = [demo.raw_value]
            
            user_ids_to_fetch: Set[str] = set()
            doc_user_map.clear()
            
            logger.info(f"🔍 user_id 수집 중: RRF 결과 {len(rrf_results)}건...")
            for doc in rrf_results:
                source = doc.get('_source', {})
                if not source and 'doc' in doc:
                    source = doc.get('doc', {}).get('_source', {})
                if not source or not isinstance(source, dict):
                    if 'payload' in doc:
                        payload = doc.get('payload', {})
                        if isinstance(payload, dict) and payload:
                            source = payload
                    elif isinstance(source, dict) and 'payload' in source:
                        payload = source.get('payload', {})
                        if isinstance(payload, dict) and payload:
                            source = payload
                user_id = None
                if isinstance(source, dict):
                    user_id = source.get('user_id')
                if not user_id:
                    user_id = doc.get('_id', '')
                if not user_id and 'payload' in doc:
                    payload = doc.get('payload', {})
                    if isinstance(payload, dict):
                        user_id = payload.get('user_id')
                if user_id:
                    user_ids_to_fetch.add(user_id)
                    doc_user_map[id(doc)] = user_id
            
            logger.info(f"  ✅ 수집된 user_id: {len(user_ids_to_fetch)}건")
            # ⭐ survey_responses_merged만 사용하므로 welcome_1st/welcome_2nd 배치 조회 제거

            # ⭐⭐⭐ Demographics 필터 또는 Behavioral 조건이 있으면 Python post-processing 실행
            has_behavioral_conditions = bool(analysis.behavioral_conditions and any(v is not None for v in analysis.behavioral_conditions.values()))

            if not filters_to_validate and not has_behavioral_conditions:
                timings["post_filter_ms"] = (perf_counter() - filter_start) * 1000
                filtered_rrf_results = rrf_results
                logger.info("  ✅ 필터 없음: Python 후처리 생략")
            else:
                def collect_doc_values(
                    user_id: str,
                    source: Dict[str, Any],
                    metadata: Dict[str, Any],
                    _unused: Dict[str, Any],  # 호환성을 위해 유지하지만 사용하지 않음
                ) -> Tuple[Dict[DemographicType, Set[str]], Dict[DemographicType, bool], Dict[str, Optional[bool]]]:
                    doc_values: Dict[DemographicType, Set[str]] = {
                        DemographicType.GENDER: set(),
                        DemographicType.AGE: set(),
                        DemographicType.OCCUPATION: set(),
                    }
                    metadata_presence: Dict[DemographicType, bool] = {
                        DemographicType.GENDER: False,
                        DemographicType.AGE: False,
                        DemographicType.OCCUPATION: False,
                    }
                    # ⭐ 동적 behavior_values 생성 (Claude가 추출한 조건 기반)
                    behavior_values: Dict[str, Optional[bool]] = {}
                    if analysis.behavioral_conditions:
                        for key in analysis.behavioral_conditions.keys():
                            behavior_values[key] = None

                    def record_behavior(key: str, value: Optional[bool]) -> None:
                        if value is None:
                            return
                        if behavior_values.get(key) is None:
                            behavior_values[key] = value

                    def parse_yes_no(text: Optional[str]) -> Optional[bool]:
                        if not text:
                            return None
                        normalized = text.lower()
                        if any(keyword in normalized for keyword in BEHAVIOR_NO_TOKENS):
                            return False
                        if any(keyword in normalized for keyword in BEHAVIOR_YES_TOKENS):
                            return True
                        return None

                    def parse_smoker_answer(raw: Optional[Any]) -> Optional[bool]:
                        if raw is None:
                            return None
                        if isinstance(raw, (list, tuple, set)):
                            for item in raw:
                                decision = parse_smoker_answer(item)
                                if decision is not None:
                                    return decision
                            return None
                        text = str(raw).strip()
                        if not text:
                            return None
                        normalized = text.lower()
                        compact = normalized.replace(" ", "")
                        for keyword in SMOKER_NEGATIVE_KEYWORDS:
                            keyword_compact = keyword.replace(" ", "")
                            if keyword in normalized or keyword_compact in compact:
                                return False
                        for keyword in SMOKER_POSITIVE_KEYWORDS:
                            keyword_compact = keyword.replace(" ", "")
                            if keyword in normalized or keyword_compact in compact:
                                return True
                        if (
                            "담배" in normalized
                            and not any(token in normalized for token in ("없", "안", "않", "no", "무", "미흡연"))
                        ):
                            return True
                        return parse_yes_no(text)

                    def parse_drinker_answer(raw: Optional[Any]) -> Optional[bool]:
                        """음주 여부 파싱: 술 종류가 있으면 True, '술을 마시지 않음'이 있으면 False"""
                        if raw is None:
                            return None
                        if isinstance(raw, (list, tuple, set)):
                            for item in raw:
                                decision = parse_drinker_answer(item)
                                if decision is not None:
                                    return decision
                            return None
                        text = str(raw).strip()
                        if not text:
                            return None
                        normalized = text.lower()
                        compact = normalized.replace(" ", "")

                        # 네거티브 키워드 우선 체크
                        for keyword in NON_DRINKER_KEYWORDS:
                            keyword_compact = keyword.replace(" ", "")
                            if keyword in normalized or keyword_compact in compact:
                                return False

                        # 포지티브 키워드 체크
                        for keyword in DRINKER_POSITIVE_KEYWORDS:
                            keyword_compact = keyword.replace(" ", "")
                            if keyword in normalized or keyword_compact in compact:
                                return True

                        # "술" 언급이 있지만 부정 표현이 없으면 음주자로 간주
                        if (
                            "술" in normalized
                            and not any(token in normalized for token in ("없", "안", "않", "no", "무", "못"))
                        ):
                            return True

                        return None

                    metadata_candidates = [
                        metadata,  # 이제 단일 metadata만 사용
                        source.get("metadata", {}) if isinstance(source, dict) else {},
                    ]

                    payload = {}
                    if isinstance(source, dict):
                        payload_candidate = source.get("payload")
                        if isinstance(payload_candidate, dict):
                            payload = payload_candidate
                    if not payload and isinstance(source, dict) and "doc" in source:
                        doc_payload = source.get("doc", {}).get("payload")
                        if isinstance(doc_payload, dict):
                            payload = doc_payload
                    if not payload and isinstance(source, dict):
                        payload = source

                    if isinstance(payload, dict):
                        metadata_candidates.append(payload.get("metadata", {}))

                    for candidate in metadata_candidates:
                        if not isinstance(candidate, dict):
                            continue

                        candidate_sources: List[Dict[str, Any]] = [candidate]
                        nested_meta = candidate.get("metadata")
                        if isinstance(nested_meta, dict):
                            candidate_sources.append(nested_meta)

                        for meta_source in candidate_sources:
                            gender_val = meta_source.get("gender") or meta_source.get("gender_code")
                            if gender_val:
                                normalized_gender = normalize_value(gender_val)
                                if normalized_gender:
                                    doc_values[DemographicType.GENDER].add(normalized_gender)
                                    metadata_presence[DemographicType.GENDER] = True

                            age_group_val = meta_source.get("age_group")
                            if age_group_val:
                                normalized_age_group = normalize_value(age_group_val)
                                if normalized_age_group:
                                    doc_values[DemographicType.AGE].add(normalized_age_group)
                                    metadata_presence[DemographicType.AGE] = True

                            age_val = meta_source.get("age")
                            if age_val:
                                pre_count = len(doc_values[DemographicType.AGE])
                                add_age_decade(doc_values[DemographicType.AGE], age_val)
                                if len(doc_values[DemographicType.AGE]) > pre_count:
                                    metadata_presence[DemographicType.AGE] = True

                            birth_year_val = meta_source.get("birth_year")
                            if birth_year_val:
                                normalized_birth_year = normalize_value(birth_year_val)
                                if normalized_birth_year:
                                    doc_values[DemographicType.AGE].add(normalized_birth_year)
                                    metadata_presence[DemographicType.AGE] = True

                            occupation_val = meta_source.get("occupation") or meta_source.get("job")
                            if occupation_val:
                                normalized_occupation = normalize_value(occupation_val)
                                if normalized_occupation:
                                    doc_values[DemographicType.OCCUPATION].add(normalized_occupation)
                                    metadata_presence[DemographicType.OCCUPATION] = True

                            vehicle_val = meta_source.get("has_vehicle")
                            if vehicle_val:
                                normalized_vehicle = normalize_value(vehicle_val)
                                record_behavior("has_vehicle", parse_yes_no(normalized_vehicle))

                    if user_id:
                        qa_sources: List[List[Dict[str, Any]]] = []
                        if isinstance(source, dict):
                            qa_sources.append(source.get("qa_pairs", []) or [])
                        # ⭐ survey_responses_merged만 사용하므로 welcome_1st/welcome_2nd 배치 제거

                        for qa_pairs in qa_sources:
                            for qa in qa_pairs:
                                if not isinstance(qa, dict):
                                    continue
                                q_text_raw = str(qa.get("q_text", "")).lower()
                                q_text = normalize_value(qa.get("q_text"))
                                answer_candidates = [
                                    qa.get("answer"),
                                    qa.get("answer_text"),
                                    qa.get("value"),
                                ]
                                answers: List[str] = []
                                for candidate in answer_candidates:
                                    if candidate is None:
                                        continue
                                    if isinstance(candidate, list):
                                        answers.extend(str(item) for item in candidate if item)
                                    else:
                                        answers.append(str(candidate))
                                normalized_answers = {normalize_value(ans) for ans in answers if ans}

                                if q_text and normalized_answers:
                                    if any(keyword in q_text_raw for keyword in ("직업", "직무", "occupation")):
                                        # ⭐ occupation 정규화: 괄호 이전 부분만 추출
                                        # 예: "전문직 (의사, 간호사...)" → "전문직"
                                        cleaned_occupations = set()
                                        for ans in normalized_answers:
                                            # 괄호가 있으면 괄호 이전 부분만 사용
                                            if '(' in ans:
                                                cleaned = ans.split('(')[0].strip()
                                                if cleaned:
                                                    cleaned_occupations.add(cleaned)
                                            else:
                                                cleaned_occupations.add(ans)
                                        doc_values[DemographicType.OCCUPATION].update(cleaned_occupations)

                                    if (
                                        not metadata_presence[DemographicType.GENDER]
                                        and any(keyword in q_text_raw for keyword in ("성별", "gender"))
                                    ):
                                        doc_values[DemographicType.GENDER].update(normalized_answers)
                                        metadata_presence[DemographicType.GENDER] = True

                        # ⭐ 범용 behavioral 추출 (동적 - Claude가 요구한 조건만 추출)
                        if analysis.behavioral_conditions:
                            qa_pairs_list = source.get("qa_pairs", []) or []
                            for behavior_key in behavior_values.keys():
                                if behavior_values.get(behavior_key) is None:
                                    extracted_value = extract_behavior_from_qa_pairs(qa_pairs_list, behavior_key, debug=False)
                                    if extracted_value is not None:
                                        behavior_values[behavior_key] = extracted_value

                    return doc_values, metadata_presence, behavior_values

                # ⭐⭐⭐ 성능 최적화: QA pairs를 1번만 순회해서 occupation과 marital_status 추출
                def extract_from_qa_pairs_once(
                    source: Dict[str, Any],
                    needs_occupation: bool,
                    needs_marital: bool,
                    occupation_expected: Set[str],
                    marital_expected: Set[str]
                ) -> Tuple[Optional[str], Optional[str]]:
                    """QA pairs를 1번만 순회해서 occupation display와 marital_status 추출"""
                    display_occupation = None
                    marital_val = None

                    qa_sources: List[List[Dict[str, Any]]] = []
                    if isinstance(source, dict):
                        qa_sources.append(source.get("qa_pairs", []) or [])

                    for qa_pairs in qa_sources:
                        for qa in qa_pairs:
                            if not isinstance(qa, dict):
                                continue
                            q_text = str(qa.get("q_text", "")).lower()

                            # Occupation 찾기
                            if needs_occupation and not display_occupation:
                                if any(keyword in q_text for keyword in ("직업", "직무", "occupation", "직종")):
                                    answer = qa.get("answer")
                                    if answer is None:
                                        answer = qa.get("answer_text")
                                    if answer:
                                        candidate_value = str(answer)
                                        normalized_candidate = normalize_value(candidate_value)

                                        # ⭐⭐⭐ 부분 매칭: "영업직" → "영업" 포함 확인
                                        # 예: "무역•영업•판매•매장관리"에 "영업" 포함
                                        matched = False
                                        for expected in occupation_expected:
                                            # "직" 제거하여 핵심 키워드 추출
                                            keyword = expected.replace('직', '')
                                            if keyword and keyword in normalized_candidate:
                                                matched = True
                                                break

                                        if matched:
                                            display_occupation = candidate_value

                            # Marital status 찾기
                            if needs_marital and not marital_val:
                                if any(keyword in q_text for keyword in ("결혼", "혼인", "marital")):
                                    answer = qa.get("answer") or qa.get("answer_text")
                                    if answer:
                                        marital_val = str(answer)

                            # 둘 다 찾았으면 조기 종료
                            if (not needs_occupation or display_occupation) and (not needs_marital or marital_val):
                                break

                        # 둘 다 찾았으면 조기 종료
                        if (not needs_occupation or display_occupation) and (not needs_marital or marital_val):
                            break

                    return display_occupation, marital_val

                # ⭐⭐⭐⭐⭐ 메모리 캐시 기반 초고속 필터링! (Stage 1+2 통합)
                # 변수 초기화 (fallback 경로에서 사용)
                expected_values_cache = {}
                stage1_duration_ms = 0
                stage2_duration_ms = 0

                if panel_cache.loaded:
                    logger.info(f"\n{'='*60}")
                    logger.info(f"⚡ 메모리 캐시 기반 통합 필터링 (Stage 1+2 한방!)")
                    logger.info(f"{'='*60}")

                    filter_start = perf_counter()

                    # ⭐ Demographics 필터 추출
                    gender_filter = None
                    age_filter = None
                    region_filter = None
                    sub_region_filter = None
                    occupation_filter = None
                    marital_filter = None

                    if DemographicType.GENDER in demographic_filters:
                        values = [d.value for d in demographic_filters[DemographicType.GENDER]]
                        gender_filter = values[0] if values else None  # 첫번째 값 사용

                    if DemographicType.AGE in demographic_filters:
                        values = [d.value for d in demographic_filters[DemographicType.AGE]]
                        age_filter = values[0] if values else None

                    if DemographicType.REGION in demographic_filters:
                        values = [d.value for d in demographic_filters[DemographicType.REGION]]
                        region_filter = values[0] if values else None

                    if DemographicType.SUB_REGION in demographic_filters:
                        values = [d.value for d in demographic_filters[DemographicType.SUB_REGION]]
                        sub_region_filter = values[0] if values else None

                    if DemographicType.OCCUPATION in demographic_filters:
                        values = [d.value for d in demographic_filters[DemographicType.OCCUPATION]]
                        occupation_filter = values[0] if values else None

                    if DemographicType.MARITAL_STATUS in demographic_filters:
                        values = [d.value for d in demographic_filters[DemographicType.MARITAL_STATUS]]
                        marital_filter = values[0] if values else None

                    # ⭐⭐⭐ 한방 필터링! (Pandas 벡터화 - 0.1초 이하!)
                    filtered_df = panel_cache.filter_all(
                        gender=gender_filter,
                        age_group=age_filter,
                        region=region_filter,
                        sub_region=sub_region_filter,
                        occupation=occupation_filter,
                        marital_status=marital_filter,
                        behavioral_conditions=analysis.behavioral_conditions
                    )

                    filter_duration_ms = (perf_counter() - filter_start) * 1000
                    timings["post_filter_ms"] = filter_duration_ms

                    logger.info(f"  ✅ 메모리 캐시 필터링: {panel_cache.total_count}건 → {len(filtered_df)}건 ({filter_duration_ms:.2f}ms) 🚀")
                    logger.info(f"{'='*60}\n")

                    # ⭐ 필터링된 user_id로 full document 가져오기
                    filtered_user_ids = filtered_df['user_id'].tolist()
                    filtered_list = panel_cache.get_user_docs(filtered_user_ids)
                    filtered_rrf_results = filtered_list

                    logger.info(f"📊 필터링 통계:")
                    logger.info(f"  - 메모리 캐시 통합 필터링: {filter_duration_ms:.2f}ms")
                    logger.info(f"  - ✅ 최종 결과: {len(filtered_list)}건")
                    
                    # ⭐ SSE 스트리밍: Filter Breakdown 이벤트 전달
                    if stream_callback:
                        try:
                            breakdown_steps = []
                            before_count = len(rrf_results)
                            after_count = len(filtered_list)
                            
                            # Demographics 필터 단계
                            if demographic_filters:
                                demo_desc = ", ".join([
                                    f"{k.value}={v[0].value}" 
                                    for k, v in demographic_filters.items() 
                                    if v
                                ])
                                breakdown_steps.append({
                                    'filter': f'demographics ({demo_desc})',
                                    'removed': before_count - after_count,
                                    'remaining': after_count
                                })
                            
                            # Behavioral 필터 단계
                            if analysis.behavioral_conditions:
                                active_behavioral = {
                                    k: v for k, v in analysis.behavioral_conditions.items() 
                                    if v is not None
                                }
                                if active_behavioral:
                                    behav_desc = ", ".join([
                                        f"{k}={v}" for k, v in active_behavioral.items()
                                    ])
                                    breakdown_steps.append({
                                        'filter': f'behavioral ({behav_desc})',
                                        'removed': 0,  # 메모리 캐시는 통합 필터링이므로 개별 단계 추적 불가
                                        'remaining': after_count
                                    })
                            
                            if breakdown_steps:
                                stream_callback('filter_breakdown', {
                                    'steps': breakdown_steps,
                                    'total_removed': before_count - after_count,
                                    'final_count': after_count
                                })
                        except Exception as e:
                            logger.warning(f"⚠️ stream_callback 오류 (filter_breakdown): {e}")
                    
                    # ⭐ SSE 스트리밍: 필터링 후 개수 전달
                    if stream_callback:
                        try:
                            stream_callback('after_filter', {'count': len(filtered_list)})
                        except Exception as e:
                            logger.warning(f"⚠️ stream_callback 오류 (after_filter): {e}")

                else:
                    # ⭐⭐⭐ Fallback: 기존 Stage 1+2 로직 (메모리 캐시 없을 때)
                    logger.warning("  ⚠️ 메모리 캐시 미사용 → 기존 Stage 1+2 로직 실행 (느림)")

                    # ⭐⭐⭐ 성능 최적화: build_expected_values를 루프 밖에서 1번만 계산
                    # 각 demographic 타입별로 expected values 사전 계산
                    expected_values_cache = {}
                    for demo_type in filters_to_validate:
                        if demo_type in demographic_filters:
                            expected = set()
                            for demo in demographic_filters[demo_type]:
                                expected.update(build_expected_values(demo))
                            expected_values_cache[demo_type] = expected
                            logger.debug(f"  ✅ {demo_type.value} expected values 사전 계산: {expected}")

                    # ⭐⭐⭐ STAGE 1: Pandas 벡터화 필터링 (metadata만 - 초고속!)
                    logger.info(f"\n{'='*60}")
                    logger.info(f"⚡ STAGE 1: Pandas metadata 필터링 시작")
                    logger.info(f"{'='*60}")

                    stage1_start = perf_counter()
                    metadata_list = []
                    doc_id_to_doc = {}

                    debug_sample_count = 0
                    for doc in rrf_results:
                        user_id = doc_user_map.get(id(doc))
                        if not user_id:
                            continue

                        # ⭐ doc에서 직접 _source 가져오기
                        source = doc.get('_source', {})

                        if not isinstance(source, dict):
                            source = {}

                        # 디버깅: 처음 3개 샘플의 _source 구조 확인
                        if debug_sample_count < 3:
                            logger.info(f"  [DEBUG {debug_sample_count+1}] user_id={user_id}")
                            logger.info(f"     doc keys: {list(doc.keys())}")
                            logger.info(f"     _source keys: {list(source.keys()) if source else 'Empty'}")
                            logger.info(f"     _source content preview: {str(source)[:200]}...")

                        metadata = source.get("metadata", {}) if isinstance(source.get("metadata"), dict) else {}

                        # 디버깅: 처음 3개 샘플 로깅
                        if debug_sample_count < 3:
                            logger.info(f"     metadata keys: {list(metadata.keys()) if metadata else 'None'}")
                            logger.info(f"     gender: {metadata.get('gender')}, region: {metadata.get('region')}")
                            debug_sample_count += 1

                        # metadata 정보 저장
                        metadata_list.append({
                            'doc_id': id(doc),
                            'user_id': user_id,
                            'gender': metadata.get('gender') or metadata.get('gender_code'),
                            'age_group': metadata.get('age_group') or metadata.get('age'),
                            'region': metadata.get('region'),
                            'sub_region': metadata.get('sub_region'),
                        })
                        doc_id_to_doc[id(doc)] = (doc, source, metadata)

                    # Pandas DataFrame 생성
                    df = pd.DataFrame(metadata_list)
                    logger.info(f"  📊 전체 문서: {len(df)}건")

                    # Pandas 벡터화 필터링 (metadata만)
                    mask = pd.Series([True] * len(df))

                    # GENDER 필터
                    if DemographicType.GENDER in filters_to_validate:
                        expected_genders = expected_values_cache.get(DemographicType.GENDER, set())
                        # normalize + expand aliases
                        expanded_expected = set()
                        for g in expected_genders:
                            expanded_expected.add(g)
                            # gender aliases
                            if g == '남성': expanded_expected.update(['남자', '남', 'male', 'm', 'man', '남성형'])
                            if g == '여성': expanded_expected.update(['여자', '여', 'female', 'f', 'woman', '여성형'])

                        # DataFrame 값 정규화 및 필터링 (None 제외)
                        df['gender_normalized'] = df['gender'].apply(lambda x: normalize_value(x) if x else None)
                        gender_mask = df['gender_normalized'].notna() & df['gender_normalized'].isin(expanded_expected)
                        mask &= gender_mask
                        logger.info(f"  ✅ GENDER 필터: {expected_genders} → {mask.sum()}건 통과")

                    # AGE 필터
                    if DemographicType.AGE in filters_to_validate:
                        expected_ages = expected_values_cache.get(DemographicType.AGE, set())
                        df['age_normalized'] = df['age_group'].apply(lambda x: normalize_value(x) if x else None)
                        age_mask = df['age_normalized'].notna() & df['age_normalized'].isin(expected_ages)
                        mask &= age_mask
                        logger.info(f"  ✅ AGE 필터: {expected_ages} → {mask.sum()}건 통과")

                    # REGION 필터
                    if DemographicType.REGION in filters_to_validate:
                        expected_regions = expected_values_cache.get(DemographicType.REGION, set())
                        df['region_normalized'] = df['region'].apply(lambda x: normalize_value(x) if x else None)
                        region_mask = df['region_normalized'].notna() & df['region_normalized'].isin(expected_regions)
                        mask &= region_mask
                        logger.info(f"  ✅ REGION 필터: {expected_regions} → {mask.sum()}건 통과 (region 값 샘플: {df['region'].value_counts().head(5).to_dict()})")

                    # SUB_REGION 필터
                    if DemographicType.SUB_REGION in filters_to_validate:
                        expected_sub_regions = expected_values_cache.get(DemographicType.SUB_REGION, set())
                        df['sub_region_normalized'] = df['sub_region'].apply(lambda x: normalize_value(x) if x else None)
                        sub_region_mask = df['sub_region_normalized'].notna() & df['sub_region_normalized'].isin(expected_sub_regions)
                        mask &= sub_region_mask
                        logger.info(f"  ✅ SUB_REGION 필터: {expected_sub_regions} → {mask.sum()}건 통과")

                    # 필터링된 결과
                    candidate_df = df[mask]
                    stage1_duration_ms = (perf_counter() - stage1_start) * 1000
                    logger.info(f"\n⚡ STAGE 1 완료: {len(df)}건 → {len(candidate_df)}건 ({stage1_duration_ms:.2f}ms)")
                    logger.info(f"{'='*60}\n")

                    # ⭐⭐⭐ STAGE 2: qa_pairs/behavioral 체크 (필터링된 문서만)
                    logger.info(f"⚡ STAGE 2: qa_pairs/behavioral 필터링 시작 ({len(candidate_df)}건)")

                    stage2_start = perf_counter()
                    filtered_list = []

                    # 카운터 초기화
                    occupation_filter_failed = 0
                    occupation_metadata_missing = 0
                    marital_status_filter_failed = 0
                    marital_status_metadata_missing = 0
                    behavior_filter_failed = 0
                    behavior_metadata_missing = 0
                    debug_counter = 0

                    for _, row in candidate_df.iterrows():
                        doc, source, metadata = doc_id_to_doc[row['doc_id']]
                        user_id = row['user_id']

                        # ⭐ collect_doc_values로 qa_pairs와 behavior_values 수집
                        doc_values, metadata_presence, behavior_values = collect_doc_values(user_id, source, metadata, {})
                        behavior_values_map[user_id] = dict(behavior_values)

                        # ⭐⭐⭐ QA pairs에서 occupation과 marital_status 1번만 순회 추출
                        needs_occupation = DemographicType.OCCUPATION in filters_to_validate
                        needs_marital = DemographicType.MARITAL_STATUS in filters_to_validate
                        qa_occupation = None
                        qa_marital = None

                        if needs_occupation or needs_marital:
                            occupation_expected = expected_values_cache.get(DemographicType.OCCUPATION, set()) if needs_occupation else set()
                            marital_expected = expected_values_cache.get(DemographicType.MARITAL_STATUS, set()) if needs_marital else set()
                            qa_occupation, qa_marital = extract_from_qa_pairs_once(source, needs_occupation, needs_marital, occupation_expected, marital_expected)

                        # ⭐ Stage 1에서 이미 gender, age, region, sub_region 필터 통과했으므로
                        # Stage 2에서는 occupation, marital_status, behavioral만 체크

                        occupation_pass = True
                        marital_status_pass = True
                        behavior_pass = True

                        # ⭐⭐⭐ OCCUPATION 필터 (부분 매칭)
                        if needs_occupation:
                            expected = expected_values_cache.get(DemographicType.OCCUPATION, set())
                            actual_occupations = doc_values[DemographicType.OCCUPATION]

                            # ⭐ 부분 매칭: "영업직" → "영업" 포함 확인
                            occupation_pass = False
                            for actual in actual_occupations:
                                for expected_val in expected:
                                    # "직" 제거하여 핵심 키워드 추출
                                    keyword = expected_val.replace('직', '')
                                    if keyword and keyword in actual:
                                        occupation_pass = True
                                        break
                                if occupation_pass:
                                    break

                            if not occupation_pass:
                                occupation_filter_failed += 1
                            else:
                                # Display occupation 저장
                                if qa_occupation:
                                    occupation_display_map[user_id] = qa_occupation

                        # ⭐⭐⭐ MARITAL_STATUS 필터
                        if needs_marital:
                            expected = expected_values_cache.get(DemographicType.MARITAL_STATUS, set())
                            marital_val = metadata.get("marital_status") or qa_marital

                            if marital_val:
                                normalized_marital = normalize_value(marital_val)
                                if normalized_marital:
                                    marital_status_pass = values_match({normalized_marital}, expected)
                                    if not marital_status_pass:
                                        marital_status_filter_failed += 1
                                else:
                                    marital_status_pass = False
                                    marital_status_filter_failed += 1
                            else:
                                marital_status_metadata_missing += 1
                                marital_status_pass = False

                        # ⭐⭐⭐ BEHAVIORAL 필터
                        if analysis.behavioral_conditions:
                            for condition_key, expected_value in analysis.behavioral_conditions.items():
                                if expected_value is None:
                                    continue

                                actual_value = behavior_values.get(condition_key)
                                if actual_value is None:
                                    behavior_metadata_missing += 1
                                    behavior_pass = False
                                    break
                                if actual_value != expected_value:
                                    behavior_filter_failed += 1
                                    behavior_pass = False
                                    break

                        # ⭐ Stage 2 필터 검증 완료
                        all_pass = occupation_pass and marital_status_pass and behavior_pass

                        if all_pass:
                            filtered_list.append(doc)

                            # ⭐⭐⭐ Early Termination (제거됨 - 정확한 전체 카운트를 위해)
                            # if len(filtered_list) >= size * 3:
                            #     logger.info(f"  ⚡ Early termination: {len(filtered_list)}건 수집 완료")
                            #     break

                    # Stage 2 완료
                    stage2_duration_ms = (perf_counter() - stage2_start) * 1000
                    logger.info(f"\n⚡ STAGE 2 완료: {len(candidate_df)}건 → {len(filtered_list)}건 ({stage2_duration_ms:.2f}ms)")
                    logger.info(f"{'='*60}\n")

                    # 전체 필터링 시간
                    total_filter_duration_ms = stage1_duration_ms + stage2_duration_ms
                    timings["post_filter_ms"] = total_filter_duration_ms
                    filtered_rrf_results = filtered_list

                    # 통계 로그
                    logger.info(f"📊 필터링 통계:")
                    logger.info(f"  - Stage 1 (Pandas metadata): {stage1_duration_ms:.2f}ms")
                    logger.info(f"  - Stage 2 (qa_pairs/behavioral): {stage2_duration_ms:.2f}ms")
                    logger.info(f"  - 전체 필터링 시간: {total_filter_duration_ms:.2f}ms")
                    
                    # ⭐ SSE 스트리밍: Filter Breakdown 이벤트 전달 (Fallback 경로)
                    if stream_callback:
                        try:
                            breakdown_steps = []
                            before_count = len(rrf_results)
                            after_stage1 = len(candidate_df)
                            after_stage2 = len(filtered_list)
                            
                            # Stage 1 (Demographics) 필터 단계
                            if before_count != after_stage1:
                                demo_desc = ", ".join([
                                    f"{k.value}={v[0].value}" 
                                    for k, v in demographic_filters.items() 
                                    if v and k in filters_to_validate
                                ])
                                breakdown_steps.append({
                                    'filter': f'demographics ({demo_desc})',
                                    'removed': before_count - after_stage1,
                                    'remaining': after_stage1
                                })
                            
                            # Stage 2 (Behavioral/Occupation/Marital) 필터 단계
                            if after_stage1 != after_stage2:
                                stage2_filters = []
                                if DemographicType.OCCUPATION in filters_to_validate:
                                    stage2_filters.append('occupation')
                                if DemographicType.MARITAL_STATUS in filters_to_validate:
                                    stage2_filters.append('marital_status')
                                if analysis.behavioral_conditions:
                                    active_behavioral = {
                                        k: v for k, v in analysis.behavioral_conditions.items() 
                                        if v is not None
                                    }
                                    if active_behavioral:
                                        stage2_filters.append('behavioral')
                                
                                filter_desc = ", ".join(stage2_filters) if stage2_filters else 'qa_pairs'
                                breakdown_steps.append({
                                    'filter': f'{filter_desc}',
                                    'removed': after_stage1 - after_stage2,
                                    'remaining': after_stage2
                                })
                            
                            if breakdown_steps:
                                stream_callback('filter_breakdown', {
                                    'steps': breakdown_steps,
                                    'total_removed': before_count - after_stage2,
                                    'final_count': after_stage2
                                })
                        except Exception as e:
                            logger.warning(f"⚠️ stream_callback 오류 (filter_breakdown): {e}")
                    
                    # ⭐ SSE 스트리밍: 필터링 후 개수 전달
                    if stream_callback:
                        try:
                            stream_callback('after_filter', {'count': len(filtered_list)})
                        except Exception as e:
                            logger.warning(f"⚠️ stream_callback 오류 (after_filter): {e}")

                    if DemographicType.OCCUPATION in filters_to_validate:
                        logger.info(f"  - OCCUPATION 미충족: {occupation_filter_failed}건")
                    if DemographicType.MARITAL_STATUS in filters_to_validate:
                        logger.info(f"  - MARITAL_STATUS 미충족: {marital_status_filter_failed}건")
                    if analysis.behavioral_conditions:
                        logger.info(f"  - BEHAVIORAL 미충족: {behavior_filter_failed}건")

                logger.info(f"  ✅ 최종 결과: {len(filtered_rrf_results)}건")
        else:
            timings.setdefault('post_filter_ms', timings.get('post_filter_ms', 0.0))

            def collect_doc_values(
                user_id: str,
                source: Dict[str, Any],
                metadata: Dict[str, Any],
                _unused: Dict[str, Any],  # 호환성을 위해 유지하지만 사용하지 않음
            ) -> Tuple[Dict[DemographicType, Set[str]], Dict[DemographicType, bool], Dict[str, Optional[bool]]]:
                doc_values: Dict[DemographicType, Set[str]] = {
                    DemographicType.GENDER: set(),
                    DemographicType.AGE: set(),
                    DemographicType.OCCUPATION: set(),
                }
                metadata_presence: Dict[DemographicType, bool] = {
                    DemographicType.GENDER: False,
                    DemographicType.AGE: False,
                    DemographicType.OCCUPATION: False,
                }
                # ⭐ 동적 behavior_values 생성 (Claude가 추출한 조건 기반)
                behavior_values: Dict[str, Optional[bool]] = {}
                if analysis.behavioral_conditions:
                    for key in analysis.behavioral_conditions.keys():
                        behavior_values[key] = None

                def record_behavior(key: str, value: Optional[bool]) -> None:
                    if value is None:
                        return
                    if behavior_values.get(key) is None:
                        behavior_values[key] = value

                def parse_yes_no(text: Optional[str]) -> Optional[bool]:
                    if not text:
                        return None
                    normalized = text.lower()
                    if any(keyword in normalized for keyword in BEHAVIOR_NO_TOKENS):
                        return False
                    if any(keyword in normalized for keyword in BEHAVIOR_YES_TOKENS):
                        return True
                    return None

                def parse_smoker_answer(raw: Optional[Any]) -> Optional[bool]:
                    if raw is None:
                        return None
                    if isinstance(raw, (list, tuple, set)):
                        for item in raw:
                            decision = parse_smoker_answer(item)
                            if decision is not None:
                                return decision
                        return None
                    text = str(raw).strip()
                    if not text:
                        return None
                    normalized = text.lower()
                    compact = normalized.replace(" ", "")
                    for keyword in SMOKER_NEGATIVE_KEYWORDS:
                        keyword_compact = keyword.replace(" ", "")
                        if keyword in normalized or keyword_compact in compact:
                            return False
                    for keyword in SMOKER_POSITIVE_KEYWORDS:
                        keyword_compact = keyword.replace(" ", "")
                        if keyword in normalized or keyword_compact in compact:
                            return True
                    if (
                        "담배" in normalized
                        and not any(token in normalized for token in ("없", "안", "않", "no", "무", "미흡연"))
                    ):
                        return True
                    return parse_yes_no(text)

                metadata_candidates = [
                    metadata,  # 이제 단일 metadata만 사용
                    source.get("metadata", {}) if isinstance(source, dict) else {},
                ]

                payload = {}
                if isinstance(source, dict):
                    payload_candidate = source.get("payload")
                    if isinstance(payload_candidate, dict):
                        payload = payload_candidate
                if not payload and isinstance(source, dict) and "doc" in source:
                    doc_payload = source.get("doc", {}).get("payload")
                    if isinstance(doc_payload, dict):
                        payload = doc_payload
                if not payload and isinstance(source, dict):
                    payload = source

                if isinstance(payload, dict):
                    metadata_candidates.append(payload.get("metadata", {}))

                for candidate in metadata_candidates:
                    if not isinstance(candidate, dict):
                        continue

                    gender_val = candidate.get("gender") or candidate.get("gender_code")
                    if gender_val:
                        normalized_gender = normalize_value(gender_val)
                        if normalized_gender:
                            doc_values[DemographicType.GENDER].add(normalized_gender)
                            metadata_presence[DemographicType.GENDER] = True

                    age_group_val = candidate.get("age_group")
                    if age_group_val:
                        normalized_age_group = normalize_value(age_group_val)
                        if normalized_age_group:
                            doc_values[DemographicType.AGE].add(normalized_age_group)
                            metadata_presence[DemographicType.AGE] = True

                    age_val = candidate.get("age")
                    if age_val:
                        pre_count = len(doc_values[DemographicType.AGE])
                        add_age_decade(doc_values[DemographicType.AGE], age_val)
                        if len(doc_values[DemographicType.AGE]) > pre_count:
                            metadata_presence[DemographicType.AGE] = True

                    birth_year_val = candidate.get("birth_year")
                    if birth_year_val:
                        normalized_birth_year = normalize_value(birth_year_val)
                        if normalized_birth_year:
                            doc_values[DemographicType.AGE].add(normalized_birth_year)
                            metadata_presence[DemographicType.AGE] = True

                    occupation_val = candidate.get("occupation") or candidate.get("job")
                    if occupation_val:
                        normalized_occupation = normalize_value(occupation_val)
                        if normalized_occupation:
                            doc_values[DemographicType.OCCUPATION].add(normalized_occupation)
                            metadata_presence[DemographicType.OCCUPATION] = True

                    job_group_val = candidate.get("job_group") or candidate.get("occupation_group")
                    if job_group_val:
                        normalized_job_group = normalize_value(job_group_val)
                        if normalized_job_group:
                            doc_values[DemographicType.OCCUPATION].add(normalized_job_group)
                            metadata_presence[DemographicType.OCCUPATION] = True

                # QA 기반 보완 (직업) - 메타데이터가 비었을 때만 사용
                if not metadata_presence[DemographicType.OCCUPATION]:
                    qa_sources: List[List[Dict[str, Any]]] = []
                    if isinstance(source, dict):
                        qa_sources.append(source.get("qa_pairs", []) or [])
                    # ⭐ survey_responses_merged만 사용하므로 welcome_2nd_batch 제거

                    for qa_pairs in qa_sources:
                        for qa in qa_pairs:
                            if not isinstance(qa, dict):
                                continue
                            q_text = str(qa.get("q_text", "")).lower()
                            answer_text = qa.get("answer") or qa.get("answer_text")
                            if not answer_text:
                                continue
                            if any(keyword in q_text for keyword in ("직업", "직무", "occupation", "직종")):
                                normalized_answer = normalize_value(answer_text)
                                if normalized_answer:
                                    doc_values[DemographicType.OCCUPATION].add(normalized_answer)

                # ⭐ 범용 behavioral 추출 (동적 - Claude가 요구한 조건만 추출)
                if analysis.behavioral_conditions:
                    qa_pairs_list = source.get("qa_pairs", []) or []
                    for behavior_key in behavior_values.keys():
                        if behavior_values.get(behavior_key) is None:
                            extracted_value = extract_behavior_from_qa_pairs(qa_pairs_list, behavior_key)
                            if extracted_value is not None:
                                behavior_values[behavior_key] = extracted_value

                # Normalize
                for demo_type, values in doc_values.items():
                    normalized = {normalize_value(v) for v in values if v}
                    if demo_type == DemographicType.GENDER:
                        expand_gender_aliases(normalized)
                    doc_values[demo_type] = normalized

                return doc_values, metadata_presence, behavior_values

            filtered_list: List[Dict[str, Any]] = []
            source_not_found_count = 0
            gender_filter_failed = 0
            age_filter_failed = 0
            occupation_filter_failed = 0
            gender_metadata_missing = 0
            age_metadata_missing = 0
            occupation_metadata_missing = 0
            behavior_filter_failed = 0
            behavior_metadata_missing = 0
            for doc in rrf_results:
                source = doc.get("_source")
                if not source and "doc" in doc:
                    source = doc.get("doc", {}).get("_source")
                if not source and "payload" in doc:
                    source = doc.get("payload")

                if not isinstance(source, dict):
                    source_not_found_count += 1
                    continue

                user_id = source.get("user_id") or doc.get("_id") or doc.get("id")
                if not user_id and "payload" in doc and isinstance(doc["payload"], dict):
                    user_id = doc["payload"].get("user_id")

                if not user_id:
                    source_not_found_count += 1
                    continue

                # ⭐ survey_responses_merged만 사용하므로 welcome_1st/welcome_2nd 배치 제거
                # source에서 직접 metadata 가져오기
                metadata = source.get("metadata", {}) if isinstance(source.get("metadata"), dict) else {}

                doc_values, metadata_presence, behavior_values = collect_doc_values(user_id, source, metadata, {})
                behavior_values_map[user_id] = dict(behavior_values)

                gender_pass = True
                age_pass = True
                occupation_pass = True

                if demographic_filters.get(DemographicType.GENDER):
                    expected = set()
                    for demo in demographic_filters[DemographicType.GENDER]:
                        expected.update(build_expected_values(demo))

                    # ⭐ Metadata 우선: metadata가 있으면 metadata만 확인
                    if metadata_presence[DemographicType.GENDER]:
                        # metadata로 수집된 값만 사용
                        gender_from_metadata = set()
                        for meta_source in [metadata, source.get("metadata", {})]:
                            if isinstance(meta_source, dict):
                                gender_val = meta_source.get("gender") or meta_source.get("gender_code")
                                if gender_val:
                                    normalized_gender = normalize_value(gender_val)
                                    if normalized_gender:
                                        gender_from_metadata.add(normalized_gender)

                        if gender_from_metadata:
                            expand_gender_aliases(gender_from_metadata)
                            gender_pass = values_match(gender_from_metadata, expected)
                        else:
                            gender_metadata_missing += 1
                            gender_pass = False
                    else:
                        # metadata 없으면 qa_pairs 사용
                        gender_metadata_missing += 1
                        gender_pass = values_match(doc_values[DemographicType.GENDER], expected)

                    if not gender_pass:
                        gender_filter_failed += 1

                if gender_pass and demographic_filters.get(DemographicType.AGE):
                    expected = set()
                    for demo in demographic_filters[DemographicType.AGE]:
                        expected.update(build_expected_values(demo))
                    if not metadata_presence[DemographicType.AGE]:
                        age_metadata_missing += 1
                    age_pass = values_match(doc_values[DemographicType.AGE], expected)
                    if not age_pass:
                        age_filter_failed += 1

                if gender_pass and age_pass and demographic_filters.get(DemographicType.OCCUPATION):
                    expected = set()
                    for demo in demographic_filters[DemographicType.OCCUPATION]:
                        expected.update(build_expected_values(demo))
                    if not metadata_presence[DemographicType.OCCUPATION]:
                        occupation_metadata_missing += 1
                    occupation_pass = values_match(doc_values[DemographicType.OCCUPATION], expected)
                    if not occupation_pass:
                        occupation_filter_failed += 1
                    else:
                        display_occupation = None
                        occupation_candidates = [
                            metadata.get("occupation") if isinstance(metadata, dict) else None,
                            metadata.get("job") if isinstance(metadata, dict) else None,
                            metadata.get("occupation_group") if isinstance(metadata, dict) else None,
                        ]
                        for candidate in occupation_candidates:
                            normalized_candidate = normalize_value(candidate)
                            if normalized_candidate and values_match({normalized_candidate}, expected):
                                display_occupation = str(candidate)
                                break
                        if not display_occupation:
                            qa_sources: List[List[Dict[str, Any]]] = []
                            if isinstance(source, dict):
                                qa_sources.append(source.get("qa_pairs", []) or [])
                            # ⭐ survey_responses_merged만 사용하므로 welcome_2nd_doc_full 제거
                            for qa_pairs in qa_sources:
                                for qa in qa_pairs:
                                    if not isinstance(qa, dict):
                                        continue
                                    q_text = str(qa.get("q_text", "")).lower()
                                    if not any(keyword in q_text for keyword in ("직업", "직무", "occupation", "직종")):
                                        continue
                                    answer = qa.get("answer")
                                    if answer is None:
                                        answer = qa.get("answer_text")
                                    if answer is None:
                                        continue
                                    candidate = str(answer)
                                    normalized_candidate = normalize_value(candidate)
                                    if normalized_candidate and values_match({normalized_candidate}, expected):
                                        display_occupation = candidate
                                        break
                                if display_occupation:
                                    break
                        if display_occupation:
                            occupation_display_map[user_id] = display_occupation

                # ⭐ Behavioral 검증: OpenSearch는 후보를 넓게 가져오고, Python에서 정확히 검증
                behavior_pass = True
                if analysis.behavioral_conditions:
                    for condition_key, expected_value in analysis.behavioral_conditions.items():
                        # ⭐ expected_value=None은 "이 조건을 체크하지 않음"을 의미 → 스킵
                        if expected_value is None:
                            continue

                        actual_value = behavior_values.get(condition_key)
                        if actual_value is None:
                            behavior_metadata_missing += 1
                            behavior_pass = False
                            break
                        if actual_value != expected_value:
                            behavior_filter_failed += 1
                            behavior_pass = False
                            break

                if gender_pass and age_pass and occupation_pass and behavior_pass:
                    filtered_list.append(doc)

            filter_duration_ms = (perf_counter() - filter_start) * 1000
            timings["post_filter_ms"] = filter_duration_ms
            filtered_rrf_results = filtered_list

            logger.info(f"  - 소스 누락 문서: {source_not_found_count}건")
            if demographic_filters.get(DemographicType.GENDER):
                logger.info(f"  - 성별 metadata 없음: {gender_metadata_missing}건")
            logger.info(f"  - 성별 필터 미충족: {gender_filter_failed}건")
            if demographic_filters.get(DemographicType.AGE):
                logger.info(f"  - 연령 metadata 없음: {age_metadata_missing}건")
            logger.info(f"  - 연령 필터 미충족: {age_filter_failed}건")
            if demographic_filters.get(DemographicType.OCCUPATION):
                logger.info(f"  - 직업 metadata 없음: {occupation_metadata_missing}건")
            logger.info(f"  - 직업 필터 미충족: {occupation_filter_failed}건")
            logger.info(f"  - 필터 조건 충족 문서: {len(filtered_rrf_results)}건")
            if analysis.behavioral_conditions:
                logger.info(f"  ✅ 행동 필터 검증 완료")
                logger.info(f"  - 행동 정보 없음: {behavior_metadata_missing}건")
                logger.info(f"  - 행동 필터 미충족: {behavior_filter_failed}건")

        lazy_join_start = perf_counter()
        # ⭐ 응답 단계: 사용자가 요청한 page_size만큼만 반환
        # window_size는 내부 검색/필터링용으로만 사용 (충분한 후보 확보)
        final_hits = filtered_rrf_results[:page_size]
        results: List[SearchResult] = []
        inner_hits_map: Dict[str, List[Dict[str, Any]]] = {}

        for doc in final_hits:
            source = doc.get("_source")
            if not source and "doc" in doc:
                source = doc.get("doc", {}).get("_source")
            if not source and "payload" in doc:
                source = doc.get("payload")
            if not isinstance(source, dict):
                source = {}

            payload = {}
            payload_candidate = source.get("payload")
            if isinstance(payload_candidate, dict):
                payload = payload_candidate
            elif isinstance(doc.get("payload"), dict):
                payload = doc["payload"]

            user_id = (
                source.get("user_id")
                or payload.get("user_id")
                or doc.get("_id")
                or doc.get("id")
            )

            doc_info = None
            if user_id and user_id in user_doc_map:
                doc_info = user_doc_map[user_id]
            elif doc.get("_id") and doc.get("_id") in id_doc_map:
                doc_info = id_doc_map[doc.get("_id")]

            if doc_info:
                src_info = doc_info.get("source")
                if isinstance(src_info, dict):
                    # ⭐ 순서 변경: Qdrant 데이터 먼저, OpenSearch 데이터로 덮어쓰기
                    # 이렇게 하면 metadata, qa_pairs 등이 보존됨
                    merged_source = {}
                    merged_source.update(source)      # Qdrant: user_id, text
                    merged_source.update(src_info)    # OpenSearch: metadata, qa_pairs (우선)
                    source = merged_source
                    
                inner_hit_wrapper = {"inner_hits": doc_info.get("inner_hits", {})}
            else:
                inner_hit_wrapper = doc

            # ⭐ survey_responses_merged만 사용하므로 source의 metadata에서 직접 가져오기
            source_metadata = source.get("metadata", {}) if isinstance(source, dict) else {}
            if not source_metadata:
                # source가 비어있으면 doc에서 직접 가져오기
                doc_source = doc.get("_source") or {}
                if isinstance(doc_source, dict):
                    source_metadata = doc_source.get("metadata", {})
                # payload에서도 확인
                if not source_metadata and isinstance(payload, dict):
                    source_metadata = payload.get("metadata", {})

            behavioral_values = behavior_values_map.get(user_id, {}) if user_id else {}
            behavioral_info: Dict[str, Any] = {}
            if behavioral_values.get("smoker") is not None:
                behavioral_info["smoker"] = behavioral_values.get("smoker")
            if behavioral_values.get("has_vehicle") is not None:
                behavioral_info["has_vehicle"] = behavioral_values.get("has_vehicle")
            if behavioral_values.get("drinker") is not None:
                behavioral_info["drinker"] = behavioral_values.get("drinker")

            demographic_info: Dict[str, Any] = {}
            if source_metadata:
                demographic_info["age_group"] = source_metadata.get("age_group")
                demographic_info["gender"] = source_metadata.get("gender")
                demographic_info["birth_year"] = source_metadata.get("birth_year")
                demographic_info["region"] = source_metadata.get("region")
                demographic_info["occupation"] = source_metadata.get("occupation")
                demographic_info["marital_status"] = source_metadata.get("marital_status")
                demographic_info["sub_region"] = source_metadata.get("sub_region")
                demographic_info["panel"] = source_metadata.get("panel")

            occupation_expected = set()
            for demo in demographic_filters.get(DemographicType.OCCUPATION, []):
                occupation_expected.update(build_expected_values(demo))

            if ("occupation" not in demographic_info or not demographic_info["occupation"]) and user_id:
                mapped_occupation = occupation_display_map.get(user_id) if has_demographic_filters else None
                if mapped_occupation:
                    demographic_info["occupation"] = mapped_occupation

            def occupation_matches(candidate: str) -> bool:
                normalized_candidate = normalize_value(candidate)
                if not normalized_candidate:
                    return False
                for expected in occupation_expected:
                    if not expected:
                        continue
                    if normalized_candidate == expected or normalized_candidate in expected or expected in normalized_candidate:
                        return True
                return False

            if ("occupation" not in demographic_info or not demographic_info["occupation"]) and isinstance(source, dict):
                qa_pairs_for_occ = source.get("qa_pairs", [])
                for qa in qa_pairs_for_occ:
                    if not isinstance(qa, dict):
                        continue
                    q_text = str(qa.get("q_text", "")).lower()
                    answer = qa.get("answer")
                    if answer is None:
                        answer = qa.get("answer_text")
                    if answer is None:
                        continue
                    answer_str = str(answer)
                    if any(keyword in q_text for keyword in ("직업", "직무", "occupation", "직종")) and occupation_matches(answer_str):
                        demographic_info["occupation"] = answer_str
                        break

            # marital_status를 qa_pairs에서 찾기
            if ("marital_status" not in demographic_info or not demographic_info["marital_status"]) and isinstance(source, dict):
                qa_pairs_list = source.get("qa_pairs", [])
                for qa in qa_pairs_list:
                    if not isinstance(qa, dict):
                        continue
                    q_text = str(qa.get("q_text", "")).lower()
                    answer = qa.get("answer")
                    if answer is None:
                        answer = qa.get("answer_text")
                    if answer is None:
                        continue
                    answer_str = str(answer)
                    if any(keyword in q_text for keyword in ("결혼", "혼인")):
                        demographic_info["marital_status"] = answer_str
                        break

            # sub_region을 qa_pairs에서 찾기
            if ("sub_region" not in demographic_info or not demographic_info["sub_region"]) and isinstance(source, dict):
                qa_pairs_list = source.get("qa_pairs", [])
                for qa in qa_pairs_list:
                    if not isinstance(qa, dict):
                        continue
                    q_text = str(qa.get("q_text", "")).lower()
                    answer = qa.get("answer")
                    if answer is None:
                        answer = qa.get("answer_text")
                    if answer is None:
                        continue
                    answer_str = str(answer)
                    if any(keyword in q_text for keyword in ("구", "군", "세부지역", "어느 구")):
                        demographic_info["sub_region"] = answer_str
                        break

            matched_qa_pairs: List[Dict[str, Any]] = extract_inner_hit_matches(inner_hit_wrapper)
            if not matched_qa_pairs and analysis.must_terms:
                matched_qa_pairs = extract_matched_qa_pairs(source, analysis.must_terms)

            # ⭐ 개선: source에 qa_pairs가 없으면 doc에서 직접 가져오기
            qa_pairs_from_source = source.get("qa_pairs", []) if isinstance(source, dict) else []
            if not qa_pairs_from_source:
                # source가 비어있으면 doc에서 직접 가져오기
                doc_source = doc.get("_source") or {}
                if isinstance(doc_source, dict):
                    qa_pairs_from_source = doc_source.get("qa_pairs", [])
                # payload에서도 확인
                if not qa_pairs_from_source and isinstance(payload, dict):
                    qa_pairs_from_source = payload.get("qa_pairs", [])

            qa_pairs_display = reorder_with_matches(
                qa_pairs_from_source if isinstance(qa_pairs_from_source, list) else [],
                matched_qa_pairs,
                limit=10
            )

            # survey_datetime 추출 (metadata에서)
            survey_datetime = None
            if source_metadata and isinstance(source_metadata, dict):
                survey_datetime = source_metadata.get("survey_datetime")
            elif isinstance(source, dict):
                metadata = source.get("metadata", {})
                if isinstance(metadata, dict):
                    survey_datetime = metadata.get("survey_datetime")

            results.append(
                SearchResult(
                    user_id=user_id,
                    score=doc.get("_score", 0.0),
                    timestamp=source.get("timestamp") if isinstance(source, dict) else None,
                    survey_datetime=survey_datetime,
                    demographic_info=demographic_info if demographic_info else None,
                    behavioral_info=behavioral_info if behavioral_info else None,
                    qa_pairs=qa_pairs_display[:5],
                    matched_qa_pairs=matched_qa_pairs,
                    highlights=doc.get("highlight"),
                )
            )
           

        timings["lazy_join_ms"] = (perf_counter() - lazy_join_start) * 1000
        timings.setdefault('post_filter_ms', timings.get('post_filter_ms', 0.0))
        timings.setdefault('rrf_recombination_ms', 0.0)
        timings.setdefault('qdrant_parallel_ms', 0.0)
        timings.setdefault(
            'opensearch_parallel_ms',
            timings.get('two_phase_stage1_ms', 0.0) + timings.get('two_phase_stage2_ms', 0.0)
        )

        total_duration_ms = (perf_counter() - overall_start) * 1000
        timings['total_ms'] = total_duration_ms
        timings['cache_hit'] = 1.0 if cache_hit else 0.0

        serialized_results = [_serialize_result(res) for res in results]
        stored_items = serialized_results
        if cache_enabled and cache_limit > 0:
            stored_items = serialized_results[:cache_limit]
        page_results, has_more_local = _slice_results(stored_items, page, page_size)
        # ⭐ total_hits는 이미 위에서 계산됨 (len(results))
        has_more = has_more_local and ((page * page_size) < total_hits)
        max_score = results[0].score if results else 0.0
        response_took_ms = int(total_duration_ms)

        logger.info("📈 성능 측정 요약 (ms):")
        for key in sorted(timings.keys()):
            logger.info(f"  - {key}: {timings[key]:.2f}")

        summary_parts = [
            f"returned={len(page_results)}건, 필터링후={len(filtered_rrf_results)}건, 전체={total_hits}건",
            f"total_ms={response_took_ms}",
        ]
        if rrf_k_used is not None:
            summary_parts.append(f"rrf_k={rrf_k_used}")
        if adaptive_threshold is not None:
            summary_parts.append(f"qdrant_threshold={adaptive_threshold:.2f}")
        logger.info("✅ 최종 요약: " + ", ".join(summary_parts))
        if rrf_reason:
            logger.info(f"   • RRF: {rrf_reason}")
        if threshold_reason:
            logger.info(f"   • Qdrant: {threshold_reason}")

        _log_final_summary(
            stage="search_nl",
            query=request.query,
            analysis=analysis,
            total_hits=total_hits,
            returned_count=len(page_results),
            page=page,
            page_size=page_size,
            cache_hit=cache_hit,
            timings=timings,
            took_ms=total_duration_ms,
            filters=filters_for_response,
            behavioral_conditions=getattr(analysis, "behavioral_conditions", {}),
            use_claude=use_claude,
        )

        # ⭐ total_hits: 필터링을 통과한 전체 결과 수
        total_hits = len(filtered_rrf_results) if filtered_rrf_results else 0

        # ⭐ requested_count 설정:
        # - 쿼리에서 size가 명시되면 (예: "전문직 100명") → requested_size 값
        #   단, 실제 반환된 결과 수(total_hits)보다 크면 total_hits로 제한
        # - size가 없으면 (예: "전문직") → page_size 사용 (기본값 30000)
        if requested_size is not None and requested_size > 0:
            # 실제 반환된 결과 수를 초과하지 않도록 제한
            requested_count = min(requested_size, total_hits)
        else:
            # size가 없으면 page_size 사용 (기본값 30000)
            requested_count = min(page_size, total_hits)
        
        if cache_enabled and cache_key and stored_items:
            # ⭐ 1차: 메모리 캐시에 즉시 저장 (전체 정보, 다음 요청부터 0.001초!)
            cache_payload_for_memory = {
                "total_hits": total_hits,
                "max_score": max_score,
                "items": stored_items,  # ✅ qa_pairs 포함 (전체)
                "page_size": page_size,
                "filters": filters_for_response,
                "extracted_entities": extracted_entities.to_dict(),
                "behavioral_conditions": getattr(analysis, "behavioral_conditions", {}),
                "use_claude": bool(use_claude),
                "requested_count": requested_count,
            }
            memory_cache[cache_key] = cache_payload_for_memory
            logger.info(f"✅ 메모리 캐시 저장 완료: {len(stored_items)}건 (전체 정보)")

            # ⭐⭐⭐ 경량화: Redis 저장용 (qa_pairs, matched_qa_pairs, highlights 제외)
            lightweight_items = []
            for item in stored_items:
                lightweight_item = {k: v for k, v in item.items() if k not in ['qa_pairs', 'matched_qa_pairs', 'highlights']}
                lightweight_items.append(lightweight_item)

            # ⭐ 2차: 백그라운드에서 Redis 압축 저장 (경량화, 영구 보존)
            background_tasks.add_task(
                save_search_cache_compressed,
                cache_key,
                cache_ttl,
                total_hits,
                max_score,
                lightweight_items,  # ⭐ 경량화된 버전!
                page_size,
                filters_for_response,
                extracted_entities.to_dict(),
                getattr(analysis, "behavioral_conditions", {}),
                bool(use_claude),
                requested_count,
            )
            logger.info(f"⏳ Redis 캐시 저장 예약 (백그라운드, 경량화): {len(lightweight_items)}건")
        
        response = SearchResponse(
            requested_count=requested_count,
            query=request.query,
            session_id=getattr(request, "session_id", None),
            total_hits=total_hits,
            max_score=max_score,
            results=page_results,
            query_analysis={
                "intent": analysis.intent,
                "must_terms": analysis.must_terms,
                "should_terms": analysis.should_terms,
                "alpha": analysis.alpha,
                "confidence": analysis.confidence,
                "size": len(page_results),  # 실제 반환된 결과 수
                "timings_ms": timings,
                "extracted_entities": extracted_entities.to_dict(),
                "behavioral_conditions": getattr(analysis, "behavioral_conditions", {}),
                "use_claude_analyzer": bool(use_claude),
            },
            took_ms=response_took_ms,
            page=page,
            page_size=page_size,
            has_more=has_more,
        )
        return _finalize_search_response(
            request=request,
            response=response,
            analysis=analysis,
            cache_hit=cache_hit,
            timings=timings,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[ERROR] 자연어 검색 중 오류: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------
# 프론트엔드 친화적 간소화 엔드포인트
# -----------------------------




class TestFiltersRequest(BaseModel):
    """필터 테스트 요청"""
    filters: List[Dict[str, Any]] = Field(..., description="테스트할 필터 리스트")
    index_name: str = Field(default="*", description="검색할 인덱스 이름")


@router.post("/debug/test-filters", summary="필터 개별 테스트 (디버깅용)")
async def test_filters(
    request: TestFiltersRequest,
    os_client: OpenSearch = Depends(lambda: router.os_client),
):
    """
    필터를 개별적으로 테스트하여 어떤 인덱스에서 작동하는지 확인
    
    - 각 필터를 개별적으로 실행
    - 인덱스별 결과 개수 확인
    
    사용 예시:
    ```json
    {
      "filters": [
        {
          "bool": {
            "should": [
              {"term": {"metadata.age_group.keyword": "30대"}}
            ],
            "minimum_should_match": 1
          }
        }
      ],
      "index_name": "*"
    }
    ```
    """
    try:
        if not os_client or not os_client.ping():
            raise HTTPException(status_code=503, detail="OpenSearch 서버에 연결할 수 없습니다.")
        
        filters = request.filters
        index_name = request.index_name
        
        results = []
        for i, filter_dict in enumerate(filters):
            # 각 필터를 개별적으로 테스트
            query = {
                "query": {
                    "bool": {
                        "must": [filter_dict]
                    }
                },
                "size": 0,  # 개수만 확인
                "aggs": {
                    "by_index": {
                        "terms": {
                            "field": "_index",
                            "size": 20
                        }
                    }
                }
            }
            
            response = os_client.search(
                index=request.index_name,
                body=query
            )
            
            # 인덱스별 결과 개수
            index_counts = {}
            if 'aggregations' in response and 'by_index' in response['aggregations']:
                for bucket in response['aggregations']['by_index']['buckets']:
                    index_counts[bucket['key']] = bucket['doc_count']
            
            results.append({
                "filter_index": i,
                "filter": filter_dict,
                "total_hits": response['hits']['total']['value'],
                "index_counts": index_counts
            })
        
        # 모든 필터를 AND로 결합한 결과도 테스트
        if len(filters) > 1:
            combined_query = {
                "query": {
                    "bool": {
                        "must": filters
                    }
                },
                "size": 0,
                "aggs": {
                    "by_index": {
                        "terms": {
                            "field": "_index",
                            "size": 20
                        }
                    }
                }
            }
            
            combined_response = os_client.search(
                index=request.index_name,
                body=combined_query
            )
            
            combined_index_counts = {}
            if 'aggregations' in combined_response and 'by_index' in combined_response['aggregations']:
                for bucket in combined_response['aggregations']['by_index']['buckets']:
                    combined_index_counts[bucket['key']] = bucket['doc_count']
            
            results.append({
                "filter_index": "combined",
                "filter": "ALL FILTERS (AND)",
                "total_hits": combined_response['hits']['total']['value'],
                "index_counts": combined_index_counts
            })
        
        return {
            "index_name": request.index_name,
            "results": results
        }
    
    except Exception as e:
        logger.error(f"[ERROR] 필터 테스트 중 오류: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/nl/stream",
    summary="자연어 검색 실시간 스트리밍 (SSE)",
)
async def search_natural_language_stream(
    query: str = Query(..., description="자연어 쿼리"),
    index_name: str = Query(default="survey_responses_merged", description="검색할 인덱스 이름"),
    size: int = Query(default=10, ge=1, le=50000, description="반환할 결과 개수"),
    use_vector_search: bool = Query(default=True, description="벡터 검색 사용 여부"),
    page: int = Query(default=1, ge=1, description="페이지 번호"),
    session_id: Optional[str] = Query(default=None, description="세션 ID"),
    os_client: OpenSearch = Depends(lambda: router.os_client),
):
    """
    검색 과정을 실시간으로 스트리밍하는 SSE 엔드포인트
    
    알파값과 필터링 전후 개수만 실시간으로 전송합니다.
    """
    async def event_generator():
        try:
            import asyncio
            import time

            # ⭐ 실시간 스트리밍을 위한 큐
            event_queue = asyncio.Queue()

            # 타이밍 추적
            timings = {}
            start_time = time.time()

            # 콜백 함수 정의 (즉시 큐에 넣음!)
            def stream_callback(event_type: str, data: dict):
                """스트리밍 콜백: 이벤트를 즉시 큐에 추가"""
                try:
                    # 비동기 큐에 넣기 (thread-safe)
                    asyncio.create_task(event_queue.put((event_type, data)))
                except:
                    pass

            # ⭐⭐⭐ 1. 시작 이벤트
            yield f"data: {json.dumps({'event': 'start', 'query': query, 'timestamp': int(time.time())}, ensure_ascii=False)}\n\n"
            yield f"data: {json.dumps({'event': 'progress', 'step': 1, 'total': 8, 'stage': '초기화'}, ensure_ascii=False)}\n\n"

            # 검색 요청 생성
            search_request = NLSearchRequest(
                query=query,
                index_name=index_name,
                size=size,
                use_vector_search=use_vector_search,
                page=page,
                session_id=session_id,
                log_conversation=False,
                log_search_history=False,
            )

            # ⭐⭐⭐ 2. 캐시 확인 (일단 false로 - 실제 구현은 search 함수에서)
            cache_start = time.time()
            yield f"data: {json.dumps({'event': 'cache_check', 'cache_hit': False}, ensure_ascii=False)}\n\n"
            yield f"data: {json.dumps({'event': 'timing', 'stage': 'cache_check', 'ms': round((time.time() - cache_start) * 1000, 2)}, ensure_ascii=False)}\n\n"

            # ⭐⭐⭐ 검색 실행을 비동기 태스크로
            from fastapi import BackgroundTasks
            background_tasks = BackgroundTasks()

            search_task = asyncio.create_task(
                search_natural_language(
                    search_request,
                    background_tasks,
                    os_client,
                    stream_callback=stream_callback
                )
            )

            # 이벤트 처리 변수
            step = 2
            query_analysis_sent = False
            filters_sent = False
            opensearch_sent = False
            qdrant_sent = False
            rrf_sent = False
            filter_before_sent = False
            filter_after_sent = False

            # ⭐⭐⭐ 큐에서 이벤트를 실시간으로 처리
            while True:
                try:
                    # 검색이 완료되었는지 확인
                    if search_task.done():
                        # 남은 이벤트 처리
                        while not event_queue.empty():
                            event_type, data = await asyncio.wait_for(event_queue.get(), timeout=0.1)

                            # 이벤트 처리 로직 (아래 참조)
                            if event_type == 'query_analysis' and not query_analysis_sent:
                                yield f"data: {json.dumps({'event': 'progress', 'step': step, 'total': 8, 'stage': '쿼리 분석'}, ensure_ascii=False)}\n\n"
                                step += 1
                                yield f"data: {json.dumps({'event': 'query_analysis', **data}, ensure_ascii=False)}\n\n"
                                query_analysis_sent = True

                            elif event_type in ['demographics', 'behavioral_conditions'] and not filters_sent:
                                if event_type == 'demographics':
                                    demo_data = data
                                else:
                                    behav_data = data

                                # 둘 다 모였을 때
                                if 'demo_data' in locals() and 'behav_data' in locals():
                                    yield f"data: {json.dumps({'event': 'progress', 'step': step, 'total': 8, 'stage': '필터 추출'}, ensure_ascii=False)}\n\n"
                                    step += 1
                                    yield f"data: {json.dumps({'event': 'filters_extracted', 'demographics': demo_data.get('demographics', []), 'behavioral': behav_data.get('behavioral_conditions', {})}, ensure_ascii=False)}\n\n"
                                    filters_sent = True

                            elif event_type == 'opensearch_results' and not opensearch_sent:
                                yield f"data: {json.dumps({'event': 'progress', 'step': step, 'total': 8, 'stage': '키워드 검색'}, ensure_ascii=False)}\n\n"
                                step += 1
                                yield f"data: {json.dumps({'event': 'opensearch_search_start'}, ensure_ascii=False)}\n\n"
                                yield f"data: {json.dumps({'event': 'opensearch_results', **data}, ensure_ascii=False)}\n\n"
                                opensearch_sent = True

                            elif event_type == 'qdrant_results' and not qdrant_sent:
                                yield f"data: {json.dumps({'event': 'progress', 'step': step, 'total': 8, 'stage': '의미 검색'}, ensure_ascii=False)}\n\n"
                                step += 1
                                yield f"data: {json.dumps({'event': 'qdrant_search_start'}, ensure_ascii=False)}\n\n"
                                yield f"data: {json.dumps({'event': 'qdrant_results', **data}, ensure_ascii=False)}\n\n"
                                qdrant_sent = True

                            elif event_type == 'rrf_fusion' and not rrf_sent:
                                yield f"data: {json.dumps({'event': 'progress', 'step': step, 'total': 8, 'stage': '결과 결합'}, ensure_ascii=False)}\n\n"
                                step += 1
                                yield f"data: {json.dumps({'event': 'rrf_fusion', **data}, ensure_ascii=False)}\n\n"
                                rrf_sent = True

                            elif event_type == 'filter_breakdown':
                                yield f"data: {json.dumps({'event': 'filter_breakdown', **data}, ensure_ascii=False)}\n\n"
                            
                            elif event_type == 'before_filter' and not filter_before_sent:
                                yield f"data: {json.dumps({'event': 'progress', 'step': step, 'total': 8, 'stage': '필터링'}, ensure_ascii=False)}\n\n"
                                step += 1
                                yield f"data: {json.dumps({'event': 'before_filter', **data}, ensure_ascii=False)}\n\n"
                                filter_before_sent = True

                            elif event_type == 'filter_breakdown':
                                yield f"data: {json.dumps({'event': 'filter_breakdown', **data}, ensure_ascii=False)}\n\n"

                            elif event_type == 'after_filter' and not filter_after_sent:
                                yield f"data: {json.dumps({'event': 'after_filter', **data}, ensure_ascii=False)}\n\n"
                                filter_after_sent = True

                        break

                    # 큐에서 이벤트 가져오기 (타임아웃 0.1초)
                    event_type, data = await asyncio.wait_for(event_queue.get(), timeout=0.1)

                    # ⭐ 즉시 처리!
                    if event_type == 'query_analysis' and not query_analysis_sent:
                        yield f"data: {json.dumps({'event': 'progress', 'step': step, 'total': 8, 'stage': '쿼리 분석'}, ensure_ascii=False)}\n\n"
                        step += 1
                        stage_start = time.time()
                        yield f"data: {json.dumps({'event': 'query_analysis', **data}, ensure_ascii=False)}\n\n"
                        yield f"data: {json.dumps({'event': 'timing', 'stage': 'query_analysis', 'ms': round((time.time() - stage_start) * 1000, 2)}, ensure_ascii=False)}\n\n"
                        query_analysis_sent = True

                    elif event_type == 'opensearch_results' and not opensearch_sent:
                        yield f"data: {json.dumps({'event': 'progress', 'step': step, 'total': 8, 'stage': '키워드 검색'}, ensure_ascii=False)}\n\n"
                        step += 1
                        yield f"data: {json.dumps({'event': 'opensearch_search_start'}, ensure_ascii=False)}\n\n"
                        yield f"data: {json.dumps({'event': 'opensearch_results', **data}, ensure_ascii=False)}\n\n"
                        opensearch_sent = True

                    # 다른 이벤트들도 유사하게 처리...

                except asyncio.TimeoutError:
                    continue

            # 검색 완료
            response = await search_task

            # ⭐ 완료
            yield f"data: {json.dumps({'event': 'done'}, ensure_ascii=False)}\n\n"

        except Exception as e:
            logger.error(f"SSE 스트리밍 오류: {e}", exc_info=True)
            yield f"data: {json.dumps({'event': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # nginx 버퍼링 비활성화
        }
    )


@router.get(
    "/logs/conversation/{session_id}",
    summary="대화 히스토리 조회 (Redis)",
)
async def get_conversation_logs_endpoint(
    session_id: str,
    limit: int = 50,
) -> Dict[str, Any]:
    client = getattr(router, "redis_client", None)
    if client is None:
        raise HTTPException(status_code=503, detail="Redis 클라이언트가 구성되지 않았습니다.")
    if limit <= 0:
        raise HTTPException(status_code=400, detail="limit 값은 1 이상이어야 합니다.")

    key = _make_conversation_key(
        getattr(router, "conversation_history_prefix", None),
        session_id,
    )
    if not key:
        raise HTTPException(status_code=400, detail="session_id 또는 prefix가 올바르지 않습니다.")

    raw_items = client.lrange(key, -limit, -1)
    messages: List[ConversationMessage] = []
    for item in raw_items:
        parsed = _parse_conversation_record(item)
        if parsed is not None:
            messages.append(parsed)
    
    # 메시지를 딕셔너리로 변환 (Pydantic 모델 직렬화)
    messages_dict = []
    for msg in messages:
        msg_dict = msg.model_dump()
        # assistant 메시지의 content가 딕셔너리인지 확인하고 그대로 유지
        # (이미 _parse_conversation_record에서 처리됨)
        messages_dict.append(msg_dict)

    return {
        "session_id": session_id,
        "count": len(messages_dict),
        "messages": messages_dict,
    }


@router.get(
    "/logs/search-history/{owner_id}",
    summary="검색 이력 조회 (Redis)",
)
async def get_search_history_endpoint(
    owner_id: str,
    limit: int = 50,
) -> Dict[str, Any]:
    client = getattr(router, "redis_client", None)
    if client is None:
        raise HTTPException(status_code=503, detail="Redis 클라이언트가 구성되지 않았습니다.")
    if limit <= 0:
        raise HTTPException(status_code=400, detail="limit 값은 1 이상이어야 합니다.")

    key = _make_history_key(
        getattr(router, "search_history_prefix", None),
        owner_id,
    )
    if not key:
        raise HTTPException(status_code=400, detail="owner_id 또는 prefix가 올바르지 않습니다.")

    raw_items = client.lrange(key, -limit, -1)
    entries: List[SearchHistoryEntry] = []
    for item in raw_items:
        parsed = _parse_search_history_record(item)
        if parsed is not None:
            entries.append(parsed)

    return {
        "owner_id": owner_id,
        "count": len(entries),
        "history": [entry.model_dump() for entry in entries],
    }


@router.get(
    "/opensearch/{user_id}",
    summary="OpenSearch에서 user_id로 문서 검색 (DevTools 스타일)",
)
def search_by_user_id(
    user_id: str,
    index_name: str = Query(default="survey_responses_merged", description="검색할 인덱스 이름"),
    os_client: OpenSearch = Depends(lambda: router.os_client),
) -> Dict[str, Any]:
    """
    OpenSearch DevTools처럼 user_id로 문서를 검색합니다.
    
    Args:
        user_id: 검색할 사용자 ID
        index_name: 검색할 인덱스 이름 (기본값: survey_responses_merged)
    
    Returns:
        OpenSearch 검색 결과 (DevTools와 동일한 형식)
    """
    if not os_client or not os_client.ping():
        raise HTTPException(status_code=503, detail="OpenSearch 서버에 연결할 수 없습니다.")
    
    try:
        # OpenSearch term 쿼리로 user_id 검색
        query = {
            "query": {
                "term": {
                    "user_id": user_id
                }
            },
            "size": 1  # user_id는 고유하므로 1개만 반환
        }
        
        logger.info(f"🔍 OpenSearch user_id 검색: {user_id} (인덱스: {index_name})")
        
        response = os_client.search(
            index=index_name,
            body=query
        )
        
        hits = response.get("hits", {})
        total = hits.get("total", {})
        total_value = total.get("value", 0) if isinstance(total, dict) else total
        
        if total_value == 0:
            return {
                "user_id": user_id,
                "found": False,
                "total": 0,
                "hits": []
            }
        
        # 첫 번째 결과 반환
        first_hit = hits.get("hits", [])[0] if hits.get("hits") else None
        
        if first_hit:
            # _source에서 timestamp 제거
            source = first_hit.get("_source", {})
            if isinstance(source, dict):
                source = source.copy()  # 원본 수정 방지
                source.pop("timestamp", None)  # timestamp 제거
            
            return {
                "user_id": user_id,
                "found": True,
                "total": total_value,
                "hits": [
                    {
                        "_id": first_hit.get("_id"),
                        "_score": first_hit.get("_score"),
                        "_source": source
                    }
                ]
            }
        else:
            return {
                "user_id": user_id,
                "found": False,
                "total": total_value,
                "hits": []
            }
            
    except Exception as e:
        logger.error(f"❌ OpenSearch user_id 검색 실패: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"검색 중 오류가 발생했습니다: {str(e)}"
        )


def _filter_to_string(filter_dict: Dict[str, Any]) -> str:
    """Helper: 필터를 문자열로 변환"""
    try:
        return json.dumps(filter_dict, ensure_ascii=False)
    except Exception:
        return str(filter_dict)


# ⭐ Two-phase search helper 함수 제거됨 (단순화)
# - is_age_or_gender_filter
# - is_occupation_filter
# → survey_responses_merged 통합 인덱스 사용으로 불필요


def get_user_id_from_doc(doc: Dict[str, Any]) -> Optional[str]:
    """문서에서 user_id 추출"""
    if not isinstance(doc, dict):
        return None
    source = doc.get('_source')
    if isinstance(source, dict):
        uid = source.get('user_id')
        if uid:
            return uid
    uid = doc.get('_id')
    if uid:
        return uid
    payload = doc.get('payload')
    if isinstance(payload, dict):
        return payload.get('user_id')
    return None
