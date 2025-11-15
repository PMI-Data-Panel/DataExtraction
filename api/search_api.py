"""검색 API 라우터"""
import asyncio
import json
import logging
import hashlib
import re
from collections import defaultdict, OrderedDict
from time import perf_counter
from datetime import datetime, timezone
from uuid import uuid4
from typing import List, Dict, Any, Optional, Set, Tuple, Literal
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from opensearchpy import OpenSearch

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

router = APIRouter(
    prefix="/search",
    tags=["Search"]
)

# OpenSearch 요청 타임아웃 (복잡한 쿼리나 대용량 검색을 위해 30초로 설정)
DEFAULT_OS_TIMEOUT = 30

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
    "정량적 지표(응답자 수, 비율 등)가 있을 경우 명시하고, 데이터의 편향이나 한계도 언급하세요."
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

    prompt = (
        "당신은 설문조사 데이터 분석 전문가입니다. "
        "주어진 검색 결과를 바탕으로 사용자의 질문에 대한 인사이트를 제공하세요.\n\n"
        f"사용자 질의: {request.query}\n"
        f"예상 검색 의도: {getattr(analysis, 'intent', 'N/A')}\n"
        f"추출된 must_terms: {getattr(analysis, 'must_terms', [])}\n"
        f"추출된 should_terms: {getattr(analysis, 'should_terms', [])}\n"
        f"총 검색 결과 수: {response.total_hits}\n"
        f"현재 반환된 결과 수: {len(response.results)}\n\n"
        f"요약 지침: {instructions}\n\n"
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
        getattr(result, "user_id", None) for result in (response.results or [])[:5]
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

            assistant_payload: Dict[str, Any] = {
                "total_hits": response.total_hits,
                "returned_count": len(response.results or []),
                "cache_hit": cache_hit,
                "top_user_ids": top_user_ids,
            }
            if response.llm_summary:
                assistant_payload["llm_summary"] = response.llm_summary

            assistant_entry = {
                "role": "assistant",
                "timestamp": timestamp,
                "content": _truncate_text(assistant_payload, 4000),
                "session_id": session_id,
                "user_id": user_id,
                "request_id": request_id,
            }
            _redis_list_append(client, conversation_key, assistant_entry, conversation_max, conversation_ttl)

    if getattr(request, "log_search_history", True):
        owner_id = user_id or session_id or "default"
        history_key = _make_history_key(search_history_prefix, owner_id)
        if history_key:
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


def _parse_conversation_record(item: str) -> Optional[ConversationMessage]:
    if not item:
        return None
    try:
        payload = json.loads(item)
    except Exception as exc:
        logger.warning(f"⚠️ 대화 로그 JSON 파싱 실패: {exc}")
        return None

    content = payload.get("content")
    if payload.get("role") == "assistant" and isinstance(content, str):
        try:
            content = json.loads(content)
        except Exception:
            pass
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
    """쿼리 특성에 따라 RRF k 값을 조정"""
    k = 60
    reason = "균형 유지 (k=60)"

    if has_filters:
        k = 40
        reason = "필터 적용 → 정확도 중시 (k=40)"
    elif use_vector_search and query_intent and query_intent.lower() in {"semantic", "semantic_search"}:
        k = 80
        reason = f"의도={query_intent} → 벡터 가중 (k=80)"

    combined = calculate_rrf_score(
        keyword_results=keyword_results,
        vector_results=vector_results,
        k=k,
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
        "filters": filters_for_response,
        "size": page_size,
        "timings_ms": timings,
        "behavioral_conditions": payload.get("behavioral_conditions", {}),
        "use_claude_analyzer": bool(payload.get("use_claude", False)),
    }
    if extracted_entities_dict is not None:
        query_analysis["extracted_entities"] = extracted_entities_dict

    return SearchResponse(
        query=request.query,
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
    timestamp: Optional[str] = None
    demographic_info: Optional[Dict[str, Any]] = Field(default=None, description="인구통계 정보 (survey_responses_merged에서 조회)")
    behavioral_info: Optional[Dict[str, Any]] = Field(default=None, description="행동/습관 정보 (예: 흡연 여부, 차량 보유 여부)")
    qa_pairs: Optional[List[Dict[str, Any]]] = None
    matched_qa_pairs: Optional[List[Dict[str, Any]]] = None
    highlights: Optional[Dict[str, Any]] = None


class SearchResponse(BaseModel):
    """검색 응답"""
    query: str
    total_hits: int
    max_score: Optional[float]
    results: List[SearchResult]
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


BEHAVIOR_YES_TOKENS = {
    "있다", "있음", "있어요", "yes", "y", "보유", "보유함", "보유중", "한다", "합니다", "해요"
}
BEHAVIOR_NO_TOKENS = {
    "없다", "없음", "없어요", "no", "n", "미보유", "안함", "안해요", "하지않는다", "하지 않는다", "않음", "안합니다"
}
SMOKER_NEGATIVE_KEYWORDS = {
    "피워본 적이 없다", "피워본적이 없다", "피워본적 없다", "피우지 않는다",
    "흡연하지 않는다", "비흡연", "금연", "담배를 피우지 않는다", "담배를 피워본적이 없다",
    "담배 안 피", "담배안피", "흡연 안 함", "흡연 안함", "담배를 피우지 않음", "피우지 않음"
}
SMOKER_POSITIVE_KEYWORDS = {
    "흡연", "담배 피", "담배피", "담배를 피", "흡연중", "흡연함", "smoker",
    "피운다", "피웁니다", "피움", "일반 담배", "일반담배", "전자 담배",
    "전자담배", "궐련형 전자담배", "궐련형전자담배", "권련형 전자담배",
    "권련형전자담배", "연초", "시가형 전자담배", "담배", "담배를 피움",
    "흡연 경험 있음", "흡연경험 있음"
}
SMOKER_QUESTION_KEYWORDS = {
    "흡연", "담배", "흡연경험", "흡연 경험", "흡연경험 담배브랜드",
    "궐련형 전자담배", "궐련형 전자담배/가열식 전자담배 이용경험",
    "가열식 전자담배", "전자담배"
}
VEHICLE_QUESTION_KEYWORDS = {
    "보유차량여부", "보유차량", "차량여부", "차량 여부", "자동차", "차량", "차 보유", "차량보유"  # ✅ "보유차량여부" 추가
}

# 음주 관련 키워드
ALCOHOL_QUESTION_KEYWORDS = {
    "음용경험 술", "음용경험", "술", "음주", "음주경험", "알콜", "알코올"
}
BEER_KEYWORDS = {
    "맥주", "beer"
}
WINE_KEYWORDS = {
    "와인", "wine"
}
SOJU_KEYWORDS = {
    "소주", "soju"
}
NON_DRINKER_KEYWORDS = {
    "술을 마시지 않음", "술 마시지 않음", "술 안 마심", "술 안마심", "술 못마심", "술 못 마심",
    "비음주", "금주", "최근 1년 이내 술을 마시지 않음", "음주 경험 없음", "음주경험 없음"
}
DRINKER_POSITIVE_KEYWORDS = {
    # 술 종류
    "맥주", "beer", "소주", "soju", "막걸리", "탁주", "와인", "wine",
    "양주", "위스키", "whiskey", "보드카", "vodka", "데킬라", "tequila", "진", "gin",
    "저도주", "청주", "매실주", "복분자주", "과일칵테일주", "KGB", "후치", "크루저",
    "일본청주", "사케", "sake", "칵테일", "cocktail",
    # 음주 긍정 표현
    "술 마심", "술 마셔", "술마심", "술마셔", "음주함", "음주 경험 있음", "음주경험 있음",
    "가끔 마심", "자주 마심", "주말에 마심"
}


def extract_behavioral_conditions_from_query(query: str) -> Dict[str, bool]:
    """쿼리 텍스트에서 behavioral 조건 자동 추출

    Args:
        query: 검색 쿼리

    Returns:
        behavioral 조건 딕셔너리 {"drinker": True, "smoker": False, ...}
    """
    query_lower = query.lower()
    query_normalized = query_lower.replace(" ", "")
    conditions = {}

    # ⭐ 음주 여부 감지
    drinker_positive = ["술마", "술도마", "음주", "술먹", "술마신", "음주경험", "음주 경험", "주류"]
    drinker_negative = ["비음주", "금주", "술안", "술을마시지", "술을안마시", "술도안"]

    has_drinker_negative = any(keyword in query_normalized for keyword in drinker_negative)
    has_drinker_positive = any(keyword in query_normalized for keyword in drinker_positive)

    if has_drinker_negative:
        conditions["drinker"] = False
    elif has_drinker_positive:
        conditions["drinker"] = True

    # ⭐ 흡연 여부 감지
    smoker_positive = ["흡연자", "담배피", "담배 피", "담배를피"]
    smoker_negative = ["비흡연", "금연", "담배안", "담배도안", "흡연안", "흡연을안", "안피는", "안 피는"]

    has_smoker_negative = any(keyword in query_normalized for keyword in smoker_negative)
    has_smoker_positive = any(keyword in query_normalized for keyword in smoker_positive)

    if has_smoker_negative:
        conditions["smoker"] = False
    elif has_smoker_positive:
        conditions["smoker"] = True

    # ⭐ 차량 보유 여부 감지
    vehicle_positive = ["차량", "자동차", "차보유", "차있는"]
    vehicle_negative = ["차없는", "차량없는", "차가없는"]

    has_vehicle_negative = any(keyword in query_normalized for keyword in vehicle_negative)
    has_vehicle_positive = any(keyword in query_normalized for keyword in vehicle_positive)

    if has_vehicle_negative:
        conditions["has_vehicle"] = False
    elif has_vehicle_positive:
        conditions["has_vehicle"] = True

    return conditions


def build_behavioral_filters(behavioral_conditions: Dict[str, bool]) -> List[Dict[str, Any]]:
    """behavioral_conditions를 OpenSearch nested 필터로 변환

    Args:
        behavioral_conditions: {"smoker": True, "has_vehicle": False, ...}

    Returns:
        OpenSearch nested 쿼리 리스트

    Example:
        {"smoker": True} →
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


@router.post("/nl", response_model=SearchResponse, summary="자연어 쿼리: 자동 추출+검색")
async def search_natural_language(
    request: NLSearchRequest,
    os_client: OpenSearch = Depends(lambda: router.os_client),
):
    """
    자연어 입력에서 인구통계(연령/성별/직업)와 요청 수량을 추출하여
    검색 쿼리와 size에 반영한 뒤 결과를 반환합니다.
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

        # ⭐ 자동으로 쿼리에서 behavioral 조건 추출 및 병합
        auto_behavioral = extract_behavioral_conditions_from_query(request.query)
        if auto_behavioral:
            # 기존 behavioral_conditions와 병합 (자동 추출이 우선)
            if not analysis.behavioral_conditions:
                analysis.behavioral_conditions = {}
            for key, value in auto_behavioral.items():
                if key not in analysis.behavioral_conditions:
                    analysis.behavioral_conditions[key] = value
            logger.info(f"✅ 자동 추출된 behavioral 조건: {auto_behavioral}")

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
        extracted_entities, requested_size = extractor.extract_with_size(request.query)
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

        # ⭐ page_size 제한 완화: 100 → 5000 (전체 결과 확인 가능하도록)
        page_size = max(1, min(requested_size, 5000))
        page = max(1, request.page)
        requested_window = page_size * page
        cache_client = getattr(router, "redis_client", None)
        cache_ttl = getattr(router, "cache_ttl_seconds", 0)
        cache_limit = getattr(router, "cache_max_results", requested_window)
        cache_prefix = getattr(router, "cache_prefix", "search:results")
        cache_enabled = bool(cache_client) and cache_ttl > 0
        min_window_size = 2000
        window_size = max(requested_window, min_window_size)
        if cache_limit and cache_limit > 0:
            window_size = min(window_size, cache_limit)
        size = window_size
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
                cached_raw = cache_client.get(cache_key)
                if cached_raw:
                    cache_payload = json.loads(cached_raw)
                    cache_hit = True
                    logger.info(f"🔁 Redis 검색 캐시 히트: key={cache_key}")
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
                logger.warning(f"⚠️ Redis 검색 캐시 조회 실패: {cache_exc}")
                cache_key = None
                cache_enabled = False
        
        age_gender_filters = [f for f in filters if is_age_or_gender_filter(f)]
        occupation_filters = [f for f in filters if is_occupation_filter(f)]
        other_filters = [f for f in filters if f not in age_gender_filters and f not in occupation_filters]

        # ⭐ occupation_filters도 포함시키기 (이전에는 two-phase search에만 사용됨)
        filters_os = age_gender_filters + occupation_filters + other_filters
        filters = filters_os  # 유지보수: 기존 로직과 호환성을 위해
        has_demographic_filters = bool(filters_for_response)
        occupation_filter_handled = False

        logger.info("🔍 필터 상태 체크:")
        logger.info(f"  - age_gender_filters: {len(age_gender_filters)}개")
        logger.info(f"  - occupation_filters: {len(occupation_filters)}개")
        logger.info(f"  - other_filters: {len(other_filters)}개")
        logger.info(f"  - filters_os (합계): {len(filters_os)}개")
        if occupation_filters:
            logger.info(f"  - occupation_filters 샘플: {json.dumps(occupation_filters[0] if occupation_filters else {}, ensure_ascii=False)[:500]}")

        two_phase_applicable = bool(age_gender_filters and occupation_filters)
        two_phase_response: Optional[SearchResponse] = None
        if two_phase_applicable:
            logger.info("✅ 2단계 검색 조건 충족 – 두 단계 검색 시도")

            try:
                response = await run_two_phase_demographic_search(
                    request=request,
                    analysis=analysis,
                    extracted_entities=extracted_entities,
                    filters=filters_for_response,
                    size=size,
                    age_gender_filters=age_gender_filters,
                    occupation_filters=occupation_filters,
                    data_fetcher=data_fetcher,
                    timings=timings,
                    overall_start=overall_start,
                )

                if response is not None:
                    two_phase_response = response
                    logger.info("✅ 2단계 검색 성공! 결과 반환")
                    logger.info(f"🔵 /search/nl 요청 완료: 결과 {len(response.results)}건, took_ms={response.took_ms}")
            except Exception as e:
                logger.warning(f"⚠️ 2단계 검색 중 오류: {e}, 기본 파이프라인으로 진행")

        if two_phase_response is not None:
            return _finalize_search_response(
                request=request,
                response=two_phase_response,
                analysis=analysis,
                cache_hit=cache_hit,
                timings=two_phase_response.query_analysis.get("timings_ms") if two_phase_response.query_analysis else timings,
            )

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

        # 🔍 Base Query 로깅
        logger.info(f"🔍 [BASE QUERY] 생성 완료")
        logger.info(json.dumps(base_query, ensure_ascii=False, indent=2))

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
        
        if filters_os:
            # ⭐ inner_hits 제거 (중복 방지)
            cleaned_filters = [remove_inner_hits(f) for f in filters_os]
            
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
            
            # ⭐ 기존 쿼리와 필터 결합 (must로 결합: 모든 필터를 만족해야 함)
            # survey_responses_merged: 모든 인구통계 정보 포함
            # 각 인덱스에서 정보를 가져와야 하므로 must로 결합
            if existing_query is None or existing_query == {"match_all": {}} or existing_query == {"match_none": {}}:
                # 키워드 쿼리가 없거나 match_all/match_none인 경우: 필터를 must로 사용
                final_query['query'] = {
                    'bool': {
                        'must': should_filters  # 모든 필터를 만족해야 함
                    }
                }
                logger.info(f"✅ 필터를 must로 적용 (모든 필터 만족 필요): {len(should_filters)}개 필터")
            elif isinstance(existing_query, dict) and existing_query.get('bool'):
                # 기존 bool 쿼리에 필터를 must로 추가
                if 'must' not in existing_query['bool']:
                    existing_query['bool']['must'] = []
                existing_query['bool']['must'].extend(should_filters)
                final_query['query'] = existing_query
                logger.info(f"✅ 필터를 must로 추가 (모든 필터 만족 필요): {len(should_filters)}개 필터")
            else:
                # 기존 쿼리를 bool로 감싸기 (must로 결합)
                final_query['query'] = {
                    'bool': {
                        'must': [existing_query] + should_filters
                    }
                }
                logger.info(f"✅ 필터를 must로 추가 (모든 필터 만족 필요): {len(should_filters)}개 필터")
        
        if 'size' not in final_query:
            final_query['size'] = size

        if filters_os:
            logger.info(f"🔍 적용된 필터 ({len(filters_os)}개):")
            for i, f in enumerate(filters_os, 1):
                logger.info(f"  필터 {i}: {json.dumps(f, ensure_ascii=False, indent=2)}")
            logger.info(f"🔍 최종 쿼리 구조:")
            logger.info(f"  {json.dumps(final_query, ensure_ascii=False, indent=2)}")
        else:
            logger.info(f"🔍 최종 쿼리 구조 (필터 없음):")
            logger.info(f"  {json.dumps(final_query, ensure_ascii=False, indent=2)}")

        # ⭐ Qdrant top-N 제한: 필터 유무에 따라 분기
        has_filters = bool(filters_os or occupation_filters)
        rrf_k_used: Optional[int] = None
        rrf_reason: str = ""
        adaptive_threshold: Optional[float] = None
        threshold_reason: str = ""
        has_behavioral = bool(getattr(analysis, "behavioral_conditions", None))

        # ⭐ 검색 크기 설정: behavioral 필터가 있으면 더 많은 결과 필요
        # ⭐ 최소값을 10으로 설정 (테스트/디버깅용)
        if has_filters or has_behavioral:
            if has_behavioral:
                qdrant_limit = min(max(size * 5, 500), 5000)
                search_size = min(max(size * 10, 10), 10000)  # 최소값 1000 → 10
            else:
                qdrant_limit = min(max(size * 3, 300), 5000)
                search_size = min(max(size * 5, 10), 10000)  # 최소값 500 → 10
            logger.info(f"🔍 필터 적용: OpenSearch size={search_size}, Qdrant limit={qdrant_limit} (behavioral={has_behavioral})")
        else:
            qdrant_limit = min(max(size, 60), 5000)
            search_size = min(max(size * 2, 10), 10000)  # 최소값 80 → 10
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
            # OpenSearch 키워드 검색
            query_body = final_query.copy()
            if not isinstance(query_body.get('query'), dict):
                logger.warning("  ⚠️ 쿼리가 비어 있어 match_all로 대체합니다")
                query_body['query'] = {"match_all": {}}

            # 🔍 OpenSearch 쿼리 로깅 (디버깅용)
            logger.info(f"🔍 OpenSearch 쿼리:")
            logger.info(json.dumps(query_body, ensure_ascii=False, indent=2))

            os_response = data_fetcher.search_opensearch(
                index_name=request.index_name,
                query=query_body,
                size=search_size,
                source_filter=source_filter,
                request_timeout=DEFAULT_OS_TIMEOUT,
            )
            keyword_results = os_response['hits']['hits']
            logger.info(f"  ✅ OpenSearch: {len(keyword_results)}건")
            
            # Qdrant 벡터 검색
            if request.use_vector_search and query_vector and hasattr(router, 'qdrant_client'):
                qdrant_client = router.qdrant_client
                try:
                    # survey_responses_merged 컬렉션만 검색
                    collection_name = request.index_name  # survey_responses_merged
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
                        logger.info(f"  ✅ Qdrant: {len(vector_results)}건")
                    except Exception as e:
                        logger.debug(f"  ⚠️ Qdrant 컬렉션 '{collection_name}' 검색 실패: {e}")
                except Exception as e:
                    logger.debug(f"  ⚠️ Qdrant 검색 실패: {e}")
        except Exception as e:
            logger.warning(f"  ⚠️ 인덱스 검색 실패: {e}")
        
        # user_id 및 _id -> 원본 문서 매핑 생성
        user_doc_map = {}
        id_doc_map = {}
        
        # 검색 결과 매핑
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

        # 후보 문서 수 제한 (후처리 부담 완화)
        fetch_size = window_size
        candidate_cap = max(
            fetch_size * 20,
            cache_limit if cache_limit else 0,
            2000
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
        
        demographic_filters: Dict[DemographicType, List["DemographicEntity"]] = defaultdict(list)
        for demo in extracted_entities.demographics:
            demographic_filters[demo.demographic_type].append(demo)

        filtered_rrf_results: List[Dict[str, Any]] = rrf_results
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
            if not values or not expected:
                return False
            for val in values:
                if not val:
                    continue
                for exp in expected:
                    if not exp:
                        continue
                    if val == exp or val in exp or exp in val:
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
            occupation_dsl_handled = bool(demographic_filters.get(DemographicType.OCCUPATION)) and occupation_filter_handled

            # ⭐ 모든 demographic_filters를 검증 (REGION, MARITAL_STATUS 포함!)
            # ⭐ 단, OCCUPATION과 JOB_FUNCTION은 OpenSearch에서 qa_pairs로 검색하므로 후처리 검증 스킵
            filters_to_validate: List[DemographicType] = [
                f for f in demographic_filters.keys()
                if f not in {DemographicType.OCCUPATION, DemographicType.JOB_FUNCTION}
            ]
            logger.info(f"  ✅ 후처리 검증 대상: {[f.value for f in filters_to_validate]}")
            logger.info(f"  ⚠️ 후처리 검증 제외 (OpenSearch 필터만 사용): {[f.value for f in demographic_filters.keys() if f in {DemographicType.OCCUPATION, DemographicType.JOB_FUNCTION}]}")

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
            
            if not filters_to_validate:
                timings["post_filter_ms"] = (perf_counter() - filter_start) * 1000
                filtered_rrf_results = rrf_results
                logger.info("  ✅ Demographic 필터 없음: Python 후처리 생략")
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
                    behavior_values: Dict[str, Optional[bool]] = {
                        "smoker": None,
                        "has_vehicle": None,
                        "drinker": None,  # ⭐ 음주 여부 추가
                    }

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
                                        doc_values[DemographicType.OCCUPATION].update(normalized_answers)

                                    if (
                                        not metadata_presence[DemographicType.GENDER]
                                        and any(keyword in q_text_raw for keyword in ("성별", "gender"))
                                    ):
                                        doc_values[DemographicType.GENDER].update(normalized_answers)
                                        metadata_presence[DemographicType.GENDER] = True

                                if behavior_values.get("smoker") is None and q_text_raw and any(keyword in q_text_raw for keyword in SMOKER_QUESTION_KEYWORDS):
                                    for ans in answers:
                                        smoker_decision = parse_smoker_answer(ans)
                                        if smoker_decision is not None:
                                            record_behavior("smoker", smoker_decision)
                                            if smoker_decision is False:
                                                break
                                    if behavior_values.get("smoker") is None:
                                        for ans in normalized_answers:
                                            smoker_decision = parse_smoker_answer(ans)
                                            if smoker_decision is not None:
                                                record_behavior("smoker", smoker_decision)
                                                break

                                if behavior_values.get("has_vehicle") is None and q_text_raw and any(keyword in q_text_raw for keyword in VEHICLE_QUESTION_KEYWORDS):
                                    # 🔍 디버깅: 실제 차량 답변 로그
                                    normalized_answers_sample = list(normalized_answers)[:3]

                                    for ans in normalized_answers:
                                        vehicle_decision = parse_yes_no(ans)

                                        if vehicle_decision is not None:
                                            record_behavior("has_vehicle", vehicle_decision)
                                            break

                                # ⭐ 음주 여부 추출
                                if behavior_values.get("drinker") is None and q_text_raw and any(keyword in q_text_raw for keyword in ALCOHOL_QUESTION_KEYWORDS):
                                    for ans in answers:
                                        drinker_decision = parse_drinker_answer(ans)
                                        if drinker_decision is not None:
                                            record_behavior("drinker", drinker_decision)
                                            if drinker_decision is False:
                                                break
                                    if behavior_values.get("drinker") is None:
                                        for ans in normalized_answers:
                                            drinker_decision = parse_drinker_answer(ans)
                                            if drinker_decision is not None:
                                                record_behavior("drinker", drinker_decision)
                                                break

                    return doc_values, metadata_presence, behavior_values

                filtered_list = []
                source_not_found_count = 0
                gender_filter_failed = 0
                age_filter_failed = 0
                occupation_filter_failed = 0
                gender_metadata_missing = 0
                age_metadata_missing = 0
                occupation_metadata_missing = 0
                region_filter_failed = 0  # ⭐ REGION 필터 카운터
                region_metadata_missing = 0  # ⭐ REGION 메타데이터 누락 카운터
                marital_status_filter_failed = 0  # ⭐ MARITAL_STATUS 필터 카운터
                marital_status_metadata_missing = 0  # ⭐ MARITAL_STATUS 메타데이터 누락 카운터
                sub_region_filter_failed = 0  # ⭐ SUB_REGION 필터 카운터
                sub_region_metadata_missing = 0  # ⭐ SUB_REGION 메타데이터 누락 카운터
                behavior_filter_failed = 0
                behavior_metadata_missing = 0

                for doc in rrf_results:
                    user_id = doc_user_map.get(id(doc))
                    if not user_id:
                        continue

                    source = user_rrf_map.get(user_id, [{}])[0].get('_source', {})
                    if not isinstance(source, dict):
                        source = {}

                    # ⭐ survey_responses_merged만 사용하므로 welcome_1st/welcome_2nd 배치 제거
                    # source에서 직접 metadata 가져오기
                    metadata = source.get("metadata", {}) if isinstance(source.get("metadata"), dict) else {}

                    doc_values, metadata_presence, behavior_values = collect_doc_values(user_id, source, metadata, {})
                    behavior_values_map[user_id] = dict(behavior_values)

                    gender_pass = True
                    age_pass = True
                    occupation_pass = True
                    region_pass = True  # ⭐ REGION 필터 검증 추가
                    marital_status_pass = True  # ⭐ MARITAL_STATUS 필터 검증 추가
                    sub_region_pass = True  # ⭐ SUB_REGION 필터 검증 추가

                    if DemographicType.GENDER in filters_to_validate:
                        expected = set()
                        for demo in demographic_filters[DemographicType.GENDER]:
                            expected.update(build_expected_values(demo))

                        # ⭐ Metadata 우선: metadata가 있으면 metadata만 확인
                        if metadata_presence[DemographicType.GENDER]:
                            # metadata로 수집된 값만 사용 (qa_pairs 무시)
                            # doc_values에서 metadata 소스만 확인하기 위해 다시 수집
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
                                # metadata_presence가 True인데 값이 없으면 오류
                                gender_metadata_missing += 1
                                gender_pass = False
                        else:
                            # metadata 없으면 qa_pairs 사용
                            expand_gender_aliases(doc_values[DemographicType.GENDER])
                            gender_pass = values_match(doc_values[DemographicType.GENDER], expected)

                        if not gender_pass:
                            gender_filter_failed += 1

                    if gender_pass and DemographicType.AGE in filters_to_validate:
                        expected = set()
                        for demo in demographic_filters[DemographicType.AGE]:
                            expected.update(build_expected_values(demo))
                        age_pass = values_match(doc_values[DemographicType.AGE], expected)
                        if not age_pass:
                            age_filter_failed += 1

                    if gender_pass and age_pass and DemographicType.OCCUPATION in filters_to_validate:
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
                                # ⭐ survey_responses_merged만 사용하므로 metadata_2nd_full 제거
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
                                        candidate_value = str(answer)
                                        normalized_candidate = normalize_value(candidate_value)
                                        if normalized_candidate and values_match({normalized_candidate}, expected):
                                            display_occupation = candidate_value
                                            break
                                    if display_occupation:
                                        break
                            if display_occupation:
                                occupation_display_map[user_id] = display_occupation

                    # ⭐ REGION 검증
                    if DemographicType.REGION in filters_to_validate:
                        expected = set()
                        for demo in demographic_filters[DemographicType.REGION]:
                            expected.update(build_expected_values(demo))

                        # metadata에서 region 가져오기
                        region_val = metadata.get("region") if isinstance(metadata, dict) else None
                        if region_val:
                            normalized_region = normalize_value(region_val)
                            if normalized_region:
                                region_pass = values_match({normalized_region}, expected)
                                if not region_pass:
                                    region_filter_failed += 1
                            else:
                                region_pass = False
                                region_filter_failed += 1
                        else:
                            region_metadata_missing += 1
                            region_pass = False

                    # ⭐ MARITAL_STATUS 검증
                    if DemographicType.MARITAL_STATUS in filters_to_validate:
                        expected = set()
                        for demo in demographic_filters[DemographicType.MARITAL_STATUS]:
                            expected.update(build_expected_values(demo))

                        # metadata에서 marital_status 가져오기
                        marital_val = metadata.get("marital_status") if isinstance(metadata, dict) else None

                        if not marital_val:
                            # qa_pairs에서도 찾아보기
                            qa_sources: List[List[Dict[str, Any]]] = []
                            if isinstance(source, dict):
                                qa_sources.append(source.get("qa_pairs", []) or [])

                            for qa_pairs in qa_sources:
                                for qa in qa_pairs:
                                    if not isinstance(qa, dict):
                                        continue
                                    q_text = str(qa.get("q_text", "")).lower()
                                    if not any(keyword in q_text for keyword in ("결혼", "혼인", "marital")):
                                        continue
                                    answer = qa.get("answer") or qa.get("answer_text")
                                    if answer:
                                        marital_val = str(answer)
                                        break
                                if marital_val:
                                    break

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

                    # ⭐ SUB_REGION 검증
                    if DemographicType.SUB_REGION in filters_to_validate:
                        expected = set()
                        for demo in demographic_filters[DemographicType.SUB_REGION]:
                            expected.update(build_expected_values(demo))

                        # metadata에서 sub_region 가져오기
                        sub_region_val = metadata.get("sub_region") if isinstance(metadata, dict) else None
                        if sub_region_val:
                            normalized_sub_region = normalize_value(sub_region_val)
                            if normalized_sub_region:
                                sub_region_pass = values_match({normalized_sub_region}, expected)
                                if not sub_region_pass:
                                    sub_region_filter_failed += 1
                            else:
                                sub_region_pass = False
                                sub_region_filter_failed += 1
                        else:
                            sub_region_metadata_missing += 1
                            sub_region_pass = False

                    # ⭐ Behavioral 검증: OpenSearch는 후보를 넓게 가져오고, Python에서 정확히 검증
                    behavior_pass = True
                    if analysis.behavioral_conditions:
                        for condition_key, expected_value in analysis.behavioral_conditions.items():
                            actual_value = behavior_values.get(condition_key)
                            if actual_value is None:
                                behavior_metadata_missing += 1
                                behavior_pass = False
                                break
                            if actual_value != expected_value:
                                behavior_filter_failed += 1
                                behavior_pass = False
                                break

                    # ⭐ 모든 demographic 필터 검증 (REGION, MARITAL_STATUS, SUB_REGION 포함)
                    if gender_pass and age_pass and occupation_pass and region_pass and marital_status_pass and sub_region_pass and behavior_pass:
                        filtered_list.append(doc)

                filter_duration_ms = (perf_counter() - filter_start) * 1000
                timings["post_filter_ms"] = filter_duration_ms
                filtered_rrf_results = filtered_list
                total_hits = len(filtered_rrf_results)

                logger.info(f"  - 소스 누락 문서: {source_not_found_count}건")
                if DemographicType.GENDER in filters_to_validate:
                    logger.info(f"  - 성별 metadata 없음: {gender_metadata_missing}건")
                    logger.info(f"  - 성별 필터 미충족: {gender_filter_failed}건")
                if DemographicType.AGE in filters_to_validate:
                    logger.info(f"  - 연령 metadata 없음: {age_metadata_missing}건")
                    logger.info(f"  - 연령 필터 미충족: {age_filter_failed}건")
                if DemographicType.OCCUPATION in filters_to_validate:
                    logger.info(f"  - 직업 metadata 없음: {occupation_metadata_missing}건")
                    logger.info(f"  - 직업 필터 미충족: {occupation_filter_failed}건")
                if DemographicType.REGION in filters_to_validate:
                    logger.info(f"  - 지역 metadata 없음: {region_metadata_missing}건")
                    logger.info(f"  - 지역 필터 미충족: {region_filter_failed}건")
                if DemographicType.MARITAL_STATUS in filters_to_validate:
                    logger.info(f"  - 결혼여부 metadata 없음: {marital_status_metadata_missing}건")
                    logger.info(f"  - 결혼여부 필터 미충족: {marital_status_filter_failed}건")
                if DemographicType.SUB_REGION in filters_to_validate:
                    logger.info(f"  - 세부지역 metadata 없음: {sub_region_metadata_missing}건")
                    logger.info(f"  - 세부지역 필터 미충족: {sub_region_filter_failed}건")
                logger.info(f"  - 필터 조건 충족 문서: {len(filtered_rrf_results)}건")
                if analysis.behavioral_conditions:
                    logger.info(f"  ✅ 행동 필터 검증 완료")
                    logger.info(f"  - 행동 정보 없음: {behavior_metadata_missing}건")
                    logger.info(f"  - 행동 필터 미충족: {behavior_filter_failed}건")
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
                behavior_values: Dict[str, Optional[bool]] = {
                    "smoker": None,
                    "has_vehicle": None,
                }

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
        final_hits = filtered_rrf_results[:window_size]
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
                    merged_source = {}
                    merged_source.update(src_info)
                    merged_source.update(source)
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

            results.append(
                SearchResult(
                    user_id=user_id,
                    score=doc.get("_score", 0.0),
                    timestamp=source.get("timestamp") if isinstance(source, dict) else None,
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
        has_more = has_more_local and ((page * page_size) < total_hits)
        total_hits = len(filtered_rrf_results)
        max_score = results[0].score if results else 0.0
        response_took_ms = int(total_duration_ms)

        logger.info("📈 성능 측정 요약 (ms):")
        for key in sorted(timings.keys()):
            logger.info(f"  - {key}: {timings[key]:.2f}")

        summary_parts = [
            f"returned={len(page_results)}/{total_hits}",
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

        if cache_enabled and cache_key and stored_items:
            cache_payload = {
                "total_hits": total_hits,
                "max_score": max_score,
                "items": stored_items,
                "page_size": page_size,
                "filters": filters_for_response,
                "extracted_entities": extracted_entities.to_dict(),
                "behavioral_conditions": getattr(analysis, "behavioral_conditions", {}),
                "use_claude": bool(use_claude),
            }
            try:
                cache_client.setex(
                    cache_key,
                    cache_ttl,
                    json.dumps(cache_payload, ensure_ascii=False),
                )
                logger.info(f"💾 Redis 검색 캐시 저장: key={cache_key}, ttl={cache_ttl}s")
            except Exception as cache_exc:
                logger.warning(f"⚠️ Redis 검색 캐시 저장 실패: {cache_exc}")

        response = SearchResponse(
            query=request.query,
            total_hits=total_hits,
            max_score=max_score,
            results=page_results,
            query_analysis={
                "intent": analysis.intent,
                "must_terms": analysis.must_terms,
                "should_terms": analysis.should_terms,
                "alpha": analysis.alpha,
                "confidence": analysis.confidence,
                "filters": filters_for_response,
                "size": page_size,
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

    return {
        "session_id": session_id,
        "count": len(messages),
        "messages": [msg.model_dump() for msg in messages],
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


def _filter_to_string(filter_dict: Dict[str, Any]) -> str:
    try:
        return json.dumps(filter_dict, ensure_ascii=False)
    except Exception:
        return str(filter_dict)


AGE_GENDER_KEYWORDS = [
    "metadata.age_group", "metadata.gender", "birth_year", "연령", "나이", "성별"
]
OCCUPATION_KEYWORDS = [
    "metadata.occupation", "occupation", "직업", "직무"
]


def is_age_or_gender_filter(filter_dict: Dict[str, Any]) -> bool:
    filter_str = _filter_to_string(filter_dict)
    return any(keyword in filter_str for keyword in AGE_GENDER_KEYWORDS)


def is_occupation_filter(filter_dict: Dict[str, Any]) -> bool:
    filter_str = _filter_to_string(filter_dict)
    return any(keyword in filter_str for keyword in OCCUPATION_KEYWORDS)


async def run_two_phase_demographic_search(
    request,
    analysis,
    extracted_entities,
    filters: List[Dict[str, Any]],
    size: int,
    age_gender_filters: List[Dict[str, Any]],
    occupation_filters: List[Dict[str, Any]],
    data_fetcher: "DataFetcher",
    timings: Dict[str, float],
    overall_start: float,
) -> Optional[SearchResponse]:
    """두 단계 검색으로 user_id를 먼저 좁히고 정밀 조회"""
    logger.info("🚀 두 단계 인구통계 최적화 실행")

    async_client = data_fetcher.os_async_client
    sync_client = data_fetcher.os_client
    if not (async_client or sync_client):
        logger.warning("⚠️ OpenSearch 클라이언트가 없어 2단계 검색을 건너뜁니다")
        return None

    stage1_start = perf_counter()
    stage1_query_size = min(max(size * 50, 2000), 10000)
    stage1_query = {
        "query": {
            "bool": {
                "must": age_gender_filters
            }
        },
        "size": stage1_query_size,
        "_source": ["user_id"],
        "track_total_hits": True
    }

    try:
        # ⭐ survey_responses_merged만 사용
        if async_client:
            response_1st = await data_fetcher.search_opensearch_async(
                index_name=request.index_name,  # survey_responses_merged
                query=stage1_query,
                size=stage1_query_size,
                source_filter=None,
                request_timeout=DEFAULT_OS_TIMEOUT,
            )
        else:
            response_1st = data_fetcher.search_opensearch(
                index_name=request.index_name,  # survey_responses_merged
                query=stage1_query,
                size=stage1_query_size,
                source_filter=None,
                request_timeout=DEFAULT_OS_TIMEOUT,
            )
    except Exception as e:
        logger.warning(f"⚠️ 2단계 검색 Stage1 실패: {e}")
        return None

    timings['two_phase_stage1_ms'] = (perf_counter() - stage1_start) * 1000
    hits_1st = response_1st.get('hits', {}).get('hits', [])
    total_stage1 = response_1st.get('hits', {}).get('total', {}).get('value', len(hits_1st))

    if not hits_1st:
        logger.info("   ⚠️ Stage1에서 조건을 만족하는 user_id가 없습니다")
        total_time = (perf_counter() - overall_start) * 1000
        timings['total_ms'] = total_time
        timings.setdefault('two_phase_stage2_ms', 0.0)
        timings.setdefault('two_phase_fetch_demographics_ms', 0.0)
        timings.setdefault('lazy_join_ms', 0.0)
        timings.setdefault('post_filter_ms', 0.0)
        timings.setdefault('rrf_recombination_ms', 0.0)
        timings.setdefault('qdrant_parallel_ms', 0.0)
        timings.setdefault('opensearch_parallel_ms', timings['two_phase_stage1_ms'])
        logger.info("📈 성능 측정 요약 (ms):")
        for key in sorted(timings.keys()):
            logger.info(f"  - {key}: {timings[key]:.2f}")
        return SearchResponse(
            query=request.query,
            total_hits=0,
            max_score=0.0,
            results=[],
            query_analysis={
                "intent": analysis.intent,
                "must_terms": analysis.must_terms,
                "should_terms": analysis.should_terms,
                "alpha": analysis.alpha,
                "confidence": analysis.confidence,
                "extracted_entities": extracted_entities.to_dict(),
                "filters": filters,
                "size": size,
                "timings_ms": timings,
            },
            took_ms=int(total_time)
        )

    user_ids_filtered = []
    for hit in hits_1st:
        src = hit.get('_source', {})
        uid = src.get('user_id') or hit.get('_id')
        if uid:
            user_ids_filtered.append(uid)
    user_ids_filtered = list(dict.fromkeys(user_ids_filtered))

    logger.info(f"   ✅ Stage1 user_id 추출: {len(user_ids_filtered)}/{total_stage1}건")
    if total_stage1 > len(user_ids_filtered):
        logger.warning("   ⚠️ Stage1 size 제한으로 일부 user_id가 제외되었습니다")

    if not user_ids_filtered:
        total_time = (perf_counter() - overall_start) * 1000
        timings['two_phase_stage2_ms'] = 0.0
        timings['two_phase_fetch_demographics_ms'] = 0.0
        timings['lazy_join_ms'] = 0.0
        timings['post_filter_ms'] = 0.0
        timings['rrf_recombination_ms'] = 0.0
        timings.setdefault('opensearch_parallel_ms', timings['two_phase_stage1_ms'])
        timings['total_ms'] = total_time
        logger.info("📈 성능 측정 요약 (ms):")
        for key in sorted(timings.keys()):
            logger.info(f"  - {key}: {timings[key]:.2f}")
        return SearchResponse(
            query=request.query,
            total_hits=0,
            max_score=0.0,
            results=[],
            query_analysis={
                "intent": analysis.intent,
                "must_terms": analysis.must_terms,
                "should_terms": analysis.should_terms,
                "alpha": analysis.alpha,
                "confidence": analysis.confidence,
                "extracted_entities": extracted_entities.to_dict(),
                "filters": filters,
                "size": size,
                "timings_ms": timings,
            },
            took_ms=int(total_time)
        )

    max_terms = 10000
    if len(user_ids_filtered) > max_terms:
        logger.warning(f"   ⚠️ user_id가 {len(user_ids_filtered)}건입니다. 상위 {max_terms}건만 사용합니다")
        user_ids_filtered = user_ids_filtered[:max_terms]

    detail_size = max(size * 2, min(len(user_ids_filtered), 500))
    stage2_query = {
        "query": {
            "bool": {
                "must": [
                    {"terms": {"_id": user_ids_filtered}},
                ]
            }
        },
        "size": detail_size,
        "_source": {
            "includes": ["user_id", "metadata", "qa_pairs", "timestamp"]
        },
        "track_total_hits": True
    }

    stage2_start = perf_counter()
    try:
        # ⭐ survey_responses_merged만 사용
        if async_client:
            response_2nd = await data_fetcher.search_opensearch_async(
                index_name=request.index_name,  # survey_responses_merged
                query=stage2_query,
                size=detail_size,
                source_filter=None,
                request_timeout=DEFAULT_OS_TIMEOUT,
            )
        else:
            response_2nd = data_fetcher.search_opensearch(
                index_name=request.index_name,  # survey_responses_merged
                query=stage2_query,
                size=detail_size,
                source_filter=None,
                request_timeout=DEFAULT_OS_TIMEOUT,
            )
    except Exception as e:
        logger.warning(f"⚠️ 2단계 검색 Stage2 실패: {e}")
        return None

    timings['two_phase_stage2_ms'] = (perf_counter() - stage2_start) * 1000
    hits_2nd = response_2nd.get('hits', {}).get('hits', [])
    total_stage2 = response_2nd.get('hits', {}).get('total', {}).get('value', len(hits_2nd))
    logger.info(f"   ✅ Stage2 결과: {len(hits_2nd)}건 (총 {total_stage2}건)")

    if not hits_2nd:
        total_time = (perf_counter() - overall_start) * 1000
        timings.setdefault('two_phase_fetch_demographics_ms', 0.0)
        timings['lazy_join_ms'] = 0.0
        timings['post_filter_ms'] = 0.0
        timings['rrf_recombination_ms'] = 0.0
        timings.setdefault('opensearch_parallel_ms', timings.get('two_phase_stage1_ms', 0.0))
        timings['total_ms'] = total_time
        logger.info("📈 성능 측정 요약 (ms):")
        for key in sorted(timings.keys()):
            logger.info(f"  - {key}: {timings[key]:.2f}")
        return SearchResponse(
            query=request.query,
            total_hits=0,
            max_score=0.0,
            results=[],
            query_analysis={
                "intent": analysis.intent,
                "must_terms": analysis.must_terms,
                "should_terms": analysis.should_terms,
                "alpha": analysis.alpha,
                "confidence": analysis.confidence,
                "extracted_entities": extracted_entities.to_dict(),
                "filters": filters,
                "size": size,
                "timings_ms": timings,
            },
            took_ms=int(total_time)
        )

    final_hits = hits_2nd[:size]
    final_user_ids = [hit.get('_id') or hit.get('_source', {}).get('user_id') for hit in final_hits]

    # ⭐ survey_responses_merged만 사용하므로 welcome_1st/welcome_2nd 조회 제거
    timings['two_phase_fetch_demographics_ms'] = 0.0

    results: List[SearchResult] = []
    lazy_join_start = perf_counter()
    final_hits = final_hits if 'final_hits' in locals() else []
    for doc in final_hits:
        source = doc.get('_source', {}) or {}
        user_id = source.get('user_id') or hit.get('_id', '')
        # ⭐ survey_responses_merged만 사용하므로 source의 metadata에서 직접 가져오기
        source_metadata = source.get('metadata', {}) if isinstance(source, dict) else {}

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
            demographic_info['occupation'] = source_metadata.get('occupation')
            demographic_info["marital_status"] = source_metadata.get("marital_status")
            demographic_info["sub_region"] = source_metadata.get("sub_region")

        if 'occupation' not in demographic_info or not demographic_info['occupation']:
            qa_pairs_for_occ = source.get('qa_pairs', []) if isinstance(source, dict) else []
            for qa in qa_pairs_for_occ:
                if isinstance(qa, dict):
                    q_text = qa.get('q_text', '')
                    answer = str(qa.get('answer', qa.get('answer_text', '')))
                    if '직업' in q_text or 'occupation' in q_text.lower() or '직무' in q_text:
                        if answer:
                            demographic_info['occupation'] = answer
                        break

        # marital_status를 qa_pairs에서 찾기
        if 'marital_status' not in demographic_info or not demographic_info['marital_status']:
            qa_pairs_list = source.get('qa_pairs', []) if isinstance(source, dict) else []
            for qa in qa_pairs_list:
                if isinstance(qa, dict):
                    q_text = qa.get('q_text', '')
                    answer = str(qa.get('answer', qa.get('answer_text', '')))
                    if '결혼' in q_text or '혼인' in q_text:
                        if answer:
                            demographic_info['marital_status'] = answer
                        break

        # sub_region을 qa_pairs에서 찾기
        if 'sub_region' not in demographic_info or not demographic_info['sub_region']:
            qa_pairs_list = source.get('qa_pairs', []) if isinstance(source, dict) else []
            for qa in qa_pairs_list:
                if isinstance(qa, dict):
                    q_text = qa.get('q_text', '')
                    answer = str(qa.get('answer', qa.get('answer_text', '')))
                    if '구' in q_text or '군' in q_text or '세부지역' in q_text or '어느 구' in q_text:
                        if answer:
                            demographic_info['sub_region'] = answer
                        break

        matched_qa = []
        inner_hits = hit.get('inner_hits', {}).get('qa_pairs', {}).get('hits', {}).get('hits', [])
        for inner_hit in inner_hits:
            qa_data = inner_hit.get('_source', {}).copy()
            qa_data['match_score'] = inner_hit.get('_score')
            if 'highlight' in inner_hit:
                qa_data['highlights'] = inner_hit['highlight']
            matched_qa.append(qa_data)

        results.append(
            SearchResult(
                user_id=user_id,
                score=hit.get('_score', 0.0),
                timestamp=source.get('timestamp') if isinstance(source, dict) else None,
                demographic_info=demographic_info if demographic_info else None,
                behavioral_info=behavioral_info if behavioral_info else None,
                qa_pairs=source.get('qa_pairs', [])[:5] if isinstance(source, dict) else [],
                matched_qa_pairs=matched_qa,
                highlights=hit.get('highlight'),
            )
        )
    timings['lazy_join_ms'] = (perf_counter() - lazy_join_start) * 1000

    timings.setdefault('post_filter_ms', 0.0)
    timings.setdefault('rrf_recombination_ms', 0.0)
    timings.setdefault('qdrant_parallel_ms', 0.0)
    timings.setdefault('opensearch_parallel_ms', timings.get('two_phase_stage1_ms', 0.0) + timings.get('two_phase_stage2_ms', 0.0))

    total_duration_ms = (perf_counter() - overall_start) * 1000
    timings['total_ms'] = total_duration_ms

    logger.info("📈 성능 측정 요약 (ms):")
    for key in sorted(timings.keys()):
        logger.info(f"  - {key}: {timings[key]:.2f}")

    response_took_ms = int(total_duration_ms)
    total_hits = len(final_hits)
    max_score = final_hits[0].get('_score', 0.0) if final_hits else 0.0

    response_payload = SearchResponse(
        query=request.query,
        total_hits=total_hits,
        max_score=max_score,
        results=results,
        query_analysis={
            "intent": analysis.intent,
            "must_terms": analysis.must_terms,
            "should_terms": analysis.should_terms,
            "alpha": analysis.alpha,
            "confidence": analysis.confidence,
            "extracted_entities": extracted_entities.to_dict(),
            "filters": filters,
            "size": size,
            "timings_ms": timings,
        },
        took_ms=response_took_ms,
    )
    return response_payload

def get_user_id_from_doc(doc: Dict[str, Any]) -> Optional[str]:
    if not isinstance(doc, dict):
        return None
    source = doc.get('_source')
    if isinstance(source, dict):
        uid = source.get('user_id')
        if uid:
            return uid
        payload = source.get('payload')
        if isinstance(payload, dict):
            uid = payload.get('user_id')
            if uid:
                return uid
    uid = doc.get('_id')
    if uid:
        return uid
    payload = doc.get('payload')
    if isinstance(payload, dict):
        return payload.get('user_id')
    return None
