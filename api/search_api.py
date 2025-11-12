"""검색 API 라우터"""
import asyncio
import json
import logging
from collections import defaultdict, OrderedDict
from time import perf_counter
from typing import List, Dict, Any, Optional, Set, Tuple
from fastapi import APIRouter, HTTPException, Depends
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
logger.setLevel(logging.DEBUG)

router = APIRouter(
    prefix="/search",
    tags=["Search"]
)

# ⚠️ 임시 확장 타임아웃 (중첩 필터 제거 전까지 8~10초 유지)
DEFAULT_OS_TIMEOUT = 10

# 런타임 공유 객체 (한 번만 초기화 후 재사용)
router.analyzer = None  # type: ignore[attr-defined]
router.embedding_model = None  # type: ignore[attr-defined]
router.config = None  # type: ignore[attr-defined]

# 간단한 인메모리 LRU 캐시 (welcome 인덱스 전용)
_WELCOME_CACHE_MAX = 5000
_welcome_cache: Dict[str, OrderedDict] = {
    "s_welcome_1st": OrderedDict(),
    "s_welcome_2nd": OrderedDict(),
}


def _cache_get_welcome_doc(index_name: str, user_id: str) -> Optional[Dict[str, Any]]:
    bucket = _welcome_cache.get(index_name)
    if bucket is None:
        return None
    doc = bucket.get(user_id)
    if doc is not None:
        # 최근 사용 업데이트
        bucket.move_to_end(user_id)
    return doc


def _cache_put_welcome_doc(index_name: str, user_id: str, doc: Dict[str, Any]) -> None:
    bucket = _welcome_cache.get(index_name)
    if bucket is None:
        return
    bucket[user_id] = doc
    bucket.move_to_end(user_id)
    if len(bucket) > _WELCOME_CACHE_MAX:
        bucket.popitem(last=False)


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
    index_name: str = Field(default="*", description="검색할 인덱스 이름 (와일드카드 지원, 기본값: 전체 인덱스 '*')")
    size: int = Field(default=10, ge=1, le=100, description="반환할 결과 개수")
    use_vector_search: bool = Field(default=True, description="벡터 검색 사용 여부")


class SearchResult(BaseModel):
    """검색 결과 항목"""
    user_id: str
    score: float
    timestamp: Optional[str] = None
    demographic_info: Optional[Dict[str, Any]] = Field(default=None, description="인구통계 정보 (welcome_1st, welcome_2nd에서 조회)")
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



@router.get("/", summary="Search API 상태")
def search_root():
    """Search API 기본 정보"""
    return {
        "message": "Search API 실행 중",
        "version": "1.0",
        "endpoints": [
            "/search/query",
            "/search/similar"
        ]
    }



@router.post("/query", response_model=SearchResponse, summary="검색 쿼리 실행")
async def search_query(
    request: SearchRequest,
    os_client: OpenSearch = Depends(lambda: router.os_client),
):
    """
    자연어 쿼리로 설문 데이터 검색

    - 쿼리 분석 (의도 파악, 키워드 추출)
    - 하이브리드 검색 (키워드 + 벡터)
    - 인구통계 필터링
    - 결과 랭킹 및 포매팅
    """
    try:
        # OpenSearch 연결 확인
        if not os_client or not os_client.ping():
            raise HTTPException(
                status_code=503,
                detail="OpenSearch 서버에 연결할 수 없습니다."
            )

        # 임베딩 모델 및 설정 확인
        embedding_model = getattr(router, 'embedding_model', None)
        config = getattr(router, 'config', None)
        if config is None:
            from rag_query_analyzer.config import get_config
            config = get_config()
            router.config = config
        if embedding_model is None and hasattr(router, 'embedding_model_factory'):
            embedding_model = router.embedding_model_factory()
            router.embedding_model = embedding_model

        logger.info(f"\n{'='*60}")
        logger.info(f"[SEARCH] 검색 쿼리: '{request.query}'")
        logger.info(f"{'='*60}")

        # 1단계: 쿼리 분석
        logger.info("\n[1/3] 쿼리 분석 중...")
        analyzer = getattr(router, 'analyzer', None)
        if analyzer is None:
            analyzer = AdvancedRAGQueryAnalyzer(config)
            router.analyzer = analyzer
        query_analysis = analyzer.analyze_query(request.query)
        analysis = query_analysis

        logger.info(f"   - 의도: {query_analysis.intent}")
        logger.info(f"   - must_terms: {query_analysis.must_terms}")
        logger.info(f"   - should_terms: {query_analysis.should_terms}")
        logger.info(f"   - alpha: {query_analysis.alpha}")

        timings: Dict[str, float] = {}
        overall_start = perf_counter()

        # 2단계: 쿼리 빌드
        logger.info("\n[2/3] 검색 쿼리 생성 중...")
        query_builder = OpenSearchHybridQueryBuilder(config)

        # 임베딩 벡터 생성
        query_vector = None
        if request.use_vector_search and embedding_model:
            query_vector = embedding_model.encode(request.query).tolist()
            logger.info(f"   - 쿼리 벡터 생성 완료 (dim: {len(query_vector)})")

        # OpenSearch 쿼리 생성
        os_query = query_builder.build_query(
            analysis=query_analysis,
            query_vector=query_vector,
            size=request.size
        )

        # 3단계: 검색 실행
        logger.info("\n[3/3] 검색 실행 중...")

        # OpenSearch는 body에 반드시 객체 형태의 query가 있어야 합니다.
        # 하이브리드 빌더가 키워드가 없을 때 {'query': None}을 돌려주는 경우가 있어,
        # 그대로 전달하면 parsing_exception이 발생하므로 match_all로 치환합니다.
        if os_query.get("query") in (None, {}):
            logger.warning("⚠️ 검색 쿼리가 비어 있어 match_all 로 대체합니다")
            os_query["query"] = {"match_all": {}}

        # 하이브리드 검색 (OpenSearch + Qdrant + RRF)
        if request.use_vector_search and query_vector and hasattr(router, 'qdrant_client'):
            logger.info("   - 하이브리드 검색 모드 (OpenSearch + Qdrant + RRF)")

            # OpenSearch 키워드 검색
            logger.info("   - [1/3] OpenSearch 키워드 검색...")
            
            # OpenSearch _source filtering: 필요한 필드만 조회
            source_filter = {
                "includes": ["user_id", "metadata", "qa_pairs", "timestamp"],
                "excludes": []
            }

            # ------------------------------------------------------------
            # OpenSearch 검색 (필요시 병렬 실행)
            # ------------------------------------------------------------
           

            data_fetcher = DataFetcher(
                opensearch_client=os_client, # 동기적 
                qdrant_client=getattr(router, 'qdrant_client', None),
                async_opensearch_client=getattr(router, 'async_os_client', None) # 비동기
            )
            # ⭐ 필터가 있는 경우, 교집합을 위해 더 많은 결과를 가져와야 함
            has_filters = bool(os_query.get('query', {}).get('bool', {}).get('must'))
            
            # Qdrant top-N 제한: 필터 유무에 따라 분기
            if has_filters:
                # 필터 있음: 후보 수를 줄여 후처리 부담 완화
                qdrant_limit = min(300, max(150, request.size * 5))
                search_size = max(500, min(request.size * 15, 3000))
                logger.info(f"🔍 필터 적용: OpenSearch size={search_size}, Qdrant limit={qdrant_limit} (size*5 전략)")
            else:
                # 필터 없음: 소량만 읽기
                qdrant_limit = min(150, max(60, request.size * 2))
                search_size = max(request.size * 2, 200)
                logger.info(f"🔍 필터 없음: OpenSearch size={search_size}, Qdrant limit={qdrant_limit}")
            
            # OpenSearch _source filtering: 필요한 필드만 조회
            source_filter = {
                "includes": ["user_id", "metadata", "qa_pairs", "timestamp"],
                "excludes": []  # 필요시 제외할 필드 추가
            }
            
            os_response = data_fetcher.search_opensearch(
                index_name=request.index_name,
                query=os_query,
                size=search_size,
                source_filter=source_filter,
                request_timeout=DEFAULT_OS_TIMEOUT,
            )
            logger.info(f"      → OpenSearch: {len(os_response['hits']['hits'])}건")

            inner_hits_map: Dict[str, List[Dict[str, Any]]] = {}
            for hit in os_response['hits']['hits']:
                user_id = hit.get('_source', {}).get('user_id') or hit.get('_id')
                if not user_id:
                    continue
                matches = extract_inner_hit_matches(hit)
                if matches:
                    inner_hits_map[user_id] = matches
                    logger.debug(
                        "[inner_hits_map] user_id=%s matches=%d", user_id, len(matches)
                    )

            # Qdrant 벡터 검색 (모든 컬렉션)
            logger.info("   - [2/3] Qdrant 벡터 검색 (모든 컬렉션)...")
            qdrant_client = router.qdrant_client

            collection_names: List[str] = []
            try:
                collections = qdrant_client.get_collections()
                collection_names = [col.name for col in collections.collections]
                logger.info(f"      → 검색할 컬렉션: {collection_names}")
            except Exception as e:
                logger.warning(f"      → Qdrant 컬렉션 목록 가져오기 실패: {e}")

            qdrant_results_raw = []
            if collection_names:
                try:
                    qdrant_start = perf_counter()
                    adaptive_threshold, threshold_reason = get_adaptive_score_threshold(
                        query=request.query,
                        has_filters=has_demographic_filters,
                        must_terms_count=len(getattr(analysis, "must_terms", []) or []),
                    )
                    results_map = await search_qdrant_collections_async(
                        qdrant_client=qdrant_client,
                        collection_names=collection_names,
                        query_vector=query_vector,
                        limit=qdrant_limit,
                        score_threshold=adaptive_threshold,
                    )
                    duration_ms = (perf_counter() - qdrant_start) * 1000
                    for name, items in results_map.items():
                        logger.info(f"      → {name}: {len(items)}건 (limit={qdrant_limit})")
                        qdrant_results_raw.extend(items)
                    logger.info(
                        f"      → 병렬 Qdrant 검색 완료: {len(qdrant_results_raw)}건 "
                        f"(총 컬렉션 {len(collection_names)}개, {duration_ms:.1f}ms)"
                    )
                except Exception as e:
                    logger.warning(f"      → Qdrant 병렬 검색 실패: {e}")

            # 점수 순으로 정렬
            qdrant_results_raw.sort(key=lambda x: x.get('_score', 0.0), reverse=True)
            qdrant_results_raw = qdrant_results_raw[:qdrant_limit]

            # RRF로 결합
            logger.info("   - [3/3] RRF 결합 중...")
            keyword_results = os_response['hits']['hits']
            vector_results = [
                {
                    '_id': item.get('_id'),
                    '_score': item.get('_score'),
                    '_source': item.get('_source', {})
                }
                for item in qdrant_results_raw
            ]

            combined_results, rrf_k_used, rrf_reason = calculate_rrf_score_adaptive(
                keyword_results=keyword_results,
                vector_results=vector_results,
                query_intent=getattr(analysis, "intent", None),
                has_filters=has_demographic_filters,
                use_vector_search=request.use_vector_search,
            )

            must_terms: List[str] = []
            if getattr(analysis, "must_terms", None):
                must_terms = [term for term in analysis.must_terms if term]
                if must_terms:
                    logger.info(f"   - Must-term 검증 시작: {must_terms}")
                    before_count = len(combined_results)
                    combined_results = [
                        doc for doc in combined_results if contains_must_terms(doc, must_terms)
                    ]
                    removed = before_count - len(combined_results)
                    logger.info(
                        f"   - Must-term 검증 완료: {len(combined_results)}/{before_count}건 유지"
                    )
                    if removed > 0:
                        logger.warning(
                            f"     ⚠️ Must-term 미일치 문서 {removed}건 제거 (Qdrant 랭크 제외)"
                        )

            # 상위 N개만 선택
            final_hits = combined_results[:request.size]
            logger.info(f"      → RRF 결합 완료: {len(final_hits)}건")

            # 최종 상세 정보 (_mget) 조회
            user_docs_map: Dict[str, Dict[str, Any]] = {}
            user_ids_for_mget: List[str] = []
            for doc in final_hits:
                source_candidate = doc.get('_source') or {}
                if not source_candidate and 'doc' in doc:
                    source_candidate = doc.get('doc', {}).get('_source', {})
                user_id_candidate = (
                    source_candidate.get('user_id')
                    or doc.get('_id')
                    or doc.get('id')
                )
                if not user_id_candidate and 'payload' in doc:
                    payload = doc['payload']
                    if isinstance(payload, dict):
                        user_id_candidate = payload.get('user_id')
                if user_id_candidate and user_id_candidate not in user_docs_map:
                    user_docs_map[user_id_candidate] = source_candidate if isinstance(source_candidate, dict) else {}
                    user_ids_for_mget.append(user_id_candidate)

            if user_ids_for_mget:
                try:
                    final_docs_raw = await data_fetcher.multi_get_documents_async(
                        index_name=request.index_name,
                        doc_ids=user_ids_for_mget,
                        source_fields=["user_id", "metadata", "demographic_info", "qa_pairs", "timestamp"],
                    )
                    for doc_item in final_docs_raw:
                        if not isinstance(doc_item, dict):
                            continue
                        if not doc_item.get('found'):
                            continue
                        doc_id = doc_item.get('_id')
                        src = doc_item.get('_source', {})
                        if doc_id and isinstance(src, dict):
                            user_docs_map[doc_id] = src
                except Exception as e:
                    logger.warning(f"     ⚠️ 최종 문서 조회 실패: {e}")

            # 결과 포매팅 (RRF 순서 유지)
            results = []
            for doc in final_hits:
                source = doc.get('_source') or {}
                if not source and 'doc' in doc:
                    source = doc.get('doc', {}).get('_source', {})
                if not source and 'payload' in doc:
                    source = doc['payload']

                if isinstance(source, dict) and 'payload' in source and 'user_id' not in source:
                    payload = source['payload']
                    user_id = payload.get('user_id', '') if isinstance(payload, dict) else ''
                else:
                    user_id = source.get('user_id') or doc.get('_id', '')

                if user_id and user_id in user_docs_map:
                    merged_source = {}
                    if isinstance(source, dict):
                        merged_source.update(source)
                    merged_source.update(user_docs_map[user_id])
                    source = merged_source
                    logger.debug(
                        "[mget_merge] user_id=%s qa_pairs=%d", 
                        user_id,
                        len(source.get('qa_pairs', [])) if isinstance(source, dict) else -1,
                    )
                elif isinstance(source, dict) and user_id:
                    user_docs_map.setdefault(user_id, source)

                qa_pairs_display = get_display_qa_pairs(source, must_terms, limit=10)
                matched_qa_pairs = inner_hits_map.get(user_id, [])
                if not matched_qa_pairs:
                    matched_qa_pairs = extract_matched_qa_pairs(source, must_terms)

                qa_pairs_display = reorder_with_matches(
                    source.get('qa_pairs', []),
                    matched_qa_pairs,
                    limit=10
                ) if isinstance(source, dict) else qa_pairs_display

                demographic_info = None
                if isinstance(source, dict):
                    demographic_info = source.get('demographic_info') or source.get('metadata')

                result = SearchResult(
                    user_id=user_id,
                    score=doc.get('_score', 0.0),
                    timestamp=source.get('timestamp') if isinstance(source, dict) else None,
                    demographic_info=demographic_info,
                    qa_pairs=qa_pairs_display[:5],
                    matched_qa_pairs=matched_qa_pairs,
                    highlights=None
                )
                results.append(result)
                logger.debug(
                    "[match_check] user_id=%s inner_hits=%d matched=%d", 
                    user_id,
                    len(inner_hits_map.get(user_id, [])),
                    len(matched_qa_pairs),
                )

            total_hits = max(os_response['hits']['total']['value'], len(qdrant_results_raw))
            max_score = final_hits[0].get('_score', 0.0) if final_hits else 0.0
            took_ms = os_response['took']

        else:
            # 기존 OpenSearch 단독 검색
            logger.info("   - OpenSearch 키워드 검색만 사용")
            data_fetcher = DataFetcher(
                opensearch_client=os_client,
                qdrant_client=getattr(router, 'qdrant_client', None),
                async_opensearch_client=getattr(router, 'async_os_client', None)
            )
            search_response = data_fetcher.search_opensearch(
                index_name=request.index_name,
                query=os_query,
                size=request.size
            )

            # 결과 포매팅
            results = []
            for hit in search_response['hits']['hits']:
                # inner_hits에서 매칭된 qa_pairs 추출
                matched_qa = []
                if 'inner_hits' in hit and 'qa_pairs' in hit['inner_hits']:
                    for inner_hit in hit['inner_hits']['qa_pairs']['hits']['hits']:
                        qa_data = inner_hit['_source'].copy()
                        qa_data['match_score'] = inner_hit['_score']
                        if 'highlight' in inner_hit:
                            qa_data['highlights'] = inner_hit['highlight']
                        matched_qa.append(qa_data)

                if not matched_qa and must_terms:
                    matched_qa = extract_matched_qa_pairs(hit['_source'], must_terms)

                qa_pairs_display = reorder_with_matches(
                    hit['_source'].get('qa_pairs', []) if isinstance(hit['_source'], dict) else [],
                    matched_qa,
                    limit=10
                )

                demographic_info = None
                if isinstance(hit['_source'], dict):
                    demographic_info = hit['_source'].get('demographic_info') or hit['_source'].get('metadata')

                result = SearchResult(
                    user_id=hit['_source'].get('user_id', ''),
                    score=hit['_score'],
                    timestamp=hit['_source'].get('timestamp'),
                    demographic_info=demographic_info,
                    qa_pairs=qa_pairs_display[:5],
                    matched_qa_pairs=matched_qa,
                    highlights=hit.get('highlight')
                )
                results.append(result)

            total_hits = search_response['hits']['total']['value']
            max_score = search_response['hits']['max_score']
            took_ms = search_response['took']

        logger.info(f"\n[OK] 검색 완료: {len(results)}건 반환")
        logger.info(f"{'='*60}\n")

        return SearchResponse(
            query=request.query,
            total_hits=total_hits,
            max_score=max_score,
            results=results,
            query_analysis={
                "intent": query_analysis.intent,
                "must_terms": query_analysis.must_terms,
                "should_terms": query_analysis.should_terms,
                "alpha": query_analysis.alpha,
                "confidence": query_analysis.confidence
            },
            took_ms=took_ms
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[ERROR] 검색 중 오류 발생: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"검색 중 오류 발생: {str(e)}"
        )


class NLSearchRequest(BaseModel):
    """자연어 기반 검색 요청 (필터/size 자동 추출)"""
    query: str = Field(..., description="자연어 쿼리 (예: '30대 사무직 300명 데이터 보여줘')")
    index_name: str = Field(default="*", description="검색할 인덱스 이름 (기본값: 전체 인덱스 '*')")
    use_vector_search: bool = Field(default=True, description="벡터 검색 사용 여부")


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

        config = getattr(router, 'config', None)
        if config is None:
            from rag_query_analyzer.config import get_config
            config = get_config()
            router.config = config

        analyzer = getattr(router, 'analyzer', None)
        if analyzer is None:
            analyzer = AdvancedRAGQueryAnalyzer(config)
            router.analyzer = analyzer
        analysis = analyzer.analyze_query(request.query)
        if analysis is None:
            raise RuntimeError("Query analysis returned None")
        query_analysis = analysis

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
        for demo in extracted_entities.demographics:
            metadata_only = demo.demographic_type in {DemographicType.AGE, DemographicType.GENDER}
            include_nested_fallback = demo.demographic_type not in {DemographicType.OCCUPATION}
            filter_clause = demo.to_opensearch_filter(
                metadata_only=metadata_only,
                include_qa_fallback=include_nested_fallback,
            )
            if filter_clause and filter_clause != {"match_all": {}}:
                filters.append(filter_clause)
        filters_for_response = list(filters)
        size = max(1, min(requested_size, 100))
        
        age_gender_filters = [f for f in filters if is_age_or_gender_filter(f)]
        occupation_filters = [f for f in filters if is_occupation_filter(f)]
        other_filters = [f for f in filters if f not in age_gender_filters and f not in occupation_filters]

        filters_os = age_gender_filters + other_filters
        filters = filters_os  # 유지보수: 기존 로직과 호환성을 위해
        has_demographic_filters = bool(filters_for_response)

        logger.info("🔍 필터 상태 체크:")
        logger.info(f"  - age_gender_filters: {len(age_gender_filters)}개")
        logger.info(f"  - occupation_filters: {len(occupation_filters)}개")
        logger.info(f"  - other_filters: {len(other_filters)}개")

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
            return two_phase_response

        # 2) 분석 + 쿼리 빌드
        # ⭐ 최종 키워드 정제: 메타 키워드, 수량 패턴, Demographics 제거
        import re

        def strip_korean_particles(term: str) -> str:
            if not term:
                return term
            particles = [
                '에는', '에서', '으로', '도', '은', '는', '이', '가',
                '을', '를', '와', '과', '인'
            ]
            normalized = term
            for _ in range(10):
                changed = False
                for particle in particles:
                    if normalized.endswith(particle) and len(normalized) > len(particle):
                        normalized = normalized[:-len(particle)]
                        changed = True
                        break
                if not changed or len(normalized) <= 1:
                    break
            return normalized

        meta_keywords = {
            '설문조사', '설문', '데이터', '자료', '정보',
            '보여줘', '보여주세요', '알려줘', '알려주세요',
            '검색', '찾아줘', '찾아주세요', '조회',
            '을', '를', '이', '가', '의', '에', '에서',
            '와', '과', '에게', '한테', '명', '개', '건',
            '사람', '인', '분', '중', '중에', '중에서'
        }

        quantity_pattern = re.compile(r'\d+\s*(명|건)')

        extracted_keywords = set()
        for demo in extracted_entities.demographics:
            extracted_keywords.add(demo.raw_value)
            extracted_keywords.update(demo.synonyms)

        extracted_keywords_stripped = set(strip_korean_particles(k) for k in extracted_keywords)

        if analysis is None:
            raise RuntimeError("Query analysis not initialized")

        original_must = analysis.must_terms.copy()
        original_should = analysis.should_terms.copy()

        def is_demographic_term(term: str) -> bool:
            if term in extracted_keywords:
                return True
            stripped = strip_korean_particles(term)
            return stripped in extracted_keywords or stripped in extracted_keywords_stripped

        analysis.must_terms = [
            t for t in analysis.must_terms
            if (
                t not in meta_keywords and
                not quantity_pattern.search(t) and
                not is_demographic_term(t)
            )
        ]

        analysis.should_terms = [
            t for t in analysis.should_terms
            if (
                t not in meta_keywords and
                not quantity_pattern.search(t) and
                not is_demographic_term(t)
            )
        ]

        removed_meta = [t for t in (original_must + original_should) if t in meta_keywords]
        removed_demo = [t for t in (original_must + original_should) if is_demographic_term(t)]
        removed_quantity = [t for t in (original_must + original_should) if quantity_pattern.search(t)]

        logger.info(f"🔍 최종 키워드 정제:")
        logger.info(f"  - Must terms: {analysis.must_terms} (원본: {original_must})")
        logger.info(f"  - Should terms: {analysis.should_terms} (원본: {original_should})")
        if removed_meta:
            logger.info(f"  - ❌ 제거된 메타 키워드: {removed_meta}")
        if removed_demo:
            logger.info(f"  - ❌ 제거된 Demographics: {removed_demo} (필터로만 처리)")
        if removed_quantity:
            logger.info(f"  - ❌ 제거된 수량 패턴: {removed_quantity}")
        logger.info(f"  - ✅ Demographics 필터: {[d.raw_value for d in extracted_entities.demographics]}")

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
                                                elif '직업' in str(q_text_val):
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
                                        elif '직업' in str(q_text_val):
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
                if len(type_filters) == 1:
                    # 단일 필터: 필터를 그대로 사용 (이미 bool 쿼리 형태)
                    filter_item = type_filters[0]
                    should_filters.append(filter_item)
                else:
                    # 같은 타입 필터는 OR
                    should_filters.append({
                        'bool': {
                            'should': type_filters,
                            "minimum_should_match": 1
                        }
                    })
            
            # ⭐ 기존 쿼리와 필터 결합 (must로 결합: 모든 필터를 만족해야 함)
            # welcome_1st: 연령/성별, welcome_2nd: 직업 정보
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
            import json
            logger.info(f"🔍 적용된 필터 ({len(filters_os)}개):")
            for i, f in enumerate(filters_os, 1):
                logger.info(f"  필터 {i}: {json.dumps(f, ensure_ascii=False, indent=2)}")
            logger.info(f"🔍 최종 쿼리 구조:")
            logger.info(f"  {json.dumps(final_query, ensure_ascii=False, indent=2)}")
        else:
            import json
            logger.info(f"🔍 최종 쿼리 구조 (필터 없음):")
            logger.info(f"  {json.dumps(final_query, ensure_ascii=False, indent=2)}")

        # ⭐ Qdrant top-N 제한: 필터 유무에 따라 분기
        has_filters = bool(filters_os or occupation_filters)
        rrf_k_used: Optional[int] = None
        rrf_reason: str = ""
        adaptive_threshold: Optional[float] = None
        threshold_reason: str = ""
        if has_filters:
            qdrant_limit = min(300, max(150, size * 5))
            search_size = max(500, min(size * 15, 3000))
            logger.info(f"🔍 필터 적용: OpenSearch size={search_size}, Qdrant limit={qdrant_limit} (size*5 전략)")
        else:
            qdrant_limit = min(150, max(60, size * 2))
            search_size = max(size * 2, 200)
            logger.info(f"🔍 필터 없음: OpenSearch size={search_size}, Qdrant limit={qdrant_limit}")

        # 4) 실행: 하이브리드 (OpenSearch + 선택적 Qdrant) with RRF
        # ⭐ STEP 1: welcome_1st와 welcome_2nd를 각각 별도로 검색
        
        # OpenSearch _source filtering: 필요한 필드만 조회
        source_filter = {
            "includes": ["user_id", "metadata", "qa_pairs", "timestamp"],
            "excludes": []  # 필요시 제외할 필드 추가
        }
        
        # welcome_1st와 welcome_2nd를 별도로 검색할지 결정
        # 필터에 연령/성별이 있으면 welcome_1st 검색, 직업이 있으면 welcome_2nd 검색
        search_welcome_1st = False
        search_welcome_2nd = False
        search_other_indices = True
        
        if filters:
            for demo in extracted_entities.demographics:
                if demo.demographic_type == DemographicType.AGE or demo.demographic_type == DemographicType.GENDER:
                    search_welcome_1st = True
                elif demo.demographic_type == DemographicType.OCCUPATION:
                    search_welcome_2nd = True
        
        # 필터가 없거나 모든 인덱스를 검색해야 하는 경우
        if not filters or request.index_name == '*':
            search_welcome_1st = True
            search_welcome_2nd = True
        
        logger.info(f"🔍 인덱스별 검색 전략:")
        logger.info(f"  - welcome_1st 검색: {search_welcome_1st}")
        logger.info(f"  - welcome_2nd 검색: {search_welcome_2nd}")
        logger.info(f"  - 기타 인덱스 검색: {search_other_indices}")
        
        # ⭐ 인덱스별 필터 분리: welcome_1st는 연령/성별만, welcome_2nd는 직업만
        logger.info(f"🔍 인덱스별 검색 전략:")
        logger.info(f"  - welcome_1st 검색: {search_welcome_1st}")
        logger.info(f"  - welcome_2nd 검색: {search_welcome_2nd}")
        logger.info(f"  - 기타 인덱스 검색: {search_other_indices}")

        def create_safe_query_template(size_value: int) -> Dict[str, Any]:
            """안전한 기본 쿼리 생성"""
            return {
                'query': {'match_all': {}},
                'size': size_value,
                '_source': {
                    'includes': ['user_id', 'metadata', 'qa_pairs', 'timestamp']
                }
            }

        welcome_1st_query = create_safe_query_template(search_size)
        welcome_2nd_query = create_safe_query_template(search_size)

        if filters:
            logger.info(f"🔍 인덱스별 필터 분리 중...")

            age_gender_filters_split = [f for f in filters if is_age_or_gender_filter(f)]
            occupation_filters_split = [f for f in filters if is_occupation_filter(f)]

            logger.info(f"  - 연령/성별 필터: {len(age_gender_filters_split)}개")
            logger.info(f"  - 직업 필터: {len(occupation_filters_split)}개")

            if age_gender_filters_split:
                welcome_1st_query['query'] = {
                    'bool': {
                        'must': age_gender_filters_split
                    }
                }
                logger.info(f"  ✅ welcome_1st: 연령/성별 필터 {len(age_gender_filters_split)}개 적용")
            else:
                logger.info(f"  ⚠️ welcome_1st: 필터 없음, match_all 사용")

            if occupation_filters_split:
                welcome_2nd_query['query'] = {
                    'bool': {
                        'must': occupation_filters_split
                    }
                }
                logger.info(f"  ✅ welcome_2nd: 직업 필터 {len(occupation_filters_split)}개 적용")
            else:
                logger.info(f"  ⚠️ welcome_2nd: 필터 없음, match_all 사용")
        else:
            logger.info(f"  ⚠️ 필터 없음: 모든 인덱스에서 match_all 사용")

        import json
        logger.info(f"📋 최종 쿼리 확인:")
        logger.info(f"  welcome_1st: {json.dumps(welcome_1st_query, ensure_ascii=False)[:200]}...")
        logger.info(f"  welcome_2nd: {json.dumps(welcome_2nd_query, ensure_ascii=False)[:200]}...")

        welcome_1st_keyword_results: List[Dict[str, Any]] = []
        welcome_1st_vector_results: List[Dict[str, Any]] = []
        if search_welcome_1st:
            logger.info(f"📊 [1/3] welcome_1st 검색 중...")
            try:
                if 'query' not in welcome_1st_query or not welcome_1st_query['query']:
                    raise ValueError("welcome_1st_query에 'query' 키가 없습니다")

                os_response_1st = data_fetcher.search_opensearch(
                    index_name="s_welcome_1st",
                    query=remove_inner_hits(welcome_1st_query),
                    size=search_size,
                    source_filter=source_filter,
                    request_timeout=DEFAULT_OS_TIMEOUT,
                )
                welcome_1st_keyword_results = os_response_1st['hits']['hits']
                logger.info(f"  ✅ OpenSearch: {len(welcome_1st_keyword_results)}건")
                
                # Qdrant 벡터 검색
                if request.use_vector_search and query_vector and hasattr(router, 'qdrant_client'):
                    qdrant_client = router.qdrant_client
                    try:
                        r = qdrant_client.search(
                            collection_name="s_welcome_1st",
                            query_vector=query_vector,
                            limit=qdrant_limit,  # 필터 유무에 따라 분기된 limit 사용
                            score_threshold=0.3,
                        )
                        for item in r:
                            welcome_1st_vector_results.append({
                                '_id': str(item.id),
                                '_score': item.score,
                                '_source': item.payload
                            })
                        logger.info(f"  ✅ Qdrant: {len(welcome_1st_vector_results)}건")
                    except Exception as e:
                        logger.debug(f"  ⚠️ Qdrant 검색 실패: {e}")
            except Exception as e:
                logger.warning(f"  ⚠️ welcome_1st 검색 실패: {e}")
        
        # welcome_2nd 검색
        welcome_2nd_keyword_results: List[Dict[str, Any]] = []
        welcome_2nd_vector_results: List[Dict[str, Any]] = []
        if search_welcome_2nd:
            logger.info(f"📊 [2/3] welcome_2nd 검색 중...")
            try:
                os_response_2nd = data_fetcher.search_opensearch(
                    index_name="s_welcome_2nd",
                    query=remove_inner_hits(welcome_2nd_query),
                    size=search_size,
                    source_filter=source_filter,
                    request_timeout=DEFAULT_OS_TIMEOUT,
                )
                welcome_2nd_keyword_results = os_response_2nd['hits']['hits']
                logger.info(f"  ✅ OpenSearch: {len(welcome_2nd_keyword_results)}건")
                
                # Qdrant 벡터 검색
                if request.use_vector_search and query_vector and hasattr(router, 'qdrant_client'):
                    qdrant_client = router.qdrant_client
                    try:
                        r = qdrant_client.search(
                            collection_name="s_welcome_2nd",
                            query_vector=query_vector,
                            limit=qdrant_limit,  # 필터 유무에 따라 분기된 limit 사용
                            score_threshold=0.3,
                        )
                        for item in r:
                            welcome_2nd_vector_results.append({
                                '_id': str(item.id),
                                '_score': item.score,
                                '_source': item.payload
                            })
                        logger.info(f"  ✅ Qdrant: {len(welcome_2nd_vector_results)}건")
                    except Exception as e:
                        logger.debug(f"  ⚠️ Qdrant 검색 실패: {e}")
            except Exception as e:
                logger.warning(f"  ⚠️ welcome_2nd 검색 실패: {e}")
        
        # 기타 인덱스 검색 (survey_* 등)
        other_keyword_results: List[Dict[str, Any]] = []
        other_vector_results: List[Dict[str, Any]] = []
        if search_other_indices:
            logger.info(f"📊 [3/3] 기타 인덱스 검색 중...")
            # welcome_1st, welcome_2nd를 제외한 인덱스 검색
            other_index_pattern = request.index_name
            if request.index_name == '*':
                # survey_* 패턴으로 검색 (welcome_1st, welcome_2nd 제외)
                other_index_pattern = "survey_*"
            elif 's_welcome_1st' in request.index_name or 's_welcome_2nd' in request.index_name:
                # welcome 인덱스를 제외한 패턴 생성
                indices = [idx.strip() for idx in request.index_name.split(',')]
                other_indices = [idx for idx in indices if idx not in ['s_welcome_1st', 's_welcome_2nd']]
                if other_indices:
                    other_index_pattern = ','.join(other_indices)
                else:
                    search_other_indices = False
            
            if search_other_indices:
                try:
                    other_query_body = final_query.copy()
                    if not isinstance(other_query_body.get('query'), dict):
                        logger.warning("  ⚠️ 기타 인덱스 쿼리가 비어 있어 match_all로 대체합니다")
                        other_query_body['query'] = {"match_all": {}}
                    os_response_other = data_fetcher.search_opensearch(
                        index_name=other_index_pattern,
                        query=other_query_body,
                        size=search_size,
                        source_filter=source_filter,
                        request_timeout=DEFAULT_OS_TIMEOUT,
                    )
                    other_keyword_results = os_response_other['hits']['hits']
                    logger.info(f"  ✅ OpenSearch: {len(other_keyword_results)}건")
                    
                    # Qdrant 벡터 검색 (기타 컬렉션)
                    if request.use_vector_search and query_vector and hasattr(router, 'qdrant_client'):
                        qdrant_client = router.qdrant_client
                        try:
                            collections = qdrant_client.get_collections()
                            for col in collections.collections:
                                if col.name not in ['s_welcome_1st', 's_welcome_2nd']:
                                    try:
                                        r = qdrant_client.search(
                                            collection_name=col.name,
                                            query_vector=query_vector,
                                            limit=qdrant_limit,  # 필터 유무에 따라 분기된 limit 사용
                                            score_threshold=0.3,
                                        )
                                        for item in r:
                                            other_vector_results.append({
                                                '_id': str(item.id),
                                                '_score': item.score,
                                                '_source': item.payload
                                            })
                                    except Exception:
                                        continue
                            logger.info(f"  ✅ Qdrant: {len(other_vector_results)}건")
                        except Exception as e:
                            logger.debug(f"  ⚠️ Qdrant 검색 실패: {e}")
                except Exception as e:
                    logger.warning(f"  ⚠️ 기타 인덱스 검색 실패: {e}")
        
        # ⭐ STEP 2: 각 인덱스별 RRF 결합
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 STEP 2: 각 인덱스별 RRF 결합")
        logger.info(f"{'='*60}")
        
        # welcome_1st RRF 결합
        welcome_1st_rrf = []
        if welcome_1st_keyword_results or welcome_1st_vector_results:
            logger.info(f"🔄 [1/3] welcome_1st RRF 결합 중...")
            logger.info(f"  - 키워드: {len(welcome_1st_keyword_results)}건, 벡터: {len(welcome_1st_vector_results)}건")
            if welcome_1st_vector_results:
                welcome_1st_rrf = calculate_rrf_score(welcome_1st_keyword_results, welcome_1st_vector_results, k=60)
                logger.info(f"  ✅ welcome_1st RRF 완료: {len(welcome_1st_rrf)}건")
            else:
                welcome_1st_rrf = welcome_1st_keyword_results
                logger.info(f"  ✅ welcome_1st 키워드만 사용: {len(welcome_1st_rrf)}건")
        
        # welcome_2nd RRF 결합
        welcome_2nd_rrf = []
        if welcome_2nd_keyword_results or welcome_2nd_vector_results:
            logger.info(f"🔄 [2/3] welcome_2nd RRF 결합 중...")
            logger.info(f"  - 키워드: {len(welcome_2nd_keyword_results)}건, 벡터: {len(welcome_2nd_vector_results)}건")
            if welcome_2nd_vector_results:
                welcome_2nd_rrf = calculate_rrf_score(welcome_2nd_keyword_results, welcome_2nd_vector_results, k=60)
                logger.info(f"  ✅ welcome_2nd RRF 완료: {len(welcome_2nd_rrf)}건")
            else:
                welcome_2nd_rrf = welcome_2nd_keyword_results
                logger.info(f"  ✅ welcome_2nd 키워드만 사용: {len(welcome_2nd_rrf)}건")
        
        # 기타 인덱스 RRF 결합
        other_rrf = []
        if other_keyword_results or other_vector_results:
            logger.info(f"🔄 [3/3] 기타 인덱스 RRF 결합 중...")
            logger.info(f"  - 키워드: {len(other_keyword_results)}건, 벡터: {len(other_vector_results)}건")
            if other_vector_results:
                other_rrf = calculate_rrf_score(other_keyword_results, other_vector_results, k=60)
                logger.info(f"  ✅ 기타 인덱스 RRF 완료: {len(other_rrf)}건")
            else:
                other_rrf = other_keyword_results
                logger.info(f"  ✅ 기타 인덱스 키워드만 사용: {len(other_rrf)}건")
        
        # user_id 및 _id -> 원본 문서 매핑 생성 (모든 인덱스 결과에서)
        user_doc_map = {}
        id_doc_map = {}  # _id 기반 매핑도 추가
        
        # welcome_1st 매핑
        for hit in welcome_1st_keyword_results:
            source = hit.get('_source', {})
            user_id = source.get('user_id')
            doc_id = hit.get('_id')
            
            doc_info = {
                'source': source,
                'inner_hits': hit.get('inner_hits', {}),
                'highlight': hit.get('highlight'),
                'index': 's_welcome_1st'
            }
            
            if user_id:
                user_doc_map[user_id] = doc_info
            if doc_id:
                id_doc_map[doc_id] = doc_info
        
        # welcome_2nd 매핑
        for hit in welcome_2nd_keyword_results:
            source = hit.get('_source', {})
            user_id = source.get('user_id')
            doc_id = hit.get('_id')
            
            doc_info = {
                'source': source,
                'inner_hits': hit.get('inner_hits', {}),
                'highlight': hit.get('highlight'),
                'index': 's_welcome_2nd'
            }
            
            if user_id:
                user_doc_map[user_id] = doc_info
            if doc_id:
                id_doc_map[doc_id] = doc_info
        
        # 기타 인덱스 매핑
        for hit in other_keyword_results:
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

        # ⭐ STEP 3: 인덱스 간 RRF 재결합
        # welcome_1st, welcome_2nd, 기타 인덱스의 RRF 결과를 user_id 기준으로 RRF 재결합
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 STEP 3: 인덱스 간 RRF 재결합")
        logger.info(f"{'='*60}")
        logger.info(f"  - welcome_1st RRF: {len(welcome_1st_rrf)}건")
        logger.info(f"  - welcome_2nd RRF: {len(welcome_2nd_rrf)}건")
        logger.info(f"  - 기타 인덱스 RRF: {len(other_rrf)}건")

        rrf_start = perf_counter()

        # user_id 기준으로 그룹화하여 RRF 재결합
        user_rrf_map = {}  # user_id -> [doc1, doc2, ...]
        
        # welcome_1st RRF 결과 그룹화
        for doc in welcome_1st_rrf:
            source = doc.get('_source', {})
            if not source and 'doc' in doc:
                source = doc.get('doc', {}).get('_source', {})
            user_id = source.get('user_id') if isinstance(source, dict) else None
            if not user_id:
                user_id = doc.get('_id', '')
            
            if user_id:
                if user_id not in user_rrf_map:
                    user_rrf_map[user_id] = []
                # 인덱스 정보 추가
                doc['_index'] = 's_welcome_1st'
                user_rrf_map[user_id].append(doc)
        
        # welcome_2nd RRF 결과 그룹화
        for doc in welcome_2nd_rrf:
            source = doc.get('_source', {})
            if not source and 'doc' in doc:
                source = doc.get('doc', {}).get('_source', {})
            user_id = source.get('user_id') if isinstance(source, dict) else None
            if not user_id:
                user_id = doc.get('_id', '')
            
            if user_id:
                if user_id not in user_rrf_map:
                    user_rrf_map[user_id] = []
                # 인덱스 정보 추가
                doc['_index'] = 's_welcome_2nd'
                user_rrf_map[user_id].append(doc)
        
        # 기타 인덱스 RRF 결과 그룹화
        for doc in other_rrf:
            source = doc.get('_source', {})
            if not source and 'doc' in doc:
                source = doc.get('doc', {}).get('_source', {})
            user_id = source.get('user_id') if isinstance(source, dict) else None
            if not user_id:
                user_id = doc.get('_id', '')
            
            if user_id:
                if user_id not in user_rrf_map:
                    user_rrf_map[user_id] = []
                # 인덱스 정보 유지 또는 추가
                if '_index' not in doc:
                    doc['_index'] = source.get('index', 'unknown') if isinstance(source, dict) else 'unknown'
                user_rrf_map[user_id].append(doc)
        
        # user_id별로 RRF 재결합
        # 같은 user_id의 여러 문서가 있으면, 각각을 독립적인 결과로 간주하고 RRF 점수를 합산
        final_rrf_results = []
        for user_id, user_docs in user_rrf_map.items():
            if len(user_docs) == 1:
                # 단일 문서: 그대로 사용
                final_rrf_results.append(user_docs[0])
            else:
                # 여러 문서: RRF 점수를 합산하여 대표 문서 선택
                # 각 문서의 RRF 점수를 합산
                total_rrf_score = sum(
                    doc.get('_score', 0.0) or doc.get('rrf_score', 0.0)
                    for doc in user_docs
                )
                # 가장 높은 점수의 문서를 대표로 선택
                best_doc = max(user_docs, key=lambda d: d.get('_score', 0.0) or d.get('rrf_score', 0.0))
                # 합산된 RRF 점수로 업데이트
                best_doc['_score'] = total_rrf_score
                best_doc['_rrf_details'] = {
                    'combined_score': total_rrf_score,
                    'source_count': len(user_docs),
                    'sources': [d.get('_index', 'unknown') for d in user_docs]
                }
                final_rrf_results.append(best_doc)
        
        # RRF 점수 기준으로 정렬
        final_rrf_results.sort(
            key=lambda d: d.get('_score', 0.0) or d.get('rrf_score', 0.0),
            reverse=True
        )
        
        rrf_results = final_rrf_results
        took_ms = 0  # 여러 검색의 합이므로 정확한 시간 측정은 어려움
        
        logger.info(f"  ✅ 인덱스 간 RRF 재결합 완료: {len(rrf_results)}건 (고유 user_id: {len(user_rrf_map)}개)")
        timings['rrf_recombination_ms'] = (perf_counter() - rrf_start) * 1000
        
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

        occupation_display_map: Dict[str, str] = {}

        if has_demographic_filters:
            filter_start = perf_counter()

            synonym_cache: Dict[str, List[str]] = {}
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
            
            # 성능 최적화: 배치 조회를 위해 먼저 모든 user_id 수집
            user_ids_to_fetch = set()
            doc_user_map = {}  # doc -> user_id 매핑
            welcome_1st_batch: Dict[str, Dict[str, Any]] = {}
            welcome_2nd_batch: Dict[str, Dict[str, Any]] = {}
            
            logger.info(f"🔍 user_id 수집 중: RRF 결과 {len(rrf_results)}건...")
            for doc in rrf_results:
                # source 추출 (여러 경로 시도)
                source = doc.get('_source', {})
                if not source and 'doc' in doc:
                    source = doc.get('doc', {}).get('_source', {})
                
                # Qdrant 결과인 경우 payload에서 추출
                if not source or not isinstance(source, dict):
                    # Qdrant 결과는 payload에 있을 수 있음
                    if 'payload' in doc:
                        payload = doc.get('payload', {})
                        if isinstance(payload, dict) and payload:
                            source = payload
                    elif isinstance(source, dict) and 'payload' in source:
                        payload = source.get('payload', {})
                        if isinstance(payload, dict) and payload:
                            source = payload
                
                # user_id 추출 (여러 경로 시도)
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
            
            # ⭐ 디버깅: user_id 샘플 로깅 (처음 10개)
            if user_ids_to_fetch:
                sample_user_ids = list(user_ids_to_fetch)[:10]
                logger.info(f"  📋 user_id 샘플 (처음 10개): {sample_user_ids}")
            
            # ⭐ 배치 조회: welcome_1st와 welcome_2nd를 작은 단위로 분할하여 조회 (타임아웃 방지)
            if user_ids_to_fetch:
                user_ids_list = list(user_ids_to_fetch)
                total_batches = (len(user_ids_list) + 199) // 200
                logger.info(f"🔍 배치 조회: welcome_1st/welcome_2nd {len(user_ids_list)}건 조회 중...")

                # 캐시된 문서 선반영
                for uid in list(user_ids_list):
                    cached_1 = _cache_get_welcome_doc("s_welcome_1st", uid)
                    if cached_1:
                        welcome_1st_batch[uid] = cached_1
                    cached_2 = _cache_get_welcome_doc("s_welcome_2nd", uid)
                    if cached_2:
                        welcome_2nd_batch[uid] = cached_2

                uncached_1st = [uid for uid in user_ids_list if uid not in welcome_1st_batch]
                uncached_2nd = [uid for uid in user_ids_list if uid not in welcome_2nd_batch]

                try:
                    if data_fetcher.os_async_client:
                        # 비동기 배치 조회
                        if uncached_1st:
                            raw_welcome_1st_docs = await data_fetcher.multi_get_documents_async(
                                index_name="s_welcome_1st",
                                doc_ids=uncached_1st,
                                source_fields=["metadata", "user_id", "qa_pairs"],
                            ) or []
                            fetched_map = data_fetcher.docs_to_user_map(raw_welcome_1st_docs)
                            welcome_1st_batch.update(fetched_map)
                            for uid, doc in fetched_map.items():
                                _cache_put_welcome_doc("s_welcome_1st", uid, doc)

                        if uncached_2nd:
                            raw_welcome_2nd_docs = await data_fetcher.multi_get_documents_async(
                                index_name="s_welcome_2nd",
                                doc_ids=uncached_2nd,
                                source_fields=["metadata", "user_id", "qa_pairs"],
                            ) or []
                            fetched_map = data_fetcher.docs_to_user_map(raw_welcome_2nd_docs)
                            welcome_2nd_batch.update(fetched_map)
                            for uid, doc in fetched_map.items():
                                _cache_put_welcome_doc("s_welcome_2nd", uid, doc)
                    else:
                        # 기존 동기 방식 유지
                        if uncached_1st:
                            found_count = 0
                            for batch_idx in range(0, len(uncached_1st), 200):
                                batch_ids = uncached_1st[batch_idx:batch_idx + 200]
                                batch_num = (batch_idx // 200) + 1
                                try:
                                    mget_body = [{"_index": "s_welcome_1st", "_id": uid} for uid in batch_ids]
                                    mget_response = os_client.mget(body={"docs": mget_body}, ignore=[404], request_timeout=60)
                                    for item in mget_response.get('docs', []):
                                        if item.get('found'):
                                            welcome_1st_batch[item['_id']] = item['_source']
                                            found_count += 1
                                            _cache_put_welcome_doc("s_welcome_1st", item['_id'], item['_source'])
                                    logger.debug(f"  📦 welcome_1st 배치 {batch_num}/{total_batches}: {len([d for d in mget_response.get('docs', []) if d.get('found')])}/{len(batch_ids)}건")
                                except Exception as e:
                                    logger.warning(f"  ⚠️ welcome_1st 배치 {batch_num}/{total_batches} 실패: {e}")
                                    continue
                            logger.info(f"  ✅ welcome_1st 배치 조회: {found_count}/{len(uncached_1st)}건 성공")

                        if uncached_2nd:
                            found_count = 0
                            for batch_idx in range(0, len(uncached_2nd), 200):
                                batch_ids = uncached_2nd[batch_idx:batch_idx + 200]
                                batch_num = (batch_idx // 200) + 1
                                try:
                                    mget_body = [{"_index": "s_welcome_2nd", "_id": uid} for uid in batch_ids]
                                    mget_response = os_client.mget(body={"docs": mget_body}, ignore=[404], request_timeout=60)
                                    for item in mget_response.get('docs', []):
                                        if item.get('found'):
                                            welcome_2nd_batch[item['_id']] = item['_source']
                                            found_count += 1
                                            _cache_put_welcome_doc("s_welcome_2nd", item['_id'], item['_source'])
                                    logger.debug(f"  📦 welcome_2nd 배치 {batch_num}/{total_batches}: {len([d for d in mget_response.get('docs', []) if d.get('found')])}/{len(batch_ids)}건")
                                except Exception as e:
                                    logger.warning(f"  ⚠️ welcome_2nd 배치 {batch_num}/{total_batches} 실패: {e}")
                                    continue
                            logger.info(f"  ✅ welcome_2nd 배치 조회: {found_count}/{len(uncached_2nd)}건 성공")

                    logger.info(f"  ✅ 배치 조회 완료: welcome_1st={len(welcome_1st_batch)}건, welcome_2nd={len(welcome_2nd_batch)}건")

                    # ⭐ 배치 조회에서 찾지 못한 user_id에 대해 개별 조회 시도 (fallback)
                    missing_1st = user_ids_to_fetch - set(welcome_1st_batch.keys())
                    missing_2nd = user_ids_to_fetch - set(welcome_2nd_batch.keys())

                    if missing_1st and len(missing_1st) <= 1000:
                        logger.info(f"  🔍 welcome_1st 추가 조회 시도: {len(missing_1st)}건...")
                        missing_ids = list(missing_1st)
                        if data_fetcher.os_async_client:
                            extra_docs_raw = await data_fetcher.multi_get_documents_async(
                                index_name="s_welcome_1st",
                                doc_ids=missing_ids,
                                source_fields=["metadata", "user_id", "qa_pairs"],
                            )
                            welcome_1st_batch.update(data_fetcher.docs_to_user_map(extra_docs_raw))
                        else:
                            response = os_client.mget(
                                index="s_welcome_1st",
                                body={"ids": missing_ids},
                                _source=["metadata", "user_id", "qa_pairs"],
                                request_timeout=60,
                                ignore=[404]
                            )
                            for doc in response.get('docs', []):
                                if doc.get('found'):
                                    welcome_1st_batch[doc['_id']] = doc['_source']
                                    _cache_put_welcome_doc("s_welcome_1st", doc['_id'], doc['_source'])
                        logger.info(f"  ✅ welcome_1st 추가 조회 후 총 {len(welcome_1st_batch)}건")

                    if missing_2nd and len(missing_2nd) <= 1000:
                        logger.info(f"  🔍 welcome_2nd 추가 조회 시도: {len(missing_2nd)}건...")
                        missing_ids = list(missing_2nd)
                        if data_fetcher.os_async_client:
                            extra_docs_raw = await data_fetcher.multi_get_documents_async(
                                index_name="s_welcome_2nd",
                                doc_ids=missing_ids,
                                source_fields=["metadata", "user_id", "qa_pairs"],
                            )
                            welcome_2nd_batch.update(data_fetcher.docs_to_user_map(extra_docs_raw))
                        else:
                            response = os_client.mget(
                                index="s_welcome_2nd",
                                body={"ids": missing_ids},
                                _source=["metadata", "user_id", "qa_pairs"],
                                request_timeout=60,
                                ignore=[404]
                            )
                            for doc in response.get('docs', []):
                                if doc.get('found'):
                                    welcome_2nd_batch[doc['_id']] = doc['_source']
                                    _cache_put_welcome_doc("s_welcome_2nd", doc['_id'], doc['_source'])
                        logger.info(f"  ✅ welcome_2nd 추가 조회 후 총 {len(welcome_2nd_batch)}건")

                except Exception as e:
                    logger.warning(f"  ⚠️ 배치 조회 실패: {e}, 개별 조회로 fallback")
            
            # ⭐ 디버깅: 필터링 전 RRF 결과 분석
            logger.info(f"📊 필터링 전 RRF 결과 분석:")
            logger.info(f"  - 총 RRF 결과: {len(rrf_results)}건")
            logger.info(f"  - welcome_1st 배치 조회: {len(welcome_1st_batch)}건")
            logger.info(f"  - welcome_2nd 배치 조회: {len(welcome_2nd_batch)}건")
            
            # 샘플 10개 분석
            for i, doc in enumerate(rrf_results[:10]):
                source = doc.get('_source', {})
                if not source and 'doc' in doc:
                    source = doc.get('doc', {}).get('_source', {})
                user_id = source.get('user_id') if isinstance(source, dict) else doc.get('_id', '')
                logger.info(f"  샘플 {i+1}. user_id={user_id}, metadata={source.get('metadata', {}) if isinstance(source, dict) else 'N/A'}")
            
            # 필터 재적용
            PLACEHOLDER_TOKENS = {
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
                if isinstance(value, (int, float)):
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
                return {normalize_value(v) for v in expected if v}

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

            def collect_doc_values(
                user_id: str,
                source: Dict[str, Any],
                metadata_1st: Dict[str, Any],
                metadata_2nd: Dict[str, Any],
            ) -> Tuple[Dict[DemographicType, Set[str]], Dict[DemographicType, bool]]:
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

                # Common metadata sources
                metadata_candidates = [
                    metadata_1st,
                    metadata_2nd,
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
                    welcome_2nd_doc = welcome_2nd_batch.get(user_id, {})
                    if isinstance(welcome_2nd_doc, dict):
                        qa_sources.append(welcome_2nd_doc.get("qa_pairs", []) or [])

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

                return doc_values, metadata_presence

            filtered_list: List[Dict[str, Any]] = []
            source_not_found_count = 0
            gender_filter_failed = 0
            age_filter_failed = 0
            occupation_filter_failed = 0
            gender_metadata_missing = 0
            age_metadata_missing = 0
            occupation_metadata_missing = 0
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

                welcome_1st_doc_full = welcome_1st_batch.get(user_id, {})
                metadata_1st = welcome_1st_doc_full.get("metadata", {}) if isinstance(welcome_1st_doc_full, dict) else {}
                welcome_2nd_doc_full = welcome_2nd_batch.get(user_id, {})
                metadata_2nd = welcome_2nd_doc_full.get("metadata", {}) if isinstance(welcome_2nd_doc_full, dict) else {}

                doc_values, metadata_presence = collect_doc_values(user_id, source, metadata_1st, metadata_2nd)

                gender_pass = True
                age_pass = True
                occupation_pass = True

                if demographic_filters.get(DemographicType.GENDER):
                    expected = set()
                    for demo in demographic_filters[DemographicType.GENDER]:
                        expected.update(build_expected_values(demo))
                    if not metadata_presence[DemographicType.GENDER]:
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
                            metadata_2nd.get("occupation"),
                            metadata_2nd.get("job"),
                            metadata_2nd.get("occupation_group"),
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
                            if isinstance(welcome_2nd_doc_full, dict):
                                qa_sources.append(welcome_2nd_doc_full.get("qa_pairs", []) or [])
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

                if gender_pass and age_pass and occupation_pass:
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

        lazy_join_start = perf_counter()
        final_hits = filtered_rrf_results[:size]
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
                    logger.debug(
                        "[mget_merge] user_id=%s qa_pairs=%d", 
                        user_id,
                        len(source.get('qa_pairs', [])) if isinstance(source, dict) else -1,
                    )
                inner_hit_wrapper = {"inner_hits": doc_info.get("inner_hits", {})}
            else:
                inner_hit_wrapper = doc

            metadata_2nd = source.get("metadata", {}) if isinstance(source, dict) else {}
            if not metadata_2nd and isinstance(payload, dict):
                metadata_2nd = payload.get("metadata", {}) or {}

            welcome_1st_doc = welcome_1st_batch.get(user_id, {}) if user_id else {}
            metadata_1st = (
                welcome_1st_doc.get("metadata", {}) if isinstance(welcome_1st_doc, dict) else {}
            )

            welcome_2nd_doc = welcome_2nd_batch.get(user_id, {}) if user_id else {}
            metadata_2nd_cached = (
                welcome_2nd_doc.get("metadata", {}) if isinstance(welcome_2nd_doc, dict) else {}
            )

            demographic_info: Dict[str, Any] = {}
            if metadata_1st:
                demographic_info["age_group"] = metadata_1st.get("age_group")
                demographic_info["gender"] = metadata_1st.get("gender")
                demographic_info["birth_year"] = metadata_1st.get("birth_year")

            occupation_candidate = metadata_2nd.get("occupation") if isinstance(metadata_2nd, dict) else None
            if not occupation_candidate and isinstance(metadata_2nd_cached, dict):
                occupation_candidate = metadata_2nd_cached.get("occupation")
            if not occupation_candidate and isinstance(payload, dict):
                occupation_candidate = payload.get("occupation")
            if occupation_candidate:
                demographic_info["occupation"] = occupation_candidate

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

            matched_qa_pairs: List[Dict[str, Any]] = extract_inner_hit_matches(inner_hit_wrapper)
            if not matched_qa_pairs and analysis.must_terms:
                matched_qa_pairs = extract_matched_qa_pairs(source, analysis.must_terms)

            qa_pairs_display = reorder_with_matches(
                source.get("qa_pairs", []) if isinstance(source, dict) else [],
                matched_qa_pairs,
                limit=10
            )

            results.append(
                SearchResult(
                    user_id=user_id,
                    score=doc.get("_score", 0.0),
                    timestamp=source.get("timestamp") if isinstance(source, dict) else None,
                    demographic_info=demographic_info if demographic_info else None,
                    qa_pairs=qa_pairs_display[:5],
                    matched_qa_pairs=matched_qa_pairs,
                    highlights=doc.get("highlight"),
                )
            )
            logger.debug(
                "[match_check] user_id=%s inner_hits=%d matched=%d", 
                user_id,
                len(inner_hits_map.get(user_id, [])),
                len(matched_qa_pairs),
            )

        timings["lazy_join_ms"] = (perf_counter() - lazy_join_start) * 1000
        timings.setdefault('post_filter_ms', timings.get('post_filter_ms', 0.0))
        timings.setdefault('rrf_recombination_ms', 0.0)
        timings.setdefault('qdrant_parallel_ms', 0.0)
        timings.setdefault('opensearch_parallel_ms', timings.get('two_phase_stage1_ms', 0.0) + timings.get('two_phase_stage2_ms', 0.0))

        total_duration_ms = (perf_counter() - overall_start) * 1000
        timings['total_ms'] = total_duration_ms

        logger.info("📈 성능 측정 요약 (ms):")
        for key in sorted(timings.keys()):
            logger.info(f"  - {key}: {timings[key]:.2f}")

        response_took_ms = int(total_duration_ms)
        total_hits = len(filtered_rrf_results)
        max_score = final_hits[0].get('_score', 0.0) if final_hits else 0.0

        summary_parts = [
            f"returned={len(results)}/{total_hits}",
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

        return SearchResponse(
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
                "filters": filters_for_response,
                "size": size,
                "timings_ms": timings,
            },
            took_ms=response_took_ms,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[ERROR] 자연어 검색 중 오류: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------
# Qdrant 진단/헬스 엔드포인트 (읽기 전용)
# -----------------------------

@router.get("/debug/welcome-1st", summary="welcome_1st 인덱스 샘플 데이터 확인 (디버깅용)")
async def get_welcome_1st_samples(
    user_id: str = None,
    age_group: str = None,
    size: int = 5,
    os_client: OpenSearch = Depends(lambda: router.os_client),
):
    """
    welcome_1st 인덱스의 샘플 데이터 확인 (디버깅용)
    
    - user_id로 특정 사용자 조회
    - age_group으로 필터링
    - metadata 구조 확인
    """
    try:
        if not os_client or not os_client.ping():
            raise HTTPException(status_code=503, detail="OpenSearch 서버에 연결할 수 없습니다.")
        
        query = {"match_all": {}}
        
        if user_id:
            # 특정 user_id 조회
            query = {"term": {"_id": user_id}}
        elif age_group:
            # age_group으로 필터링
            query = {
                "term": {
                    "metadata.age_group.keyword": age_group
                }
            }
        
        response = os_client.search(
            index="s_welcome_1st",
            body={
                "query": query,
                "size": size,
                "_source": {
                    "includes": ["user_id", "metadata", "qa_pairs"]
                }
            }
        )
        
        results = []
        for hit in response['hits']['hits']:
            source = hit.get('_source', {})
            results.append({
                "_id": hit.get('_id'),
                "user_id": source.get('user_id'),
                "metadata": source.get('metadata', {}),
                "qa_pairs_sample": source.get('qa_pairs', [])[:10] if source.get('qa_pairs') else []
            })
        
        return {
            "index_name": "s_welcome_1st",
            "query": {
                "user_id": user_id,
                "age_group": age_group
            },
            "total_hits": response['hits']['total']['value'],
            "samples": results
        }
    
    except Exception as e:
        logger.error(f"[ERROR] welcome_1st 샘플 데이터 조회 중 오류: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/debug/welcome-2nd", summary="welcome_2nd 인덱스 샘플 데이터 확인 (디버깅용)")
async def get_welcome_2nd_samples(
    user_id: str = None,
    occupation: str = None,
    size: int = 5,
    os_client: OpenSearch = Depends(lambda: router.os_client),
):
    """
    welcome_2nd 인덱스의 샘플 데이터 확인 (디버깅용)
    
    - user_id로 특정 사용자 조회
    - occupation으로 필터링 (qa_pairs에서)
    - metadata 구조 확인
    """
    try:
        if not os_client or not os_client.ping():
            raise HTTPException(status_code=503, detail="OpenSearch 서버에 연결할 수 없습니다.")
        
        query = {"match_all": {}}
        
        if user_id:
            # 특정 user_id 조회
            query = {"term": {"_id": user_id}}
        elif occupation:
            # qa_pairs에서 직업으로 필터링
            query = {
                "nested": {
                    "path": "qa_pairs",
                    "query": {
                        "bool": {
                            "must": [
                                {"match": {"qa_pairs.q_text": "직업"}},
                                {"match": {"qa_pairs.answer_text": occupation}}
                            ]
                        }
                    }
                }
            }
        
        response = os_client.search(
            index="s_welcome_2nd",
            body={
                "query": query,
                "size": size,
                "_source": {
                    "includes": ["user_id", "metadata", "qa_pairs"]
                }
            }
        )
        
        results = []
        for hit in response['hits']['hits']:
            source = hit.get('_source', {})
            results.append({
                "_id": hit.get('_id'),
                "user_id": source.get('user_id'),
                "metadata": source.get('metadata', {}),
                "qa_pairs_sample": source.get('qa_pairs', [])[:10] if source.get('qa_pairs') else []
            })
        
        return {
            "index_name": "s_welcome_2nd",
            "query": {
                "user_id": user_id,
                "occupation": occupation
            },
            "total_hits": response['hits']['total']['value'],
            "samples": results
        }
    
    except Exception as e:
        logger.error(f"[ERROR] welcome_2nd 샘플 데이터 조회 중 오류: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/debug/sample-data", summary="인덱스별 샘플 데이터 확인 (디버깅용)")
async def get_sample_data(
    index_name: str = "*",
    question_keyword: str = None,
    answer_keyword: str = None,
    os_client: OpenSearch = Depends(lambda: router.os_client),
):
    """
    인덱스별 샘플 데이터 확인 (디버깅용)
    
    - 특정 질문 키워드로 샘플 데이터 조회
    - 특정 답변 키워드로 샘플 데이터 조회
    - 실제 답변 형식 확인
    """
    try:
        if not os_client or not os_client.ping():
            raise HTTPException(status_code=503, detail="OpenSearch 서버에 연결할 수 없습니다.")
        
        query = {"match_all": {}}
        if question_keyword or answer_keyword:
            nested_query = {}
            if question_keyword and answer_keyword:
                nested_query = {
                    "bool": {
                        "must": [
                            {"match": {"qa_pairs.q_text": question_keyword}},
                            {
                                "bool": {
                                    "should": [
                                        {"match_phrase": {"qa_pairs.answer_text": answer_keyword}},
                                        {"match_phrase": {"qa_pairs.answer": answer_keyword}},
                                        {"match": {"qa_pairs.answer_text": {"query": answer_keyword, "operator": "or"}}},
                                        {"match": {"qa_pairs.answer": {"query": answer_keyword, "operator": "or"}}}
                                    ],
                                    "minimum_should_match": 1
                                }
                            }
                        ]
                    }
                }
            elif question_keyword:
                nested_query = {"match": {"qa_pairs.q_text": question_keyword}}
            elif answer_keyword:
                nested_query = {
                    "bool": {
                        "should": [
                            {"match_phrase": {"qa_pairs.answer_text": answer_keyword}},
                            {"match_phrase": {"qa_pairs.answer": answer_keyword}},
                            {"match": {"qa_pairs.answer_text": {"query": answer_keyword, "operator": "or"}}},
                            {"match": {"qa_pairs.answer": {"query": answer_keyword, "operator": "or"}}}
                        ],
                        "minimum_should_match": 1
                    }
                }
            
            query = {
                "nested": {
                    "path": "qa_pairs",
                    "query": nested_query,
                    "inner_hits": {
                        "size": 5,
                        "_source": {"includes": ["qa_pairs.q_text", "qa_pairs.answer_text", "qa_pairs.answer"]}
                    }
                }
            }
        
        response = os_client.search(
            index=index_name,
            body={
                "query": query,
                "size": 5,
                "_source": {"includes": ["user_id", "metadata", "qa_pairs"]}
            }
        )
        
        results = []
        for hit in response['hits']['hits']:
            source = hit.get('_source', {})
            result = {
                "index": hit.get('_index'),
                "user_id": source.get('user_id'),
                "metadata": source.get('metadata', {}),
                "qa_pairs_sample": source.get('qa_pairs', [])[:5]
            }
            
            if (question_keyword or answer_keyword) and 'inner_hits' in hit:
                result['matched_qa_pairs'] = []
                for inner_hit in hit['inner_hits']['qa_pairs']['hits']['hits']:
                    result['matched_qa_pairs'].append(inner_hit.get('_source', {}))
            
            results.append(result)
        
        return {
            "index_name": index_name,
            "question_keyword": question_keyword,
            "answer_keyword": answer_keyword,
            "total_hits": response['hits']['total']['value'],
            "samples": results
        }
    
    except Exception as e:
        logger.error(f"[ERROR] 샘플 데이터 조회 중 오류: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


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


@router.get("/qdrant/collections", summary="Qdrant 컬렉션 목록 및 통계")
async def list_qdrant_collections():
    qdrant_client = getattr(router, 'qdrant_client', None)
    if not qdrant_client:
        raise HTTPException(status_code=503, detail="Qdrant 클라이언트가 초기화되지 않았습니다.")
    try:
        cols = qdrant_client.get_collections()
        items = []
        for c in cols.collections:
            try:
                info = qdrant_client.get_collection(c.name)
                items.append({
                    "name": c.name,
                    "vectors_count": info.vectors_count if hasattr(info, 'vectors_count') else None,
                    "points_count": getattr(info, 'points_count', None),
                    "config": getattr(info, 'config', None).__dict__ if hasattr(info, 'config') else None,
                })
            except Exception:
                items.append({"name": c.name})
        return {"collections": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class QdrantTestSearchRequest(BaseModel):
    query: str = Field(..., description="임베딩으로 검색할 텍스트")
    limit: int = Field(5, ge=1, le=100)


@router.post("/qdrant/test-search", summary="Qdrant 전 컬렉션 테스트 검색 (읽기 전용)")
async def qdrant_test_search(req: QdrantTestSearchRequest):
    qdrant_client = getattr(router, 'qdrant_client', None)
    embedding_model = getattr(router, 'embedding_model', None)
    if not qdrant_client:
        raise HTTPException(status_code=503, detail="Qdrant 클라이언트가 초기화되지 않았습니다.")
    if not embedding_model:
        raise HTTPException(status_code=503, detail="임베딩 모델이 로드되지 않았습니다.")

    try:
        qvec = embedding_model.encode(req.query).tolist()
        cols = qdrant_client.get_collections()
        results = []
        for c in cols.collections:
            try:
                r = qdrant_client.search(
                    collection_name=c.name,
                    query_vector=qvec,
                    limit=req.limit,
                )
                results.append({
                    "collection": c.name,
                    "hits": [
                        {
                            "id": str(h.id),
                            "score": h.score,
                            "payload": getattr(h, 'payload', None)
                        } for h in r
                    ]
                })
            except Exception as e:
                results.append({"collection": c.name, "error": str(e)})
        return {"query": req.query, "results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/similar", summary="유사 문서 검색 (플레이스홀더)")
async def search_similar(
    user_id: str,
    index_name: str = "s_welcome_2nd",
    size: int = 10
):
    """
    특정 사용자와 유사한 응답을 가진 사용자 검색 (향후 구현)
    """
    raise HTTPException(
        status_code=501,
        detail="유사 문서 검색 기능은 향후 구현 예정입니다."
    )


@router.get("/stats/{index_name}", summary="검색 통계")
async def get_search_stats(
    index_name: str,
    os_client: OpenSearch = Depends(lambda: router.os_client),
):
    """인덱스 검색 통계 조회"""
    try:
        if not os_client.indices.exists(index=index_name):
            raise HTTPException(
                status_code=404,
                detail=f"인덱스를 찾을 수 없습니다: {index_name}"
            )

        stats = os_client.indices.stats(index=index_name)
        count = os_client.count(index=index_name)

        return {
            "index_name": index_name,
            "doc_count": count['count'],
            "size_mb": round(stats['_all']['total']['store']['size_in_bytes'] / 1024 / 1024, 2),
            "search_total": stats['_all']['total']['search']['query_total'],
            "search_time_ms": stats['_all']['total']['search']['query_time_in_millis']
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[ERROR] 통계 조회 중 오류: {e}")
        raise HTTPException(status_code=500, detail=str(e))


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
        if async_client:
            response_1st = await data_fetcher.search_opensearch_async(
                index_name="s_welcome_1st",
                query=stage1_query,
                size=stage1_query_size,
                source_filter=None,
                request_timeout=DEFAULT_OS_TIMEOUT,
            )
        else:
            response_1st = data_fetcher.search_opensearch(
                index_name="s_welcome_1st",
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
        if async_client:
            response_2nd = await data_fetcher.search_opensearch_async(
                index_name="s_welcome_2nd",
                query=stage2_query,
                size=detail_size,
                source_filter=None,
                request_timeout=DEFAULT_OS_TIMEOUT,
            )
        else:
            response_2nd = data_fetcher.search_opensearch(
                index_name="s_welcome_2nd",
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

    fetch_start = perf_counter()
    welcome_1st_docs: Dict[str, Dict[str, Any]] = {}
    welcome_2nd_docs: Dict[str, Dict[str, Any]] = {}

    if final_user_ids:
        if async_client:
            welcome_1st_docs = await data_fetcher.multi_get_documents_async(
                index_name="s_welcome_1st",
                doc_ids=final_user_ids,
                batch_size=200,
                source_fields=["metadata", "user_id", "qa_pairs"],
            )
            welcome_2nd_docs = await data_fetcher.multi_get_documents_async(
                index_name="s_welcome_2nd",
                doc_ids=final_user_ids,
                batch_size=200,
                source_fields=["metadata", "user_id", "qa_pairs"],
            )
        else:
            response = sync_client.mget(index="s_welcome_1st", body={"ids": final_user_ids}, _source=["metadata", "user_id", "qa_pairs"])
            for doc in response.get('docs', []):
                if doc.get('found'):
                    welcome_1st_docs[doc['_id']] = doc['_source']
            response = sync_client.mget(index="s_welcome_2nd", body={"ids": final_user_ids}, _source=["metadata", "user_id", "qa_pairs"])
            for doc in response.get('docs', []):
                if doc.get('found'):
                    welcome_2nd_docs[doc['_id']] = doc['_source']
    timings['two_phase_fetch_demographics_ms'] = (perf_counter() - fetch_start) * 1000

    results: List[SearchResult] = []
    lazy_join_start = perf_counter()
    final_hits = final_hits if 'final_hits' in locals() else []
    for doc in final_hits:
        source = doc.get('_source', {}) or {}
        user_id = source.get('user_id') or hit.get('_id', '')
        metadata_2nd = source.get('metadata', {}) if isinstance(source, dict) else {}
        welcome_1st_doc = welcome_1st_docs.get(user_id, {})
        metadata_1st = welcome_1st_doc.get('metadata', {}) if isinstance(welcome_1st_doc, dict) else {}

        demographic_info: Dict[str, Any] = {}
        if metadata_1st:
            demographic_info['age_group'] = metadata_1st.get('age_group')
            demographic_info['gender'] = metadata_1st.get('gender')
            demographic_info['birth_year'] = metadata_1st.get('birth_year')
        if metadata_2nd:
            demographic_info['occupation'] = metadata_2nd.get('occupation')

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
