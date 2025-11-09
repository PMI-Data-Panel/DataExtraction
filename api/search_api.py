"""검색 API 라우터"""
import asyncio
import json
import logging
from collections import defaultdict
from time import perf_counter
from typing import List, Dict, Any, Optional, Set, Tuple
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from opensearchpy import OpenSearch
import os
# 분석기 및 쿼리 빌더
from rag_query_analyzer.analyzers.main_analyzer import AdvancedRAGQueryAnalyzer
from rag_query_analyzer.analyzers.demographic_extractor import DemographicExtractor
from rag_query_analyzer.models.entities import DemographicType, DemographicEntity
from connectors.hybrid_searcher import OpenSearchHybridQueryBuilder, calculate_rrf_score
from connectors.data_fetcher import DataFetcher
from connectors.qdrant_helper import search_qdrant_async, search_qdrant_collections_async

from redis_celery.celery_app import celery_app
from celery.result import AsyncResult
from redis_celery.tasks.search_tasks import search_with_rrf_task
import redis


logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/search",
    tags=["Search"]
)

# ⚠️ 임시 확장 타임아웃 (중첩 필터 제거 전까지 8~10초 유지)
DEFAULT_OS_TIMEOUT = 10
class NLSearchRequest(BaseModel):
    """자연어 기반 검색 요청 (필터/size 자동 추출)"""
    query: str = Field(..., description="자연어 쿼리 (예: '30대 사무직 300명 데이터 보여줘')")
    index_name: str = Field(default="*", description="검색할 인덱스 이름 (기본값: 전체 인덱스 '*')")
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
    

def get_redis_client() -> redis.StrictRedis:
    """Redis 연결 객체를 반환합니다."""
    REDIS_HOST = os.getenv('REDIS_HOST', 'redis') # Docker Compose 서비스 이름 사용
    REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
    # 캐시 DB 2번 사용 (docker-compose.yml 참고)
    CACHE_DB = int(os.getenv('CACHE_DB', '2')) 
    try:
        r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=CACHE_DB, decode_responses=True)
        r.ping()
        return r
    except Exception as e:
        logger.error(f"Redis 연결 실패: {e}")
        raise HTTPException(status_code=503, detail="Redis 서버에 연결할 수 없습니다.")
    
@router.post("/nl-async", response_model=Dict[str, Any], summary="자연어 검색 (동기)")
async def search_natural_language_async(request: NLSearchRequest):
    """
    복잡한 하이브리드 검색 작업을 Celery Worker에 위임하고 Task ID를 반환합니다.
    """
    # 1. Celery 태스크 호출
    task = search_with_rrf_task.delay(
        query=request.query,
        index_name=request.index_name or "*",
        size=100, # 초기 요청 사이즈를 넉넉하게 설정
        use_vector_search=request.use_vector_search,
    )

    return {
        "message": "비동기 검색 시작",
        "task_id": task.id,
        "status_url": f"/search/status/{task.id}"
    }
    

@router.get("/status/{task_id}", response_model=Dict[str, Any], summary="Celery 작업 상태 및 결과 조회")
async def get_task_status(task_id: str):
    """클라이언트가 이 엔드포인트를 폴링하여 결과를 가져옵니다."""
    task = AsyncResult(task_id, app=celery_app) # celery_app을 인수로 전달
    
    response = {
        'state': task.state,
        'message': task.status,
        'ready': task.ready()
    }
    
    if task.ready():
        if task.state == 'SUCCESS':
            # SUCCESS 응답은 SearchResponse와 유사한 구조를 가집니다.
            # result는 Celery Task에서 반환된 딕셔너리입니다.
            response['result'] = task.result 
        elif task.state == 'FAILURE':
            response['result'] = {
                'error': str(task.result), 
                'error_type': task.result.get('error_type', 'UnknownError') if isinstance(task.result, dict) else 'UnknownError'
            }
        
    return response


from fastapi import Query
@router.get("/scroll/{task_id}", response_model=SearchResponse, summary="무한 스크롤: RRF 결과 페이징 조회")
async def get_scrolled_results(
    task_id: str,
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    r: redis.StrictRedis = Depends(get_redis_client) # Redis 클라이언트 DI
):
    """
    Redis에 저장된 RRF 순서 기반의 검색 결과를 offset/limit 방식으로 조회합니다.
    """
    id_list_key = f"task:{task_id}:ids"
    
    # 1. Redis에서 전체 ID 목록 길이 확인
    total_hits = r.llen(id_list_key)
        
    if total_hits == 0:
        raise HTTPException(
            status_code=404, 
            detail="검색 결과가 만료되었거나 Task ID를 찾을 수 없습니다."
        )

    # 2. Redis List에서 ID 목록 추출
    end_index = offset + limit - 1
    user_ids = r.lrange(id_list_key, offset, end_index)
    
    if not user_ids:
        # 끝까지 스크롤한 경우
        return SearchResponse(
            query=f"Task {task_id} Scrolled",
            total_hits=total_hits,
            max_score=0.0,
            results=[],
            took_ms=0
        )

    # 3. MGET으로 상세 JSON 데이터 일괄 조회
    data_keys = [f"task:{task_id}:data:{uid}" for uid in user_ids]
    detailed_results_json = r.mget(data_keys)
    
    results: List[SearchResult] = []
    max_score = 0.0
    
    for item_json in detailed_results_json:
        if item_json:
            try:
                data = json.loads(item_json)
                result = SearchResult(**data)
                results.append(result)
                max_score = max(max_score, result.score)
            except json.JSONDecodeError as e:
                logger.error(f"Redis 데이터 파싱 오류: {e}")
        
    # 4. 응답 포맷 구성
    return SearchResponse(
        query=f"Task {task_id} Scrolled (Offset: {offset}, Limit: {limit})",
        total_hits=total_hits,
        max_score=max_score,
        results=results,
        query_analysis={"note": "페이징 데이터는 전체 분석 결과를 포함하지 않습니다."},
        took_ms=0
    )

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
                batch_size=200
            )
            welcome_2nd_docs = await data_fetcher.multi_get_documents_async(
                index_name="s_welcome_2nd",
                doc_ids=final_user_ids,
                batch_size=200
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

        demographic_info = {}
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