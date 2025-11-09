"""검색 API 라우터"""
import logging
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from opensearchpy import OpenSearch

# 분석기 및 쿼리 빌더
from rag_query_analyzer.analyzers.main_analyzer import AdvancedRAGQueryAnalyzer
from rag_query_analyzer.analyzers.demographic_extractor import DemographicExtractor
from rag_query_analyzer.models.entities import DemographicType
from connectors.hybrid_searcher import OpenSearchHybridQueryBuilder, calculate_rrf_score
from connectors.data_fetcher import DataFetcher
from redis_celery.tasks.search_tasks import search_nl_task
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/search",
    tags=["Search"]
)

# 작업 상태 확인 엔드포인트
@router.get("/celery-test/{task_id}")
def get_celery_task_status(task_id: str):
    """
    Celery Task 상태 확인
    """
    from celery.result import AsyncResult
    from redis_celery.celery_app import celery_app
    
    task = AsyncResult(task_id, app=celery_app)
    
    return {
        "task_id": task_id,
        "status": task.status,
        "result": task.result if task.ready() else None
    }

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




class NLSearchRequest(BaseModel):
    """자연어 기반 검색 요청 (필터/size 자동 추출)"""
    query: str = Field(..., description="자연어 쿼리 (예: '30대 사무직 300명 데이터 보여줘')")
    index_name: str = Field(default="*", description="검색할 인덱스 이름 (기본값: 전체 인덱스 '*')")
    use_vector_search: bool = Field(default=True, description="벡터 검색 사용 여부")


@router.post("/hybrid_search")
def nl_async(request: NLSearchRequest):
    task = search_nl_task.delay(
        query=request.query,
        index_name=request.index_name,
        use_vector_search=request.use_vector_search
    )
    return {"task_id": task.id}

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
