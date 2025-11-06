"""검색 API 라우터"""
import logging
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from opensearchpy import OpenSearch

# 분석기 및 쿼리 빌더
from rag_query_analyzer.analyzers.main_analyzer import AdvancedRAGQueryAnalyzer
from connectors.hybrid_searcher import OpenSearchHybridQueryBuilder, calculate_rrf_score
from connectors.data_fetcher import DataFetcher

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/search",
    tags=["Search"]
)


class SearchRequest(BaseModel):
    """검색 요청"""
    query: str = Field(..., description="검색 쿼리")
    index_name: str = Field(default="survey_*,s_welcome_*", description="검색할 인덱스 이름 (와일드카드 지원)")
    size: int = Field(default=10, ge=1, le=100, description="반환할 결과 개수")
    use_vector_search: bool = Field(default=True, description="벡터 검색 사용 여부")


class SearchResult(BaseModel):
    """검색 결과 항목"""
    user_id: str
    score: float
    timestamp: Optional[str] = None
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

        # 임베딩 모델 확인
        embedding_model = getattr(router, 'embedding_model', None)
        config = getattr(router, 'config', None)

        logger.info(f"\n{'='*60}")
        logger.info(f"[SEARCH] 검색 쿼리: '{request.query}'")
        logger.info(f"{'='*60}")

        # 1단계: 쿼리 분석
        logger.info("\n[1/3] 쿼리 분석 중...")
        analyzer = AdvancedRAGQueryAnalyzer(config)
        query_analysis = analyzer.analyze_query(request.query)

        logger.info(f"   - 의도: {query_analysis.intent}")
        logger.info(f"   - must_terms: {query_analysis.must_terms}")
        logger.info(f"   - should_terms: {query_analysis.should_terms}")
        logger.info(f"   - alpha: {query_analysis.alpha}")

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

        # 하이브리드 검색 (OpenSearch + Qdrant + RRF)
        if request.use_vector_search and query_vector and hasattr(router, 'qdrant_client'):
            logger.info("   - 하이브리드 검색 모드 (OpenSearch + Qdrant + RRF)")

            # OpenSearch 키워드 검색
            logger.info("   - [1/3] OpenSearch 키워드 검색...")
            data_fetcher = DataFetcher(opensearch_client=os_client)
            os_response = data_fetcher.search_opensearch(
                index_name=request.index_name,
                query=os_query,
                size=request.size * 2  # RRF를 위해 더 많이 가져옴
            )
            logger.info(f"      → OpenSearch: {len(os_response['hits']['hits'])}건")

            # Qdrant 벡터 검색 (모든 컬렉션)
            logger.info("   - [2/3] Qdrant 벡터 검색 (모든 컬렉션)...")
            qdrant_client = router.qdrant_client

            # 모든 Qdrant 컬렉션 가져오기
            try:
                collections = qdrant_client.get_collections()
                collection_names = [col.name for col in collections.collections]
                logger.info(f"      → 검색할 컬렉션: {collection_names}")
            except Exception as e:
                logger.warning(f"      → Qdrant 컬렉션 목록 가져오기 실패: {e}")
                collection_names = []

            # 각 컬렉션에서 검색 후 결합
            qdrant_results = []
            for collection_name in collection_names:
                try:
                    results = qdrant_client.search(
                        collection_name=collection_name,
                        query_vector=query_vector,
                        limit=request.size * 2,
                        score_threshold=0.3  # 최소 유사도 임계값
                    )
                    qdrant_results.extend(results)
                    logger.info(f"      → {collection_name}: {len(results)}건")
                except Exception as e:
                    logger.warning(f"      → {collection_name} 검색 실패: {e}")

            # 점수 순으로 정렬
            qdrant_results.sort(key=lambda x: x.score, reverse=True)
            qdrant_results = qdrant_results[:request.size * 2]  # 상위 N개만
            logger.info(f"      → 총 Qdrant 결과: {len(qdrant_results)}건")

            # RRF로 결합
            logger.info("   - [3/3] RRF 결합 중...")
            keyword_results = os_response['hits']['hits']
            vector_results = [
                {
                    '_id': str(r.id),
                    '_score': r.score,
                    '_source': r.payload
                }
                for r in qdrant_results
            ]

            combined_results = calculate_rrf_score(
                keyword_results=keyword_results,
                vector_results=vector_results,
                k=60  # RRF 상수
            )

            # 상위 N개만 선택
            final_hits = combined_results[:request.size]
            logger.info(f"      → RRF 결합 완료: {len(final_hits)}건")

            # 결과 포매팅 (RRF 순서 유지)
            results = []
            for doc in final_hits:
                source = doc.get('_source', {})

                # Qdrant 결과인 경우 payload에서 user_id 추출
                if 'payload' in source:
                    user_id = source['payload'].get('user_id', '')
                else:
                    user_id = source.get('user_id', '')

                result = SearchResult(
                    user_id=user_id,
                    score=doc.get('_score', 0.0),
                    timestamp=source.get('timestamp'),
                    qa_pairs=source.get('qa_pairs', [])[:5],
                    matched_qa_pairs=[],
                    highlights=None
                )
                results.append(result)

            total_hits = max(os_response['hits']['total']['value'], len(qdrant_results))
            max_score = final_hits[0].get('_score', 0.0) if final_hits else 0.0
            took_ms = os_response['took']

        else:
            # 기존 OpenSearch 단독 검색
            logger.info("   - OpenSearch 키워드 검색만 사용")
            data_fetcher = DataFetcher(opensearch_client=os_client)
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

                result = SearchResult(
                    user_id=hit['_source'].get('user_id', ''),
                    score=hit['_score'],
                    timestamp=hit['_source'].get('timestamp'),
                    qa_pairs=hit['_source'].get('qa_pairs', [])[:5],
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
