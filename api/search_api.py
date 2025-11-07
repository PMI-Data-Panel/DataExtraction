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

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/search",
    tags=["Search"]
)


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
            # ⭐ 필터가 있는 경우, 교집합을 위해 더 많은 결과를 가져와야 함
            has_filters = bool(os_query.get('query', {}).get('bool', {}).get('must'))
            
            # Qdrant top-N 제한: 필터 유무에 따라 분기
            if has_filters:
                # 필터 있음: 300~500개 (교집합 확보를 위해)
                qdrant_limit = min(500, max(300, request.size * 10))
                search_size = max(1000, min(request.size * 20, 5000))
                logger.info(f"🔍 필터 적용: OpenSearch size={search_size}, Qdrant limit={qdrant_limit} (교집합 확보를 위해)")
            else:
                # 필터 없음: 100~200개
                qdrant_limit = min(200, max(100, request.size * 2))
                search_size = request.size * 2
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
                source_filter=source_filter
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
                    # qdrant_limit 사용 (필터 유무에 따라 분기)
                    # HNSW 튜닝: ef=128로 설정 (탐색 품질과 속도 균형)
                    results = qdrant_client.search(
                        collection_name=collection_name,
                        query_vector=query_vector,
                        limit=qdrant_limit,
                        score_threshold=0.3,  # 최소 유사도 임계값
                        # ef 파라미터는 Qdrant client의 search 메서드에 직접 전달 불가
                        # 대신 limit로 제한하여 성능 최적화
                    )
                    qdrant_results.extend(results)
                    logger.info(f"      → {collection_name}: {len(results)}건 (limit={qdrant_limit})")
                except Exception as e:
                    logger.warning(f"      → {collection_name} 검색 실패: {e}")

            # 점수 순으로 정렬
            qdrant_results.sort(key=lambda x: x.score, reverse=True)
            qdrant_results = qdrant_results[:qdrant_limit]  # 상위 N개만
            logger.info(f"      → 총 Qdrant 결과: {len(qdrant_results)}건 (limit={qdrant_limit})")

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
        if not os_client or not os_client.ping():
            raise HTTPException(status_code=503, detail="OpenSearch 서버에 연결할 수 없습니다.")

        embedding_model = getattr(router, 'embedding_model', None)
        config = getattr(router, 'config', None)

        # 1) 추출: filters + size
        extractor = DemographicExtractor()
        extracted_entities, requested_size = extractor.extract_with_size(request.query)
        filters = extracted_entities.to_filters()
        size = max(1, min(requested_size, 100))
        
        # ⭐ 필터가 있는 경우, 교집합을 위해 더 많은 결과를 가져와야 함
        # 이론상 교집합이 수백~수천 명일 수 있으므로 충분히 큰 size 사용
        # 예: welcome_1st에서 5,192명, welcome_2nd에서 10,000명 이상
        # → 각각 1,000개씩 가져와도 교집합이 충분히 나올 수 있음
        search_size = size * 2  # 기본값
        if filters:
            # 필터가 있으면 더 많이 가져오기 (교집합을 위해)
            # 최소 1,000개, 최대 5,000개 (성능 고려)
            search_size = max(1000, min(size * 20, 5000))
            logger.info(f"🔍 필터 적용: 검색 size를 {search_size}로 증가 (교집합 확보를 위해)")

        # 2) 분석 + 쿼리 빌드
        analyzer = AdvancedRAGQueryAnalyzer(config)
        analysis = analyzer.analyze_query(request.query)
        
        # ⭐ 최종 키워드 정제: 메타 키워드, 수량 패턴, Demographics 제거
        import re
        
        # 메타 키워드 정의 (검색 조건에서 제외)
        meta_keywords = {
            '설문조사', '설문', '데이터', '자료', '정보',
            '보여줘', '보여주세요', '알려줘', '알려주세요',
            '검색', '찾아줘', '찾아주세요', '조회',
            '을', '를', '이', '가', '의', '에', '에서',
            '와', '과', '에게', '한테', '명', '개', '건',
            '사람', '인', '분', '중', '중에', '중에서'
        }
        
        # 수량 패턴("숫자+명/건") 제거
        quantity_pattern = re.compile(r'\d+\s*(명|건)')
        
        # 추출된 Demographics 키워드 집합
        extracted_keywords = set()
        for demo in extracted_entities.demographics:
            extracted_keywords.add(demo.raw_value)
            extracted_keywords.update(demo.synonyms)
        
        # 정제 전 키워드 저장 (로깅용)
        original_must = analysis.must_terms.copy()
        original_should = analysis.should_terms.copy()
        
        # must_terms 정제
        analysis.must_terms = [
            t for t in analysis.must_terms
            if (t not in meta_keywords and
                t not in extracted_keywords and
                not quantity_pattern.search(t))
        ]
        
        # should_terms 정제
        analysis.should_terms = [
            t for t in analysis.should_terms
            if (t not in meta_keywords and
                t not in extracted_keywords and
                not quantity_pattern.search(t))
        ]
        
        # 제거된 키워드 추적
        removed_meta = [t for t in (original_must + original_should) if t in meta_keywords]
        removed_demo = [t for t in (original_must + original_should) if t in extracted_keywords]
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
                """
                ExtractedEntities의 모든 엔티티(raw_value)에 대해
                Qdrant에서 유사 벡터를 찾아 동의어를 자동 확장
                정적 사전 없이 완전 동적 방식
                """
                import re
                phrases = [request.query]  # 원본 쿼리 포함
                qdrant_client = getattr(router, 'qdrant_client', None)
                
                if not qdrant_client:
                    # Qdrant 없으면 원본 쿼리만 임베딩
                    try:
                        vec = embedding_model.encode(request.query).tolist()
                        return vec
                    except Exception:
                        return None
                
                # 모든 추출된 엔티티에 대해 동적 확장
                all_entity_values = []
                
                # Demographics: raw_value 수집
                for demo in extracted_entities.demographics:
                    if demo.raw_value:
                        all_entity_values.append(demo.raw_value)
                
                # Topics: name + keywords 수집
                for topic in extracted_entities.topics:
                    if topic.name:
                        all_entity_values.append(topic.name)
                    all_entity_values.extend(list(topic.keywords)[:3])  # 상위 3개만
                
                # Questions: question_text 수집
                for q in extracted_entities.questions:
                    if q.question_text:
                        all_entity_values.append(q.question_text)
                
                # 각 엔티티 값에 대해 Qdrant에서 유사 텍스트 수집
                syn_candidates = set()  # 중복 제거용
                collections = qdrant_client.get_collections()
                
                for entity_val in all_entity_values[:5]:  # 최대 5개 엔티티만 처리 (성능)
                    try:
                        base_vec = embedding_model.encode(entity_val).tolist()
                        for col in collections.collections:
                            try:
                                results = qdrant_client.search(
                                    collection_name=col.name,
                                    query_vector=base_vec,
                                    limit=10,  # 각 엔티티당 10개
                                    score_threshold=0.3  # 최소 유사도
                                )
                                for r in results:
                                    payload = getattr(r, 'payload', {}) or {}
                                    txt = payload.get('answer_text') or payload.get('text') or payload.get('q_text')
                                    if isinstance(txt, str) and len(txt.strip()) > 0:
                                        # 긴 문장은 그대로 사용 (임베딩이 의미를 포착)
                                        syn_candidates.add(txt.strip())
                            except Exception:
                                continue
                    except Exception:
                        continue
                
                # 수집된 유사 텍스트를 phrases에 추가 (최대 10개)
                phrases.extend(list(syn_candidates)[:10])
                
                # 모든 phrases를 임베딩하여 평균
                try:
                    vecs = embedding_model.encode(phrases, convert_to_tensor=False)
                    if hasattr(vecs, 'tolist'):
                        vecs = vecs.tolist()
                    if isinstance(vecs, list) and vecs:
                        dim = len(vecs[0])
                        avg = [0.0] * dim
                        for v in vecs:
                            for i in range(dim):
                                avg[i] += v[i]
                        avg = [x / len(vecs) for x in avg]
                        return avg
                except Exception:
                    # 실패 시 원본 쿼리만
                    try:
                        return embedding_model.encode(request.query).tolist()
                    except Exception:
                        return None
                return None

            if request.use_vector_search:
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
                            'minimum_should_match': 1
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

        # ⭐ 필터 적용 확인 로깅
        if filters:
            import json
            logger.info(f"🔍 적용된 필터 ({len(filters)}개):")
            for i, f in enumerate(filters, 1):
                logger.info(f"  필터 {i}: {json.dumps(f, ensure_ascii=False, indent=2)}")
            logger.info(f"🔍 최종 쿼리 구조:")
            logger.info(f"  {json.dumps(final_query, ensure_ascii=False, indent=2)}")

        # ⭐ Qdrant top-N 제한: 필터 유무에 따라 분기
        has_filters = bool(filters)
        if has_filters:
            # 필터 있음: 300~500개 (교집합 확보를 위해)
            qdrant_limit = min(500, max(300, size * 10))
            search_size = max(1000, min(size * 20, 5000))
            logger.info(f"🔍 필터 적용: OpenSearch size={search_size}, Qdrant limit={qdrant_limit} (교집합 확보를 위해)")
        else:
            # 필터 없음: 100~200개
            qdrant_limit = min(200, max(100, size * 2))
            search_size = size * 2
            logger.info(f"🔍 필터 없음: OpenSearch size={search_size}, Qdrant limit={qdrant_limit}")

        # 4) 실행: 하이브리드 (OpenSearch + 선택적 Qdrant) with RRF
        # ⭐ STEP 1: welcome_1st와 welcome_2nd를 각각 별도로 검색
        data_fetcher = DataFetcher(opensearch_client=os_client)
        
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
        welcome_1st_query = final_query.copy()
        welcome_2nd_query = final_query.copy()
        
        if filters and 'query' in final_query:
            logger.info(f"🔍 인덱스별 필터 분리 중...")
            # 필터를 타입별로 분리
            age_gender_filters = []
            occupation_filters = []
            
            for demo in extracted_entities.demographics:
                if demo.demographic_type in [DemographicType.AGE, DemographicType.GENDER]:
                    # welcome_1st용 필터
                    demo_filter = demo.to_opensearch_filter()
                    if demo_filter:
                        age_gender_filters.append(demo_filter)
                elif demo.demographic_type == DemographicType.OCCUPATION:
                    # welcome_2nd용 필터
                    demo_filter = demo.to_opensearch_filter()
                    if demo_filter:
                        occupation_filters.append(demo_filter)
            
            # welcome_1st 쿼리: 연령/성별 필터만 적용
            if age_gender_filters:
                base_query = final_query['query'].get('bool', {}).get('must', [])
                # match_all/match_none 제거
                base_query = [q for q in base_query if q not in [{"match_all": {}}, {"match_none": {}}] and q is not None]
                
                # ⭐ 키워드 쿼리와 필터 분리
                keyword_queries = []  # 키워드 검색 쿼리 (nested with match on answer_text)
                filtered_base = []    # 연령/성별 필터만
                
                for q in base_query:
                    if isinstance(q, dict):
                        # 키워드 쿼리인지 확인 (nested + match on answer_text, 필터가 아닌 것)
                        is_keyword_query = False
                        if 'nested' in q:
                            nested_query = q['nested'].get('query', {})
                            # match 쿼리이고 answer_text를 검색하는 경우 (키워드 검색)
                            if 'match' in nested_query:
                                match_field = list(nested_query['match'].keys())[0]
                                if 'answer_text' in match_field:
                                    is_keyword_query = True
                            # bool 쿼리 내부에 match가 있는 경우도 키워드 쿼리
                            elif 'bool' in nested_query:
                                for bool_type in ['must', 'should']:
                                    if bool_type in nested_query['bool']:
                                        for subq in nested_query['bool'][bool_type]:
                                            if isinstance(subq, dict) and 'match' in subq:
                                                match_field = list(subq['match'].keys())[0]
                                                if 'answer_text' in match_field:
                                                    is_keyword_query = True
                                                    break
                        
                        if is_keyword_query:
                            # 키워드 쿼리는 그대로 유지
                            keyword_queries.append(q)
                        else:
                            # 필터인지 확인 (term, nested with q_text 등)
                            is_age_gender = False
                            for f in age_gender_filters:
                                if q == f or (isinstance(q, dict) and isinstance(f, dict) and q.get('term') == f.get('term')):
                                    is_age_gender = True
                                    break
                            if is_age_gender:
                                filtered_base.append(q)
                    else:
                        # 기타 쿼리는 그대로 유지
                        keyword_queries.append(q)
                
                # ⭐ 키워드 쿼리와 필터 결합
                all_must_clauses = keyword_queries + filtered_base + age_gender_filters
                if all_must_clauses:
                    welcome_1st_query['query'] = {
                        'bool': {
                            'must': all_must_clauses
                        }
                    }
                    logger.info(f"  ✅ welcome_1st 쿼리: 키워드 {len(keyword_queries)}개 + 연령/성별 필터 {len(age_gender_filters)}개 적용")
                elif age_gender_filters:
                    # 필터만 있는 경우
                    welcome_1st_query['query'] = {
                        'bool': {
                            'must': age_gender_filters
                        }
                    }
                    logger.info(f"  ✅ welcome_1st 필터: 연령/성별 {len(age_gender_filters)}개만 적용")
            
            # welcome_2nd 쿼리: 직업 필터만 적용 (키워드 쿼리도 포함)
            if occupation_filters:
                base_query = final_query['query'].get('bool', {}).get('must', [])
                base_query = [q for q in base_query if q not in [{"match_all": {}}, {"match_none": {}}] and q is not None]
                
                # ⭐ 키워드 쿼리와 필터 분리
                keyword_queries_2nd = []  # 키워드 검색 쿼리
                filtered_base_2nd = []    # 직업 필터만
                
                for q in base_query:
                    if isinstance(q, dict):
                        # 키워드 쿼리인지 확인
                        is_keyword_query = False
                        if 'nested' in q:
                            nested_query = q['nested'].get('query', {})
                            if 'match' in nested_query:
                                match_field = list(nested_query['match'].keys())[0]
                                if 'answer_text' in match_field:
                                    is_keyword_query = True
                            elif 'bool' in nested_query:
                                for bool_type in ['must', 'should']:
                                    if bool_type in nested_query['bool']:
                                        for subq in nested_query['bool'][bool_type]:
                                            if isinstance(subq, dict) and 'match' in subq:
                                                match_field = list(subq['match'].keys())[0]
                                                if 'answer_text' in match_field:
                                                    is_keyword_query = True
                                                    break
                        
                        if is_keyword_query:
                            keyword_queries_2nd.append(q)
                        else:
                            # 필터인지 확인
                            is_occupation = False
                            for f in occupation_filters:
                                if q == f or (isinstance(q, dict) and isinstance(f, dict) and q.get('term') == f.get('term')):
                                    is_occupation = True
                                    break
                            if is_occupation:
                                filtered_base_2nd.append(q)
                    else:
                        keyword_queries_2nd.append(q)
                
                # ⭐ 키워드 쿼리와 필터 결합
                all_must_clauses_2nd = keyword_queries_2nd + filtered_base_2nd + occupation_filters
                if all_must_clauses_2nd:
                    welcome_2nd_query['query'] = {
                        'bool': {
                            'must': all_must_clauses_2nd
                        }
                    }
                    logger.info(f"  ✅ welcome_2nd 쿼리: 키워드 {len(keyword_queries_2nd)}개 + 직업 필터 {len(occupation_filters)}개 적용")
                elif occupation_filters:
                    welcome_2nd_query['query'] = {
                        'bool': {
                            'must': occupation_filters
                        }
                    }
                    logger.info(f"  ✅ welcome_2nd 필터: 직업 {len(occupation_filters)}개만 적용")
        
        # welcome_1st 검색
        welcome_1st_keyword_results = []
        welcome_1st_vector_results = []
        if search_welcome_1st:
            logger.info(f"📊 [1/3] welcome_1st 검색 중...")
            try:
                os_response_1st = data_fetcher.search_opensearch(
                    index_name="s_welcome_1st",
                    query=welcome_1st_query,  # ⭐ 연령/성별 필터만 적용된 쿼리
                    size=search_size,
                    source_filter=source_filter
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
        welcome_2nd_keyword_results = []
        welcome_2nd_vector_results = []
        if search_welcome_2nd:
            logger.info(f"📊 [2/3] welcome_2nd 검색 중...")
            try:
                os_response_2nd = data_fetcher.search_opensearch(
                    index_name="s_welcome_2nd",
                    query=welcome_2nd_query,  # ⭐ 직업 필터만 적용된 쿼리
                    size=search_size,
                    source_filter=source_filter
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
        other_keyword_results = []
        other_vector_results = []
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
                    os_response_other = data_fetcher.search_opensearch(
                        index_name=other_index_pattern,
                        query=final_query,
                        size=search_size,
                        source_filter=source_filter
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
        
        # RRF 점수 디버깅: 상위 10개 출력
        if rrf_results:
            logger.info(f"  - RRF 점수 상위 10개:")
            for i, doc in enumerate(rrf_results[:10], 1):
                rrf_score = doc.get('_score') or doc.get('rrf_score', 0.0)
                rrf_details = doc.get('_rrf_details', {})
                doc_index = doc.get('_index', 'unknown')
                logger.info(f"    {i}. doc_id={doc.get('_id', 'N/A')}, index={doc_index}, RRF={rrf_score:.6f}, "
                          f"keyword_rank={rrf_details.get('keyword_rank')}, vector_rank={rrf_details.get('vector_rank')}")
        
        # 필터가 있는 경우, 필터 조건에 맞는 결과만 유지
        final_hits = rrf_results
        # 배치 조회 결과를 루프 밖에서 선언 (필터 재적용과 결과 포맷팅 모두에서 사용)
        welcome_1st_batch = {}
        welcome_2nd_batch = {}
        
        if filters:
            # 성능 최적화: 배치 조회를 위해 먼저 모든 user_id 수집
            user_ids_to_fetch = set()
            doc_user_map = {}  # doc -> user_id 매핑
            
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
                batch_size = 200  # 배치 크기: 200건씩 분할
                total_batches = (len(user_ids_list) + batch_size - 1) // batch_size
                logger.info(f"🔍 배치 조회: welcome_1st/welcome_2nd {len(user_ids_list)}건 조회 중... (배치 크기: {batch_size}, 총 {total_batches}개 배치)")
                
                try:
                    # welcome_1st 배치 조회 (분할)
                    if user_ids_list:
                        found_count = 0
                        for batch_idx in range(0, len(user_ids_list), batch_size):
                            batch_ids = user_ids_list[batch_idx:batch_idx + batch_size]
                            batch_num = (batch_idx // batch_size) + 1
                            try:
                                mget_body = [{"_index": "s_welcome_1st", "_id": uid} for uid in batch_ids]
                                mget_response = os_client.mget(body={"docs": mget_body}, ignore=[404], request_timeout=60)
                                for item in mget_response.get('docs', []):
                                    if item.get('found'):
                                        welcome_1st_batch[item['_id']] = item['_source']
                                        found_count += 1
                                logger.debug(f"  📦 welcome_1st 배치 {batch_num}/{total_batches}: {len([d for d in mget_response.get('docs', []) if d.get('found')])}/{len(batch_ids)}건")
                            except Exception as e:
                                logger.warning(f"  ⚠️ welcome_1st 배치 {batch_num}/{total_batches} 실패: {e}")
                                continue
                        logger.info(f"  ✅ welcome_1st 배치 조회: {found_count}/{len(user_ids_list)}건 성공")
                    
                    # welcome_2nd 배치 조회 (분할)
                    if user_ids_list:
                        found_count = 0
                        for batch_idx in range(0, len(user_ids_list), batch_size):
                            batch_ids = user_ids_list[batch_idx:batch_idx + batch_size]
                            batch_num = (batch_idx // batch_size) + 1
                            try:
                                mget_body = [{"_index": "s_welcome_2nd", "_id": uid} for uid in batch_ids]
                                mget_response = os_client.mget(body={"docs": mget_body}, ignore=[404], request_timeout=60)
                                for item in mget_response.get('docs', []):
                                    if item.get('found'):
                                        welcome_2nd_batch[item['_id']] = item['_source']
                                        found_count += 1
                                logger.debug(f"  📦 welcome_2nd 배치 {batch_num}/{total_batches}: {len([d for d in mget_response.get('docs', []) if d.get('found')])}/{len(batch_ids)}건")
                            except Exception as e:
                                logger.warning(f"  ⚠️ welcome_2nd 배치 {batch_num}/{total_batches} 실패: {e}")
                                continue
                        logger.info(f"  ✅ welcome_2nd 배치 조회: {found_count}/{len(user_ids_list)}건 성공")
                    
                    logger.info(f"  ✅ 배치 조회 완료: welcome_1st={len(welcome_1st_batch)}건, welcome_2nd={len(welcome_2nd_batch)}건")
                    
                    # ⭐ 배치 조회에서 찾지 못한 user_id에 대해 개별 조회 시도 (fallback) - 제한적으로만
                    missing_1st = user_ids_to_fetch - set(welcome_1st_batch.keys())
                    missing_2nd = user_ids_to_fetch - set(welcome_2nd_batch.keys())
                    
                    # 개별 조회는 최대 100건까지만 (성능 고려)
                    if missing_1st and len(missing_1st) <= 100:
                        logger.info(f"  🔍 welcome_1st 개별 조회 시도: {len(missing_1st)}건...")
                        for uid in list(missing_1st)[:50]:  # 최대 50건만
                            try:
                                os_doc = os_client.get(index="s_welcome_1st", id=uid, ignore=[404], request_timeout=60)
                                if os_doc.get('found'):
                                    welcome_1st_batch[uid] = os_doc['_source']
                            except Exception:
                                continue
                        logger.info(f"  ✅ welcome_1st 개별 조회: {len([k for k in missing_1st if k in welcome_1st_batch])}건 추가 성공")
                    
                    if missing_2nd and len(missing_2nd) <= 100:
                        logger.info(f"  🔍 welcome_2nd 개별 조회 시도: {len(missing_2nd)}건...")
                        for uid in list(missing_2nd)[:50]:  # 최대 50건만
                            try:
                                os_doc = os_client.get(index="s_welcome_2nd", id=uid, ignore=[404], request_timeout=60)
                                if os_doc.get('found'):
                                    welcome_2nd_batch[uid] = os_doc['_source']
                            except Exception:
                                continue
                        logger.info(f"  ✅ welcome_2nd 개별 조회: {len([k for k in missing_2nd if k in welcome_2nd_batch])}건 추가 성공")
                        
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
            filtered_rrf_results = []
            source_not_found_count = 0
            low_score_count = 0
            opposite_count = 0
            
            # 필터별 미충족 통계
            age_filter_failed = 0
            gender_filter_failed = 0
            occupation_filter_failed = 0
            both_filters_failed = 0
            age_filter_failed_count = 0  # 디버깅용 카운터
            
            for doc in rrf_results:
                # source 추출 (여러 경로 시도)
                source = doc.get('_source', {})
                if not source and 'doc' in doc:
                    source = doc.get('doc', {}).get('_source', {})
                
                # Qdrant 결과인 경우 payload에서 추출
                if not source or not isinstance(source, dict):
                    payload = source.get('payload', {}) if isinstance(source, dict) else {}
                    if isinstance(payload, dict) and payload:
                        source = payload
                
                # user_id로 OpenSearch에서 실제 문서 조회 (필터 확인을 위해)
                user_id = source.get('user_id') if isinstance(source, dict) else None
                if not user_id:
                    user_id = doc.get('_id', '')
                
                # OpenSearch에서 실제 문서 조회 (필터 확인을 위해)
                if user_id and user_id in user_doc_map:
                    source = user_doc_map[user_id]['source']
                elif user_id:
                    # 직접 조회 시도
                    try:
                        for idx_name in [request.index_name] if request.index_name != '*' else ['s_welcome_2nd', 'survey_250106', 'survey_250107']:
                            try:
                                os_doc = os_client.get(index=idx_name, id=user_id, ignore=[404], request_timeout=60)
                                if os_doc.get('found'):
                                    source = os_doc['_source']
                                    break
                            except Exception:
                                continue
                    except Exception:
                        pass
                
                if not source or not isinstance(source, dict):
                    # source를 찾을 수 없으면 필터 통과 불가
                    source_not_found_count += 1
                    continue
                
                # ⭐ 필터 조건 확인 (must: 모든 필터를 만족해야 함)
                # welcome_1st: 연령/성별 정보, welcome_2nd: 직업 정보
                # user_id로 인덱스 간 데이터를 연결하여 확인
                matches_all_filters = True
                
                # user_id로 welcome_1st와 welcome_2nd에서 각각 정보 확인
                user_id = source.get('user_id') if isinstance(source, dict) else None
                if not user_id:
                    user_id = doc.get('_id', '')
                    # doc_user_map에서도 확인
                    if not user_id:
                        user_id = doc_user_map.get(id(doc))
                
                # ⭐ 배치 조회 결과에서 가져오기 (캐시된 데이터 사용)
                welcome_1st_source = welcome_1st_batch.get(user_id) if user_id else None
                welcome_1st_found = welcome_1st_source is not None
                
                # ⭐ 배치 조회에서 찾지 못한 경우 개별 조회 시도 (fallback)
                if not welcome_1st_found and user_id:
                    try:
                        os_doc = os_client.get(index="s_welcome_1st", id=user_id, ignore=[404], request_timeout=60)
                        if os_doc.get('found'):
                            welcome_1st_source = os_doc['_source']
                            welcome_1st_batch[user_id] = welcome_1st_source  # 캐시에 추가
                            welcome_1st_found = True
                    except Exception:
                        pass
                
                # welcome_2nd에서 직업 정보 확인 (현재 source가 welcome_2nd일 수 있음)
                welcome_2nd_source = source if source.get('metadata', {}).get('occupation') != '미정' or any('직업' in str(qa.get('q_text', '')) for qa in source.get('qa_pairs', [])) else None
                welcome_2nd_found = bool(welcome_2nd_source)
                
                # 배치 조회 결과에서 가져오기 (fallback)
                if not welcome_2nd_source and user_id:
                    welcome_2nd_source = welcome_2nd_batch.get(user_id)
                    welcome_2nd_found = welcome_2nd_source is not None
                
                # ⭐ 배치 조회에서 찾지 못한 경우 개별 조회 시도 (fallback)
                if not welcome_2nd_found and user_id:
                    try:
                        os_doc = os_client.get(index="s_welcome_2nd", id=user_id, ignore=[404], request_timeout=60)
                        if os_doc.get('found'):
                            welcome_2nd_source = os_doc['_source']
                            welcome_2nd_batch[user_id] = welcome_2nd_source  # 캐시에 추가
                            welcome_2nd_found = True
                    except Exception:
                        pass
                
                # 디버깅: welcome_1st/welcome_2nd 조회 결과 로깅 (처음 10개만)
                # ⚠️ 연령 필터 실패가 많으므로 더 자세히 로깅
                if opposite_count < 10 or (opposite_count < 20 and not welcome_1st_found):
                    logger.warning(f"🔍 user_id={user_id}: welcome_1st={welcome_1st_found}, welcome_2nd={welcome_2nd_found}, source_index={source.get('_index', 'unknown')}")
                    if not welcome_1st_found and user_id:
                        logger.warning(f"   ⚠️ welcome_1st 조회 실패 (배치+개별 모두 시도했지만 찾지 못함): user_id={user_id}")
                
                # 각 demographic 필터 확인 (must: 모든 필터를 만족해야 함)
                filter_match_details = {}  # 디버깅용
                for demo in extracted_entities.demographics:
                    matches_this_filter = False
                    match_source = None  # 어디서 매칭되었는지 추적
                    
                    if demo.demographic_type == DemographicType.AGE:
                        from datetime import datetime
                        # ⭐ 1순위: welcome_1st에서 연령 정보 확인
                        if welcome_1st_source:
                            age_group = welcome_1st_source.get('metadata', {}).get('age_group', '')
                            birth_year = welcome_1st_source.get('metadata', {}).get('birth_year', '')
                            
                            if age_group == demo.raw_value:
                                matches_this_filter = True
                                match_source = f"welcome_1st.metadata.age_group={age_group}"
                            elif birth_year and birth_year != '미정':
                                # 출생년도로 계산
                                current_year = datetime.now().year
                                try:
                                    birth_year_int = int(birth_year)
                                    age = current_year - birth_year_int
                                    
                                    if demo.raw_value == "30대" and 30 <= age < 40:
                                        matches_this_filter = True
                                        match_source = f"welcome_1st.metadata.birth_year={birth_year} (age={age})"
                                    elif demo.raw_value == "20대" and 20 <= age < 30:
                                        matches_this_filter = True
                                        match_source = f"welcome_1st.metadata.birth_year={birth_year} (age={age})"
                                    elif demo.raw_value == "40대" and 40 <= age < 50:
                                        matches_this_filter = True
                                        match_source = f"welcome_1st.metadata.birth_year={birth_year} (age={age})"
                                except (ValueError, TypeError):
                                    pass
                        
                        # ⭐ 연령 정보는 welcome_1st에만 있으므로, welcome_1st_source가 없으면 필터 통과 불가
                        # 디버깅: 연령 필터 실패 시 상세 로깅 (처음 10개만)
                        if not matches_this_filter and age_filter_failed_count < 10:
                            logger.warning(f"🔍 [연령 필터 실패] user_id={user_id}:")
                            logger.warning(f"   - 요청 연령: {demo.raw_value}")
                            if welcome_1st_source:
                                age_group = welcome_1st_source.get('metadata', {}).get('age_group', '')
                                birth_year = welcome_1st_source.get('metadata', {}).get('birth_year', '')
                                logger.warning(f"   - welcome_1st.age_group: '{age_group}'")
                                logger.warning(f"   - welcome_1st.birth_year: '{birth_year}'")
                                logger.warning(f"   - age_group 매칭: {age_group == demo.raw_value}")
                                if birth_year and birth_year != '미정':
                                    try:
                                        age = datetime.now().year - int(birth_year)
                                        logger.warning(f"   - 계산된 나이: {age}세")
                                        logger.warning(f"   - 30대 범위 체크: {30 <= age < 40}")
                                    except:
                                        pass
                            else:
                                logger.warning(f"   - welcome_1st: 없음 (연령 정보는 welcome_1st에만 있음)")
                        
                        # ⭐ 필터 실패 시 카운터 증가
                        if not matches_this_filter:
                            age_filter_failed_count += 1
                    
                    elif demo.demographic_type == DemographicType.GENDER:
                        # ⭐ 동의어 확장기 사용
                        try:
                            from rag_query_analyzer.utils.synonym_expander import get_synonym_expander
                            expander = get_synonym_expander()
                            gender_synonyms = expander.expand(demo.raw_value)
                        except Exception:
                            # 동의어 확장기 실패 시 기본 동의어 사용
                            gender_synonyms = [demo.raw_value]
                            gender_synonyms.extend([syn for syn in demo.synonyms if syn])
                        
                        # welcome_1st에서 성별 정보 확인
                        if welcome_1st_source:
                            gender = welcome_1st_source.get('metadata', {}).get('gender', '')
                            # ⭐ 동의어 확장된 값들과 매칭
                            if gender in gender_synonyms:
                                matches_this_filter = True
                                match_source = f"welcome_1st.metadata.gender={gender}"
                        
                        # qa_pairs에서도 확인 (fallback)
                        if not matches_this_filter:
                            for src in [welcome_1st_source, source]:
                                if not src:
                                    continue
                                qa_pairs_list = src.get('qa_pairs', [])
                                if isinstance(qa_pairs_list, list):
                                    for qa in qa_pairs_list:
                                        if isinstance(qa, dict):
                                            q_text = qa.get('q_text', '')
                                            answer = qa.get('answer', qa.get('answer_text', ''))
                                            
                                            if '성별' in q_text or 'gender' in q_text.lower():
                                                answer_str = str(answer).lower()
                                                # ⭐ 동의어 확장된 값들과 매칭
                                                if any(syn.lower() in answer_str or syn in str(answer) for syn in gender_synonyms):
                                                    matches_this_filter = True
                                                    match_source = f"qa_pairs.{q_text}={answer}"
                                                    break
                    
                    elif demo.demographic_type == DemographicType.OCCUPATION:
                        # ⭐ 동의어 확장기 사용
                        try:
                            from rag_query_analyzer.utils.synonym_expander import get_synonym_expander
                            expander = get_synonym_expander()
                            occupation_synonyms = expander.expand(demo.raw_value)
                        except Exception:
                            # 동의어 확장기 실패 시 기본 동의어 사용
                            occupation_synonyms = [demo.raw_value]
                            occupation_synonyms.extend([syn for syn in demo.synonyms if syn])
                        
                        # ⭐ 직업 정보는 qa_pairs에서만 확인 (metadata.occupation 필드가 없거나 "미정"인 경우가 많음)
                        # welcome_2nd_source 우선 확인
                        if welcome_2nd_source:
                            qa_pairs_list = welcome_2nd_source.get('qa_pairs', [])
                            if isinstance(qa_pairs_list, list):
                                for qa in qa_pairs_list:
                                    if isinstance(qa, dict):
                                        q_text = qa.get('q_text', '')
                                        answer = qa.get('answer', qa.get('answer_text', ''))
                                        
                                        # 직업 질문 확인
                                        if '직업' in q_text or 'occupation' in q_text.lower() or '직무' in q_text:
                                            answer_str = str(answer).lower()
                                            # ⭐ 동의어 확장된 값들과 매칭
                                            if any(syn.lower() in answer_str or syn in str(answer) for syn in occupation_synonyms):
                                                matches_this_filter = True
                                                match_source = f"welcome_2nd.qa_pairs.{q_text}={answer}"
                                                break
                        
                        # ⭐ welcome_2nd_source에서 못 찾으면 현재 source의 qa_pairs에서 확인 (fallback)
                        if not matches_this_filter:
                            for src in [source]:
                                if not src:
                                    continue
                                qa_pairs_list = src.get('qa_pairs', [])
                                if isinstance(qa_pairs_list, list):
                                    for qa in qa_pairs_list:
                                        if isinstance(qa, dict):
                                            q_text = qa.get('q_text', '')
                                            answer = qa.get('answer', qa.get('answer_text', ''))
                                            
                                            # 직업 질문 확인
                                            if '직업' in q_text or 'occupation' in q_text.lower() or '직무' in q_text:
                                                answer_str = str(answer).lower()
                                                # ⭐ 동의어 확장된 값들과 매칭
                                                if any(syn.lower() in answer_str or syn in str(answer) for syn in occupation_synonyms):
                                                    matches_this_filter = True
                                                    match_source = f"source.qa_pairs.{q_text}={answer}"
                                                    break
                    
                    # 필터 매칭 결과 저장
                    filter_match_details[demo.demographic_type.value] = {
                        'matched': matches_this_filter,
                        'source': match_source,
                        'raw_value': demo.raw_value
                    }
                    
                    # 하나라도 필터를 만족하지 않으면 제외
                    if not matches_this_filter:
                        matches_all_filters = False
                        # 필터별 미충족 통계
                        if demo.demographic_type == DemographicType.AGE:
                            age_filter_failed += 1
                        elif demo.demographic_type == DemographicType.GENDER:
                            gender_filter_failed += 1
                        elif demo.demographic_type == DemographicType.OCCUPATION:
                            occupation_filter_failed += 1
                        logger.debug(f"❌ user_id={user_id}: {demo.demographic_type.value} 필터 미충족 (요구: {demo.raw_value})")
                        break
                    else:
                        logger.debug(f"✅ user_id={user_id}: {demo.demographic_type.value} 필터 충족 (요구: {demo.raw_value}, 매칭: {match_source})")
                
                # 모든 필터를 만족하는 문서만 포함 (for 루프 밖에서 확인)
                if matches_all_filters:
                    filtered_rrf_results.append(doc)
                    logger.debug(f"✅ user_id={user_id}: 모든 필터 충족 - 포함됨")
                else:
                    opposite_count += 1
                    # 두 필터 모두 미충족인지 확인
                    age_matched = filter_match_details.get('age', {}).get('matched', False)
                    occupation_matched = filter_match_details.get('occupation', {}).get('matched', False)
                    if not age_matched and not occupation_matched:
                        both_filters_failed += 1
                    # ⭐ 제외된 문서 샘플 상세 로깅 (처음 10개만)
                    if opposite_count <= 10:
                        logger.warning(f"❌ 제외된 문서 샘플 {opposite_count}:")
                        logger.warning(f"   user_id: {user_id}")
                        logger.warning(f"   welcome_1st: {welcome_1st_source is not None}")
                        logger.warning(f"   welcome_2nd: {welcome_2nd_source is not None}")
                        if welcome_1st_source:
                            metadata_1st = welcome_1st_source.get('metadata', {})
                            logger.warning(f"   age_group: {metadata_1st.get('age_group', 'N/A')}")
                            logger.warning(f"   gender: {metadata_1st.get('gender', 'N/A')}")
                            logger.warning(f"   birth_year: {metadata_1st.get('birth_year', 'N/A')}")
                        if welcome_2nd_source:
                            metadata_2nd = welcome_2nd_source.get('metadata', {})
                            logger.warning(f"   occupation (metadata): {metadata_2nd.get('occupation', 'N/A')}")
                            qa_pairs = welcome_2nd_source.get('qa_pairs', [])
                            qa_texts = [qa.get('q_text', '') for qa in qa_pairs[:5] if isinstance(qa, dict)]
                            logger.warning(f"   qa_pairs (처음 5개): {qa_texts}")
                        logger.warning(f"   필터 매칭 상세: {filter_match_details}")
                    logger.debug(f"❌ user_id={user_id}: 필터 미충족 - 제외됨 (상세: {filter_match_details})")
            
            final_hits = filtered_rrf_results[:size]
            logger.info(f"🔍 RRF 후 필터 재적용: {len(rrf_results)}건 → {len(filtered_rrf_results)}건")
            logger.info(f"  - source를 찾지 못한 문서: {source_not_found_count}건")
            logger.info(f"  - RRF 점수 낮음 (0.001 미만): {low_score_count}건")
            logger.info(f"  - 필터 조건 미충족 문서: {opposite_count}건")
            logger.info(f"  - 필터 조건 충족 문서: {len(filtered_rrf_results)}건 (요청 size: {size})")
            logger.info(f"📊 필터별 미충족 통계:")
            logger.info(f"  - 연령 필터 미충족: {age_filter_failed}건")
            logger.info(f"  - 성별 필터 미충족: {gender_filter_failed}건")
            logger.info(f"  - 직업 필터 미충족: {occupation_filter_failed}건")
            logger.info(f"  - 두 필터 모두 미충족: {both_filters_failed}건")
            
            # 필터 조건 미충족 문서가 많으면 경고
            if opposite_count > len(filtered_rrf_results) * 2:
                logger.warning(f"⚠️ 필터 조건 미충족 문서가 많습니다 ({opposite_count}건). 필터 로직을 확인해주세요.")
        else:
            final_hits = rrf_results[:size]
            logger.info(f"🔍 RRF 결과 사용 (필터 없음): {len(final_hits)}건")
        
        logger.info(f"🔍 최종 결과: {len(final_hits)}건")

        results = []
        for doc in final_hits:
            # RRF 결과에서 user_id 추출 (여러 경로 시도)
            source = doc.get('_source', {})
            if not source and 'doc' in doc:
                # RRF 결과 구조 확인
                source = doc.get('doc', {}).get('_source', {})
            
            payload = source.get('payload', {}) if isinstance(source.get('payload'), dict) else {}
            user_id = (
                source.get('user_id') or 
                payload.get('user_id') or 
                doc.get('_id', '') or
                doc.get('doc', {}).get('_id', '')
            )
            
            # OpenSearch에서 실제 문서 조회
            doc_id = doc.get('_id', '')
            welcome_1st_source = None  # 연령/성별 정보용
            welcome_2nd_source = None  # 직업 정보용
            
            if user_id in user_doc_map:
                # user_id로 매핑된 경우
                doc_data = user_doc_map[user_id]
                source = doc_data['source']
                inner_hits = doc_data['inner_hits']
                highlight = doc_data['highlight']
            elif doc_id in id_doc_map:
                # _id로 매핑된 경우
                doc_data = id_doc_map[doc_id]
                source = doc_data['source']
                inner_hits = doc_data['inner_hits']
                highlight = doc_data['highlight']
            else:
                # Qdrant 결과인 경우, OpenSearch에서 조회 시도
                source = {}
                inner_hits = {}
                highlight = None
                
                # Qdrant payload에서 index 정보 확인
                qdrant_index = payload.get('index')
                index_candidates = []
                if qdrant_index:
                    index_candidates.append(qdrant_index)
                
                # index_name에서 실제 인덱스 목록 추출
                if request.index_name == '*':
                    # 모든 인덱스 시도 (일반적인 인덱스 이름들)
                    index_candidates.extend(['s_welcome_2nd', 'survey_250106', 'survey_250107'])
                else:
                    index_candidates.extend([idx.strip() for idx in request.index_name.split(',')])
                
                # 각 인덱스에서 문서 조회 시도
                for idx_name in index_candidates:
                    try:
                        os_doc = os_client.get(index=idx_name, id=user_id, ignore=[404], request_timeout=60)
                        if os_doc.get('found'):
                            source = os_doc['_source']
                            break
                    except Exception:
                        continue
            
            # ⭐ welcome_1st와 welcome_2nd에서 정보 조회 (결과에 포함하기 위해)
            # 배치 조회 결과에서 가져오기 (이미 조회한 데이터 재사용)
            welcome_1st_source = None
            welcome_2nd_source = None
            
            # ⭐ 필터가 있는 경우, 최종 결과 포매팅 단계에서 필터 조건 재확인
            if filters and extracted_entities:
                # 필터 조건을 만족하는지 확인
                matches_all_filters = True
                
                if user_id:
                    # 배치 조회 결과에서 가져오기
                    welcome_1st_source = welcome_1st_batch.get(user_id) if user_id in welcome_1st_batch else None
                    welcome_2nd_source = welcome_2nd_batch.get(user_id) if user_id in welcome_2nd_batch else None
                    
                    # 개별 조회 fallback
                    if not welcome_1st_source and user_id:
                        try:
                            os_doc = os_client.get(index="s_welcome_1st", id=user_id, ignore=[404], request_timeout=60)
                            if os_doc.get('found'):
                                welcome_1st_source = os_doc['_source']
                        except Exception:
                            pass
                    
                    if not welcome_2nd_source and user_id:
                        try:
                            os_doc = os_client.get(index="s_welcome_2nd", id=user_id, ignore=[404], request_timeout=60)
                            if os_doc.get('found'):
                                welcome_2nd_source = os_doc['_source']
                        except Exception:
                            pass
                
                # 각 필터 조건 확인
                for demo in extracted_entities.demographics:
                    matches_this_filter = False
                    
                    if demo.demographic_type == DemographicType.AGE:
                        if welcome_1st_source:
                            age_group = welcome_1st_source.get('metadata', {}).get('age_group', '')
                            birth_year = welcome_1st_source.get('metadata', {}).get('birth_year', '')
                            
                            if age_group == demo.raw_value:
                                matches_this_filter = True
                            elif birth_year and birth_year != '미정':
                                from datetime import datetime
                                try:
                                    age = datetime.now().year - int(birth_year)
                                    if demo.raw_value == "30대" and 30 <= age < 40:
                                        matches_this_filter = True
                                    elif demo.raw_value == "20대" and 20 <= age < 30:
                                        matches_this_filter = True
                                    elif demo.raw_value == "40대" and 40 <= age < 50:
                                        matches_this_filter = True
                                except (ValueError, TypeError):
                                    pass
                        
                        # qa_pairs에서도 확인 (fallback)
                        if not matches_this_filter:
                            for src in [welcome_1st_source, source]:
                                if not src:
                                    continue
                                qa_pairs_list = src.get('qa_pairs', [])
                                if isinstance(qa_pairs_list, list):
                                    for qa in qa_pairs_list:
                                        if isinstance(qa, dict):
                                            q_text = qa.get('q_text', '')
                                            answer = qa.get('answer', qa.get('answer_text', ''))
                                            if any(kw in q_text for kw in ['출생년도', '출생', '연령', '나이', '연령대', 'age']):
                                                # 동의어 확장 사용
                                                try:
                                                    from rag_query_analyzer.utils.synonym_expander import get_synonym_expander
                                                    expander = get_synonym_expander()
                                                    age_synonyms = expander.expand(demo.raw_value)
                                                except Exception:
                                                    age_synonyms = [demo.raw_value]
                                                    age_synonyms.extend([syn for syn in demo.synonyms if syn])
                                                
                                                answer_str = str(answer).lower()
                                                if any(syn.lower() in answer_str or syn in str(answer) for syn in age_synonyms):
                                                    matches_this_filter = True
                                                    break
                    
                    elif demo.demographic_type == DemographicType.GENDER:
                        # ⭐ 성별 필터 확인 추가
                        # welcome_1st에서 성별 정보 확인
                        if welcome_1st_source:
                            gender = welcome_1st_source.get('metadata', {}).get('gender', '')
                            # 동의어 확장 사용
                            try:
                                from rag_query_analyzer.utils.synonym_expander import get_synonym_expander
                                expander = get_synonym_expander()
                                gender_synonyms = expander.expand(demo.raw_value)
                            except Exception:
                                gender_synonyms = [demo.raw_value]
                                gender_synonyms.extend([syn for syn in demo.synonyms if syn])
                            
                            if gender in gender_synonyms:
                                matches_this_filter = True
                        
                        # qa_pairs에서도 확인 (fallback)
                        if not matches_this_filter:
                            for src in [welcome_1st_source, source]:
                                if not src:
                                    continue
                                qa_pairs_list = src.get('qa_pairs', [])
                                if isinstance(qa_pairs_list, list):
                                    for qa in qa_pairs_list:
                                        if isinstance(qa, dict):
                                            q_text = qa.get('q_text', '')
                                            answer = qa.get('answer', qa.get('answer_text', ''))
                                            if '성별' in q_text or 'gender' in q_text.lower():
                                                # 동의어 확장 사용
                                                try:
                                                    from rag_query_analyzer.utils.synonym_expander import get_synonym_expander
                                                    expander = get_synonym_expander()
                                                    gender_synonyms = expander.expand(demo.raw_value)
                                                except Exception:
                                                    gender_synonyms = [demo.raw_value]
                                                    gender_synonyms.extend([syn for syn in demo.synonyms if syn])
                                                
                                                answer_str = str(answer).lower()
                                                if any(syn.lower() in answer_str or syn in str(answer) for syn in gender_synonyms):
                                                    matches_this_filter = True
                                                    break
                    
                    elif demo.demographic_type == DemographicType.OCCUPATION:
                        # welcome_2nd_source 우선 확인
                        if welcome_2nd_source:
                            qa_pairs_list = welcome_2nd_source.get('qa_pairs', [])
                            if isinstance(qa_pairs_list, list):
                                for qa in qa_pairs_list:
                                    if isinstance(qa, dict):
                                        q_text = qa.get('q_text', '')
                                        answer = qa.get('answer', qa.get('answer_text', ''))
                                        if '직업' in q_text or 'occupation' in q_text.lower() or '직무' in q_text:
                                            # 동의어 확장 사용
                                            try:
                                                from rag_query_analyzer.utils.synonym_expander import get_synonym_expander
                                                expander = get_synonym_expander()
                                                occupation_synonyms = expander.expand(demo.raw_value)
                                            except Exception:
                                                occupation_synonyms = [demo.raw_value]
                                                occupation_synonyms.extend([syn for syn in demo.synonyms if syn])
                                            
                                            answer_str = str(answer).lower()
                                            if any(syn.lower() in answer_str or syn in str(answer) for syn in occupation_synonyms):
                                                matches_this_filter = True
                                                break
                        
                        # welcome_2nd_source에서 못 찾으면 현재 source의 qa_pairs에서 확인 (fallback)
                        if not matches_this_filter:
                            for src in [source]:
                                if not src:
                                    continue
                                qa_pairs_list = src.get('qa_pairs', [])
                                if isinstance(qa_pairs_list, list):
                                    for qa in qa_pairs_list:
                                        if isinstance(qa, dict):
                                            q_text = qa.get('q_text', '')
                                            answer = qa.get('answer', qa.get('answer_text', ''))
                                            if '직업' in q_text or 'occupation' in q_text.lower() or '직무' in q_text:
                                                # 동의어 확장 사용
                                                try:
                                                    from rag_query_analyzer.utils.synonym_expander import get_synonym_expander
                                                    expander = get_synonym_expander()
                                                    occupation_synonyms = expander.expand(demo.raw_value)
                                                except Exception:
                                                    occupation_synonyms = [demo.raw_value]
                                                    occupation_synonyms.extend([syn for syn in demo.synonyms if syn])
                                                
                                                answer_str = str(answer).lower()
                                                if any(syn.lower() in answer_str or syn in str(answer) for syn in occupation_synonyms):
                                                    matches_this_filter = True
                                                    break
                    
                    if not matches_this_filter:
                        matches_all_filters = False
                        break
                
                # 필터 조건을 만족하지 않으면 이 문서를 건너뛰기
                if not matches_all_filters:
                    logger.debug(f"⚠️ 최종 결과에서 제외: user_id={user_id} (필터 조건 미충족)")
                    continue
            else:
                # 필터가 없는 경우에만 welcome_1st/welcome_2nd 조회
                if user_id:
                    # welcome_1st: 배치 조회 결과 사용
                    if user_id in welcome_1st_batch:
                        welcome_1st_source = welcome_1st_batch[user_id]
                    else:
                        # 배치 조회에서 못 찾은 경우에만 개별 조회 (fallback)
                        try:
                            os_doc = os_client.get(index='s_welcome_1st', id=user_id, ignore=[404], request_timeout=60)
                            if os_doc.get('found'):
                                welcome_1st_source = os_doc['_source']
                        except Exception:
                            pass
                    
                    # welcome_2nd: 배치 조회 결과 사용
                    # 먼저 현재 source에서 직업 정보 확인
                    if source and isinstance(source, dict):
                        metadata = source.get('metadata', {})
                        occupation = metadata.get('occupation', '')
                        qa_pairs = source.get('qa_pairs', [])
                        # metadata에 occupation이 있고 "미정"이 아니면 현재 source 사용
                        if occupation and occupation != '미정':
                            welcome_2nd_source = source
                        # qa_pairs에 직업 정보가 있으면 현재 source 사용
                        elif any('직업' in str(qa.get('q_text', '')) for qa in qa_pairs if isinstance(qa, dict)):
                            welcome_2nd_source = source
                    
                    # 배치 조회 결과에서 가져오기
                    if not welcome_2nd_source and user_id in welcome_2nd_batch:
                        welcome_2nd_source = welcome_2nd_batch[user_id]
                    
                    # 배치 조회에서도 못 찾은 경우에만 개별 조회 (fallback)
                    if not welcome_2nd_source:
                        try:
                            os_doc = os_client.get(index='s_welcome_2nd', id=user_id, ignore=[404], request_timeout=60)
                            if os_doc.get('found'):
                                welcome_2nd_source = os_doc['_source']
                                # source가 없으면 welcome_2nd를 source로 사용
                                if not source:
                                    source = welcome_2nd_source
                        except Exception:
                            pass

            # matched_qa_pairs 추출 (inner_hits에서)
            matched_qa = []
            
            # inner_hits가 dict인 경우
            if isinstance(inner_hits, dict):
                # 모든 nested path 순회 (qa_pairs, qa_pairs.answer 등)
                for path_name, nested_data in inner_hits.items():
                    if isinstance(nested_data, dict) and 'hits' in nested_data:
                        hits_list = nested_data['hits'].get('hits', [])
                        for inner_hit in hits_list:
                            source = inner_hit.get('_source', {})
                            if source:
                                qa_data = {
                                    'q_text': source.get('q_text', ''),
                                    'answer': source.get('answer', source.get('answer_text', '')),
                                    'answer_text': source.get('answer_text', source.get('answer', '')),
                                    'match_score': inner_hit.get('_score', 0.0)
                                }
                                if 'highlight' in inner_hit:
                                    qa_data['highlights'] = inner_hit['highlight']
                                matched_qa.append(qa_data)
            
            # RRF 결과에서 직접 inner_hits 확인 (fallback)
            if not matched_qa and 'inner_hits' in doc:
                doc_inner_hits = doc.get('inner_hits', {})
                if isinstance(doc_inner_hits, dict):
                    for path_name, nested_data in doc_inner_hits.items():
                        if isinstance(nested_data, dict) and 'hits' in nested_data:
                            hits_list = nested_data['hits'].get('hits', [])
                            for inner_hit in hits_list:
                                source = inner_hit.get('_source', {})
                                if source:
                                    qa_data = {
                                        'q_text': source.get('q_text', ''),
                                        'answer': source.get('answer', source.get('answer_text', '')),
                                        'answer_text': source.get('answer_text', source.get('answer', '')),
                                        'match_score': inner_hit.get('_score', 0.0)
                                    }
                                    if 'highlight' in inner_hit:
                                        qa_data['highlights'] = inner_hit['highlight']
                                    matched_qa.append(qa_data)
            
            # ⭐ 필터 매칭 결과도 추출 (qa_pairs에서 직접 찾기)
            if not matched_qa and source and 'qa_pairs' in source:
                qa_pairs_list = source.get('qa_pairs', [])
                if isinstance(qa_pairs_list, list):
                    # 추출된 엔티티와 매칭되는 qa_pairs 찾기
                    for demo in extracted_entities.demographics:
                        demo_raw = demo.raw_value
                        demo_value = demo.value
                        
                        # qa_pairs에서 매칭되는 항목 찾기
                        for qa in qa_pairs_list:
                            if isinstance(qa, dict):
                                q_text = qa.get('q_text', '')
                                answer = qa.get('answer', qa.get('answer_text', ''))
                                
                                # 질문 키워드 매칭
                                is_demo_question = False
                                if demo.demographic_type == DemographicType.AGE:
                                    is_demo_question = any(kw in q_text for kw in ['연령', '나이', '연령대', 'age', '출생'])
                                elif demo.demographic_type == DemographicType.GENDER:
                                    is_demo_question = any(kw in q_text for kw in ['성별', 'gender'])
                                elif demo.demographic_type == DemographicType.OCCUPATION:
                                    is_demo_question = any(kw in q_text for kw in ['직업', 'occupation', '직무'])
                                
                                # 답변 매칭 (raw_value 또는 value 포함)
                                if is_demo_question and answer:
                                    answer_str = str(answer).lower()
                                    if (demo_raw.lower() in answer_str or 
                                        demo_value.lower() in answer_str or
                                        any(syn.lower() in answer_str for syn in demo.synonyms)):
                                        # 중복 체크
                                        if not any(m.get('q_text') == q_text and m.get('answer') == answer for m in matched_qa):
                                            matched_qa.append({
                                                'q_text': q_text,
                                                'answer': answer,
                                                'answer_text': answer,
                                                'match_score': 1.0,  # 필터 매칭은 높은 점수
                                                'match_type': 'filter'
                                            })

            # ⭐ welcome_1st와 welcome_2nd 정보를 결과에 포함
            # 연령/성별 정보 (welcome_1st)
            demographic_info = {}
            if welcome_1st_source:
                metadata_1st = welcome_1st_source.get('metadata', {})
                demographic_info['age_group'] = metadata_1st.get('age_group', '미정')
                demographic_info['gender'] = metadata_1st.get('gender', '미정')
                demographic_info['birth_year'] = metadata_1st.get('birth_year', '미정')
            
            # ⭐ 직업 정보 (qa_pairs에서만 추출 - metadata.occupation 필드가 없거나 "미정"인 경우가 많음)
            occupation_value = '미정'
            
            # welcome_2nd_source의 qa_pairs에서 추출
            if welcome_2nd_source:
                qa_pairs_list = welcome_2nd_source.get('qa_pairs', [])
                if isinstance(qa_pairs_list, list):
                    for qa in qa_pairs_list:
                        if isinstance(qa, dict):
                            q_text = qa.get('q_text', '')
                            answer = str(qa.get('answer', qa.get('answer_text', '')))
                            
                            # "직업" 질문에서 답변 추출
                            if '직업' in q_text or 'occupation' in q_text.lower() or '직무' in q_text:
                                if answer and answer != '미정':
                                    # 직업 타입 매핑
                                    answer_lower = answer.lower()
                                    if '사무직' in answer:
                                        occupation_value = 'office'
                                    elif '전문직' in answer:
                                        occupation_value = 'professional'
                                    elif '서비스' in answer or '서비스직' in answer:
                                        occupation_value = 'service'
                                    elif '학생' in answer or '대학생' in answer or '대학원생' in answer:
                                        occupation_value = 'student'
                                    elif '주부' in answer:
                                        occupation_value = 'housewife'
                                    elif '자영업' in answer:
                                        occupation_value = 'self_employed'
                                    elif '무직' in answer or '없음' in answer:
                                        occupation_value = 'unemployed'
                                    else:
                                        # 원본 값 사용 (20자 제한)
                                        occupation_value = answer[:20]
                                    break
            
            # welcome_2nd_source에서 못 찾은 경우, 현재 source의 qa_pairs에서 확인
            if occupation_value == '미정' and source:
                qa_pairs_list = source.get('qa_pairs', [])
                if isinstance(qa_pairs_list, list):
                    for qa in qa_pairs_list:
                        if isinstance(qa, dict):
                            q_text = qa.get('q_text', '')
                            answer = str(qa.get('answer', qa.get('answer_text', '')))
                            
                            # "직업" 질문에서 답변 추출
                            if '직업' in q_text or 'occupation' in q_text.lower() or '직무' in q_text:
                                if answer and answer != '미정':
                                    # 직업 타입 매핑
                                    answer_lower = answer.lower()
                                    if '사무직' in answer:
                                        occupation_value = 'office'
                                    elif '전문직' in answer:
                                        occupation_value = 'professional'
                                    elif '서비스' in answer or '서비스직' in answer:
                                        occupation_value = 'service'
                                    elif '학생' in answer or '대학생' in answer or '대학원생' in answer:
                                        occupation_value = 'student'
                                    elif '주부' in answer:
                                        occupation_value = 'housewife'
                                    elif '자영업' in answer:
                                        occupation_value = 'self_employed'
                                    elif '무직' in answer or '없음' in answer:
                                        occupation_value = 'unemployed'
                                    else:
                                        # 원본 값 사용 (20자 제한)
                                        occupation_value = answer[:20]
                                    break
            
            demographic_info['occupation'] = occupation_value
            
            # user_id가 없으면 doc_id 사용
            final_user_id = user_id or doc_id or 'unknown'
            
            results.append(
                SearchResult(
                    user_id=final_user_id,
                    score=doc.get('_score', 0.0),
                    timestamp=source.get('timestamp'),
                    demographic_info=demographic_info if demographic_info else None,  # ⭐ 인구통계 정보 추가
                    qa_pairs=source.get('qa_pairs', [])[:5] if source else [],
                    matched_qa_pairs=matched_qa,
                    highlights=highlight,
                )
            )

        # ⭐ total_hits 수정: 실제 결과 개수 사용 (RRF 후 결과 개수)
        actual_total_hits = len(results)
        
        return SearchResponse(
            query=request.query,
            total_hits=actual_total_hits,  # ⭐ 실제 결과 개수
            max_score=final_hits[0].get('_score', 0.0) if final_hits else 0.0,
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
            },
            took_ms=took_ms,
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
