# redis_celery/tasks/search_tasks.py
"""
Celery 병렬 검색 태스크 (전체 인덱스 병렬 처리)
"""
import asyncio
import json
import logging
from typing import Dict, Any, List, Tuple
from time import perf_counter

from redis_celery.celery_app import celery_app
from opensearchpy import OpenSearch
from qdrant_client import QdrantClient
from connectors.data_fetcher import DataFetcher
from connectors.hybrid_searcher import calculate_rrf_score
from rag_query_analyzer.config import get_config
from sentence_transformers import SentenceTransformer
import redis
import os

logger = logging.getLogger(__name__)

# 전역 캐시
_embedding_model = None
_config = None


def get_clients():
    """클라이언트 초기화"""
    os_client = OpenSearch(
        hosts=[{
            'host': os.getenv('OPENSEARCH_HOST', 'localhost'),
            'port': int(os.getenv('OPENSEARCH_PORT', '9200'))
        }],
        http_auth=(
            os.getenv('OPENSEARCH_USER', 'admin'),
            os.getenv('OPENSEARCH_PASSWORD', 'admin')
        ),
        use_ssl=False,
        verify_certs=False,
        timeout=30,
    )
    
    qdrant_client = QdrantClient(
        host=os.getenv('QDRANT_HOST', 'localhost'),
        port=int(os.getenv('QDRANT_PORT', '6333')),
        timeout=30
    )
    
    redis_client = redis.StrictRedis(
        host=os.getenv('REDIS_HOST', 'localhost'),
        port=int(os.getenv('REDIS_PORT', '6379')),
        db=int(os.getenv('CACHE_DB', '2')),
        decode_responses=True
    )
    
    return os_client, qdrant_client, redis_client


def get_embedding_model():
    """임베딩 모델 로드"""
    global _embedding_model, _config
    
    if _embedding_model is None:
        _config = get_config()
        _embedding_model = SentenceTransformer(_config.EMBEDDING_MODEL)
        logger.info("✅ 임베딩 모델 로드 완료")
    
    return _embedding_model, _config


@celery_app.task(
    name='tasks.search_tasks.parallel_hybrid_search_all',
    bind=True,
    max_retries=3,
    default_retry_delay=5,
)
def parallel_hybrid_search_all(
    self,
    query: str,
    index_name: str = "*",
    size: int = 10,
    use_vector_search: bool = True
):
    """
    🚀 전체 인덱스 병렬 하이브리드 검색
    
    - welcome_1st, welcome_2nd, survey_25_* (30개) 동시 검색
    - 각 인덱스마다 OpenSearch + Qdrant 병렬 실행
    - RRF 결합 후 Redis 캐싱
    """
    task_id = self.request.id
    logger.info(f"🚀 [Task {task_id}] 전체 인덱스 병렬 검색 시작: query='{query}'")
    
    try:
        os_client, qdrant_client, redis_client = get_clients()
        embedding_model, config = get_embedding_model()
        
        data_fetcher = DataFetcher(
            opensearch_client=os_client,
            qdrant_client=qdrant_client,
            async_opensearch_client=None
        )
        
        # 비동기 병렬 검색 실행
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            result = loop.run_until_complete(
                execute_all_indices_parallel_search(
                    query=query,
                    index_name=index_name,
                    size=size,
                    use_vector_search=use_vector_search,
                    data_fetcher=data_fetcher,
                    embedding_model=embedding_model,
                    config=config,
                    redis_client=redis_client,
                    task_id=task_id,
                )
            )
        finally:
            loop.close()
        
        logger.info(f"✅ [Task {task_id}] 전체 인덱스 병렬 검색 완료: {result['total_hits']}건")
        return result
        
    except Exception as exc:
        logger.error(f"❌ [Task {task_id}] 검색 실패: {exc}", exc_info=True)
        
        try:
            raise self.retry(exc=exc, countdown=5)
        except self.MaxRetriesExceededError:
            return {
                'status': 'failed',
                'error': str(exc),
                'error_type': type(exc).__name__,
                'query': query,
                'task_id': task_id,
            }


async def execute_all_indices_parallel_search(
    query: str,
    index_name: str,
    size: int,
    use_vector_search: bool,
    data_fetcher: DataFetcher,
    embedding_model: SentenceTransformer,
    config: Any,
    redis_client: redis.StrictRedis,
    task_id: str,
) -> Dict[str, Any]:
    """
    전체 인덱스 병렬 검색 핵심 로직
    """
    from rag_query_analyzer.analyzers.main_analyzer import AdvancedRAGQueryAnalyzer
    from rag_query_analyzer.analyzers.demographic_extractor import DemographicExtractor
    
    timings = {}
    overall_start = perf_counter()
    
    # 1. 쿼리 분석
    analyzer = AdvancedRAGQueryAnalyzer(config)
    analysis = analyzer.analyze_query(query)
    
    extractor = DemographicExtractor()
    extracted_entities, requested_size = extractor.extract_with_size(query)
    
    actual_size = max(1, min(requested_size, 100))
    
    # 2. 임베딩 벡터 생성
    query_vector = None
    if use_vector_search:
        query_vector = embedding_model.encode(query).tolist()
    
    # 3. 필터 구성
    filters = build_filters(extracted_entities)
    age_gender_filters = [f for f in filters if is_age_or_gender_filter(f)]
    occupation_filters = [f for f in filters if is_occupation_filter(f)]
    
    has_filters = bool(filters)
    
    if has_filters:
        qdrant_limit = min(500, max(300, actual_size * 10))
        search_size = max(1000, min(actual_size * 20, 5000))
    else:
        qdrant_limit = min(200, max(100, actual_size * 2))
        search_size = actual_size * 2
    
    logger.info(f"🔍 [Task {task_id}] 병렬 검색 파라미터: size={search_size}, qdrant_limit={qdrant_limit}")
    
    # 🚀 4. 전체 인덱스 병렬 검색
    parallel_start = perf_counter()
    
    # 4-1. 검색할 인덱스 목록 가져오기
    all_indices = get_all_survey_indices(data_fetcher.os_client)
    logger.info(f"📋 [Task {task_id}] 검색할 인덱스: {len(all_indices)}개")
    
    # 4-2. 인덱스별 검색 태스크 생성
    search_tasks = []
    
    # welcome_1st (연령/성별 필터)
    search_tasks.append(
        search_index_parallel(
            data_fetcher=data_fetcher,
            index_name="s_welcome_1st",
            search_size=search_size,
            qdrant_limit=qdrant_limit,
            query_vector=query_vector,
            filters=age_gender_filters,
            task_id=task_id,
        )
    )
    
    # welcome_2nd (직업 필터)
    search_tasks.append(
        search_index_parallel(
            data_fetcher=data_fetcher,
            index_name="s_welcome_2nd",
            search_size=search_size,
            qdrant_limit=qdrant_limit,
            query_vector=query_vector,
            filters=occupation_filters,
            task_id=task_id,
        )
    )
    
    # survey_25_* 인덱스들 (30개)
    survey_indices = [idx for idx in all_indices if idx.startswith('survey_25')]
    for survey_index in survey_indices:
        search_tasks.append(
            search_index_parallel(
                data_fetcher=data_fetcher,
                index_name=survey_index,
                search_size=search_size,
                qdrant_limit=qdrant_limit,
                query_vector=query_vector,
                filters=filters,  # 전체 필터 적용
                task_id=task_id,
            )
        )
    
    # 🔥 4-3. 모든 인덱스 동시 검색 (32개 병렬 실행)
    all_results = await asyncio.gather(*search_tasks, return_exceptions=True)
    
    timings['parallel_search_ms'] = (perf_counter() - parallel_start) * 1000
    logger.info(f"⚡ [Task {task_id}] 전체 인덱스 병렬 검색 완료: {timings['parallel_search_ms']:.2f}ms")
    
    # 5. 결과 수집 및 RRF 결합
    rrf_start = perf_counter()
    
    all_rrf_results = []
    for i, result in enumerate(all_results):
        if isinstance(result, Exception):
            logger.warning(f"⚠️ 인덱스 {i} 검색 실패: {result}")
            continue
        
        keyword_results, vector_results, index_name = result
        
        # 인덱스별 RRF
        if vector_results:
            index_rrf = calculate_rrf_score(keyword_results, vector_results, k=60)
        else:
            index_rrf = keyword_results
        
        # 인덱스 정보 추가
        for doc in index_rrf:
            doc['_index'] = index_name
        
        all_rrf_results.append(index_rrf)
        logger.info(f"  ✅ {index_name}: {len(index_rrf)}건")
    
    # user_id 기준 결합
    combined_results = combine_by_user_id(all_rrf_results)
    
    timings['rrf_combination_ms'] = (perf_counter() - rrf_start) * 1000
    logger.info(f"  ✅ RRF 결합 완료: {len(combined_results)}건")
    
    # 6. Redis 캐싱
    cache_start = perf_counter()
    cache_results_to_redis(
        redis_client=redis_client,
        task_id=task_id,
        results=combined_results,
        ttl=3600
    )
    timings['redis_cache_ms'] = (perf_counter() - cache_start) * 1000
    
    # 7. 최종 결과
    final_hits = combined_results[:actual_size]
    results = format_search_results(final_hits)
    
    total_duration_ms = (perf_counter() - overall_start) * 1000
    timings['total_ms'] = total_duration_ms
    
    return {
        'status': 'completed',
        'task_id': task_id,
        'query': query,
        'total_hits': len(combined_results),
        'max_score': final_hits[0].get('_score', 0.0) if final_hits else 0.0,
        'results': results,
        'query_analysis': {
            'intent': analysis.intent,
            'must_terms': analysis.must_terms,
            'should_terms': analysis.should_terms,
            'extracted_entities': extracted_entities.to_dict(),
            'filters': filters,
        },
        'search_summary': {
            'total_indices_searched': len(all_indices),
            'welcome_1st': len([r for r in combined_results if r.get('_index') == 's_welcome_1st']),
            'welcome_2nd': len([r for r in combined_results if r.get('_index') == 's_welcome_2nd']),
            'survey_indices': len([r for r in combined_results if r.get('_index', '').startswith('survey_25')]),
        },
        'timings_ms': timings,
        'took_ms': int(total_duration_ms),
    }


def get_all_survey_indices(os_client: OpenSearch) -> List[str]:
    """
    OpenSearch에서 모든 survey 인덱스 목록 가져오기
    
    Returns:
        ['s_welcome_1st', 's_welcome_2nd', 'survey_25_01', 'survey_25_02', ...]
    """
    try:
        # cat.indices API로 모든 인덱스 조회
        indices_response = os_client.cat.indices(format='json')
        
        # survey 관련 인덱스만 필터링
        survey_indices = [
            idx['index'] for idx in indices_response
            if idx['index'].startswith('s_welcome') or idx['index'].startswith('survey_25')
        ]
        
        survey_indices.sort()  # 정렬
        logger.info(f"📋 발견된 인덱스: {survey_indices}")
        
        return survey_indices
        
    except Exception as e:
        logger.error(f"❌ 인덱스 목록 조회 실패: {e}")
        # 실패 시 기본 인덱스만 반환
        return ['s_welcome_1st', 's_welcome_2nd']


async def search_index_parallel(
    data_fetcher: DataFetcher,
    index_name: str,
    search_size: int,
    qdrant_limit: int,
    query_vector: List[float],
    filters: List[Dict[str, Any]],
    task_id: str,
) -> Tuple[List[Dict], List[Dict], str]:
    """
    단일 인덱스 병렬 검색 (OpenSearch + Qdrant 동시)
    
    Returns:
        (keyword_results, vector_results, index_name)
    """
    # 쿼리 구성
    query = {
        'query': {'match_all': {}},
        'size': search_size,
        '_source': ['user_id', 'metadata', 'qa_pairs', 'timestamp']
    }
    
    if filters:
        # inner_hits 제거 (성능 최적화)
        cleaned_filters = [remove_inner_hits(f) for f in filters]
        query['query'] = {'bool': {'must': cleaned_filters}}
    
    # 🔥 OpenSearch + Qdrant 동시 실행
    opensearch_task = asyncio.to_thread(
        search_opensearch_sync,
        os_client=data_fetcher.os_client,
        index_name=index_name,
        query=query,
        search_size=search_size,
    )
    
    qdrant_task = asyncio.to_thread(
        search_qdrant_sync,
        qdrant_client=data_fetcher.qdrant_client,
        collection_name=index_name,
        query_vector=query_vector,
        limit=qdrant_limit,
    )
    
    try:
        os_response, qdrant_results = await asyncio.gather(opensearch_task, qdrant_task)
        
        keyword_results = os_response['hits']['hits'] if os_response else []
        
        return keyword_results, qdrant_results, index_name
        
    except Exception as e:
        logger.warning(f"⚠️ {index_name} 검색 실패: {e}")
        return [], [], index_name


def search_opensearch_sync(
    os_client: OpenSearch,
    index_name: str,
    query: Dict[str, Any],
    search_size: int,
) -> Dict[str, Any]:
    """OpenSearch 검색 (동기)"""
    try:
        return os_client.search(
            index=index_name,
            body=query,
            size=search_size,
            request_timeout=10,
            ignore=[404]  # 인덱스 없으면 무시
        )
    except Exception as e:
        logger.warning(f"⚠️ OpenSearch {index_name} 검색 실패: {e}")
        return {'hits': {'hits': []}}


def search_qdrant_sync(
    qdrant_client: QdrantClient,
    collection_name: str,
    query_vector: List[float],
    limit: int,
) -> List[Dict[str, Any]]:
    """Qdrant 검색 (동기)"""
    if not query_vector:
        return []
    
    try:
        results = qdrant_client.search(
            collection_name=collection_name,
            query_vector=query_vector,
            limit=limit,
            score_threshold=0.3,
        )
        
        return [
            {
                '_id': str(r.id),
                '_score': r.score,
                '_source': r.payload
            }
            for r in results
        ]
    except Exception as e:
        logger.debug(f"⚠️ Qdrant {collection_name} 검색 실패: {e}")
        return []


def remove_inner_hits(query_dict: Dict[str, Any]) -> Dict[str, Any]:
    """재귀적으로 inner_hits 제거 (성능 최적화)"""
    import copy
    cleaned = copy.deepcopy(query_dict)
    
    if isinstance(cleaned, dict):
        if 'nested' in cleaned:
            if 'inner_hits' in cleaned['nested']:
                del cleaned['nested']['inner_hits']
            if 'query' in cleaned['nested']:
                cleaned['nested']['query'] = remove_inner_hits(cleaned['nested']['query'])
        
        if 'bool' in cleaned:
            for key in ['must', 'should', 'must_not', 'filter']:
                if key in cleaned['bool']:
                    if isinstance(cleaned['bool'][key], list):
                        cleaned['bool'][key] = [remove_inner_hits(item) for item in cleaned['bool'][key]]
                    else:
                        cleaned['bool'][key] = remove_inner_hits(cleaned['bool'][key])
    
    return cleaned


def combine_by_user_id(rrf_results_list: List[List[Dict]]) -> List[Dict]:
    """user_id 기준으로 결과 결합"""
    user_map = {}
    
    for rrf_results in rrf_results_list:
        for doc in rrf_results:
            user_id = extract_user_id(doc)
            if user_id:
                if user_id not in user_map:
                    user_map[user_id] = []
                user_map[user_id].append(doc)
    
    # RRF 점수 합산
    final_results = []
    for user_id, docs in user_map.items():
        if len(docs) == 1:
            final_results.append(docs[0])
        else:
            total_score = sum(d.get('_score', 0.0) for d in docs)
            best_doc = max(docs, key=lambda d: d.get('_score', 0.0))
            best_doc['_score'] = total_score
            best_doc['_rrf_details'] = {
                'combined_score': total_score,
                'source_count': len(docs),
                'sources': [d.get('_index', 'unknown') for d in docs]
            }
            final_results.append(best_doc)
    
    final_results.sort(key=lambda d: d.get('_score', 0.0), reverse=True)
    return final_results


def cache_results_to_redis(
    redis_client: redis.StrictRedis,
    task_id: str,
    results: List[Dict],
    ttl: int = 3600
):
    """Redis에 검색 결과 캐싱"""
    # 1. user_id 리스트 저장
    id_list_key = f"task:{task_id}:ids"
    user_ids = [extract_user_id(doc) for doc in results if extract_user_id(doc)]
    
    if user_ids:
        redis_client.delete(id_list_key)
        redis_client.rpush(id_list_key, *user_ids)
        redis_client.expire(id_list_key, ttl)
    
    # 2. 각 문서 상세 정보 저장
    for doc in results:
        user_id = extract_user_id(doc)
        if user_id:
            data_key = f"task:{task_id}:data:{user_id}"
            result_data = {
                'user_id': user_id,
                'score': doc.get('_score', 0.0),
                'timestamp': doc.get('_source', {}).get('timestamp'),
                'qa_pairs': doc.get('_source', {}).get('qa_pairs', [])[:5],
                'index': doc.get('_index', 'unknown'),
            }
            redis_client.setex(
                data_key,
                ttl,
                json.dumps(result_data, ensure_ascii=False)
            )
    
    logger.info(f"✅ Redis 캐싱 완료: {len(user_ids)}건")


def extract_user_id(doc: Dict) -> str:
    """문서에서 user_id 추출"""
    source = doc.get('_source', {})
    if not source and 'doc' in doc:
        source = doc.get('doc', {}).get('_source', {})
    
    return source.get('user_id') or doc.get('_id', '')


def format_search_results(hits: List[Dict]) -> List[Dict]:
    """검색 결과 포맷"""
    results = []
    for doc in hits:
        source = doc.get('_source', {})
        results.append({
            'user_id': extract_user_id(doc),
            'score': doc.get('_score', 0.0),
            'timestamp': source.get('timestamp'),
            'qa_pairs': source.get('qa_pairs', [])[:5],
            'index': doc.get('_index', 'unknown'),
        })
    return results


def build_filters(extracted_entities: Any) -> List[Dict]:
    """필터 구성"""
    from rag_query_analyzer.models.entities import DemographicType
    
    filters = []
    for demo in extracted_entities.demographics:
        metadata_only = demo.demographic_type in {DemographicType.AGE, DemographicType.GENDER}
        filter_clause = demo.to_opensearch_filter(
            metadata_only=metadata_only,
            include_qa_fallback=True,
        )
        if filter_clause and filter_clause != {"match_all": {}}:
            filters.append(filter_clause)
    
    return filters


def is_age_or_gender_filter(filter_dict: Dict[str, Any]) -> bool:
    """연령/성별 필터 여부 확인"""
    import json
    try:
        filter_str = json.dumps(filter_dict, ensure_ascii=False)
    except Exception:
        filter_str = str(filter_dict)
    
    age_gender_keywords = [
        "metadata.age_group", "metadata.gender", "birth_year", "연령", "나이", "성별"
    ]
    return any(keyword in filter_str for keyword in age_gender_keywords)


def is_occupation_filter(filter_dict: Dict[str, Any]) -> bool:
    """직업 필터 여부 확인"""
    import json
    try:
        filter_str = json.dumps(filter_dict, ensure_ascii=False)
    except Exception:
        filter_str = str(filter_dict)
    
    occupation_keywords = [
        "metadata.occupation", "occupation", "직업", "직무"
    ]
    return any(keyword in filter_str for keyword in occupation_keywords)