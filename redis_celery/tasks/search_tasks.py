# redis_celery/tasks/search_tasks.py
"""
🚀 개선된 Celery 병렬 검색 태스크

주요 개선 사항:
1. 인덱스별 개별 Celery Task 생성 → Worker 간 병렬 처리
2. Redis 캐싱 강화 (쿼리 해시 기반)
3. Task Chord를 활용한 RRF 결합
4. 에러 핸들링 강화
"""
import asyncio
import json
import logging
import hashlib
import os

from typing import Dict, Any, List, Tuple, Optional
from time import perf_counter
from celery import group, chord, signals

from redis_celery.celery_app import celery_app
from opensearchpy import OpenSearch
from qdrant_client import QdrantClient
from connectors.data_fetcher import DataFetcher
from connectors.hybrid_searcher import calculate_rrf_score
from rag_query_analyzer.config import get_config
from sentence_transformers import SentenceTransformer
from redis import ConnectionPool, StrictRedis

logger = logging.getLogger(__name__)
_os_client = None
_qdrant_client = None
_redis_pool = None
_embedding_model = None
_config = None

# ==========================================
# 📌 1. 쿼리 캐싱 유틸리티
# ==========================================
@signals.worker_process_init.connect
def setup_worker_environment(**kwargs):
    """
    Worker 프로세스 시작 시 클라이언트와 모델을 로드하여 캐시합니다.
    이 함수는 Task 실행 비용을 획기적으로 줄여줍니다.
    """
    global _os_client, _qdrant_client, _redis_pool, _embedding_model, _config

    try:
        # 1. 설정 로드 (Task 실행 비용이 아님)
        os.environ['SENTENCE_TRANSFORMERS_HOME'] = '/app/.cache/models'
        _config = get_config()
        
        # 2. 클라이언트 연결 풀 생성
        # Docker Compose 환경에서는 'redis' 서비스 이름 사용
        _redis_pool = ConnectionPool(
            host=os.getenv('REDIS_HOST', 'redis'), 
            port=int(os.getenv('REDIS_PORT', '6379')),
            db=int(os.getenv('CACHE_DB', '2')),
            decode_responses=True, max_connections=20,
            socket_connect_timeout=5, socket_timeout=5
        )
        _redis_client = StrictRedis(connection_pool=_redis_pool) # 이 인스턴스는 Task에서 사용

        # 3. OpenSearch 클라이언트 생성 (인라인 통합)
        _os_client = OpenSearch(
            hosts=[{
                'host': os.getenv('OPENSEARCH_HOST', 'redis'), # Docker service name fix
                'port': int(os.getenv('OPENSEARCH_PORT', '9200'))
            }],
            http_auth=(os.getenv('OPENSEARCH_USER', 'admin'), os.getenv('OPENSEARCH_PASSWORD', 'admin')),
            use_ssl=False, verify_certs=False, timeout=30,
        )
        
        # 4. Qdrant 클라이언트 생성 (인라인 통합)
        _qdrant_client = QdrantClient(
            host=os.getenv('QDRANT_HOST', 'redis'), 
            port=int(os.getenv('QDRANT_PORT', '6333')), 
            timeout=30
        )

        logger.info("[OK] External Clients (OS/Qdrant) initialized.")
        load_model_flag = os.getenv('LOAD_EMBEDDING_MODEL', 'False').lower() == 'true'
        
        if load_model_flag:
            model_name = _config.EMBEDDING_MODEL
            _embedding_model = SentenceTransformer(model_name)
            logger.info(f"✅ Worker 시작: 임베딩 모델 '{model_name}' 로드 완료")
        else:
            logger.info("ℹ️ Worker 시작: 임베딩 모델 로드를 건너뜁니다 (LOAD_EMBEDDING_MODEL=False)")
        
        logger.info(f"✅ Worker 시작: 임베딩 모델 '{model_name}' 로드 완료")

    except Exception as e:
        logger.critical(f"❌ Worker 초기화 실패: {e}")
        raise # Worker가 초기화 실패하면 죽도록 강제 (Healthcheck 실패 유도)


def get_redis_client() -> StrictRedis:
    """Task에서 캐시된 Redis 연결 풀을 사용하여 클라이언트 반환"""
    global _redis_pool
    if _redis_pool is None:
        raise RuntimeError("Redis Pool not initialized.")
    return StrictRedis(connection_pool=_redis_pool)


def get_cache_key(query: str, index_name: str, filters: List[Dict] = None) -> str:
    """캐시 키 생성 (쿼리 해시)"""
    cache_data = {
        'query': query,
        'index_name': index_name,
        'filters': filters or []
    }
    cache_str = json.dumps(cache_data, sort_keys=True, ensure_ascii=False)
    return f"search:{hashlib.md5(cache_str.encode()).hexdigest()}"


def get_cached_results(redis_client: StrictRedis, cache_key: str) -> Optional[Dict]:
    """Redis에서 캐시된 결과 조회"""
    try:
        cached = redis_client.get(cache_key)
        if cached:
            logger.info(f"✅ 캐시 HIT: {cache_key}")
            return json.loads(cached)
    except Exception as e:
        logger.warning(f"캐시 조회 실패: {e}")
    return None


def cache_results(redis_client: StrictRedis, cache_key: str, results: Dict, ttl: int = 3600):
    """Redis에 결과 캐싱"""
    try:
        redis_client.setex(
            cache_key,
            ttl,
            json.dumps(results, ensure_ascii=False)
        )
        logger.info(f"✅ 캐시 저장: {cache_key}")
    except Exception as e:
        logger.warning(f"캐시 저장 실패: {e}")

# ==========================================
# 📌 2. 단일 인덱스 검색 Task (Worker 분산 처리)
# ==========================================
@celery_app.task(
    name='tasks.search_single_index',
    bind=True,
    max_retries=2,
    default_retry_delay=3,
)
def search_single_index_task(
    self,
    query: str,
    index_name: str,
    query_vector: List[float],
    filters: List[Dict[str, Any]],
    search_size: int,
    qdrant_limit: int,
) -> Dict[str, Any]:
    """단일 인덱스 검색"""
    task_id = self.request.id
    start_time = perf_counter()
    
    try:
        logger.info(f"🔍 [{task_id}] {index_name} 검색 시작")
        
        # Redis 캐시 확인
        redis_client = get_redis_client()
        os_client, qdrant_client = get_search_clients() # 캐시된 클라이언트 반환

        cache_key = get_cache_key(query, index_name, filters)
        cached = get_cached_results(redis_client, cache_key)
        if cached:
            return cached
        
        # ✅ EventLoop 생성 및 실행
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        loop_start = perf_counter()
        keyword_results, vector_results = loop.run_until_complete(
            execute_hybrid_search_async(
                os_client=os_client,
                qdrant_client=qdrant_client,
                index_name=index_name,
                query_vector=query_vector,
                filters=filters,
                search_size=search_size,
                qdrant_limit=qdrant_limit,
            )
        )
        
        # RRF 결합
        rrf_results = calculate_rrf_score(
            keyword_results, vector_results, k=60
        ) if vector_results else keyword_results
        
        for doc in rrf_results:
            doc['_index'] = index_name
        
        took_ms = (perf_counter() - start_time) * 1000
        
        result = {
            'index_name': index_name,
            'keyword_results': keyword_results,
            'vector_results': vector_results,
            'rrf_results': rrf_results,
            'took_ms': took_ms
        }
        
        # 캐싱
        cache_results(redis_client, cache_key, result)
        
        logger.info(f"✅ [{task_id}] {index_name} 완료: {len(rrf_results)}건 ({took_ms:.2f}ms)")
        return result
        
    except Exception as exc:
        logger.error(f"❌ [{task_id}] {index_name} 실패: {exc}", exc_info=True)
        raise self.retry(exc=exc, countdown=3)
    
    finally:
        # ✅ EventLoop 안전한 정리
        if loop is not None:
            try:
                # 남은 Task 취소
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                
                # 정리 실행
                if not loop.is_closed():
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                    loop.close()
                    
            except Exception as e:
                logger.warning(f"⚠️ EventLoop 정리 실패: {e}")
            finally:
                # 이전 이벤트 루프 초기화
                asyncio.set_event_loop(None)


async def execute_hybrid_search_async(
    os_client: OpenSearch,
    qdrant_client: QdrantClient,
    index_name: str,
    query_vector: List[float],
    filters: List[Dict[str, Any]],
    search_size: int,
    qdrant_limit: int,
) -> Tuple[List[Dict], List[Dict]]:
    """OpenSearch + Qdrant 비동기 병렬 실행"""
    
    # 쿼리 구성
    os_query = {
        'query': {'match_all': {}},
        'size': search_size,
        '_source': ['user_id', 'metadata', 'qa_pairs', 'timestamp']
    }
    
    if filters:
        cleaned_filters = [remove_inner_hits(f) for f in filters]
        os_query['query'] = {'bool': {'must': cleaned_filters}}
    
    # 🔥 OpenSearch + Qdrant 동시 실행
    opensearch_task = asyncio.to_thread(
        search_opensearch_sync,
        os_client=os_client,
        index_name=index_name,
        query=os_query,
        search_size=search_size,
    )
    
    qdrant_task = asyncio.to_thread(
        search_qdrant_sync,
        qdrant_client=qdrant_client,
        collection_name=index_name,
        query_vector=query_vector,
        limit=qdrant_limit,
    )
    
    os_response, qdrant_results = await asyncio.gather(opensearch_task, qdrant_task)
    
    keyword_results = os_response['hits']['hits'] if os_response else []
    
    return keyword_results, qdrant_results


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
            ignore=[404]
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


# ==========================================
# 📌 3. 전체 인덱스 병렬 검색 Orchestrator
# ==========================================
def get_cached_config_model():
    """캐시된 Config 및 SentenceTransformer 객체를 반환합니다."""
    # 🚨 이 함수는 파일 상단의 전역 변수 _config와 _embedding_model을 참조합니다.
    global _config, _embedding_model
    if _config is None or _embedding_model is None:
        raise RuntimeError("Worker environment failed to initialize model/config.")
    return _config, _embedding_model

@celery_app.task(
    name='tasks.parallel_hybrid_search_orchestrator',
    bind=True,
)
def parallel_hybrid_search_orchestrator(
    self,
    query: str,
    index_name: str = "*",
    size: int = 10,
    use_vector_search: bool = True
):
    """
    🚀 전체 인덱스 병렬 검색 오케스트레이터
    
    - Celery Chord를 사용하여 인덱스별 Task 병렬 실행
    - 모든 Task 완료 후 RRF 재결합
    """
    task_id = self.request.id
    logger.info(f"🚀 [{task_id}] 전체 인덱스 병렬 검색 시작: query='{query}'")
    
    try:
        # 1. 설정 및 쿼리 분석
        config, embedding_model = get_cached_config_model()
        
        from rag_query_analyzer.analyzers.main_analyzer import AdvancedRAGQueryAnalyzer
        from rag_query_analyzer.analyzers.demographic_extractor import DemographicExtractor
        
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
        
        # 4. 인덱스 목록 가져오기
        os_client, _ = get_search_clients()
        all_indices = get_all_survey_indices(os_client)
        
        logger.info(f"📋 [{task_id}] 검색할 인덱스: {len(all_indices)}개")
        
        # 5. 🔥 인덱스별 Task 생성 (Celery Worker Pool에 분산)
        search_tasks = []
        
        # welcome_1st
        search_tasks.append(
            search_single_index_task.s(
                query=query,
                index_name="s_welcome_1st",
                query_vector=query_vector,
                filters=age_gender_filters,
                search_size=search_size,
                qdrant_limit=qdrant_limit,
            )
        )
        
        # welcome_2nd
        search_tasks.append(
            search_single_index_task.s(
                query=query,
                index_name="s_welcome_2nd",
                query_vector=query_vector,
                filters=occupation_filters,
                search_size=search_size,
                qdrant_limit=qdrant_limit,
            )
        )
        
        # survey_25_* 인덱스들
        survey_indices = [idx for idx in all_indices if idx.startswith('survey_25')]
        for survey_index in survey_indices:
            search_tasks.append(
                search_single_index_task.s(
                    query=query,
                    index_name=survey_index,
                    query_vector=query_vector,
                    filters=filters,
                    search_size=search_size,
                    qdrant_limit=qdrant_limit,
                )
            )
        
        # 6. 🔥 Celery Chord: 병렬 실행 + 콜백
        # group()으로 모든 Task 병렬 실행 → combine_results_task()로 결합
        job = chord(
            group(search_tasks),
            on_error=handle_chord_error.s(task_id=task_id)  # ✅ 에러 핸들러
        )(
            combine_results_task.s(
                query=query,
                size=actual_size,
                task_id=task_id,
            )
        )
        final_result = job.get(timeout=120, propagate=True)
        logger.info(f"✅ [{task_id}] {len(search_tasks)}개 Task 병렬 실행 시작")
        
        return final_result
        
    except Exception as exc:
        logger.error(f"❌ [{task_id}] 오케스트레이터 실패: {exc}", exc_info=True)
        return {
            'status': 'failed',
            'error': str(exc),
            'task_id': task_id,
        }

# ✅ Chord 에러 핸들러
@celery_app.task(name='tasks.handle_chord_error')
def handle_chord_error(request, exc, traceback, task_id: str):
    """Chord Task 실패 시 처리"""
    logger.error(f"❌ Chord Task 실패: task_id={task_id}, error={exc}")
    logger.error(f"Traceback: {traceback}")
    
    # Redis에 실패 상태 기록
    redis_client = get_redis_client()
    redis_client.setex(
        f"task:{task_id}:error",
        3600,
        json.dumps({
            'error': str(exc),
            'traceback': str(traceback),
            'timestamp': perf_counter()
        })
    )
    
# ==========================================
# 📌 4. RRF 재결합 Task (Callback)
# ==========================================

@celery_app.task(
    name='tasks.combine_results',
    bind=True,
)
def combine_results_task(
    self,
    index_results: List[Dict],
    query: str,
    size: int,
    task_id: str,
):
    """
    🔥 모든 인덱스 검색 완료 후 RRF 재결합
    
    - Celery Chord의 콜백으로 실행
    - user_id 기준 결합 + Redis 캐싱
    """
    try:
        logger.info(f"🔄 [{task_id}] RRF 재결합 시작: {len(index_results)}개 인덱스")
        
        # 1. 각 인덱스의 RRF 결과 수집
        all_rrf_results = []
        for result in index_results:
            if isinstance(result, dict) and 'rrf_results' in result:
                rrf_results = result['rrf_results']
                index_name = result['index_name']
                
                # 인덱스 정보 추가
                for doc in rrf_results:
                    doc['_index'] = index_name
                
                all_rrf_results.append(rrf_results)
                logger.info(f"  ✅ {index_name}: {len(rrf_results)}건")
        
        # 2. user_id 기준 결합
        combined_results = combine_by_user_id(all_rrf_results)
        
        logger.info(f"  ✅ RRF 재결합 완료: {len(combined_results)}건")
        
        # 3. Redis 캐싱
        redis_client = get_redis_client()
        
        cache_results_to_redis(
            redis_client=redis_client,
            task_id=task_id,
            results=combined_results,
            ttl=3600
        )
        
        # 4. 최종 결과
        final_hits = combined_results[:size]
        results = format_search_results(final_hits)
        
        return {
            'status': 'completed',
            'task_id': task_id,
            'query': query,
            'total_hits': len(combined_results),
            'max_score': final_hits[0].get('_score', 0.0) if final_hits else 0.0,
            'results': results,
        }
        
    except Exception as exc:
        logger.error(f"❌ [{task_id}] RRF 재결합 실패: {exc}", exc_info=True)
        raise


# ==========================================
# 📌 유틸리티 함수들
# ==========================================
def get_search_clients() -> Tuple[OpenSearch, QdrantClient]:
    """OpenSearch & Qdrant 클라이언트 초기화 (Worker 캐시 반환으로 대체)"""
    # 💡 Worker 시작 시 _os_client, _qdrant_client가 생성되었다고 가정
    global _os_client, _qdrant_client
    
    if _os_client is None:
        # 초기화 실패 시 예외를 발생시켜 Task가 정상 종료되도록 함
        logger.error("Search clients not initialized. Worker setup failed.")
        # 🚨 여기서 RuntimeError를 발생시키지 않으면 Task 실패 로그가 명확하지 않을 수 있음
        raise RuntimeError("Search clients not initialized. Worker setup failed.")
    
    return _os_client, _qdrant_client


def get_all_survey_indices(os_client: OpenSearch) -> List[str]:
    """OpenSearch 인덱스 목록 조회"""
    try:
        indices_response = os_client.cat.indices(format='json')
        survey_indices = [
            idx['index'] for idx in indices_response
            if idx['index'].startswith('s_welcome') or idx['index'].startswith('survey_25')
        ]
        survey_indices.sort()
        return survey_indices
    except Exception as e:
        logger.error(f"❌ 인덱스 목록 조회 실패: {e}")
        return ['s_welcome_1st', 's_welcome_2nd']


def combine_by_user_id(rrf_results_list: List[List[Dict]]) -> List[Dict]:
    """user_id 기준 RRF 점수 합산"""
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
    redis_client: StrictRedis,
    task_id: str,
    results: List[Dict],
    ttl: int = 3600
):
    """Redis 페이징 캐시"""
    id_list_key = f"task:{task_id}:ids"
    user_ids = [extract_user_id(doc) for doc in results if extract_user_id(doc)]
    
    if user_ids:
        redis_client.delete(id_list_key)
        redis_client.rpush(id_list_key, *user_ids)
        redis_client.expire(id_list_key, ttl)
    
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
    """연령/성별 필터 확인"""
    filter_str = json.dumps(filter_dict, ensure_ascii=False)
    age_gender_keywords = [
        "metadata.age_group", "metadata.gender", "birth_year", "연령", "나이", "성별"
    ]
    return any(keyword in filter_str for keyword in age_gender_keywords)


def is_occupation_filter(filter_dict: Dict[str, Any]) -> bool:
    """직업 필터 확인"""
    filter_str = json.dumps(filter_dict, ensure_ascii=False)
    occupation_keywords = [
        "metadata.occupation", "occupation", "직업", "직무"
    ]
    return any(keyword in filter_str for keyword in occupation_keywords)


def remove_inner_hits(query_dict: Dict[str, Any]) -> Dict[str, Any]:
    """inner_hits 제거 (재귀)"""
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