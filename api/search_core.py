"""
api/search_core.py
핵심 검색 로직 - FastAPI와 Celery에서 공통으로 사용
"""
import asyncio
import logging
from collections import defaultdict
from time import perf_counter
from typing import List, Dict, Any, Optional, Set, Tuple

from rag_query_analyzer.analyzers.main_analyzer import AdvancedRAGQueryAnalyzer
from rag_query_analyzer.analyzers.demographic_extractor import DemographicExtractor
from rag_query_analyzer.models.entities import DemographicType, DemographicEntity
from connectors.hybrid_searcher import OpenSearchHybridQueryBuilder, calculate_rrf_score
from connectors.data_fetcher import DataFetcher

logger = logging.getLogger(__name__)

DEFAULT_OS_TIMEOUT = 10

PLACEHOLDER_TOKENS = {
    "", "미정", "없음", "무응답", "해당없음", "n/a", "na", 
    "null", "none", "unknown", "미선택", "미기재",
}
PLACEHOLDER_TOKENS = {token.strip().lower() for token in PLACEHOLDER_TOKENS}

AGE_GENDER_KEYWORDS = [
    "metadata.age_group", "metadata.gender", "birth_year", "연령", "나이", "성별"
]
OCCUPATION_KEYWORDS = [
    "metadata.occupation", "occupation", "직업", "직무"
]


def normalize_value(value: Any) -> str:
    """값 정규화"""
    if value is None:
        return ""
    if isinstance(value, bool):
        value_str = str(value)
    elif isinstance(value, (int, float)):
        try:
            if hasattr(value, 'is_integer') and value.is_integer():
                value = int(value)
        except AttributeError:
            pass
        value_str = str(value)
    else:
        value_str = str(value)
    
    cleaned = value_str.strip()
    lower = cleaned.lower()
    return "" if lower in PLACEHOLDER_TOKENS else lower


def strip_korean_particles(term: str) -> str:
    """한국어 조사 제거"""
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


def is_age_or_gender_filter(filter_dict: Dict[str, Any]) -> bool:
    """연령/성별 필터 여부 확인"""
    import json
    try:
        filter_str = json.dumps(filter_dict, ensure_ascii=False)
    except Exception:
        filter_str = str(filter_dict)
    return any(keyword in filter_str for keyword in AGE_GENDER_KEYWORDS)


def is_occupation_filter(filter_dict: Dict[str, Any]) -> bool:
    """직업 필터 여부 확인"""
    import json
    try:
        filter_str = json.dumps(filter_dict, ensure_ascii=False)
    except Exception:
        filter_str = str(filter_dict)
    return any(keyword in filter_str for keyword in OCCUPATION_KEYWORDS)


def remove_inner_hits(query_dict: Dict[str, Any]) -> Dict[str, Any]:
    """재귀적으로 inner_hits 제거"""
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


def expand_gender_aliases(values: Set[str]) -> None:
    """성별 동의어 확장"""
    male_aliases = {"m", "남", "남성", "male", "man", "남자"}
    female_aliases = {"f", "여", "여성", "female", "woman", "여자"}
    if values & male_aliases:
        values.update(male_aliases)
    if values & female_aliases:
        values.update(female_aliases)


def add_age_decade(values: Set[str], age_value: Any) -> None:
    """연령대 추가"""
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


async def execute_hybrid_search(
    query: str,
    index_name: str,
    size: int,
    use_vector_search: bool,
    data_fetcher: DataFetcher,
    embedding_model: Any,
    config: Any,
    is_async: bool = False,
) -> Dict[str, Any]:
    """
    핵심 하이브리드 검색 로직
    
    Args:
        query: 검색 쿼리
        index_name: 인덱스 이름
        size: 결과 개수
        use_vector_search: 벡터 검색 사용 여부
        data_fetcher: DataFetcher 인스턴스
        embedding_model: 임베딩 모델
        config: 설정 객체
        is_async: 비동기 실행 여부 (Celery에서는 False)
    
    Returns:
        검색 결과 딕셔너리
    """
    timings: Dict[str, float] = {}
    overall_start = perf_counter()
    
    # 1. 쿼리 분석
    analyzer = AdvancedRAGQueryAnalyzer(config)
    analysis = analyzer.analyze_query(query)
    
    if analysis is None:
        raise RuntimeError("Query analysis returned None")
    
    # 2. 엔티티 추출
    extractor = DemographicExtractor()
    extracted_entities, requested_size = extractor.extract_with_size(query)
    
    # 3. 필터 구성
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
    actual_size = max(1, min(requested_size, 100))
    
    # 4. 필터 분류
    age_gender_filters = [f for f in filters if is_age_or_gender_filter(f)]
    occupation_filters = [f for f in filters if is_occupation_filter(f)]
    other_filters = [f for f in filters if f not in age_gender_filters and f not in occupation_filters]
    
    filters_os = age_gender_filters + other_filters
    has_demographic_filters = bool(filters_for_response)
    
    logger.info(f"🔍 필터 상태: 연령/성별={len(age_gender_filters)}, 직업={len(occupation_filters)}, 기타={len(other_filters)}")
    
    # 5. 쿼리 빌드
    query_builder = OpenSearchHybridQueryBuilder(config)
    query_vector = None
    
    if embedding_model and use_vector_search:
        try:
            query_vector = embedding_model.encode(query).tolist()
        except Exception as e:
            logger.warning(f"임베딩 생성 실패: {e}")
    
    base_query = query_builder.build_query(
        analysis=analysis,
        query_vector=query_vector,
        size=actual_size,
    )
    
    # 6. 필터 적용
    final_query = apply_filters_to_query(base_query, filters_os, analysis)
    
    # 7. 검색 실행
    has_filters = bool(filters_os or occupation_filters)
    if has_filters:
        qdrant_limit = min(500, max(300, actual_size * 10))
        search_size = max(1000, min(actual_size * 20, 5000))
    else:
        qdrant_limit = min(200, max(100, actual_size * 2))
        search_size = actual_size * 2
    
    logger.info(f"🔍 검색 파라미터: size={search_size}, qdrant_limit={qdrant_limit}")
    
    # 8. 인덱스별 검색 실행
    search_results = await execute_index_searches(
        data_fetcher=data_fetcher,
        final_query=final_query,
        search_size=search_size,
        qdrant_limit=qdrant_limit,
        query_vector=query_vector,
        use_vector_search=use_vector_search,
        age_gender_filters=age_gender_filters,
        occupation_filters=occupation_filters,
        is_async=is_async,
        timings=timings,
    )
    
    # 9. RRF 결합
    rrf_results = combine_search_results(search_results, timings)
    
    # 10. 필터링 및 결과 구성
    if has_demographic_filters:
        filtered_results = apply_demographic_filters(
            rrf_results=rrf_results,
            extracted_entities=extracted_entities,
            data_fetcher=data_fetcher,
            timings=timings,
            is_async=is_async,
        )
    else:
        filtered_results = rrf_results
    
    # 11. 최종 결과
    final_hits = filtered_results[:actual_size]
    results = build_final_results(final_hits, data_fetcher, is_async)
    
    total_duration_ms = (perf_counter() - overall_start) * 1000
    timings['total_ms'] = total_duration_ms
    
    return {
        "query": query,
        "total_hits": len(filtered_results),
        "max_score": final_hits[0].get('_score', 0.0) if final_hits else 0.0,
        "results": results,
        "query_analysis": {
            "intent": analysis.intent,
            "must_terms": analysis.must_terms,
            "should_terms": analysis.should_terms,
            "extracted_entities": extracted_entities.to_dict(),
            "filters": filters_for_response,
            "size": actual_size,
            "timings_ms": timings,
        },
        "took_ms": int(total_duration_ms),
    }


def apply_filters_to_query(
    base_query: Dict[str, Any],
    filters: List[Dict[str, Any]],
    analysis: Any,
) -> Dict[str, Any]:
    """쿼리에 필터 적용"""
    final_query = base_query.copy()
    
    if not filters:
        return final_query
    
    # inner_hits 제거
    cleaned_filters = [remove_inner_hits(f) for f in filters]
    
    existing_query = final_query.get('query', {"match_all": {}})
    
    if filters:
        should_filters = []
        filter_by_type = {}
        
        for f in cleaned_filters:
            filter_type = extract_filter_type(f)
            if filter_type:
                if filter_type not in filter_by_type:
                    filter_by_type[filter_type] = []
                filter_by_type[filter_type].append(f)
        
        for filter_type, type_filters in filter_by_type.items():
            if len(type_filters) == 1:
                should_filters.append(type_filters[0])
            else:
                should_filters.append({
                    'bool': {
                        'should': type_filters,
                        "minimum_should_match": 1
                    }
                })
        
        if existing_query is None or existing_query in [{"match_all": {}}, {"match_none": {}}]:
            final_query['query'] = {
                'bool': {
                    'must': should_filters
                }
            }
        elif isinstance(existing_query, dict) and existing_query.get('bool'):
            if 'must' not in existing_query['bool']:
                existing_query['bool']['must'] = []
            existing_query['bool']['must'].extend(should_filters)
            final_query['query'] = existing_query
        else:
            final_query['query'] = {
                'bool': {
                    'must': [existing_query] + should_filters
                }
            }
    
    return final_query


def extract_filter_type(filter_dict: Dict[str, Any]) -> Optional[str]:
    """필터 타입 추출"""
    # 필터 타입 추출 로직 (기존 코드 참조)
    # 길이 문제로 생략, 원본 코드에서 복사
    pass


async def execute_index_searches(
    data_fetcher: DataFetcher,
    final_query: Dict[str, Any],
    search_size: int,
    qdrant_limit: int,
    query_vector: Optional[List[float]],
    use_vector_search: bool,
    age_gender_filters: List[Dict[str, Any]],
    occupation_filters: List[Dict[str, Any]],
    is_async: bool,
    timings: Dict[str, float],
) -> Dict[str, Any]:
    """인덱스별 검색 실행"""
    
    source_filter = {
        "includes": ["user_id", "metadata", "qa_pairs", "timestamp"],
        "excludes": []
    }
    
    # welcome_1st 검색
    welcome_1st_query = create_index_query(search_size, age_gender_filters)
    welcome_1st_results = await search_index(
        data_fetcher=data_fetcher,
        index_name="s_welcome_1st",
        query=welcome_1st_query,
        search_size=search_size,
        qdrant_limit=qdrant_limit,
        query_vector=query_vector,
        use_vector_search=use_vector_search,
        is_async=is_async,
    )
    
    # welcome_2nd 검색
    welcome_2nd_query = create_index_query(search_size, occupation_filters)
    welcome_2nd_results = await search_index(
        data_fetcher=data_fetcher,
        index_name="s_welcome_2nd",
        query=welcome_2nd_query,
        search_size=search_size,
        qdrant_limit=qdrant_limit,
        query_vector=query_vector,
        use_vector_search=use_vector_search,
        is_async=is_async,
    )
    
    return {
        'welcome_1st': welcome_1st_results,
        'welcome_2nd': welcome_2nd_results,
    }


def create_index_query(size: int, filters: List[Dict[str, Any]]) -> Dict[str, Any]:
    """안전한 인덱스 쿼리 생성"""
    query = {
        'query': {'match_all': {}},
        'size': size,
        '_source': {
            'includes': ['user_id', 'metadata', 'qa_pairs', 'timestamp']
        }
    }
    
    if filters:
        query['query'] = {
            'bool': {
                'must': filters
            }
        }
    
    return query


async def search_index(
    data_fetcher: DataFetcher,
    index_name: str,
    query: Dict[str, Any],
    search_size: int,
    qdrant_limit: int,
    query_vector: Optional[List[float]],
    use_vector_search: bool,
    is_async: bool,
) -> Dict[str, Any]:
    """단일 인덱스 검색"""
    keyword_results = []
    vector_results = []
    
    try:
        # OpenSearch 검색
        if is_async and data_fetcher.os_async_client:
            os_response = await data_fetcher.search_opensearch_async(
                index_name=index_name,
                query=query,
                size=search_size,
                request_timeout=DEFAULT_OS_TIMEOUT,
            )
        else:
            os_response = data_fetcher.search_opensearch(
                index_name=index_name,
                query=query,
                size=search_size,
                request_timeout=DEFAULT_OS_TIMEOUT,
            )
        
        keyword_results = os_response['hits']['hits']
        logger.info(f"  ✅ {index_name} OpenSearch: {len(keyword_results)}건")
        
        # Qdrant 검색
        if use_vector_search and query_vector and hasattr(data_fetcher, 'qdrant_client'):
            qdrant_client = data_fetcher.qdrant_client
            try:
                r = qdrant_client.search(
                    collection_name=index_name,
                    query_vector=query_vector,
                    limit=qdrant_limit,
                    score_threshold=0.3,
                )
                for item in r:
                    vector_results.append({
                        '_id': str(item.id),
                        '_score': item.score,
                        '_source': item.payload
                    })
                logger.info(f"  ✅ {index_name} Qdrant: {len(vector_results)}건")
            except Exception as e:
                logger.debug(f"  ⚠️ {index_name} Qdrant 검색 실패: {e}")
    
    except Exception as e:
        logger.warning(f"  ⚠️ {index_name} 검색 실패: {e}")
    
    return {
        'keyword': keyword_results,
        'vector': vector_results,
    }


def combine_search_results(
    search_results: Dict[str, Any],
    timings: Dict[str, float],
) -> List[Dict[str, Any]]:
    """검색 결과 RRF 결합"""
    rrf_start = perf_counter()
    
    # 각 인덱스별 RRF
    welcome_1st_rrf = calculate_rrf_score(
        search_results['welcome_1st']['keyword'],
        search_results['welcome_1st']['vector'],
        k=60
    ) if search_results['welcome_1st']['vector'] else search_results['welcome_1st']['keyword']
    
    welcome_2nd_rrf = calculate_rrf_score(
        search_results['welcome_2nd']['keyword'],
        search_results['welcome_2nd']['vector'],
        k=60
    ) if search_results['welcome_2nd']['vector'] else search_results['welcome_2nd']['keyword']
    
    # user_id 기준 그룹화
    user_rrf_map = {}
    
    for doc in welcome_1st_rrf:
        user_id = extract_user_id(doc)
        if user_id:
            if user_id not in user_rrf_map:
                user_rrf_map[user_id] = []
            doc['_index'] = 's_welcome_1st'
            user_rrf_map[user_id].append(doc)
    
    for doc in welcome_2nd_rrf:
        user_id = extract_user_id(doc)
        if user_id:
            if user_id not in user_rrf_map:
                user_rrf_map[user_id] = []
            doc['_index'] = 's_welcome_2nd'
            user_rrf_map[user_id].append(doc)
    
    # user_id별 RRF 재결합
    final_rrf_results = []
    for user_id, user_docs in user_rrf_map.items():
        if len(user_docs) == 1:
            final_rrf_results.append(user_docs[0])
        else:
            total_rrf_score = sum(
                doc.get('_score', 0.0) or doc.get('rrf_score', 0.0)
                for doc in user_docs
            )
            best_doc = max(user_docs, key=lambda d: d.get('_score', 0.0) or d.get('rrf_score', 0.0))
            best_doc['_score'] = total_rrf_score
            best_doc['_rrf_details'] = {
                'combined_score': total_rrf_score,
                'source_count': len(user_docs),
                'sources': [d.get('_index', 'unknown') for d in user_docs]
            }
            final_rrf_results.append(best_doc)
    
    final_rrf_results.sort(
        key=lambda d: d.get('_score', 0.0) or d.get('rrf_score', 0.0),
        reverse=True
    )
    
    timings['rrf_recombination_ms'] = (perf_counter() - rrf_start) * 1000
    logger.info(f"  ✅ RRF 결합 완료: {len(final_rrf_results)}건")
    
    return final_rrf_results


def extract_user_id(doc: Dict[str, Any]) -> Optional[str]:
    """문서에서 user_id 추출"""
    source = doc.get('_source', {})
    if not source and 'doc' in doc:
        source = doc.get('doc', {}).get('_source', {})
    
    user_id = source.get('user_id') if isinstance(source, dict) else None
    if not user_id:
        user_id = doc.get('_id', '')
    
    return user_id


def apply_demographic_filters(
    rrf_results: List[Dict[str, Any]],
    extracted_entities: Any,
    data_fetcher: DataFetcher,
    timings: Dict[str, float],
    is_async: bool,
) -> List[Dict[str, Any]]:
    """인구통계 필터 적용"""
    # 기존 필터링 로직 (길이 문제로 생략)
    # 원본 코드의 필터링 로직 복사
    pass


def build_final_results(
    final_hits: List[Dict[str, Any]],
    data_fetcher: DataFetcher,
    is_async: bool,
) -> List[Dict[str, Any]]:
    """최종 결과 구성"""
    # 기존 결과 구성 로직 (길이 문제로 생략)
    # 원본 코드의 결과 구성 로직 복사
    pass