import asyncio
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


async def search_qdrant_async(
    qdrant_client,
    collection_name: str,
    query_vector: List[float],
    limit: int,
    score_threshold: float = 0.3,
    with_payload: bool = True,
    with_vectors: bool = False,
) -> List[Dict[str, Any]]:
    """Qdrant 검색을 비동기로 실행"""
    if not qdrant_client:
        logger.debug("⚠️ Qdrant 클라이언트가 없습니다")
        return []

    def _search():
        return qdrant_client.search(
            collection_name=collection_name,
            query_vector=query_vector,
            limit=limit,
            score_threshold=score_threshold,
            with_payload=with_payload,
            with_vectors=with_vectors,
        )

    try:
        results = await asyncio.to_thread(_search)
        formatted = [
            {
                '_id': str(item.id),
                '_score': item.score,
                '_source': item.payload if item.payload else {},
            }
            for item in results
        ]
        return formatted
    except Exception as e:
        logger.debug(f"⚠️ Qdrant async 검색 실패 ({collection_name}): {e}")
        return []


async def search_qdrant_collections_async(
    qdrant_client,
    collection_names: List[str],
    query_vector: List[float],
    limit: int,
    score_threshold: float = 0.3,
) -> Dict[str, List[Dict[str, Any]]]:
    """여러 컬렉션에 대해 병렬 Qdrant 검색"""
    tasks = [
        search_qdrant_async(
            qdrant_client=qdrant_client,
            collection_name=name,
            query_vector=query_vector,
            limit=limit,
            score_threshold=score_threshold,
        )
        for name in collection_names
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    formatted: Dict[str, List[Dict[str, Any]]] = {}
    for name, result in zip(collection_names, results):
        if isinstance(result, Exception):
            logger.debug(f"⚠️ Qdrant 컬렉션 검색 실패 ({name}): {result}")
            formatted[name] = []
        else:
            formatted[name] = result
    return formatted
