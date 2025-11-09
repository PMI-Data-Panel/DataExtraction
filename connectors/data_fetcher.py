"""클라우드 데이터 페처 - OpenSearch 및 Qdrant에서 데이터 조회"""
import logging
from typing import List, Dict, Any, Optional
from opensearchpy import OpenSearch, AsyncOpenSearch

logger = logging.getLogger(__name__)


class DataFetcher:
    """
    통합 데이터 페처

    OpenSearch, Qdrant 등 다양한 소스에서 데이터를 조회하는 통합 인터페이스
    """

    def __init__(
        self,
        opensearch_client: OpenSearch = None,
        qdrant_client=None,
        async_opensearch_client: Optional[AsyncOpenSearch] = None,
    ):
        """
        Args:
            opensearch_client: OpenSearch 클라이언트
            qdrant_client: Qdrant 클라이언트 (선택)
            async_opensearch_client: 비동기 OpenSearch 클라이언트 (선택)
        """
        self.os_client = opensearch_client
        self.os_async_client = async_opensearch_client
        self.qdrant_client = qdrant_client

    def search_opensearch(
        self,
        index_name: str,
        query: Dict[str, Any],
        size: int = 10,
        source_filter: Optional[Dict[str, Any]] = None,
        request_timeout: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        OpenSearch에서 검색

        Args:
            index_name: 인덱스 이름
            query: OpenSearch 쿼리 DSL
            size: 반환할 문서 개수
            source_filter: _source 필터링 (예: {"includes": ["user_id", "metadata"], "excludes": ["qa_pairs"]})

        Returns:
            검색 결과
        """
        if not self.os_client:
            raise ValueError("OpenSearch 클라이언트가 초기화되지 않았습니다")

        try:
            # 쿼리 디버깅
            import json
            logger.info(f"🔍 OpenSearch 쿼리:\n{json.dumps(query, indent=2, ensure_ascii=False)}")

            # _source 필터링 추가
            search_body = query.copy()
            if source_filter:
                search_body["_source"] = source_filter
                logger.debug(f"  📋 _source 필터링 적용: {source_filter}")

            response = self.os_client.search(
                index=index_name,
                body=search_body,
                size=size,
                request_timeout=request_timeout
            )

            logger.info(f"✅ OpenSearch 검색 완료: {response['hits']['total']['value']}건")
            return response

        except Exception as e:
            logger.error(f"❌ OpenSearch 검색 실패: {e}")
            raise

    async def search_opensearch_async(
        self,
        index_name: str,
        query: Dict[str, Any],
        size: int = 10,
        source_filter: Optional[Dict[str, Any]] = None,
        request_timeout: Optional[int] = None,
    ) -> Dict[str, Any]:
        """OpenSearch 비동기 검색"""
        if not self.os_async_client:
            raise ValueError("Async OpenSearch 클라이언트가 초기화되지 않았습니다")

        try:
            import json
            logger.info(f"🔍 [async] OpenSearch 쿼리:\n{json.dumps(query, indent=2, ensure_ascii=False)}")

            search_body = query.copy()
            if source_filter:
                search_body["_source"] = source_filter
                logger.debug(f"  📋 _source 필터링 적용 (async): {source_filter}")

            response = await self.os_async_client.search(
                index=index_name,
                body=search_body,
                size=size,
                request_timeout=request_timeout
            )

            hits_total = response.get('hits', {}).get('total', {}).get('value', 0)
            logger.info(f"✅ [async] OpenSearch 검색 완료: {hits_total}건")
            return response

        except Exception as e:
            logger.error(f"❌ [async] OpenSearch 검색 실패: {e}")
            raise

    async def get_document_by_id_async(
        self,
        index_name: str,
        doc_id: str,
        **kwargs
    ) -> Optional[Dict[str, Any]]:
        """ID로 문서 비동기 조회"""
        if not self.os_async_client:
            raise ValueError("Async OpenSearch 클라이언트가 초기화되지 않았습니다")

        try:
            response = await self.os_async_client.get(
                index=index_name,
                id=doc_id,
                **kwargs
            )
            if response.get('found'):
                return response.get('_source')
            return None
        except Exception as e:
            logger.warning(f"⚠️ [async] 문서 조회 실패 (ID: {doc_id}): {e}")
            return None

    async def multi_get_documents_async(
        self,
        index_name: str,
        doc_ids: List[str],
        batch_size: int = 200,
        request_timeout: int = 60
    ) -> List[Dict[str, Any]]:
        """비동기 문서 일괄 조회 (배치) -> raw docs 리스트 반환"""
        if not self.os_async_client:
            raise ValueError("Async OpenSearch 클라이언트가 초기화되지 않았습니다")

        if not doc_ids:
            return []

        results: List[Dict[str, Any]] = []
        total_batches = (len(doc_ids) + batch_size - 1) // batch_size
        for batch_idx in range(0, len(doc_ids), batch_size):
            batch_ids = doc_ids[batch_idx:batch_idx + batch_size]
            batch_num = (batch_idx // batch_size) + 1
            mget_body = [{"_index": index_name, "_id": uid} for uid in batch_ids]
            try:
                response = await self.os_async_client.mget(
                    body={"docs": mget_body},
                    ignore=[404],
                    request_timeout=request_timeout
                )
                docs = response.get('docs', [])
                found = sum(1 for item in docs if item.get('found'))
                results.extend(docs)
                logger.debug(f"  📦 [async] {index_name} 배치 {batch_num}/{total_batches}: {found}/{len(batch_ids)}건")
            except Exception as e:
                logger.warning(f"  ⚠️ [async] {index_name} 배치 {batch_num}/{total_batches} 실패: {e}")
                continue
        logger.info(f"  ✅ [async] {index_name} 배치 조회 완료: {len(results)}/{len(doc_ids)}건 (raw docs)")
        return results

    @staticmethod
    def docs_to_user_map(docs: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """mget 결과를 user_id -> source dict로 변환"""
        result = {}
        for doc in docs or []:
            if doc.get('found'):
                result[doc['_id']] = doc.get('_source', {})
        return result

    def get_document_by_id(
        self,
        index_name: str,
        doc_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        ID로 문서 조회

        Args:
            index_name: 인덱스 이름
            doc_id: 문서 ID

        Returns:
            문서 데이터 또는 None
        """
        if not self.os_client:
            raise ValueError("OpenSearch 클라이언트가 초기화되지 않았습니다")

        try:
            response = self.os_client.get(
                index=index_name,
                id=doc_id
            )
            return response['_source']

        except Exception as e:
            logger.warning(f"⚠️ 문서 조회 실패 (ID: {doc_id}): {e}")
            return None

    def multi_get_documents(
        self,
        index_name: str,
        doc_ids: List[str]
    ) -> List[Dict[str, Any]]:
        """
        여러 문서 일괄 조회

        Args:
            index_name: 인덱스 이름
            doc_ids: 문서 ID 리스트

        Returns:
            문서 리스트
        """
        if not self.os_client:
            raise ValueError("OpenSearch 클라이언트가 초기화되지 않았습니다")

        try:
            response = self.os_client.mget(
                index=index_name,
                body={"ids": doc_ids}
            )

            documents = []
            for doc in response['docs']:
                if doc.get('found'):
                    documents.append(doc['_source'])

            logger.info(f"✅ 문서 일괄 조회 완료: {len(documents)}/{len(doc_ids)}건")
            return documents

        except Exception as e:
            logger.error(f"❌ 문서 일괄 조회 실패: {e}")
            raise

    def scroll_search(
        self,
        index_name: str,
        query: Dict[str, Any],
        batch_size: int = 100,
        scroll_time: str = "2m"
    ):
        """
        대량 데이터 스크롤 검색 (제너레이터)

        Args:
            index_name: 인덱스 이름
            query: OpenSearch 쿼리 DSL
            batch_size: 배치 크기
            scroll_time: 스크롤 유지 시간

        Yields:
            문서 배치
        """
        if not self.os_client:
            raise ValueError("OpenSearch 클라이언트가 초기화되지 않았습니다")

        try:
            # 초기 검색
            response = self.os_client.search(
                index=index_name,
                body=query,
                scroll=scroll_time,
                size=batch_size
            )

            scroll_id = response['_scroll_id']
            hits = response['hits']['hits']

            while hits:
                yield [hit['_source'] for hit in hits]

                # 다음 배치
                response = self.os_client.scroll(
                    scroll_id=scroll_id,
                    scroll=scroll_time
                )
                scroll_id = response['_scroll_id']
                hits = response['hits']['hits']

            # 스크롤 정리
            self.os_client.clear_scroll(scroll_id=scroll_id)

        except Exception as e:
            logger.error(f"❌ 스크롤 검색 실패: {e}")
            raise

    def aggregate_data(
        self,
        index_name: str,
        query: Dict[str, Any],
        aggregations: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        집계 쿼리 실행

        Args:
            index_name: 인덱스 이름
            query: 필터 쿼리
            aggregations: 집계 정의

        Returns:
            집계 결과
        """
        if not self.os_client:
            raise ValueError("OpenSearch 클라이언트가 초기화되지 않았습니다")

        try:
            query_body = query.copy()
            query_body["aggs"] = aggregations
            query_body["size"] = 0  # 문서는 반환하지 않음

            response = self.os_client.search(
                index=index_name,
                body=query_body
            )

            logger.info(f"✅ 집계 쿼리 완료")
            return response.get('aggregations', {})

        except Exception as e:
            logger.error(f"❌ 집계 쿼리 실패: {e}")
            raise

    # Qdrant 메서드 (향후 확장)
    def search_qdrant(self, collection_name: str, vector: List[float], limit: int = 10):
        """
        Qdrant에서 벡터 검색 (플레이스홀더)

        Args:
            collection_name: 컬렉션 이름
            vector: 쿼리 벡터
            limit: 반환할 결과 개수

        Returns:
            검색 결과
        """
        if not self.qdrant_client:
            raise NotImplementedError("Qdrant 클라이언트가 구현되지 않았습니다")

        # TODO: Qdrant 검색 구현
        logger.warning("⚠️ Qdrant 검색은 아직 구현되지 않았습니다")
        return []
