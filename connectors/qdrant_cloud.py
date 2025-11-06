"""
Qdrant Cloud Connector
클라우드 Qdrant 연결 및 벡터 검색 처리
"""

import os
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance, VectorParams, PointStruct,
    Filter, FieldCondition, MatchValue,
    SearchRequest, QueryResponse
)
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


@dataclass
class QdrantConfig:
    """Qdrant Cloud 설정"""

    # Qdrant 연결 정보
    QDRANT_HOST: str = os.getenv("QDRANT_HOST", "104.248.144.17")
    QDRANT_PORT: int = int(os.getenv("QDRANT_PORT", "6333"))
    QDRANT_API_KEY: Optional[str] = os.getenv("QDRANT_API_KEY", None)  # 보안 없으면 None
    QDRANT_COLLECTION: str = os.getenv("QDRANT_COLLECTION", "survey_vectors")

    # 벡터 설정
    VECTOR_SIZE: int = 1024  # KURE-v1 차원
    DISTANCE_METRIC: str = "Cosine"  # Cosine, Euclidean, Dot

    # 검색 설정
    SEARCH_LIMIT: int = 50
    SCORE_THRESHOLD: float = 0.7


class QdrantCloudConnector:
    """Qdrant Cloud 연결 및 관리"""

    def __init__(self, config: QdrantConfig = None):
        """
        초기화

        Args:
            config: Qdrant 설정 객체
        """
        self.config = config or QdrantConfig()
        self.client = self._create_client()
        self._verify_connection()
        logger.info(f"Qdrant 연결 성공: {self.config.QDRANT_HOST}:{self.config.QDRANT_PORT}")

    def _create_client(self) -> QdrantClient:
        """Qdrant 클라이언트 생성"""

        # API 키가 있으면 사용, 없으면 None
        if self.config.QDRANT_API_KEY:
            client = QdrantClient(
                host=self.config.QDRANT_HOST,
                port=self.config.QDRANT_PORT,
                api_key=self.config.QDRANT_API_KEY,
                timeout=30
            )
            logger.info("Qdrant 클라이언트 생성 (API Key 인증)")
        else:
            # 보안 없음
            client = QdrantClient(
                host=self.config.QDRANT_HOST,
                port=self.config.QDRANT_PORT,
                timeout=30
            )
            logger.info("Qdrant 클라이언트 생성 (인증 없음)")

        return client

    def _verify_connection(self):
        """연결 확인"""
        try:
            collections = self.client.get_collections()
            logger.info(f"Qdrant 컬렉션 개수: {len(collections.collections)}")
        except Exception as e:
            logger.error(f"Qdrant 연결 실패: {e}")
            raise

    def create_collection(self, force_recreate: bool = False):
        """
        컬렉션 생성

        Args:
            force_recreate: 기존 컬렉션 삭제 후 재생성 여부
        """
        collection_name = self.config.QDRANT_COLLECTION

        # 기존 컬렉션 확인
        collections = self.client.get_collections().collections
        collection_exists = any(c.name == collection_name for c in collections)

        if collection_exists:
            if force_recreate:
                self.client.delete_collection(collection_name=collection_name)
                logger.info(f"기존 컬렉션 {collection_name} 삭제")
            else:
                logger.info(f"컬렉션 {collection_name} 이미 존재")
                return

        # 거리 메트릭 설정
        distance_map = {
            "Cosine": Distance.COSINE,
            "Euclidean": Distance.EUCLID,
            "Dot": Distance.DOT
        }
        distance = distance_map.get(self.config.DISTANCE_METRIC, Distance.COSINE)

        # 컬렉션 생성
        self.client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(
                size=self.config.VECTOR_SIZE,
                distance=distance
            )
        )

        logger.info(f"Qdrant 컬렉션 {collection_name} 생성 완료 (차원: {self.config.VECTOR_SIZE}, 거리: {self.config.DISTANCE_METRIC})")

    def upsert_vectors(self,
                      vectors: List[List[float]],
                      payloads: List[Dict[str, Any]],
                      ids: Optional[List[str]] = None):
        """
        벡터 업로드 (upsert)

        Args:
            vectors: 벡터 리스트
            payloads: 메타데이터 리스트
            ids: 문서 ID 리스트 (None이면 자동 생성)
        """
        if len(vectors) != len(payloads):
            raise ValueError("벡터와 payload 개수가 일치하지 않습니다")

        # ID가 없으면 자동 생성
        if ids is None:
            ids = [f"doc_{i}" for i in range(len(vectors))]

        # PointStruct 생성
        points = [
            PointStruct(
                id=idx if isinstance(idx, int) else hash(idx) % (2**63),  # str을 int로 변환
                vector=vec,
                payload=payload
            )
            for idx, vec, payload in zip(ids, vectors, payloads)
        ]

        # 업로드
        self.client.upsert(
            collection_name=self.config.QDRANT_COLLECTION,
            points=points
        )

        logger.info(f"Qdrant 벡터 업로드 완료: {len(points)}개")

    def search(self,
              query_vector: List[float],
              limit: int = None,
              score_threshold: float = None,
              filters: Optional[Dict[str, Any]] = None) -> List[Dict]:
        """
        벡터 유사도 검색

        Args:
            query_vector: 쿼리 벡터
            limit: 결과 개수
            score_threshold: 최소 유사도 점수
            filters: 필터 조건 (예: {"age_group": "20대", "gender": "남성"})

        Returns:
            검색 결과 리스트
        """
        if limit is None:
            limit = self.config.SEARCH_LIMIT

        if score_threshold is None:
            score_threshold = self.config.SCORE_THRESHOLD

        # 필터 구성
        query_filter = None
        if filters:
            must_conditions = []
            for key, value in filters.items():
                must_conditions.append(
                    FieldCondition(
                        key=f"metadata.{key}",
                        match=MatchValue(value=value)
                    )
                )

            if must_conditions:
                query_filter = Filter(must=must_conditions)

        # 검색 실행
        results = self.client.search(
            collection_name=self.config.QDRANT_COLLECTION,
            query_vector=query_vector,
            limit=limit,
            score_threshold=score_threshold,
            query_filter=query_filter,
            with_payload=True,
            with_vectors=False
        )

        # 결과 포매팅
        formatted_results = []
        for result in results:
            formatted_results.append({
                "id": result.id,
                "score": result.score,
                "payload": result.payload
            })

        logger.info(f"Qdrant 검색 완료: {len(formatted_results)}개 결과 (임계값: {score_threshold})")
        return formatted_results

    def batch_search(self,
                    query_vectors: List[List[float]],
                    limit: int = None,
                    score_threshold: float = None) -> List[List[Dict]]:
        """
        배치 벡터 검색

        Args:
            query_vectors: 쿼리 벡터 리스트
            limit: 결과 개수
            score_threshold: 최소 유사도 점수

        Returns:
            검색 결과 리스트의 리스트
        """
        if limit is None:
            limit = self.config.SEARCH_LIMIT

        if score_threshold is None:
            score_threshold = self.config.SCORE_THRESHOLD

        # 배치 검색 요청 생성
        search_requests = [
            SearchRequest(
                vector=vec,
                limit=limit,
                score_threshold=score_threshold,
                with_payload=True
            )
            for vec in query_vectors
        ]

        # 배치 검색 실행
        results = self.client.search_batch(
            collection_name=self.config.QDRANT_COLLECTION,
            requests=search_requests
        )

        # 결과 포매팅
        all_results = []
        for batch_result in results:
            formatted_batch = [
                {
                    "id": r.id,
                    "score": r.score,
                    "payload": r.payload
                }
                for r in batch_result
            ]
            all_results.append(formatted_batch)

        logger.info(f"Qdrant 배치 검색 완료: {len(query_vectors)}개 쿼리")
        return all_results

    def get_collection_info(self) -> Dict:
        """컬렉션 정보 조회"""
        try:
            info = self.client.get_collection(collection_name=self.config.QDRANT_COLLECTION)
            return {
                "name": self.config.QDRANT_COLLECTION,
                "vectors_count": info.vectors_count,
                "points_count": info.points_count,
                "segments_count": info.segments_count,
                "status": info.status,
                "optimizer_status": info.optimizer_status,
                "vector_size": info.config.params.vectors.size,
                "distance": info.config.params.vectors.distance
            }
        except Exception as e:
            logger.error(f"컬렉션 정보 조회 오류: {e}")
            return {}

    def delete_collection(self):
        """컬렉션 삭제"""
        try:
            self.client.delete_collection(collection_name=self.config.QDRANT_COLLECTION)
            logger.info(f"컬렉션 {self.config.QDRANT_COLLECTION} 삭제 완료")
        except Exception as e:
            logger.error(f"컬렉션 삭제 오류: {e}")
            raise

    def close(self):
        """연결 종료"""
        if self.client:
            self.client.close()
            logger.info("Qdrant 연결 종료")


# 레거시 함수 (기존 코드 호환성)
def create_qdrant_client(
    host: str = "104.248.144.17",
    port: int = 6333,
    api_key: Optional[str] = None
) -> QdrantClient:
    """레거시: Qdrant 클라이언트 생성"""
    config = QdrantConfig()
    config.QDRANT_HOST = host
    config.QDRANT_PORT = port
    config.QDRANT_API_KEY = api_key

    connector = QdrantCloudConnector(config)
    return connector.client


# 사용 예시
if __name__ == "__main__":
    # 설정
    config = QdrantConfig()

    # 연결
    connector = QdrantCloudConnector(config)

    # 컬렉션 생성
    connector.create_collection()

    # 벡터 업로드 예시
    sample_vectors = [
        [0.1] * 1024,  # 더미 벡터
        [0.2] * 1024
    ]
    sample_payloads = [
        {"user_id": "user1", "metadata": {"age_group": "20대", "gender": "남성"}},
        {"user_id": "user2", "metadata": {"age_group": "30대", "gender": "여성"}}
    ]

    # connector.upsert_vectors(sample_vectors, sample_payloads)

    # 검색 테스트
    # query_vec = [0.15] * 1024
    # results = connector.search(
    #     query_vector=query_vec,
    #     limit=10,
    #     filters={"age_group": "20대"}
    # )

    # 컬렉션 정보
    info = connector.get_collection_info()
    print(f"컬렉션 정보: {info}")

    # 연결 종료
    connector.close()
