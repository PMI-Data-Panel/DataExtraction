"""
OpenSearch Cloud Connector
클라우드 OpenSearch 연결 및 텍스트 검색 처리
CRAG (Corrective RAG) 기능 포함
"""

import os
import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
import json

from opensearchpy import OpenSearch, RequestsHttpConnection
from opensearchpy.helpers import bulk

try:
    import boto3
    from requests_aws4auth import AWS4Auth
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


@dataclass
class OpenSearchConfig:
    """OpenSearch Cloud 설정"""

    # AWS OpenSearch Service 설정
    AWS_REGION: str = os.getenv("AWS_REGION", "ap-northeast-2")
    AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID", "")
    AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")

    # OpenSearch 엔드포인트
    OPENSEARCH_ENDPOINT: str = os.getenv("OPENSEARCH_ENDPOINT", "")  # https://domain.region.es.amazonaws.com
    OPENSEARCH_PORT: int = int(os.getenv("OPENSEARCH_PORT", "9200"))
    OPENSEARCH_INDEX: str = os.getenv("OPENSEARCH_INDEX", "survey_responses")

    # 또는 일반 OpenSearch (비-AWS) 설정
    OPENSEARCH_HOST: str = os.getenv("OPENSEARCH_HOST", "")
    OPENSEARCH_USERNAME: str = os.getenv("OPENSEARCH_USERNAME", "admin")
    OPENSEARCH_PASSWORD: str = os.getenv("OPENSEARCH_PASSWORD", "admin")
    OPENSEARCH_USE_SSL: bool = os.getenv("OPENSEARCH_USE_SSL", "false").lower() == "true"

    # 검색 설정
    USE_KOREAN_ANALYZER: bool = True
    MAX_SEARCH_SIZE: int = 100

    def is_aws_service(self) -> bool:
        """AWS OpenSearch Service 사용 여부"""
        return bool(self.OPENSEARCH_ENDPOINT and "amazonaws.com" in self.OPENSEARCH_ENDPOINT)


class OpenSearchCloudConnector:
    """OpenSearch Cloud 연결 및 관리"""

    def __init__(self, config: OpenSearchConfig = None):
        """
        초기화

        Args:
            config: OpenSearch 설정 객체
        """
        self.config = config or OpenSearchConfig()
        self.client = self._create_client()
        self._verify_connection()
        logger.info("OpenSearch Cloud 연결 성공")

    def _create_client(self) -> OpenSearch:
        """OpenSearch 클라이언트 생성"""

        if self.config.is_aws_service():
            if not AWS_AVAILABLE:
                raise ImportError("AWS OpenSearch 사용을 위해 boto3와 requests-aws4auth를 설치하세요: pip install boto3 requests-aws4auth")

            # AWS OpenSearch Service 연결
            credentials = boto3.Session(
                aws_access_key_id=self.config.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=self.config.AWS_SECRET_ACCESS_KEY
            ).get_credentials()

            awsauth = AWS4Auth(
                credentials.access_key,
                credentials.secret_key,
                self.config.AWS_REGION,
                'es',
                session_token=credentials.token
            )

            # 엔드포인트에서 https:// 제거
            host = self.config.OPENSEARCH_ENDPOINT.replace('https://', '').replace('/', '')

            client = OpenSearch(
                hosts=[{'host': host, 'port': self.config.OPENSEARCH_PORT}],
                http_auth=awsauth,
                use_ssl=True,
                verify_certs=True,
                connection_class=RequestsHttpConnection,
                timeout=30,
                max_retries=3,
                retry_on_timeout=True
            )

            logger.info(f"AWS OpenSearch Service 연결: {host}")

        else:
            # 일반 OpenSearch 클러스터 연결 (온프레미스 또는 다른 클라우드)
            if self.config.OPENSEARCH_HOST:
                client = OpenSearch(
                    hosts=[{
                        'host': self.config.OPENSEARCH_HOST,
                        'port': self.config.OPENSEARCH_PORT
                    }],
                    http_auth=(self.config.OPENSEARCH_USERNAME, self.config.OPENSEARCH_PASSWORD),
                    use_ssl=self.config.OPENSEARCH_USE_SSL,
                    verify_certs=False,
                    ssl_show_warn=False,
                    timeout=30,
                    max_retries=3,
                    retry_on_timeout=True
                )
                logger.info(f"OpenSearch 연결: {self.config.OPENSEARCH_HOST}:{self.config.OPENSEARCH_PORT} (SSL: {self.config.OPENSEARCH_USE_SSL})")
            else:
                raise ValueError("OpenSearch 연결 정보가 없습니다. OPENSEARCH_ENDPOINT 또는 OPENSEARCH_HOST를 설정하세요.")

        return client

    def _verify_connection(self):
        """연결 확인"""
        try:
            info = self.client.info()
            logger.info(f"OpenSearch 버전: {info['version']['number']}")
        except Exception as e:
            logger.error(f"OpenSearch 연결 실패: {e}")
            raise

    def create_index(self, force_recreate: bool = False):
        """
        인덱스 생성 (Nested 구조 + 한국어 분석기)

        Args:
            force_recreate: 기존 인덱스 삭제 후 재생성 여부
        """
        index_name = self.config.OPENSEARCH_INDEX

        # 기존 인덱스 확인
        if self.client.indices.exists(index=index_name):
            if force_recreate:
                self.client.indices.delete(index=index_name)
                logger.info(f"기존 인덱스 {index_name} 삭제")
            else:
                logger.info(f"인덱스 {index_name} 이미 존재")
                return

        # 인덱스 설정
        index_body = {
            "settings": {
                "number_of_shards": 2,
                "number_of_replicas": 1,
                "analysis": {
                    "analyzer": {
                        "korean": {
                            "type": "custom",
                            "tokenizer": "nori_tokenizer",
                            "filter": ["lowercase", "nori_readingform"]
                        } if self.config.USE_KOREAN_ANALYZER else {
                            "type": "standard"
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "user_id": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "metadata": {
                        "type": "object",
                        "properties": {
                            "age_group": {"type": "keyword"},
                            "gender": {"type": "keyword"},
                            "region": {"type": "keyword"},
                            "occupation": {"type": "keyword"}
                        }
                    },
                    "qa_pairs": {
                        "type": "nested",
                        "properties": {
                            "q_code": {"type": "keyword"},
                            "q_text": {
                                "type": "text",
                                "analyzer": "korean" if self.config.USE_KOREAN_ANALYZER else "standard"
                            },
                            "q_type": {"type": "keyword"},
                            "answer_text": {
                                "type": "text",
                                "analyzer": "korean" if self.config.USE_KOREAN_ANALYZER else "standard",
                                "fields": {
                                    "keyword": {"type": "keyword"}  # 정확한 매칭용
                                }
                            },
                            "answer_code": {"type": "keyword"},
                            "confidence_score": {"type": "float"}  # CRAG용 신뢰도 점수
                        }
                    }
                }
            }
        }

        self.client.indices.create(index=index_name, body=index_body)
        logger.info(f"인덱스 {index_name} 생성 완료")

    def keyword_search(self,
                      query: str,
                      filters: Optional[Dict] = None,
                      size: int = 50) -> List[Dict]:
        """
        키워드 기반 텍스트 검색

        Args:
            query: 검색 쿼리
            filters: 추가 필터 조건
            size: 결과 개수

        Returns:
            검색 결과 리스트
        """
        # 기본 검색 쿼리 구성
        must_queries = []

        # Nested 쿼리로 텍스트 검색
        nested_query = {
            "nested": {
                "path": "qa_pairs",
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": [
                            "qa_pairs.answer_text^2",  # 답변 텍스트 가중치 높임
                            "qa_pairs.q_text"
                        ],
                        "type": "best_fields",
                        "operator": "or",
                        "minimum_should_match": "30%"
                    }
                },
                "score_mode": "sum",
                "inner_hits": {
                    "_source": ["qa_pairs.q_text", "qa_pairs.answer_text", "qa_pairs.confidence_score"],
                    "size": 5,
                    "highlight": {
                        "fields": {
                            "qa_pairs.answer_text": {
                                "fragment_size": 150,
                                "number_of_fragments": 3
                            }
                        }
                    }
                }
            }
        }
        must_queries.append(nested_query)

        # 필터 추가
        filter_queries = []
        if filters:
            for field, value in filters.items():
                if field in ["age_group", "gender", "region", "occupation"]:
                    filter_queries.append({"term": {f"metadata.{field}": value}})

        # 최종 쿼리
        search_body = {
            "query": {
                "bool": {
                    "must": must_queries,
                    "filter": filter_queries if filter_queries else []
                }
            },
            "size": size,
            "_source": ["user_id", "timestamp", "metadata"],
            "sort": ["_score", {"timestamp": "desc"}]
        }

        try:
            response = self.client.search(
                index=self.config.OPENSEARCH_INDEX,
                body=search_body
            )

            results = []
            for hit in response["hits"]["hits"]:
                result = {
                    "doc_id": hit["_id"],
                    "user_id": hit["_source"].get("user_id"),
                    "score": hit["_score"],
                    "metadata": hit["_source"].get("metadata", {}),
                    "matched_qa_pairs": self._extract_inner_hits(hit)
                }
                results.append(result)

            logger.info(f"OpenSearch 텍스트 검색 완료: {len(results)}개 결과")
            return results

        except Exception as e:
            logger.error(f"OpenSearch 검색 오류: {e}")
            return []

    def evaluate_retrieval_quality(self, results: List[Dict], query: str) -> str:
        """
        CRAG: 검색 품질 평가

        Args:
            results: 검색 결과
            query: 원본 쿼리

        Returns:
            "correct", "incorrect", "ambiguous" 중 하나
        """
        if not results:
            return "incorrect"

        # 상위 결과들의 점수 분석
        top_scores = [r["score"] for r in results[:5]]
        avg_score = sum(top_scores) / len(top_scores) if top_scores else 0

        # 신뢰도 임계값 (조정 가능)
        HIGH_CONFIDENCE = 5.0
        LOW_CONFIDENCE = 2.0

        if avg_score >= HIGH_CONFIDENCE:
            return "correct"
        elif avg_score <= LOW_CONFIDENCE:
            return "incorrect"
        else:
            return "ambiguous"

    def _extract_inner_hits(self, hit: Dict) -> List[Dict]:
        """Inner hits에서 매칭된 QA 쌍 추출"""
        matched_pairs = []

        if "inner_hits" in hit and "qa_pairs" in hit["inner_hits"]:
            for inner_hit in hit["inner_hits"]["qa_pairs"]["hits"]["hits"]:
                qa_pair = inner_hit["_source"]
                qa_pair["match_score"] = inner_hit["_score"]

                # 하이라이트 추가
                if "highlight" in inner_hit:
                    qa_pair["highlights"] = inner_hit["highlight"].get("qa_pairs.answer_text", [])

                matched_pairs.append(qa_pair)

        return matched_pairs

    def bulk_index(self, documents: List[Dict], batch_size: int = 100):
        """
        대량 문서 색인

        Args:
            documents: 색인할 문서 리스트
            batch_size: 배치 크기
        """
        actions = []
        for doc in documents:
            action = {
                "_index": self.config.OPENSEARCH_INDEX,
                "_id": doc.get("user_id"),
                "_source": doc
            }
            actions.append(action)

            if len(actions) >= batch_size:
                success, failed = bulk(self.client, actions, raise_on_error=False)
                logger.info(f"Bulk 색인: 성공 {success}, 실패 {len(failed)}")
                actions = []

        # 남은 문서 처리
        if actions:
            success, failed = bulk(self.client, actions, raise_on_error=False)
            logger.info(f"Bulk 색인: 성공 {success}, 실패 {len(failed)}")

    def get_index_stats(self) -> Dict:
        """인덱스 통계 조회"""
        try:
            stats = self.client.indices.stats(index=self.config.OPENSEARCH_INDEX)
            return {
                "total_docs": stats["_all"]["primaries"]["docs"]["count"],
                "index_size": stats["_all"]["primaries"]["store"]["size_in_bytes"],
                "segments": stats["_all"]["primaries"]["segments"]["count"]
            }
        except Exception as e:
            logger.error(f"통계 조회 오류: {e}")
            return {}

    def close(self):
        """연결 종료"""
        if self.client:
            self.client.close()
            logger.info("OpenSearch 연결 종료")


# 레거시 함수들 (기존 코드 호환성 유지)
def create_opensearch_client(
    host: str,
    port: int,
    user: str,
    password: str,
    use_ssl: bool = True,
    verify_certs: bool = False,
    timeout: int = 30
) -> OpenSearch:
    """레거시: OpenSearch 클라이언트 생성"""
    config = OpenSearchConfig()
    config.OPENSEARCH_HOST = host
    config.OPENSEARCH_PORT = port
    config.OPENSEARCH_USERNAME = user
    config.OPENSEARCH_PASSWORD = password

    connector = OpenSearchCloudConnector(config)
    return connector.client


# 사용 예시
if __name__ == "__main__":
    # 설정
    config = OpenSearchConfig()

    # 연결
    connector = OpenSearchCloudConnector(config)

    # 인덱스 생성
    connector.create_index()

    # 검색 테스트
    results = connector.keyword_search(
        query="30대 남성 직장인",
        filters={"region": "서울"},
        size=10
    )

    # CRAG 품질 평가
    quality = connector.evaluate_retrieval_quality(results, "30대 남성 직장인")
    print(f"검색 품질: {quality}")

    # 통계
    stats = connector.get_index_stats()
    print(f"인덱스 통계: {stats}")

    # 연결 종료
    connector.close()
