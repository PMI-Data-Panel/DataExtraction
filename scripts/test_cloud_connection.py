"""OpenSearch 연결 테스트 스크립트"""
import logging
import sys
import os

# 상위 디렉토리를 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connectors.opensearch_cloud import OpenSearchCloudConnector, OpenSearchConfig
from rag_query_analyzer.config import get_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_opensearch_connection():
    """OpenSearch 연결 테스트"""
    logger.info("="*60)
    logger.info("OpenSearch 연결 테스트")
    logger.info("="*60)

    try:
        config = get_config()

        logger.info("\n연결 정보:")
        logger.info(f"  - 호스트: {config.OPENSEARCH_HOST}")
        logger.info(f"  - 포트: {config.OPENSEARCH_PORT}")
        logger.info(f"  - 사용자: {config.OPENSEARCH_USER}")
        logger.info(f"  - SSL: {config.OPENSEARCH_USE_SSL}")

        # OpenSearchConfig 설정
        os_config = OpenSearchConfig()
        os_config.OPENSEARCH_HOST = config.OPENSEARCH_HOST
        os_config.OPENSEARCH_PORT = config.OPENSEARCH_PORT
        os_config.OPENSEARCH_USERNAME = config.OPENSEARCH_USER
        os_config.OPENSEARCH_PASSWORD = config.OPENSEARCH_PASSWORD

        # 클라이언트 생성
        logger.info("\nOpenSearch 커넥터 생성 중...")
        connector = OpenSearchCloudConnector(os_config)

        # Ping 테스트
        logger.info("\nPing 테스트...")
        if connector.client.ping():
            logger.info("[OK] Ping 성공")
        else:
            logger.error("[ERROR] Ping 실패")
            return False

        # 클러스터 정보
        logger.info("\n클러스터 정보:")
        info = connector.client.info()
        logger.info(f"  - 버전: {info['version']['number']}")
        logger.info(f"  - 클러스터명: {info['cluster_name']}")
        logger.info(f"  - 노드명: {info['name']}")

        # 인덱스 목록
        logger.info("\n인덱스 목록:")
        indices = connector.client.cat.indices(format="json")
        if indices:
            for idx in indices[:10]:  # 처음 10개만
                logger.info(f"  - {idx['index']}: {idx['docs.count']} docs, {idx['store.size']}")
        else:
            logger.info("  (인덱스 없음)")

        # 특정 인덱스 통계 (있는 경우)
        test_index = "s_welcome_2nd"
        if connector.client.indices.exists(index=test_index):
            logger.info(f"\n'{test_index}' 인덱스 통계:")

            # 수동으로 통계 조회
            stats = connector.client.indices.stats(index=test_index)
            count = connector.client.count(index=test_index)

            logger.info(f"  - 문서 개수: {count['count']}")
            logger.info(f"  - 크기: {round(stats['_all']['total']['store']['size_in_bytes'] / 1024 / 1024, 2)} MB")
            logger.info(f"  - 검색 횟수: {stats['_all']['total']['search']['query_total']}")

        # 연결 종료
        connector.close()

        logger.info("\n" + "="*60)
        logger.info("[OK] 모든 테스트 통과!")
        logger.info("="*60)

        return True

    except Exception as e:
        logger.error(f"\n[ERROR] 테스트 실패: {e}", exc_info=True)
        return False


def test_qdrant_connection():
    """Qdrant 연결 테스트"""
    logger.info("\n[INFO] Qdrant 테스트는 test_qdrant_connection.py를 사용하세요.")
    logger.info("  python scripts/test_qdrant_connection.py")
    return True


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="클라우드 연결 테스트")
    parser.add_argument("--service", choices=["opensearch", "qdrant", "all"], default="opensearch")

    args = parser.parse_args()

    success = True

    if args.service in ["opensearch", "all"]:
        success = success and test_opensearch_connection()

    if args.service in ["qdrant", "all"]:
        success = success and test_qdrant_connection()

    sys.exit(0 if success else 1)
