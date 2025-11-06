"""Qdrant 연결 테스트 스크립트"""
import sys
import os
import logging

# 상위 디렉토리를 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connectors.qdrant_cloud import QdrantCloudConnector, QdrantConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_qdrant_connection():
    """Qdrant 연결 및 기본 작업 테스트"""

    logger.info("="*60)
    logger.info("Qdrant 연결 테스트 시작")
    logger.info("="*60)

    try:
        # 설정
        config = QdrantConfig()
        logger.info(f"\n연결 정보:")
        logger.info(f"  - 호스트: {config.QDRANT_HOST}")
        logger.info(f"  - 포트: {config.QDRANT_PORT}")
        logger.info(f"  - API 키: {'설정됨' if config.QDRANT_API_KEY else '없음'}")
        logger.info(f"  - 컬렉션: {config.QDRANT_COLLECTION}")

        # 연결
        logger.info("\nQdrant 클라이언트 생성 중...")
        connector = QdrantCloudConnector(config)

        # 기존 컬렉션 조회
        logger.info("\n기존 컬렉션 조회...")
        collections = connector.client.get_collections()
        logger.info(f"  총 컬렉션 개수: {len(collections.collections)}")

        if collections.collections:
            logger.info("\n  컬렉션 목록:")
            for coll in collections.collections:
                logger.info(f"    - {coll.name}")
        else:
            logger.info("  (컬렉션 없음)")

        # 테스트 컬렉션 생성 (선택사항)
        logger.info(f"\n'{config.QDRANT_COLLECTION}' 컬렉션 확인/생성 중...")
        connector.create_collection(force_recreate=False)

        # 컬렉션 정보 조회
        logger.info(f"\n컬렉션 상세 정보:")
        info = connector.get_collection_info()
        if info:
            logger.info(f"  - 이름: {info.get('name')}")
            logger.info(f"  - 벡터 개수: {info.get('vectors_count', 0)}")
            logger.info(f"  - 포인트 개수: {info.get('points_count', 0)}")
            logger.info(f"  - 세그먼트 개수: {info.get('segments_count', 0)}")
            logger.info(f"  - 상태: {info.get('status')}")
            logger.info(f"  - 벡터 차원: {info.get('vector_size')}")
            logger.info(f"  - 거리 메트릭: {info.get('distance')}")

        # 샘플 벡터 업로드 테스트 (주석 처리)
        logger.info("\n[샘플 벡터 업로드 테스트는 스킵됨]")
        # sample_vectors = [[0.1] * 1024, [0.2] * 1024]
        # sample_payloads = [
        #     {"user_id": "test1", "metadata": {"age_group": "20대"}},
        #     {"user_id": "test2", "metadata": {"age_group": "30대"}}
        # ]
        # connector.upsert_vectors(sample_vectors, sample_payloads, ids=["test1", "test2"])

        # 연결 종료
        connector.close()

        logger.info("\n" + "="*60)
        logger.info("[OK] 모든 테스트 통과!")
        logger.info("="*60)

        return True

    except Exception as e:
        logger.error(f"\n[ERROR] 테스트 실패: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = test_qdrant_connection()
    sys.exit(0 if success else 1)
