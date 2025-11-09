"""클라우드 데이터 동기화 스크립트"""
import logging
import sys
import os

# 상위 디렉토리를 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connectors.opensearch_cloud import create_opensearch_client, get_index_stats
from rag_query_analyzer.config import get_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def sync_local_to_remote(
 local_index: str,
 remote_index: str,
 batch_size: int = 100
):
 """
 로컬 OpenSearch에서 원격 OpenSearch로 데이터 동기화

 Args:
 local_index: 로컬 인덱스 이름
 remote_index: 원격 인덱스 이름
 batch_size: 배치 크기
 """
 config = get_config()

 logger.info("="*60)
 logger.info(" 클라우드 데이터 동기화 시작")
 logger.info("="*60)

 # 로컬 클라이언트
 logger.info("로컬 OpenSearch 연결 중...")
 local_client = create_opensearch_client(
 host=config.OPENSEARCH_HOST,
 port=config.OPENSEARCH_PORT,
 user=config.OPENSEARCH_USER,
 password=config.OPENSEARCH_PASSWORD,
 use_ssl=config.OPENSEARCH_USE_SSL
 )

 # 원격 클라이언트 (환경 변수에서 읽어올 예정)
 logger.info("원격 OpenSearch 연결 중...")
 # TODO: 원격 설정 추가
 # remote_client = create_opensearch_client(...)

 logger.info(f"\n로컬 인덱스: {local_index}")
 logger.info(f"원격 인덱스: {remote_index}")

 # 로컬 통계
 local_stats = get_index_stats(local_client, local_index)
 logger.info(f"\n로컬 통계:")
 logger.info(f" - 문서 개수: {local_stats.get('doc_count', 0)}")
 logger.info(f" - 크기: {local_stats.get('store_size_mb', 0)} MB")

 # TODO: 실제 동기화 로직 구현
 logger.warning("\n[WARNING] 동기화 로직은 아직 구현되지 않았습니다.")
 logger.info("\n구현 예정 기능:")
 logger.info(" 1. 로컬에서 데이터 스크롤 읽기")
 logger.info(" 2. 원격으로 배치 업로드")
 logger.info(" 3. 진행 상황 모니터링")
 logger.info(" 4. 오류 처리 및 재시도")

 logger.info("\n="*60)


def sync_remote_to_local(
 remote_index: str,
 local_index: str,
 batch_size: int = 100
):
 """
 원격 OpenSearch에서 로컬 OpenSearch로 데이터 동기화 (플레이스홀더)
 """
 logger.info("원격 → 로컬 동기화는 향후 구현 예정입니다.")


if __name__ == "__main__":
 import argparse

 parser = argparse.ArgumentParser(description="클라우드 데이터 동기화")
 parser.add_argument("--local-index", default="s_welcome_2nd", help="로컬 인덱스 이름")
 parser.add_argument("--remote-index", default="s_welcome_2nd", help="원격 인덱스 이름")
 parser.add_argument("--direction", choices=["local-to-remote", "remote-to-local"], default="local-to-remote")
 parser.add_argument("--batch-size", type=int, default=100, help="배치 크기")

 args = parser.parse_args()

 if args.direction == "local-to-remote":
 sync_local_to_remote(args.local_index, args.remote_index, args.batch_size)
 else:
 sync_remote_to_local(args.remote_index, args.local_index, args.batch_size)
