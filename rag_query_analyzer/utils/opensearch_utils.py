"""OpenSearch 유틸리티 및 인덱스 관리"""
import logging
from typing import List, Dict, Any
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
from ..config import Config

logger = logging.getLogger(__name__)


def create_opensearch_client(config: Config) -> OpenSearch:
    """OpenSearch 클라이언트 생성"""
    client = OpenSearch(
        hosts=[{
            "host": config.OPENSEARCH_HOST,
            "port": config.OPENSEARCH_PORT
        }],
        http_auth=(config.OPENSEARCH_USER, config.OPENSEARCH_PASSWORD),
        use_ssl=config.OPENSEARCH_USE_SSL,
        verify_certs=config.OPENSEARCH_VERIFY_CERTS,
        ssl_assert_hostname=config.OPENSEARCH_SSL_ASSERT_HOSTNAME,
        ssl_show_warn=False,
        timeout=180,  # 대량 데이터 조회 대응 (전체 데이터 약 35000개)
        max_retries=3,
        retry_on_timeout=True
    )

    # 연결 테스트
    try:
        info = client.info()
        logger.info(f"✅ OpenSearch 연결 성공: v{info['version']['number']}")
        return client
    except Exception as e:
        logger.error(f"❌ OpenSearch 연결 실패: {e}")
        raise


def ensure_max_result_window(client: OpenSearch, index_name: str, max_result_window: int = 50000) -> bool:
    """
    인덱스의 max_result_window 설정을 확인하고 필요시 업데이트
    
    Args:
        client: OpenSearch 클라이언트
        index_name: 인덱스 이름
        max_result_window: 설정할 최대 결과 창 크기 (기본값: 50000)
    
    Returns:
        설정이 성공적으로 업데이트되었거나 이미 충분한 경우 True
    """
    try:
        # 현재 인덱스 설정 확인
        current_settings = client.indices.get_settings(index=index_name)
        
        if index_name not in current_settings:
            logger.warning(f"⚠️ 인덱스 {index_name}가 존재하지 않습니다.")
            return False
        
        index_settings = current_settings[index_name]
        current_max_window = index_settings.get('settings', {}).get('index', {}).get('max_result_window')
        
        if current_max_window:
            current_max_window = int(current_max_window)
            if current_max_window >= max_result_window:
                logger.info(f"✅ 인덱스 {index_name}의 max_result_window가 이미 {current_max_window}로 설정되어 있습니다.")
                return True
        
        # 설정 업데이트
        logger.info(f"🔧 인덱스 {index_name}의 max_result_window를 {max_result_window}로 업데이트 중...")
        client.indices.put_settings(
            index=index_name,
            body={
                "index": {
                    "max_result_window": max_result_window
                }
            }
        )
        
        logger.info(f"✅ 인덱스 {index_name}의 max_result_window를 {max_result_window}로 업데이트 완료")
        return True
        
    except Exception as e:
        logger.error(f"❌ 인덱스 {index_name}의 max_result_window 설정 실패: {e}")
        return False
