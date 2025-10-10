import logging
import sys
from typing import Optional
from ..config import Config


def setup_logging(config: Optional[Config] = None):
    """로깅 설정
    
    Args:
        config: 설정 객체
    """
    if config is None:
        config = Config()
    
    # 로거 설정
    logger = logging.getLogger("rag_query_analyzer")
    logger.setLevel(getattr(logging, config.LOG_LEVEL))
    
    # 기존 핸들러 제거
    logger.handlers.clear()
    
    # 콘솔 핸들러 생성
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, config.LOG_LEVEL))
    
    # 포맷터 설정
    formatter = logging.Formatter(config.LOG_FORMAT)
    console_handler.setFormatter(formatter)
    
    # 핸들러 추가
    logger.addHandler(console_handler)
    
    # 외부 라이브러리 로깅 레벨 조정
    logging.getLogger("anthropic").setLevel(logging.WARNING)
    logging.getLogger("sentence_transformers").setLevel(logging.WARNING)
    
    logger.info("로깅 설정 완료")
    
    return logger