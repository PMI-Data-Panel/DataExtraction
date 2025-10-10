from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


@dataclass
class QueryPerformanceLog:
    """쿼리 성능 로그를 담는 데이터 클래스"""
    
    # 쿼리 정보
    query: str                               # 원본 쿼리
    intent: str                              # 분석된 의도
    alpha: float                             # 사용된 alpha 값
    keywords: List[str]                      # 사용된 키워드
    
    # 성능 정보
    result_quality: float                    # 결과 품질 (0-1)
    timestamp: datetime                      # 실행 시간
    execution_time: float                    # 소요 시간 (초)
    
    # 평가 정보
    auto_evaluated: bool = False             # 자동 평가 여부
    user_feedback: Optional[float] = None    # 사용자 피드백 (0-1)
    
    # 추가 정보
    result_count: int = 0                    # 결과 수
    cache_hit: bool = False                  # 캐시 히트 여부
    error: Optional[str] = None              # 에러 메시지
    
    def to_dict(self) -> dict:
        """딕셔너리로 변환 (JSON 직렬화용)"""
        return {
            "query": self.query,
            "intent": self.intent,
            "alpha": self.alpha,
            "keywords": self.keywords,
            "result_quality": self.result_quality,
            "timestamp": self.timestamp.isoformat() if isinstance(self.timestamp, datetime) else self.timestamp,
            "execution_time": self.execution_time,
            "auto_evaluated": self.auto_evaluated,
            "user_feedback": self.user_feedback,
            "result_count": self.result_count,
            "cache_hit": self.cache_hit,
            "error": self.error
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'QueryPerformanceLog':
        """딕셔너리에서 객체 생성"""
        if isinstance(data.get('timestamp'), str):
            data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return cls(**data)
    
    def get_quality_score(self) -> float:
        """최종 품질 점수 반환 (사용자 피드백 우선)"""
        return self.user_feedback if self.user_feedback is not None else self.result_quality