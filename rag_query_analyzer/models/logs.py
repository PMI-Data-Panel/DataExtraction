from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any


@dataclass
class QueryPerformanceLog:
    """쿼리 성능 로그를 담는 데이터 클래스"""
    
    session_id: str                          # 동일 사용자의 연속된 질문을 묶기 위한 ID
    result_quality: float                    # 시스템 자체 평가 결과 품질 (0-1)
    total_execution_time: float              # 총 소요 시간 (초)
    query: str                               # 원본 쿼리
    user_id: Optional[str] = None            # 사용자 식별자
    app_version: str = "1.0.0"               # 로그가 기록될 당시의 애플리케이션 버전

    # QueryAnalysis 객체를 to_dict()로 변환하여 저장
    analysis_plan: Dict[str, Any] = field(default_factory=dict)
    performance_breakdown: Dict[str, float] = field(default_factory=dict)
    
    # --- 평가 정보 ---
    user_feedback: Optional[float] = None    # 사용자 피드백 (0-1)

    # --- 최종 결과 요약 ---
    final_answer_snippet: str = ""           # 생성된 최종 답변의 일부 (앞 100자)

    # --- 추가 메타데이터 ---
    timestamp: datetime = field(default_factory=datetime.now)
    result_count: int = 0
    cache_hit: bool = False
    error: Optional[str] = None

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