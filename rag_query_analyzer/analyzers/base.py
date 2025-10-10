from abc import ABC, abstractmethod
from typing import Optional
from ..models.query import QueryAnalysis


class BaseAnalyzer(ABC):
    """분석기 추상 클래스
    
    모든 분석기가 구현해야 하는 기본 인터페이스를 정의합니다.
    """
    
    @abstractmethod
    def analyze(self, query: str, context: str = "") -> QueryAnalysis:
        """쿼리 분석 수행
        
        Args:
            query: 분석할 쿼리
            context: 추가 맥락 정보
            
        Returns:
            분석 결과
        """
        pass
    
    @abstractmethod
    def get_name(self) -> str:
        """분석기 이름 반환
        
        Returns:
            분석기 이름
        """
        pass
    
    def validate_query(self, query: str) -> bool:
        """쿼리 유효성 검증
        
        Args:
            query: 검증할 쿼리
            
        Returns:
            유효 여부
        """
        if not query or not query.strip():
            return False
        
        # 너무 짧은 쿼리
        if len(query.strip()) < 2:
            return False
        
        # 너무 긴 쿼리
        if len(query) > 1000:
            return False
        
        return True
    
    def preprocess_query(self, query: str) -> str:
        """쿼리 전처리
        
        Args:
            query: 원본 쿼리
            
        Returns:
            전처리된 쿼리
        """
        # 공백 정리
        query = " ".join(query.split())
        
        # 특수문자 정리 (선택적)
        # query = re.sub(r'[^\w\s가-힣]', ' ', query)
        
        return query.strip()