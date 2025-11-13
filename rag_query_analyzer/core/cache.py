import re
import hashlib
import logging
from collections import OrderedDict
from typing import Optional, Dict, Set, List
from ..models.query import QueryAnalysis
from ..config import Config

logger = logging.getLogger(__name__)


class LRUCachedAnalyzer:
    """LRU 캐시를 활용한 고속 분석기
    
    자주 사용되는 쿼리의 분석 결과를 캐싱하여 성능을 향상시킵니다.
    """
    
    def __init__(self, config: Config = None):
        """초기화
        
        Args:
            config: 설정 객체
        """
        self.config = config or Config()
        self.cache_size = self.config.CACHE_SIZE
        self.similarity_threshold = self.config.CACHE_SIMILARITY_THRESHOLD
        
        # LRU 캐시 (OrderedDict 사용)
        self.cache: OrderedDict[str, QueryAnalysis] = OrderedDict()
        
        # 통계
        self.hit_count = 0
        self.miss_count = 0
        
        # 캐시 인덱스 (빠른 유사 검색용)
        self.cache_index: Dict[str, Set[str]] = {}  # 키워드 -> 캐시키 매핑
        
        logger.info(f"LRUCachedAnalyzer 초기화 (크기: {self.cache_size})")
    
    def _get_cache_key(self, query: str, use_claude: Optional[bool]) -> str:
        """쿼리의 캐시 키 생성
        
        Args:
            query: 원본 쿼리
            
        Returns:
            MD5 해시 기반 캐시 키
        """
        # 정규화: 공백 정리, 소문자 변환
        normalized = re.sub(r'\s+', ' ', query.lower().strip())
        prefix = "1" if use_claude else "0"
        digest = hashlib.md5(normalized.encode()).hexdigest()
        return f"{prefix}:{digest}"
    
    def get_cached(self, query: str, use_claude: Optional[bool] = None) -> Optional[QueryAnalysis]:
        """캐시에서 분석 결과 가져오기
        
        Args:
            query: 조회할 쿼리
            
        Returns:
            캐시된 분석 결과 또는 None
        """
        key = self._get_cache_key(query, use_claude)
        
        # 정확한 매칭
        if key in self.cache:
            # LRU: 최근 사용 항목을 끝으로 이동
            self.cache.move_to_end(key)
            self.hit_count += 1
            
            result = self.cache[key]
            result.cache_hit = True
            
            logger.debug(f"💾 캐시 히트! (히트율: {self.get_hit_rate():.1%})")
            return result
        
        # 유사 쿼리 검색
        similar_result = self._find_similar_cached(query, use_claude)
        if similar_result:
            self.hit_count += 1
            similar_result.cache_hit = True
            logger.debug(f"💾 유사 쿼리 캐시 히트!")
            return similar_result
        
        self.miss_count += 1
        return None
    
    def _find_similar_cached(self, query: str, use_claude: Optional[bool]) -> Optional[QueryAnalysis]:
        """유사한 캐시된 쿼리 찾기
        
        Args:
            query: 찾을 쿼리
            
        Returns:
            유사한 캐시된 분석 결과 또는 None
        """
        query_words = set(query.lower().split())
        
        if not query_words:
            return None
        
        best_match = None
        best_similarity = 0
        
        # 키워드 기반 후보 찾기
        candidate_keys = set()
        for word in query_words:
            if word in self.cache_index:
                candidate_keys.update(self.cache_index[word])
        
        # 후보들과 유사도 계산
        for cache_key in candidate_keys:
            if cache_key not in self.cache:
                continue
            if use_claude is not None:
                desired_flag = bool(use_claude)
                candidate_flag = cache_key.startswith("1:")
                if candidate_flag != desired_flag:
                    continue
                continue
            
            cached_analysis = self.cache[cache_key]
            
            # 캐시된 쿼리의 키워드 추출
            cached_words = set(cached_analysis.must_terms + cached_analysis.should_terms)
            
            if not cached_words:
                continue
            
            # Jaccard 유사도 계산
            similarity = len(query_words & cached_words) / len(query_words | cached_words)
            
            if similarity > best_similarity and similarity >= self.similarity_threshold:
                best_similarity = similarity
                best_match = cached_analysis
        
        if best_match:
            logger.debug(f"유사도 {best_similarity:.2f}로 캐시 매칭")
        
        return best_match
    
    def set_cached(self, query: str, analysis: QueryAnalysis, use_claude: Optional[bool] = None):
        """분석 결과 캐싱
        
        Args:
            query: 원본 쿼리
            analysis: 분석 결과
        """
        key = self._get_cache_key(query, use_claude)
        
        # 이미 캐시에 있으면 끝으로 이동
        if key in self.cache:
            self.cache.move_to_end(key)
        else:
            # 캐시 크기 제한 체크
            if len(self.cache) >= self.cache_size:
                # 가장 오래된 항목 제거
                oldest_key = next(iter(self.cache))
                self._remove_from_index(oldest_key)
                self.cache.pop(oldest_key)
                logger.debug(f"캐시 공간 확보: 오래된 항목 제거")
        
        # 캐시에 저장
        self.cache[key] = analysis
        
        # 인덱스 업데이트
        self._add_to_index(key, analysis)
        
        logger.debug(f"캐시 저장: {key[:8]}...")
    
    def _add_to_index(self, cache_key: str, analysis: QueryAnalysis):
        """캐시 인덱스에 추가
        
        Args:
            cache_key: 캐시 키
            analysis: 분석 결과
        """
        # 키워드 추출
        keywords = analysis.must_terms + analysis.should_terms
        
        for keyword in keywords:
            keyword_lower = keyword.lower()
            if keyword_lower not in self.cache_index:
                self.cache_index[keyword_lower] = set()
            self.cache_index[keyword_lower].add(cache_key)
    
    def _remove_from_index(self, cache_key: str):
        """캐시 인덱스에서 제거
        
        Args:
            cache_key: 제거할 캐시 키
        """
        # 해당 키를 가진 모든 인덱스 엔트리 정리
        for keyword, keys in list(self.cache_index.items()):
            if cache_key in keys:
                keys.remove(cache_key)
                if not keys:
                    del self.cache_index[keyword]
    
    def get_hit_rate(self) -> float:
        """캐시 히트율 반환
        
        Returns:
            히트율 (0-1)
        """
        total = self.hit_count + self.miss_count
        return self.hit_count / total if total > 0 else 0.0
    
    def get_statistics(self) -> Dict:
        """캐시 통계 반환
        
        Returns:
            통계 정보 딕셔너리
        """
        return {
            "cache_size": len(self.cache),
            "max_size": self.cache_size,
            "hit_count": self.hit_count,
            "miss_count": self.miss_count,
            "hit_rate": self.get_hit_rate(),
            "index_size": len(self.cache_index)
        }
    
    def clear_cache(self):
        """캐시 초기화"""
        self.cache.clear()
        self.cache_index.clear()
        self.hit_count = 0
        self.miss_count = 0
        logger.info("💾 캐시 초기화 완료")
    
    def warm_up(self, common_queries: List[str]):
        """자주 사용되는 쿼리로 캐시 예열
        
        Args:
            common_queries: 예열할 쿼리 리스트
        """
        logger.info(f"캐시 예열 시작: {len(common_queries)}개 쿼리")
        # 실제 구현에서는 각 쿼리에 대한 분석 결과를 미리 생성하여 캐싱
        pass
