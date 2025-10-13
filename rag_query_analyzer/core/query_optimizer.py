import os
import json
import logging
import numpy as np
from typing import List, Optional, Dict
from datetime import datetime
from ..models.query import QueryAnalysis, SearchResult
from ..models.logs import QueryPerformanceLog
from ..config import Config

logger = logging.getLogger(__name__)


class QueryOptimizer:
    """피드백 기반 쿼리 최적화
    
    과거 쿼리 성능을 학습하여 최적의 파라미터를 추천합니다.
    """
    
    def __init__(self, config: Config = None):
        """초기화
        
        Args:
            config: 설정 객체
        """
        self.config = config or Config()
        self.log_file = self.config.QUERY_LOG_FILE
        self.performance_logs: List[QueryPerformanceLog] = []
        self._load_logs()
        logger.info("QueryOptimizer 초기화 완료")
    
    def _load_logs(self):
        """저장된 로그 불러오기"""
        if os.path.exists(self.log_file):
            try:
                with open(self.log_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.performance_logs = [
                        QueryPerformanceLog.from_dict(log_data) 
                        for log_data in data
                    ]
                logger.info(f"📊 {len(self.performance_logs)}개의 로그 로드")
            except Exception as e:
                logger.warning(f"로그 로드 실패: {e}")
    
    def _save_logs(self):
        """로그 저장"""
        try:
            # 최대 10000개 로그만 유지
            if len(self.performance_logs) > 10000:
                self.performance_logs = self.performance_logs[-10000:]
            
            with open(self.log_file, 'w', encoding='utf-8') as f:
                logs_data = [log.to_dict() for log in self.performance_logs]
                json.dump(logs_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"로그 저장 실패: {e}")
    
    def log_performance(self,
                    query: str,
                    analysis: QueryAnalysis,
                    results: List[SearchResult],
                    user_feedback: Optional[float] = None,
                    auto_evaluated: bool = True):
        """쿼리 성능 로깅
        
        Args:
            query: 원본 쿼리
            analysis: 분석 결과
            results: 검색 결과
            user_feedback: 사용자 피드백 (0-1)
            auto_evaluated: 자동 평가 여부
        """
        # 자동 평가
        auto_score = self.auto_evaluate_results(results, analysis) if auto_evaluated else 0.0
        
        log = QueryPerformanceLog(
            query=query,
            intent=analysis.intent,
            alpha=analysis.alpha,
            keywords=analysis.must_terms,
            result_quality=user_feedback if user_feedback is not None else auto_score,
            timestamp=datetime.now(),
            execution_time=analysis.execution_time,
            auto_evaluated=auto_evaluated,
            user_feedback=user_feedback,
            result_count=len(results),
            cache_hit=analysis.cache_hit
        )
        
        self.performance_logs.append(log)
        self._save_logs()
        
        quality = user_feedback if user_feedback is not None else auto_score
        logger.info(f"📝 성능 로그 저장: 품질={quality:.2f}")
    
    def auto_evaluate_results(self, 
                            results: List[SearchResult], 
                            analysis: QueryAnalysis) -> float:
        """검색 결과 품질 자동 평가
        
        Args:
            results: 검색 결과
            analysis: 쿼리 분석 결과
            
        Returns:
            품질 점수 (0-1)
        """
        if not results:
            return 0.0
        
        quality_score = 0.0
        
        # 1. 결과 수 평가 (30%)
        result_count_score = min(len(results) / 10, 1.0)
        quality_score += 0.3 * result_count_score
        
        # 2. 리랭킹 점수 분포 평가 (30%)
        if results and results[0].rerank_score is not None:
            rerank_scores = [r.rerank_score for r in results[:5] 
                        if r.rerank_score is not None]
            if len(rerank_scores) > 1:
                score_variance = np.var(rerank_scores)
                # 분산이 적당히 있으면 좋음 (너무 작거나 크면 안좋음)
                if 0.05 < score_variance < 0.5:
                    quality_score += 0.3
                else:
                    quality_score += 0.15
        
        # 3. 키워드 매칭률 평가 (40%)
        if analysis.must_terms:
            top_results = results[:min(5, len(results))]
            match_scores = []
            
            for result in top_results:
                text_to_check = f"{result.summary} {str(result.answers)}".lower()
                matched = sum(1 for term in analysis.must_terms 
                            if term.lower() in text_to_check)
                match_rate = matched / len(analysis.must_terms)
                match_scores.append(match_rate)
            
            avg_match_rate = np.mean(match_scores) if match_scores else 0
            quality_score += 0.4 * avg_match_rate
        
        return min(quality_score, 1.0)
    
    def find_optimal_params(self, query: str, top_k: int = 5) -> Optional[Dict]:
        """유사한 과거 쿼리의 최적 파라미터 찾기
        
        Args:
            query: 현재 쿼리
            top_k: 참고할 상위 쿼리 수
            
        Returns:
            최적 파라미터 딕셔너리 또는 None
        """
        if len(self.performance_logs) < 10:
            return None
        
        # 간단한 키워드 기반 유사도 계산
        query_words = set(query.lower().split())
        
        similar_logs = []
        for log in self.performance_logs:
            log_words = set(log.query.lower().split())
            
            # Jaccard 유사도
            if query_words and log_words:
                similarity = len(query_words & log_words) / len(query_words | log_words)
                
                # 품질이 좋은 로그만 고려
                if similarity > 0.3 and log.result_quality > 0.5:
                    weighted_score = similarity * log.result_quality
                    similar_logs.append((weighted_score, log))
        
        if not similar_logs:
            return None
        
        # 상위 k개 선택
        similar_logs.sort(key=lambda x: x[0], reverse=True)
        top_logs = similar_logs[:top_k]
        
        # 가중 평균 계산
        total_weight = sum(score for score, _ in top_logs)
        weighted_alpha = sum(score * log.alpha for score, log in top_logs) / total_weight
        
        # 공통 키워드 추출
        keyword_freq = {}
        for score, log in top_logs:
            for keyword in log.keywords:
                keyword_freq[keyword] = keyword_freq.get(keyword, 0) + score
        
        suggested_keywords = sorted(keyword_freq.items(), 
                                key=lambda x: x[1], 
                                reverse=True)[:10]
        
        logger.info(f"🎯 유사 쿼리 {len(top_logs)}개 발견")
        
        return {
            "optimal_alpha": weighted_alpha,
            "suggested_keywords": [kw for kw, _ in suggested_keywords],
            "confidence": total_weight / len(top_logs) if top_logs else 0,
            "similar_queries": len(top_logs)
        }