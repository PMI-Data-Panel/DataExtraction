import time
import logging
import asyncio
from typing import List, Tuple, Optional, Dict
from concurrent.futures import ThreadPoolExecutor

from .base import BaseAnalyzer
from .claude_analyzer import ClaudeAnalyzer
from .semantic_analyzer import SemanticAnalyzer
from .rule_analyzer import RuleBasedAnalyzer

from ..models.query import QueryAnalysis, SearchResult
from ..core import (
    SemanticModel,
    MultiStepQueryRewriter,
    QueryOptimizer,
    LRUCachedAnalyzer
)
from ..utils import Reranker
from connectors.hybrid_searcher import OpenSearchHybridQueryBuilder
from ..config import Config

logger = logging.getLogger(__name__)


class AdvancedRAGQueryAnalyzer:
    """고급 RAG 쿼리 분석기
    
    여러 분석 전략을 통합하고 최적화된 검색을 제공합니다.
    """
    
    def __init__(self, config: Config = None):
        """초기화
        
        Args:
            config: 설정 객체
        """
        self.config = config or Config()
        self.config.validate()
        
        # 핵심 컴포넌트 초기화
        self._init_components()
        
        # 분석기 체인 초기화
        self._init_analyzers()
        
        # 비동기 처리 설정
        if self.config.ENABLE_ASYNC:
            self.executor = ThreadPoolExecutor(max_workers=self.config.MAX_WORKERS)
        else:
            self.executor = None
        
        logger.info("AdvancedRAGQueryAnalyzer 초기화 완료")
    
    def _init_components(self):
        """컴포넌트 초기화"""
        self.semantic_model = SemanticModel()
        self.query_rewriter = MultiStepQueryRewriter(self.config)
        self.query_optimizer = QueryOptimizer(self.config)
        # ⚠️ QueryExpander 제거: 하드코딩된 동의어 대신 HybridSynonymExpander 사용
        self.es_query_builder = OpenSearchHybridQueryBuilder(self.config)
        
        # 캐시
        if self.config.ENABLE_CACHE:
            self.cache = LRUCachedAnalyzer(self.config)
        else:
            self.cache = None
        
        # 리랭커
        if self.config.ENABLE_RERANKING:
            self.reranker = Reranker(self.config)
        else:
            self.reranker = None
    
    def _init_analyzers(self):
        """분석기 체인 초기화"""
        self.analyzers = [
            ("Claude", ClaudeAnalyzer(self.config)),
            ("Semantic", SemanticAnalyzer(self.config)),
            ("Rule", RuleBasedAnalyzer())
        ]
    
    def analyze_query(self, 
                     query: str, 
                     context: str = "",
                     metadata: Dict = None) -> QueryAnalysis:
        """쿼리 분석 (메인 엔트리 포인트)
        
        Args:
            query: 분석할 쿼리
            context: 설문 맥락
            metadata: 추가 메타데이터
            
        Returns:
            분석 결과
        """
        start_time = time.time()
        
        # 캐시 확인
        if self.cache:
            cached = self.cache.get_cached(query)
            if cached:
                cached.execution_time = time.time() - start_time
                return cached
        
        # 쿼리 확장 (메타데이터 활용)
        # ⚠️ QueryExpander.expand_with_context 제거: 필요시 별도 구현
        # if metadata:
        #     query = self.query_expander.expand_with_context(query, metadata)
        
        # 폴백 체인으로 분석
        analysis = self._analyze_with_fallback(query, context)
        
        # 과거 성능 데이터 활용
        optimal_params = self.query_optimizer.find_optimal_params(query)
        if optimal_params and optimal_params["confidence"] > 0.7:
            analysis.alpha = optimal_params["optimal_alpha"]
            logger.info(f"과거 데이터 기반 alpha 조정: {analysis.alpha:.2f}")
        
        # 실행 시간 기록
        analysis.execution_time = time.time() - start_time
        
        # 캐시 저장
        if self.cache:
            self.cache.set_cached(query, analysis)
        
        return analysis
    
    def _analyze_with_fallback(self, query: str, context: str) -> QueryAnalysis:
        """폴백 체인을 통한 분석
        
        Args:
            query: 분석할 쿼리
            context: 맥락
            
        Returns:
            분석 결과
        """
        for name, analyzer in self.analyzers:
            try:
                logger.info(f"🔍 {name} 분석기 시도 중...")
                analysis = analyzer.analyze(query, context)
                
                # 성공적인 분석인지 확인
                if analysis.confidence >= 0.3 and analysis.must_terms:
                    logger.info(f"✅ {name} 분석기 성공")
                    return analysis
                    
            except Exception as e:
                logger.warning(f"{name} 분석기 실패: {e}")
                continue
        
        # 모든 분석기 실패시 기본값
        logger.warning("모든 분석기 실패, 기본값 반환")
        return self._create_default_analysis(query)
    
    def analyze_with_rewriting(self, 
                              query: str, 
                              context: str = "",
                              metadata: Dict = None) -> Tuple[QueryAnalysis, List[str]]:
        """쿼리 재작성을 포함한 종합 분석
        
        Args:
            query: 원본 쿼리
            context: 맥락
            metadata: 메타데이터
            
        Returns:
            (분석 결과, 재작성된 쿼리들)
        """
        # 1. 쿼리 재작성
        rewrites = self.query_rewriter.rewrite_query(query, context)
        
        # 2. 원본 쿼리 분석
        main_analysis = self.analyze_query(query, context, metadata)
        
        # 3. 재작성된 쿼리들도 분석하여 통합
        if rewrites:
            for rw_type, rw_query in rewrites[:2]:  # 상위 2개만
                try:
                    sub_analysis = self.analyze_query(rw_query, context, metadata)
                    main_analysis.merge_with(sub_analysis)
                except Exception as e:
                    logger.warning(f"재작성 쿼리 분석 실패 ({rw_type}): {e}")
        
        # 재작성 쿼리 저장
        main_analysis.rewritten_queries = [q for _, q in rewrites]
        
        return main_analysis, main_analysis.rewritten_queries
    
    def rerank_results(self, 
                      query: str, 
                      results: List[SearchResult],
                      top_k: Optional[int] = None) -> List[SearchResult]:
        """검색 결과 리랭킹
        
        Args:
            query: 원본 쿼리
            results: 검색 결과
            top_k: 상위 k개 반환
            
        Returns:
            리랭킹된 결과
        """
        if not self.reranker:
            return results[:top_k] if top_k else results
        
        return self.reranker.rerank(query, results, top_k)
    
    def build_search_query(self, 
                         analysis: QueryAnalysis,
                         query_vector: Optional[List[float]] = None,
                         size: Optional[int] = None,
                         filters: List[Dict] = None) -> Dict:
        """Elasticsearch 검색 쿼리 구성
        
        Args:
            analysis: 쿼리 분석 결과
            query_vector: 임베딩 벡터
            size: 요청할 문서 개수
            filters: 필터 조건
            
        Returns:
            Elasticsearch 쿼리
        """
        if size is None:
            size = self.config.INITIAL_SEARCH_SIZE

        return self.es_query_builder.build_complete_request(
            analysis=analysis,
            query_vector=query_vector,
            size=size,
            filters=filters
        )
    
    async def analyze_batch_async(self, 
                                 queries: List[str],
                                 context: str = "") -> List[QueryAnalysis]:
        """배치 쿼리 비동기 분석
        
        Args:
            queries: 쿼리 리스트
            context: 맥락
            
        Returns:
            분석 결과 리스트
        """
        if not self.executor:
            # 동기 처리
            return [self.analyze_query(q, context) for q in queries]
        
        loop = asyncio.get_event_loop()
        tasks = []
        
        for query in queries:
            task = loop.run_in_executor(
                self.executor,
                self.analyze_query,
                query,
                context
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        logger.info(f"✅ {len(results)}개 쿼리 배치 처리 완료")
        
        return results
    
    def log_performance(self, 
                       query: str,
                       analysis: QueryAnalysis,
                       results: List[SearchResult],
                       user_feedback: Optional[float] = None):
        """성능 로깅
        
        Args:
            query: 원본 쿼리
            analysis: 분석 결과
            results: 검색 결과
            user_feedback: 사용자 피드백
        """
        self.query_optimizer.log_performance(
            query, analysis, results, user_feedback
        )
    
    def explain_analysis(self, analysis: QueryAnalysis) -> str:
        """분석 결과를 사용자 친화적으로 설명
        
        Args:
            analysis: 분석 결과
            
        Returns:
            설명 문자열
        """
        lines = []
        lines.append("📊 쿼리 분석 결과")
        lines.append("=" * 50)
        
        # 검색 전략
        intent_map = {
            "exact_match": "정확한 조건 매칭",
            "semantic_search": "의미적 유사성 검색",
            "hybrid": "복합 검색 (조건 + 의미)"
        }
        lines.append(f"검색 전략: {intent_map.get(analysis.intent, analysis.intent)}")
        
        # 신뢰도
        if analysis.confidence >= 0.7:
            conf_level = "높음 ✅"
        elif analysis.confidence >= 0.4:
            conf_level = "보통 ⚠️"
        else:
            conf_level = "낮음 ❌"
        lines.append(f"신뢰도: {analysis.confidence:.0%} ({conf_level})")
        
        # 키워드
        if analysis.must_terms:
            lines.append(f"\n필수 조건: {', '.join(analysis.must_terms)}")
        if analysis.should_terms:
            lines.append(f"선택 조건: {', '.join(analysis.should_terms)}")
        if analysis.must_not_terms:
            lines.append(f"제외 조건: {', '.join(analysis.must_not_terms)}")
        
        # 확장 키워드
        if analysis.expanded_keywords:
            lines.append("\n확장된 키워드:")
            for key, values in list(analysis.expanded_keywords.items())[:3]:
                lines.append(f"  • {key} → {', '.join(values[:3])}")
        
        # 추론 과정
        if analysis.reasoning_steps:
            lines.append("\n분석 과정:")
            for step in analysis.reasoning_steps[:3]:
                lines.append(f"  • {step}")
        
        # 성능 정보
        if analysis.execution_time > 0:
            lines.append(f"\n실행 시간: {analysis.execution_time:.3f}초")
        
        return "\n".join(lines)
    
    def get_statistics(self) -> Dict:
        """시스템 통계 반환
        
        Returns:
            통계 정보
        """
        stats = {
            "cache_enabled": self.config.ENABLE_CACHE,
            "reranking_enabled": self.config.ENABLE_RERANKING,
            "async_enabled": self.config.ENABLE_ASYNC
        }
        
        if self.cache:
            stats.update(self.cache.get_statistics())
        
        stats["performance_logs"] = len(self.query_optimizer.performance_logs)
        
        return stats
    
    def _create_default_analysis(self, query: str) -> QueryAnalysis:
        """기본 분석 결과 생성
        
        Args:
            query: 원본 쿼리
            
        Returns:
            기본 분석 결과
        """
        return QueryAnalysis(
            intent="hybrid",
            must_terms=[query],
            should_terms=[],
            must_not_terms=[],
            alpha=0.5,
            expanded_keywords={},
            confidence=0.1,
            explanation="기본 분석 (폴백)",
            analyzer_used="default",
            fallback_used=True
        )

