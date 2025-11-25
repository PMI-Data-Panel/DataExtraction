import time
import logging
import asyncio
from typing import List, Tuple, Optional, Dict, Set
from concurrent.futures import ThreadPoolExecutor

from .base import BaseAnalyzer
from .claude_analyzer import ClaudeAnalyzer
from .demographic_extractor import DemographicExtractor
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
        self.rule_analyzer = RuleBasedAnalyzer()
        self.semantic_analyzer = SemanticAnalyzer(self.config)
        self.claude_analyzer = None
        self.analyzers = [
            ("Semantic", self.semantic_analyzer),
            ("Rule", self.rule_analyzer),
        ]
        if self.config.ENABLE_CLAUDE_ANALYZER:
            self.claude_analyzer = ClaudeAnalyzer(self.config)
            self.analyzers.insert(0, ("Claude", self.claude_analyzer))
    
    def analyze_query(self, 
                     query: str, 
                     context: str = "",
                     metadata: Dict = None,
                     use_claude: Optional[bool] = None) -> QueryAnalysis:
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
        if use_claude is None:
            use_claude = self.config.ENABLE_CLAUDE_ANALYZER

        if self.cache:
            cached = self.cache.get_cached(query, use_claude=use_claude)
            if cached:
                cached.execution_time = time.time() - start_time
                return cached
        
        # 폴백 체인으로 분석
        analysis = self._analyze_with_fallback(query, context, use_claude=use_claude)
        analysis = self._normalize_analysis(analysis, query, context)
        
        # 과거 성능 데이터 활용
        optimal_params = self.query_optimizer.find_optimal_params(query)
        if optimal_params and optimal_params["confidence"] > 0.7:
            analysis.alpha = optimal_params["optimal_alpha"]
            logger.info(f"과거 데이터 기반 alpha 조정: {analysis.alpha:.2f}")
        
        # 실행 시간 기록
        analysis.execution_time = time.time() - start_time
        
        # 캐시 저장
        if self.cache:
            self.cache.set_cached(query, analysis, use_claude=use_claude)
        
        return analysis
    
    def _analyze_with_fallback(self, query: str, context: str, use_claude: Optional[bool]) -> QueryAnalysis:
        """폴백 체인을 통한 분석
        
        Args:
            query: 분석할 쿼리
            context: 맥락
            
        Returns:
            분석 결과
        """
        pipeline: List[Tuple[str, BaseAnalyzer]] = []
        if use_claude:
            if self.claude_analyzer is None:
                if not self.config.CLAUDE_API_KEY:
                    logger.warning("Claude 분석기가 요청되었지만 CLAUDE_API_KEY가 설정되지 않았습니다. Claude 단계를 건너뜁니다.")
                else:
                    try:
                        self.claude_analyzer = ClaudeAnalyzer(self.config)
                    except Exception as exc:
                        logger.warning(f"Claude 분석기 초기화 실패: {exc}")
            if self.claude_analyzer is not None:
                pipeline.append(("Claude", self.claude_analyzer))

        pipeline.extend([
            ("Semantic", self.semantic_analyzer),
            ("Rule", self.rule_analyzer),
        ])

        for name, analyzer in pipeline:
            try:
                logger.info(f"🔍 {name} 분석기 시도 중...")
                analysis = analyzer.analyze(query, context)

                # 성공적인 분석인지 확인
                # ⭐ must_terms가 비어있어도 demographics나 behavioral_conditions가 있으면 성공
                has_useful_content = (
                    analysis.must_terms or
                    (hasattr(analysis, 'demographic_entities') and analysis.demographic_entities) or
                    (hasattr(analysis, 'behavioral_conditions') and analysis.behavioral_conditions)
                )
                if analysis.confidence >= 0.3 and has_useful_content:
                    logger.info(f"✅ {name} 분석기 성공 (must_terms={len(analysis.must_terms) if analysis.must_terms else 0}, demographics={len(analysis.demographic_entities) if hasattr(analysis, 'demographic_entities') and analysis.demographic_entities else 0}, behavioral={bool(getattr(analysis, 'behavioral_conditions', {}))})")
                    return analysis
                else:
                    logger.info(f"⚠️ {name} 분석기 결과 부족 (confidence={analysis.confidence:.2f}, has_content={has_useful_content})")

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
    
    def _normalize_analysis(self, analysis: QueryAnalysis, query: str, context: str) -> QueryAnalysis:
        """분석 결과 정규화 (행동 조건/불용어 보강)"""
        if not analysis:
            return analysis

        # ⭐ 디버깅: Claude 분석 결과 확인
        logger.warning(f"[정규화 전] must_terms={analysis.must_terms}")
        logger.warning(f"[정규화 전] demographic_entities={len(analysis.demographic_entities) if analysis.demographic_entities else 0}개")
        logger.warning(f"[정규화 전] behavioral_conditions={analysis.behavioral_conditions}")
        if analysis.demographic_entities:
            for entity in analysis.demographic_entities:
                logger.warning(f"  - {entity.demographic_type.value}: {entity.value}")

        # Rule 기반 불용어/행동 키워드
        rule_analyzer = getattr(self, "rule_analyzer", None) or RuleBasedAnalyzer()
        meta_lower = {kw.lower() for kw in rule_analyzer.meta_keywords}
        behavior_lower = {kw.lower() for kw in rule_analyzer.behavior_keywords}
        demographic_extractor = DemographicExtractor()

        def _is_meta(term: str) -> bool:
            lowered = term.lower()
            if lowered in meta_lower:
                return True
            return any(kw in lowered for kw in meta_lower)

        def _is_behavior(term: str) -> bool:
            lowered = term.lower()
            if lowered in behavior_lower:
                return True
            return any(kw in lowered for kw in behavior_lower)

        # must_terms 정리
        sanitized_must: List[str] = []
        removed_behavior_terms: List[str] = []
        removed_demographic_terms: List[str] = []
        for term in analysis.must_terms:
            if not term:
                continue
            if _is_meta(term):
                continue
            if _is_behavior(term):
                removed_behavior_terms.append(term)
                continue
            sanitized_must.append(term)

        # should_terms 정리
        sanitized_should: List[str] = []
        for term in analysis.should_terms:
            if not term or _is_meta(term):
                continue
            if _is_behavior(term):
                removed_behavior_terms.append(term)
                continue
            sanitized_should.append(term)

        # Demographics 추출 및 제거
        # ⭐ Claude가 이미 추출한 demographic_entities가 있으면 사용, 없으면 DemographicExtractor 사용
        logger.info(f"🔍 Demographics 추출 시작 (명시적 표현만):")
        logger.info(f"   📝 Query: '{query}'")

        if analysis.demographic_entities:
            # Claude가 추출한 demographics 사용
            demographics_list = analysis.demographic_entities
            logger.info(f"   ✅ Claude가 추출한 demographics: {len(demographics_list)}개")
            if demographics_list:
                for demo in demographics_list:
                    logger.info(f"      * {demo.demographic_type.value}: '{demo.value}' (원본: '{demo.raw_value}')")
            else:
                logger.info(f"      ℹ️ 명시적 인구통계 표현 없음 (문맥어는 추출하지 않음)")
        else:
            # 폴백: DemographicExtractor 사용
            demographics = demographic_extractor.extract(query)
            demographics_list = demographics.demographics
            logger.info(f"   ⚠️ Claude demographics 없음 → DemographicExtractor 사용: {len(demographics_list)}개")
            if demographics_list:
                for demo in demographics_list:
                    logger.info(f"      * {demo.demographic_type.value}: '{demo.value}' (원본: '{demo.raw_value}')")

        demographic_tokens: Set[str] = set()
        for entity in demographics_list:
            demographic_tokens.add(entity.raw_value.lower())
            demographic_tokens.add(entity.value.lower())
            for syn in entity.synonyms:
                demographic_tokens.add(str(syn).lower())

        if demographic_tokens:
            sanitized_must = [
                term for term in sanitized_must
                if term.lower() not in demographic_tokens
            ]
            sanitized_should = [
                term for term in sanitized_should
                if term.lower() not in demographic_tokens
            ]
            removed_demographic_terms = [
                term for term in analysis.must_terms + analysis.should_terms
                if term and term.lower() in demographic_tokens
            ]

        # 행동 조건 보강 (Rule 분석 결과와 병합) - must/should 할당 전에 먼저 수행
        if not analysis.behavioral_conditions:
            try:
                rule_analysis = rule_analyzer.analyze(query, context)
                if rule_analysis.behavioral_conditions:
                    analysis.behavioral_conditions = dict(rule_analysis.behavioral_conditions)
            except Exception as exc:
                logger.debug(f"Rule 분석 보강 실패: {exc}")

        # 행동 키워드 처리:
        # - behavioral_conditions가 있으면: 완전 제거 (OpenSearch 필터로 처리됨)
        # - behavioral_conditions가 없으면: should_terms로 완화 (의미 검색)
        if removed_behavior_terms:
            if not analysis.behavioral_conditions:
                # behavioral_conditions가 없으면 should_terms로 완화
                existing_should_lower = {term.lower() for term in sanitized_should}
                for term in removed_behavior_terms:
                    lowered = term.lower()
                    if lowered in existing_should_lower:
                        continue
                    sanitized_should.append(term)
                    existing_should_lower.add(lowered)
                logger.info(f"⚠️ Behavioral conditions 없음 → 행동 키워드를 should_terms로 완화: {removed_behavior_terms}")
            else:
                # behavioral_conditions가 있으면 완전 제거 (필터로 처리됨)
                logger.info(f"✅ Behavioral conditions 있음 → 행동 키워드 제거: {removed_behavior_terms}")

        # 정리된 리스트 반영 (입력 순서 유지)
        def _dedupe(items: List[str]) -> List[str]:
            seen = set()
            ordered: List[str] = []
            for item in items:
                lowered = item.lower()
                if lowered in seen:
                    continue
                seen.add(lowered)
                ordered.append(item)
            return ordered

        analysis.must_terms = _dedupe(sanitized_must)
        analysis.should_terms = _dedupe(sanitized_should)

        # Demographics 정보 저장 (Claude 결과를 유지하거나 DemographicExtractor 결과 저장)
        # ⭐ Claude가 이미 추출한 경우 유지, 아니면 DemographicExtractor 결과 저장
        if not analysis.demographic_entities:
            analysis.demographic_entities = demographics_list
        analysis.removed_demographic_terms = removed_demographic_terms

        logger.info(f"📊 Demographics 처리 완료:")
        logger.info(f"   ✅ 최종 저장: {len(analysis.demographic_entities)}개")
        if removed_demographic_terms:
            logger.info(f"   🗑️ 제거된 Demographics 키워드: {removed_demographic_terms}")
            logger.info(f"      → 이유: Demographics는 OpenSearch 필터로 처리됨 (검색어에서 제거)")
        else:
            logger.info(f"   ℹ️ 제거된 키워드 없음 (검색어 유지)")

        return analysis

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
            fallback_used=True,
            behavioral_conditions={},
        )

