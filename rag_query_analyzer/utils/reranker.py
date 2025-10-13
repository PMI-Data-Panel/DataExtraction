import logging
from typing import List, Optional
from sentence_transformers import CrossEncoder
from ..models.query import SearchResult
from ..config import Config

logger = logging.getLogger(__name__)


class Reranker:
    """검색 결과 리랭킹
    
    Cross-Encoder를 사용하여 검색 결과의 관련성을 재평가합니다.
    """
    
    def __init__(self, config: Config = None):
        """초기화
        
        Args:
            config: 설정 객체
        """
        self.config = config or Config()
        self.model = None
        self.enabled = False
        
        if self.config.ENABLE_RERANKING:
            self._init_model()
    
    def _init_model(self):
        """리랭킹 모델 초기화"""
        models_to_try = self.config.RERANKER_MODELS
        
        for model_name in models_to_try:
            try:
                logger.info(f"리랭킹 모델 로딩 시도: {model_name}")
                self.model = CrossEncoder(model_name)
                self.enabled = True
                logger.info(f"✅ 리랭킹 모델 로딩 완료: {model_name}")
                break
            except Exception as e:
                logger.warning(f"❌ {model_name} 로딩 실패: {e}")
                continue
        
        if not self.enabled:
            logger.warning("⚠️ 모든 리랭킹 모델 로딩 실패")
    
    def rerank(self, 
            query: str, 
            results: List[SearchResult], 
            top_k: Optional[int] = None) -> List[SearchResult]:
        """검색 결과 리랭킹
        
        Args:
            query: 원본 쿼리
            results: 검색 결과 리스트
            top_k: 반환할 상위 결과 수
            
        Returns:
            리랭킹된 결과 리스트
        """
        if not self.enabled or self.model is None:
            logger.debug("리랭킹 비활성화 상태")
            return results[:top_k] if top_k else results
        
        if len(results) <= 1:
            return results
        
        if top_k is None:
            top_k = self.config.RERANK_TOP_K
        
        try:
            # 쿼리-문서 쌍 생성
            query_doc_pairs = []
            for result in results:
                # 문서 텍스트 구성
                doc_text = self._prepare_document_text(result)
                query_doc_pairs.append([query, doc_text])
            
            logger.debug(f"🔄 {len(query_doc_pairs)}개 결과 리랭킹 중...")
            
            # 점수 계산
            scores = self.model.predict(query_doc_pairs)
            
            # 점수 할당
            for i, result in enumerate(results):
                result.rerank_score = float(scores[i])
            
            # 점수 기준 정렬
            reranked = sorted(
                results,
                key=lambda x: x.rerank_score if x.rerank_score is not None else -1,
                reverse=True
            )
            
            logger.info(f"✅ 리랭킹 완료: Top-{min(top_k, len(reranked))} 반환")
            
            return reranked[:top_k]
            
        except Exception as e:
            logger.error(f"리랭킹 중 오류: {e}")
            # 원본 점수로 정렬
            return sorted(results, key=lambda x: x.score, reverse=True)[:top_k]
    
    def _prepare_document_text(self, result: SearchResult) -> str:
        """리랭킹용 문서 텍스트 준비
        
        Args:
            result: 검색 결과
            
        Returns:
            준비된 텍스트
        """
        parts = []
        
        # 요약 추가
        if result.summary:
            parts.append(result.summary)
        
        # 답변 내용 추가
        if result.answers:
            answer_text = " ".join(
                f"{k}: {v}" for k, v in result.answers.items() 
                if v and str(v).strip()
            )
            if answer_text:
                parts.append(answer_text)
        
        # 하이라이트 추가
        if result.highlights:
            parts.extend(result.highlights)
        
        # 메타데이터 중 중요한 것들 추가
        if result.metadata:
            important_keys = ["title", "description", "category"]
            for key in important_keys:
                if key in result.metadata and result.metadata[key]:
                    parts.append(f"{key}: {result.metadata[key]}")
        
        # 텍스트 결합 (최대 길이 제한)
        combined = " ".join(parts)
        max_length = 512  # 모델에 따라 조정
        
        if len(combined) > max_length:
            combined = combined[:max_length] + "..."
        
        return combined
    
    def batch_rerank(self, 
                    queries_results: List[tuple], 
                    top_k: Optional[int] = None) -> List[List[SearchResult]]:
        """배치 리랭킹
        
        Args:
            queries_results: [(쿼리, 검색결과리스트)] 리스트
            top_k: 각 쿼리별 반환할 상위 결과 수
            
        Returns:
            리랭킹된 결과 리스트의 리스트
        """
        reranked_results = []
        
        for query, results in queries_results:
            reranked = self.rerank(query, results, top_k)
            reranked_results.append(reranked)
        
        return reranked_results