import re
import json
import time
import logging
from typing import Optional, Dict
import anthropic
from .base import BaseAnalyzer
from ..models.query import QueryAnalysis
from ..config import Config

logger = logging.getLogger(__name__)


class ClaudeAnalyzer(BaseAnalyzer):
    """Claude API 기반 쿼리 분석기
    
    Claude를 사용하여 자연어 쿼리를 심층 분석합니다.
    """
    
    def __init__(self, config: Config = None):
        """초기화
        
        Args:
            config: 설정 객체
        """
        self.config = config or Config()
        self.client = anthropic.Anthropic(api_key=self.config.CLAUDE_API_KEY)
        logger.info("ClaudeAnalyzer 초기화 완료")
    
    def get_name(self) -> str:
        """분석기 이름 반환"""
        return "ClaudeAnalyzer"
    
    def analyze(self, query: str, context: str = "") -> QueryAnalysis:
        """Claude를 사용한 쿼리 분석
        
        Args:
            query: 분석할 쿼리
            context: 추가 맥락
            
        Returns:
            분석 결과
        """
        start_time = time.time()
        
        if not self.validate_query(query):
            return self._create_empty_analysis()
        
        query = self.preprocess_query(query)
        
        # 프롬프트 구성
        prompt = self._build_prompt(query, context)
        
        try:
            # Claude API 호출
            response = self.client.messages.create(
                model=self.config.CLAUDE_MODEL,
                max_tokens=self.config.CLAUDE_MAX_TOKENS,
                temperature=self.config.CLAUDE_TEMPERATURE,
                messages=[{"role": "user", "content": prompt}]
            )
            
            # 응답 파싱
            analysis = self._parse_response(response.content[0].text)
            analysis.execution_time = time.time() - start_time
            analysis.analyzer_used = self.get_name()
            
            return analysis
            
        except Exception as e:
            logger.error(f"Claude API 오류: {e}")
            return self._create_fallback_analysis(query, str(e))
    
    def _build_prompt(self, query: str, context: str) -> str:
        """Claude용 프롬프트 구성"""
        return f"""
설문조사 데이터 검색을 위한 쿼리를 분석해주세요.

쿼리: "{query}"
맥락: {context if context else "일반 설문조사"}

다음 단계별로 분석하고 JSON으로 출력하세요:

1. 검색 의도 파악
   - exact_match: 정확한 조건 매칭이 필요
   - semantic_search: 의미적 유사성 검색이 필요
   - hybrid: 둘 다 필요

2. 키워드 추출
   - must_terms: 반드시 포함되어야 할 키워드 (AND)
   - should_terms: 포함되면 좋은 키워드 (OR)
   - must_not_terms: 제외해야 할 키워드

3. 키워드 확장
   - 각 주요 키워드의 동의어, 유사어, 관련어

4. 검색 파라미터
   - alpha: 0.0~1.0 (키워드 중심이면 낮게, 의미 중심이면 높게)

JSON 형식:
{{
  "reasoning_steps": ["분석 과정 설명"],
  "intent": "exact_match|semantic_search|hybrid",
  "must_terms": ["필수키워드"],
  "should_terms": ["선택키워드"],
  "must_not_terms": ["제외키워드"],
  "alpha": 0.5,
  "expanded_keywords": {{"키워드": ["확장어"]}},
  "confidence": 0.85,
  "explanation": "최종 설명"
}}
"""
    
    def _parse_response(self, response_text: str) -> QueryAnalysis:
        """Claude 응답 파싱"""
        try:
            # JSON 추출
            json_match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
            if json_match:
                json_text = json_match.group(1)
            else:
                # JSON 마커가 없으면 중괄호 찾기
                json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
                json_text = json_match.group(0) if json_match else response_text
            
            # JSON 파싱
            data = json.loads(json_text)
            
            return QueryAnalysis(
                intent=data.get("intent", "hybrid"),
                must_terms=data.get("must_terms", []),
                should_terms=data.get("should_terms", []),
                must_not_terms=data.get("must_not_terms", []),
                alpha=float(data.get("alpha", 0.5)),
                expanded_keywords=data.get("expanded_keywords", {}),
                confidence=float(data.get("confidence", 0.5)),
                explanation=data.get("explanation", ""),
                reasoning_steps=data.get("reasoning_steps", [])
            )
            
        except Exception as e:
            logger.error(f"응답 파싱 실패: {e}")
            raise
    
    def _create_fallback_analysis(self, query: str, error: str) -> QueryAnalysis:
        """폴백 분석 결과"""
        return QueryAnalysis(
            intent="hybrid",
            must_terms=[query],
            should_terms=[],
            must_not_terms=[],
            alpha=0.5,
            expanded_keywords={},
            confidence=0.1,
            explanation=f"Claude API 오류로 기본값 사용: {error}",
            analyzer_used=self.get_name(),
            fallback_used=True
        )
    
    def _create_empty_analysis(self) -> QueryAnalysis:
        """빈 분석 결과"""
        return QueryAnalysis(
            intent="hybrid",
            must_terms=[],
            should_terms=[],
            must_not_terms=[],
            alpha=0.5,
            expanded_keywords={},
            confidence=0.0,
            explanation="유효하지 않은 쿼리",
            analyzer_used=self.get_name()
        )