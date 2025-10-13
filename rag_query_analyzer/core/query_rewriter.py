import logging
from typing import List, Tuple, Optional
import anthropic
from ..config import Config

logger = logging.getLogger(__name__)


class MultiStepQueryRewriter:
    """다단계 쿼리 재작성 전략
    
    원본 쿼리를 다양한 관점에서 재작성하여 검색 품질을 향상시킵니다.
    """
    
    def __init__(self, config: Config = None):
        """초기화
        
        Args:
            config: 설정 객체
        """
        self.config = config or Config()
        self.client = anthropic.Anthropic(api_key=self.config.CLAUDE_API_KEY)
        logger.info("MultiStepQueryRewriter 초기화 완료")
    
    def rewrite_query(self, 
                    original_query: str, 
                    survey_context: str = "") -> List[Tuple[str, str]]:
        """원본 쿼리를 여러 변형으로 재작성
        
        Args:
            original_query: 원본 쿼리
            survey_context: 설문 맥락
            
        Returns:
            [(재작성_유형, 재작성된_쿼리)] 리스트
        """
        rewrites = []
        
        try:
            # 1. HyDE (Hypothetical Document Embeddings)
            hyde_doc = self._generate_hyde(original_query, survey_context)
            if hyde_doc:
                rewrites.append(("HyDE", hyde_doc))
            
            # 2. 다중 관점 재작성
            perspectives = self._rewrite_perspectives(original_query, survey_context)
            rewrites.extend(perspectives)
            
            # 3. 구체화 재작성
            specific_query = self._make_specific(original_query)
            if specific_query and specific_query != original_query:
                rewrites.append(("specific", specific_query))
            
            # 4. 추상화 재작성
            abstract_query = self._make_abstract(original_query)
            if abstract_query and abstract_query != original_query:
                rewrites.append(("abstract", abstract_query))
            
            logger.info(f"✅ {len(rewrites)}개의 쿼리 재작성 완료")
            
        except Exception as e:
            logger.warning(f"쿼리 재작성 실패: {e}")
            rewrites.append(("original", original_query))
        
        return rewrites[:self.config.MAX_REWRITTEN_QUERIES]
    
    def _generate_hyde(self, query: str, context: str) -> str:
        """HyDE 문서 생성
        
        가상의 이상적인 응답 문서를 생성합니다.
        """
        prompt = f"""
다음 질문에 대한 이상적인 설문 응답자의 특성을 상세히 설명해주세요.
실제 설문 데이터에서 이런 응답을 한 사람의 프로필을 작성하는 것처럼 작성해주세요.

질문: {query}
설문 맥락: {context if context else '일반 설문조사'}

이상적인 응답자 프로필 (구체적인 특성 나열):
"""
        return self._call_claude(prompt, max_tokens=300)
    
    def _rewrite_perspectives(self, 
                            query: str, 
                            context: str) -> List[Tuple[str, str]]:
        """다양한 관점에서 쿼리 재작성"""
        perspectives = [
            ("demographic", "인구통계학적 조건(나이, 성별, 지역, 직업 등)으로"),
            ("behavioral", "행동 패턴과 활동 중심으로"),
            ("psychological", "심리적/감정적 상태와 태도 중심으로")
        ]
        
        results = []
        for persp_type, perspective in perspectives:
            prompt = f"""
다음 쿼리를 {perspective} 재작성해주세요. 원본 의미는 유지하되, 해당 관점을 강조하세요.

원본 쿼리: {query}
설문 맥락: {context if context else '일반 설문조사'}

재작성된 쿼리 (한 문장으로):
"""
            rewritten = self._call_claude(prompt, max_tokens=150)
            if rewritten:
                results.append((persp_type, rewritten))
        
        return results
    
    def _make_specific(self, query: str) -> str:
        """쿼리를 더 구체적으로 만들기"""
        prompt = f"""
다음 쿼리를 더 구체적이고 명확하게 만들어주세요.
모호한 표현을 구체적인 조건으로 바꿔주세요.

원본: {query}

구체화된 쿼리:
"""
        return self._call_claude(prompt, max_tokens=100)
    
    def _make_abstract(self, query: str) -> str:
        """쿼리를 더 추상적으로 만들기"""
        prompt = f"""
다음 쿼리를 더 일반적이고 포괄적으로 만들어주세요.
구체적인 조건을 더 넓은 개념으로 확장하세요.

원본: {query}

추상화된 쿼리:
"""
        return self._call_claude(prompt, max_tokens=100)
    
    def _call_claude(self, prompt: str, max_tokens: int = 500) -> str:
        """Claude API 호출 헬퍼"""
        try:
            response = self.client.messages.create(
                model=self.config.CLAUDE_MODEL_FAST,
                max_tokens=max_tokens,
                temperature=0.3,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text.strip()
        except Exception as e:
            logger.error(f"Claude API 오류: {e}")
            return ""

