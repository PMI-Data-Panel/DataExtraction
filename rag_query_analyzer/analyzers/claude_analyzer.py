import re
import json
import time
import logging
from typing import List
import anthropic
from rag_query_analyzer.core.semantic_model import SemanticModel
from .base import BaseAnalyzer
from ..models.query import FilterClause, FilterCondition, FilterOperator, QueryAnalysis
from ..config import Config

logger = logging.getLogger(__name__)


class ClaudeAnalyzer(BaseAnalyzer):
    """Claude API 기반 쿼리 분석기
    
    Claude를 사용하여 자연어 쿼리를 심층 분석합니다.
    """
    
    def __init__(self, semantic_model: SemanticModel, config: Config = None):
        """초기화"""
        self.config = config or Config()
        self.semantic_model = semantic_model # SemanticModel 인스턴스 저장
        self.client = anthropic.Anthropic(api_key=self.config.CLAUDE_API_KEY)
        logger.info("ClaudeAnalyzer 초기화 완료")
    
    def get_name(self) -> str:
        """분석기 이름 반환"""
        return "ClaudeAnalyzer"
    
    def analyze(self, query: str, context: str = "") -> QueryAnalysis:
        """Claude를 사용한 쿼리 분석"""
        start_time = time.time()
        
        if not self.validate_query(query):
            return self._create_empty_analysis()
        
        query = self.preprocess_query(query)
        
        # self.semantic_model 인스턴스필요(초기화 시 주입).
        available_fields = self.semantic_model.get_all_attribute_names()
        # 프롬프트 구성 시 이 정보를 함께 전달
        prompt = self._build_prompt(query, context, available_fields)
        
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
    
    def _build_prompt(self, query: str, context: str, fields: List[str]) -> str:
        """Claude용 프롬프트 구성 (개선됨)"""
        return f"""
당신은 설문조사 데이터 분석 전문가입니다. 사용자의 자연어 쿼리를 분석하여 Elasticsearch 검색에 사용할 수 있는 JSON 객체를 생성해야 합니다.

<사용_가능한_필드>
{fields}
</사용_가능한_필드>
- 반드시 위 목록에 있는 필드 이름만 사용해야 합니다.

<출력_가이드>
- 모든 분석 결과를 단일 JSON 객체로 출력해야 합니다.
- `filters` 배열에는 쿼리를 충족하는 데 필요한 모든 조건을 담아야 합니다.

<예시>
쿼리: "미혼인 자영업자"
JSON:
```json
{{
    "reasoning_steps": ["두 가지 인구통계학적 조건을 AND로 결합합니다."],
    "intent": "filter_only",
    "filters": [
    {{ "field": "결혼여부", "operator": "term", "value": "미혼", "clause": "must" }},
    {{ "field": "직업", "operator": "term", "value": "자영업", "clause": "must" }}
    ],
    "target_metric": null,
    "alpha": 0.1,
    "confidence": 0.95,
    "explanation": "결혼여부가 '미혼'이고 직업이 '자영업'인 응답자를 찾습니다."
}}

이제 다음 쿼리를 분석해주세요.

<쿼리>
{query}
</쿼리>

<맥락>
{context if context else "일반 설문조사"}
</맥락>

분석 결과 JSON:
"""
    
    def _parse_response(self, response_text: str) -> QueryAnalysis:
        """Claude 응답 파싱"""
        try:
            # JSON 텍스트 추출
            json_match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
            if not json_match:
                json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            
            if not json_match:
                raise ValueError("응답에서 JSON 객체를 찾을 수 없습니다.")

            json_text = json_match.group(1) if len(json_match.groups()) > 0 else json_match.group(0)
            
            data = json.loads(json_text)

            # [버그 수정] filters 파싱 로직 추가
            filters = []
            if "filters" in data and isinstance(data["filters"], list):
                for f in data["filters"]:
                    try:
                        filters.append(
                            FilterCondition(
                                field=f["field"],
                                operator=FilterOperator(f["operator"]),
                                value=f["value"],
                                clause=FilterClause(f.get("clause", "must")),
                            )
                        )
                    except (KeyError, ValueError) as e:
                        logger.warning(f"잘못된 필터 객체 건너뛰기: {f}. 오류: {e}")
            
            # JSON 파싱
            data = json.loads(json_text)
            return QueryAnalysis(
                intent=data.get("intent", "hybrid"),
                filters=filters, # 수정된 필터 리스트 사용
                keywords=data.get("keywords", []), # must_terms 대신 keywords 사용 제안
                alpha=float(data.get("alpha", 0.5)),
                confidence=float(data.get("confidence", 0.5)),
                explanation=data.get("explanation", ""),
                reasoning_steps=data.get("reasoning_steps", []),
                target_metric=data.get("target_metric")
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