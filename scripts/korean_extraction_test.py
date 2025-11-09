"""
간단한 한국어 쿼리 추출/분석 테스트 스크립트

사용법:
    python scripts/korean_extraction_test.py --query "20대 남성이 제품 만족도는 어때?"

또는 인터랙티브 모드:
    python scripts/korean_extraction_test.py
    > 30대 여성의 AI 관련 만족도 알려줘

출력:
  - SemanticModel 기반 엔티티 추출 결과
  - AdvancedRAGQueryAnalyzer 분석 결과 요약
  - OpenSearch 하이브리드 쿼리(키워드/필터 기반, 벡터 없을 때 키워드만)
"""

import argparse
import json
import logging
import os
import sys
from typing import Optional

# Ensure project root is on sys.path when running from scripts/
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from rag_query_analyzer.core.semantic_model import SemanticModel
from rag_query_analyzer.analyzers.main_analyzer import AdvancedRAGQueryAnalyzer
from rag_query_analyzer.analyzers.demographic_extractor import DemographicExtractor


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def run_once(query: str, context: str = "") -> None:
    # 1) 의미 모델 엔티티 추출
    semantic_model = SemanticModel()
    extracted = semantic_model.extract_entities(query)

    # 2) 통합 분석기 실행
    analyzer = AdvancedRAGQueryAnalyzer()
    analysis = analyzer.analyze_query(query, context)

    # 3) DemographicEntity 기반 추출 (ExtractedEntities + 요청 수량)
    demo_extractor = DemographicExtractor()
    extracted_entities, requested_size = demo_extractor.extract_with_size(query)

    # 4) 하이브리드 쿼리 구성 (벡터 없음 → 키워드만)
    os_query = analyzer.build_search_query(
        analysis=analysis,
        query_vector=None,
        size=requested_size,
        filters=None,
    )

    # 5) 결과 출력
    print("\n===== 입력 쿼리 =====")
    print(query)

    print("\n===== 엔티티 추출 (SemanticModel) =====")
    print(json.dumps(extracted, ensure_ascii=False, indent=2))

    print("\n===== ExtractedEntities (DemographicEntity 기반) =====")
    print(json.dumps(extracted_entities.to_dict(), ensure_ascii=False, indent=2))

    print("\n----- ExtractedEntities → OpenSearch Filters -----")
    print(json.dumps(extracted_entities.to_filters(), ensure_ascii=False, indent=2))

    print("\n----- Requested size parsed from query -----")
    print(requested_size)

    print("\n===== 분석 결과 요약 (AdvancedRAGQueryAnalyzer) =====")
    print(f"intent: {analysis.intent}")
    print(f"confidence: {analysis.confidence:.2f}")
    print(f"must_terms: {analysis.must_terms}")
    print(f"should_terms: {analysis.should_terms}")
    print(f"must_not_terms: {analysis.must_not_terms}")

    print("\n===== OpenSearch 쿼리 (샘플) =====")
    print(json.dumps(os_query, ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Korean extraction/analyzer test")
    parser.add_argument("--query", type=str, default=None, help="한국어 쿼리 문장")
    args = parser.parse_args()

    if args.query:
        run_once(args.query)
        return

    # 인터랙티브 모드
    print("한국어 문장을 입력하세요. 종료하려면 빈 줄 또는 Ctrl+C.")
    while True:
        try:
            line: Optional[str] = input("> ").strip()
            if not line:
                break
            run_once(line)
        except (EOFError, KeyboardInterrupt):
            print()
            break


if __name__ == "__main__":
    main()


