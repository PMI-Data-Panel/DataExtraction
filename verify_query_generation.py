#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""실제 API 로직에 맞춘 쿼리 생성 검증 스크립트."""

import copy
import json
import os
import sys
from typing import Any, Dict, List

from opensearchpy import OpenSearch
from rag_query_analyzer.analyzers.demographic_extractor import DemographicExtractor
from rag_query_analyzer.analyzers.main_analyzer import AdvancedRAGQueryAnalyzer
from rag_query_analyzer.config import get_config
from connectors.hybrid_searcher import OpenSearchHybridQueryBuilder

# FastAPI 모듈을 import 할 수 있도록 경로 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from api.search_api import build_behavioral_filters  # type: ignore  # pylint: disable=wrong-import-position

# UTF-8 출력 설정 (Windows 대응)
if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

config = get_config()

analyzer = AdvancedRAGQueryAnalyzer(config)
extractor = DemographicExtractor()
query_builder = OpenSearchHybridQueryBuilder(config)

client = OpenSearch(
    hosts=[{"host": config.OPENSEARCH_HOST, "port": config.OPENSEARCH_PORT}],
    http_auth=(config.OPENSEARCH_USER, config.OPENSEARCH_PASSWORD),
    use_ssl=config.OPENSEARCH_USE_SSL,
    verify_certs=config.OPENSEARCH_VERIFY_CERTS,
    ssl_assert_hostname=config.OPENSEARCH_SSL_ASSERT_HOSTNAME,
    ssl_show_warn=False,
    timeout=60,
)


def ensure_bool_query(query: Any) -> Dict[str, Any]:
    """기존 쿼리를 bool 형태로 감싸고 filter 리스트를 확보한다."""
    if not query or query in ({"match_all": {}}, {"match_none": {}}):
        return {"bool": {"must": [], "filter": []}}

    if isinstance(query, dict) and "bool" in query:
        bool_clause = copy.deepcopy(query)
        bool_clause["bool"].setdefault("must", [])
        bool_clause["bool"].setdefault("should", [])
        bool_clause["bool"].setdefault("filter", [])
        return bool_clause

    return {"bool": {"must": [copy.deepcopy(query)], "filter": []}}


def count_nested_queries(query_dict: Any, path: str = "qa_pairs") -> int:
    """특정 path의 nested 쿼리 개수를 세어 교집합 문제를 탐지한다."""
    count = 0
    if isinstance(query_dict, dict):
        for key, value in query_dict.items():
            if key == "nested" and isinstance(value, dict):
                if value.get("path") == path:
                    count += 1
                count += count_nested_queries(value.get("query"), path)
            else:
                count += count_nested_queries(value, path)
    elif isinstance(query_dict, list):
        for item in query_dict:
            count += count_nested_queries(item, path)
    return count


def display_filters(filters: List[Dict[str, Any]]) -> None:
    """필터 절을 사람이 읽기 좋게 출력한다."""
    if not filters:
        print("  (필터 없음)")
        return
    for idx, clause in enumerate(filters, 1):
        if "nested" in clause:
            print(f"  {idx}. nested (path={clause['nested'].get('path')})")
        elif "term" in clause:
            field, value = next(iter(clause["term"].items()))
            print(f"  {idx}. term ({field}={value})")
        elif "match" in clause:
            field, value = next(iter(clause["match"].items()))
            print(f"  {idx}. match ({field}={value})")
        else:
            print(f"  {idx}. {list(clause.keys())}")


print("=" * 80)
print("쿼리 생성 검증 (현재 API 로직 기준)")
print("=" * 80)

test_queries = [
    "30대 남성 100명",
    "차를 소유하는 30대 남성 100명",
]

for query_text in test_queries:
    print(f"\n{'=' * 80}")
    print(f"쿼리: {query_text}")
    print("=" * 80)

    try:
        # 1. 쿼리 분석
        print("\n[1단계] 쿼리 분석...")
        analysis = analyzer.analyze_query(query_text, use_claude=config.ENABLE_CLAUDE_ANALYZER)
        if analysis is None:
            raise RuntimeError("Analyzer returned None")

        print("\n분석 결과:")
        print(f"  - must_terms: {analysis.must_terms}")
        print(f"  - should_terms: {analysis.should_terms}")
        print(f"  - behavioral_conditions: {analysis.behavioral_conditions}")

        # 2. 엔티티 추출
        print("\n[2단계] 엔티티 추출...")
        extracted_entities, requested_size = extractor.extract_with_size(query_text)

        print("\n추출된 엔티티:")
        print(f"  - Demographics: {len(extracted_entities.demographics)}개")
        for demo in extracted_entities.demographics:
            print(f"    • {demo.name}({demo.canonical_form}) → value={demo.value}, raw={demo.raw_value}")
        if analysis.behavioral_conditions:
            print(f"  - Behavioral conditions: {analysis.behavioral_conditions}")
        else:
            print("  - Behavioral conditions: 없음")

        # 3. 필터 구성 (실제 /search/nl 로직과 동일한 파라미터)
        filters: List[Dict[str, Any]] = []
        for demo in extracted_entities.demographics:
            filter_clause = demo.to_opensearch_filter(
                metadata_only=True,
                include_qa_fallback=False,
            )
            if filter_clause and filter_clause != {"match_all": {}}:
                filters.append(filter_clause)

        if analysis.behavioral_conditions:
            filters.extend(build_behavioral_filters(analysis.behavioral_conditions))

        print("\n생성된 필터 절:")
        display_filters(filters)

        # 4. OpenSearch 하이브리드 쿼리 생성
        print("\n[3단계] OpenSearch 쿼리 빌드...")
        size_hint = requested_size or config.FINAL_RESULT_SIZE
        size_hint = max(1, min(size_hint, config.FINAL_RESULT_SIZE))

        base_body = query_builder.build_query(
            analysis=analysis,
            query_vector=None,
            size=size_hint,
        )

        final_body = copy.deepcopy(base_body)
        existing_query = final_body.get("query")

        if filters:
            bool_query = ensure_bool_query(existing_query)
            bool_query["bool"]["filter"].extend(filters)
            final_body["query"] = bool_query
        else:
            if existing_query is None:
                final_body["query"] = {"match_all": {}}

        print("\n생성된 OpenSearch 본문:")
        print("-" * 80)
        print(json.dumps(final_body, indent=2, ensure_ascii=False))
        print("-" * 80)

        # 5. nested 쿼리 개수 확인
        nested_count = count_nested_queries(final_body.get("query"))
        print(f"\n⚠️ qa_pairs path의 nested 쿼리 개수: {nested_count}개")
        if nested_count > 1:
            print("   ❌ 경고: 하나의 문서에서 모든 조건을 동시에 만족해야 하므로 교집합 이슈 발생 가능!")
        else:
            print("   ✅ OK: nested 쿼리가 1개 이하입니다.")

        # 6. 실제 OpenSearch 실행
        print("\n[4단계] OpenSearch 실행...")
        response = client.search(
            index=config.WELCOME_INDEX,
            body=final_body,
        )

        total = response["hits"]["total"]["value"]
        print(f"결과: {total}건")
        if total == 0 and "차를 소유" in query_text:
            print("❌ 여전히 0건입니다. 필터 구조를 다시 점검해 보세요.")
        elif total > 0:
            print(f"✅ 성공! {total}건 검색됨")

    except Exception as exc:  # pylint: disable=broad-except
        print(f"에러 발생: {exc}")
        import traceback

        traceback.print_exc()

print("\n" + "=" * 80)
print("검증 완료!")
print("=" * 80)
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""실제 API 코드 경로를 사용한 쿼리 생성 검증"""
import json
import sys
from opensearchpy import OpenSearch
from rag_query_analyzer.config import get_config

# UTF-8 출력 설정
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

config = get_config()

# API 코드에서 필요한 부분들을 임포트
# 실제 쿼리 생성 로직을 사용하기 위해
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("=" * 80)
print("쿼리 생성 검증 (실제 API 코드 경로 사용)")
print("=" * 80)

# 테스트 쿼리들
test_queries = [
    "30대 남성 100명",
    "차를 소유하는 30대 남성 100명"
]

for query_text in test_queries:
    print(f"\n{'=' * 80}")
    print(f"쿼리: {query_text}")
    print("=" * 80)

    try:
        # 실제 API의 쿼리 분석 수행
        from api.search_api import (
            analyze_query_with_retries,
            build_opensearch_query,
            extract_entities_from_analysis
        )

        # 1. 쿼리 분석
        print("\n[1단계] 쿼리 분석...")
        analysis = analyze_query_with_retries(query_text)

        print(f"\n분석 결과:")
        print(f"  - must_terms: {analysis.must_terms}")
        print(f"  - should_terms: {analysis.should_terms}")
        print(f"  - behavioral_conditions: {analysis.behavioral_conditions}")

        # 2. 엔티티 추출
        print(f"\n[2단계] 엔티티 추출...")
        extracted = extract_entities_from_analysis(analysis)

        print(f"\n추출된 엔티티:")
        print(f"  - Demographics: {len(extracted.demographics)}개")
        for demo in extracted.demographics:
            print(f"    • {demo.field}: {demo.value}")
        print(f"  - Behavioral: {len(extracted.behavioral)}개")
        for behav in extracted.behavioral:
            print(f"    • {behav.condition}: {behav.value}")

        # 3. OpenSearch 쿼리 빌드
        print(f"\n[3단계] OpenSearch 쿼리 빌드...")

        # 실제 API에서 사용하는 파라미터 설정
        from api.search_api import (
            build_text_queries_for_opensearch,
            apply_filters_to_query
        )

        # 텍스트 쿼리 빌드
        text_query_clauses = build_text_queries_for_opensearch(
            must_terms=analysis.must_terms,
            should_terms=analysis.should_terms,
            embedding_vector=None,  # 벡터는 생략
            enable_vector=False
        )

        # 기본 쿼리 구조
        if text_query_clauses:
            base_query = {
                "bool": {
                    "must": text_query_clauses.get("must", []),
                    "should": text_query_clauses.get("should", []),
                    "minimum_should_match": text_query_clauses.get("minimum_should_match", 0)
                }
            }
        else:
            base_query = {"match_all": {}}

        # 필터 적용
        final_query = apply_filters_to_query(
            base_query=base_query,
            extracted_entities=extracted,
            metadata_only=True,  # ✅ 검증: 이 값이 실제로 사용되는지 확인
            include_nested_fallback=False  # ✅ 검증: 이 값이 실제로 사용되는지 확인
        )

        print(f"\n생성된 OpenSearch 쿼리:")
        print("-" * 80)
        print(json.dumps(final_query, indent=2, ensure_ascii=False))
        print("-" * 80)

        # 4. 쿼리 구조 분석
        print(f"\n[쿼리 구조 분석]")

        if "bool" in final_query:
            bool_query = final_query["bool"]

            # must 절 확인
            if "must" in bool_query:
                print(f"\nMUST 절 ({len(bool_query['must'])}개):")
                for i, clause in enumerate(bool_query['must'], 1):
                    if "nested" in clause:
                        nested_path = clause["nested"]["path"]
                        print(f"  {i}. nested (path={nested_path})")
                        # nested 내부 쿼리 확인
                        inner_query = clause["nested"]["query"]
                        print(f"     내부 쿼리: {list(inner_query.keys())}")
                    else:
                        print(f"  {i}. {list(clause.keys())}")

            # filter 절 확인
            if "filter" in bool_query:
                print(f"\nFILTER 절 ({len(bool_query['filter'])}개):")
                for i, clause in enumerate(bool_query['filter'], 1):
                    if "nested" in clause:
                        nested_path = clause["nested"]["path"]
                        print(f"  {i}. nested (path={nested_path})")
                    elif "term" in clause:
                        field = list(clause["term"].keys())[0]
                        value = clause["term"][field]
                        print(f"  {i}. term ({field}={value})")
                    elif "match" in clause:
                        field = list(clause["match"].keys())[0]
                        value = clause["match"][field]
                        print(f"  {i}. match ({field}={value})")
                    else:
                        print(f"  {i}. {list(clause.keys())}")

        # 5. 중요: nested 쿼리 개수 확인
        def count_nested_queries(query_dict, path="qa_pairs"):
            """특정 path의 nested 쿼리 개수 카운트"""
            count = 0
            if isinstance(query_dict, dict):
                for key, value in query_dict.items():
                    if key == "nested" and isinstance(value, dict):
                        if value.get("path") == path:
                            count += 1
                    elif isinstance(value, (dict, list)):
                        count += count_nested_queries(value, path)
            elif isinstance(query_dict, list):
                for item in query_dict:
                    count += count_nested_queries(item, path)
            return count

        nested_count = count_nested_queries(final_query)
        print(f"\n⚠️ qa_pairs path의 nested 쿼리 개수: {nested_count}개")
        if nested_count > 1:
            print(f"   ❌ 문제: must 절에 {nested_count}개의 nested 쿼리가 있으면 교집합 문제 발생!")
            print(f"   → 하나의 qa_pair가 모든 조건을 만족해야 함")
        else:
            print(f"   ✅ OK: nested 쿼리가 1개 이하입니다.")

        # 6. 실제 OpenSearch 실행
        print(f"\n[4단계] OpenSearch 실행...")
        client = OpenSearch(
            hosts=[{'host': config.OPENSEARCH_HOST, 'port': config.OPENSEARCH_PORT}],
            http_auth=(config.OPENSEARCH_USER, config.OPENSEARCH_PASSWORD),
            use_ssl=config.OPENSEARCH_USE_SSL,
            verify_certs=config.OPENSEARCH_VERIFY_CERTS,
            ssl_assert_hostname=config.OPENSEARCH_SSL_ASSERT_HOSTNAME,
            ssl_show_warn=False,
            timeout=60
        )

        response = client.search(
            index=config.WELCOME_INDEX,
            body={"query": final_query, "size": 0}
        )

        total = response['hits']['total']['value']
        print(f"결과: {total}건")

        if total == 0 and "차를 소유" in query_text:
            print(f"\n❌ 여전히 0건입니다!")
            print(f"   → 쿼리 구조를 다시 확인해야 합니다.")
        elif total > 0:
            print(f"\n✅ 성공! {total}건 검색됨")

    except Exception as e:
        print(f"에러 발생: {e}")
        import traceback
        traceback.print_exc()

print("\n" + "=" * 80)
print("검증 완료!")
print("=" * 80)
