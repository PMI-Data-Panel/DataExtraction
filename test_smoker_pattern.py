#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""담배피고 패턴 테스트"""
import sys
from rag_query_analyzer.analyzers.rule_analyzer import RuleBasedAnalyzer
from rag_query_analyzer.config import get_config

# UTF-8 출력 설정
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

config = get_config()
analyzer = RuleBasedAnalyzer(config)

test_queries = [
    "담배피고 차를 소유하는 30대 남성",
    "흡연하고 차를 소유하는 30대 남성",
    "담배 피우고 차 있는 30대 남자"
]

print("=" * 80)
print("Smoker 패턴 테스트")
print("=" * 80)

for query in test_queries:
    print(f"\n쿼리: '{query}'")
    print("-" * 80)

    result = analyzer.analyze(query)

    print(f"Behavioral conditions: {result.behavioral_conditions}")
    print(f"Must terms: {result.must_terms}")
    print(f"Should terms: {result.should_terms}")

    if result.behavioral_conditions.get('smoker') == True:
        print("✅ 흡연 조건 인식 성공!")
    else:
        print("❌ 흡연 조건 인식 실패!")

    if result.behavioral_conditions.get('has_vehicle') == True:
        print("✅ 차량 조건 인식 성공!")
    else:
        print("❌ 차량 조건 인식 실패!")
