#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""음주 여부 자동 추출 테스트"""
import sys
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from api.search_api import extract_behavioral_conditions_from_query

test_queries = [
    "술 마신 경험이 있는 20대 남성",
    "술을 마시지 않는 사람",
    "비음주자 찾아줘",
    "흡연자이면서 술도 마시는 사람",
    "술 안 마시고 담배도 안 피는 사람",
    "차량 보유하고 음주 경험 있는 사람",
]

print("=" * 80)
print("음주 여부 자동 추출 테스트")
print("=" * 80)

for query in test_queries:
    print(f"\n쿼리: '{query}'")
    print("-" * 80)

    conditions = extract_behavioral_conditions_from_query(query)

    if conditions:
        print(f"✅ 추출된 behavioral 조건:")
        for key, value in conditions.items():
            print(f"   - {key}: {value}")
    else:
        print("❌ 추출된 조건 없음")

print("\n" + "=" * 80)
print("테스트 완료!")
print("=" * 80)
