#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Metadata 필드 값 확인 스크립트"""
import json
import sys
from opensearchpy import OpenSearch
from rag_query_analyzer.config import get_config

# UTF-8 출력 설정
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

config = get_config()

# OpenSearch 연결
client = OpenSearch(
    hosts=[{'host': config.OPENSEARCH_HOST, 'port': config.OPENSEARCH_PORT}],
    http_auth=(config.OPENSEARCH_USER, config.OPENSEARCH_PASSWORD),
    use_ssl=config.OPENSEARCH_USE_SSL,
    verify_certs=config.OPENSEARCH_VERIFY_CERTS,
    ssl_assert_hostname=config.OPENSEARCH_SSL_ASSERT_HOSTNAME,
    ssl_show_warn=False,
    timeout=60
)

print("=" * 80)
print("Metadata 필드 값 확인")
print("=" * 80)

# 1. s_welcome_2nd 인덱스의 gender 집계
print("\n[1] s_welcome_2nd - gender 값 집계")
print("-" * 80)
agg1 = {
    "size": 0,
    "aggs": {
        "genders": {
            "terms": {"field": "metadata.gender", "size": 20}
        }
    }
}

try:
    response = client.search(index="s_welcome_2nd", body=agg1)
    buckets = response['aggregations']['genders']['buckets']
    for bucket in buckets:
        print(f"  {bucket['key']}: {bucket['doc_count']}건")
except Exception as e:
    print(f"에러: {e}")

# 2. s_welcome_2nd 인덱스의 age_group 집계
print("\n[2] s_welcome_2nd - age_group 값 집계")
print("-" * 80)
agg2 = {
    "size": 0,
    "aggs": {
        "ages": {
            "terms": {"field": "metadata.age_group", "size": 20}
        }
    }
}

try:
    response = client.search(index="s_welcome_2nd", body=agg2)
    buckets = response['aggregations']['ages']['buckets']
    for bucket in buckets:
        print(f"  {bucket['key']}: {bucket['doc_count']}건")
except Exception as e:
    print(f"에러: {e}")

# 3. welcome_all 인덱스의 gender 집계
print("\n[3] welcome_all - gender 값 집계")
print("-" * 80)
try:
    response = client.search(index="welcome_all", body=agg1)
    buckets = response['aggregations']['genders']['buckets']
    for bucket in buckets:
        print(f"  {bucket['key']}: {bucket['doc_count']}건")
except Exception as e:
    print(f"에러: {e}")

# 4. welcome_all 인덱스의 age_group 집계
print("\n[4] welcome_all - age_group 값 집계")
print("-" * 80)
try:
    response = client.search(index="welcome_all", body=agg2)
    buckets = response['aggregations']['ages']['buckets']
    for bucket in buckets:
        print(f"  {bucket['key']}: {bucket['doc_count']}건")
except Exception as e:
    print(f"에러: {e}")

# 5. welcome_all에서 30대 남성 찾기
print("\n[5] welcome_all - 30대 남성 검색 (여러 조합 시도)")
print("-" * 80)

gender_values = ["남자", "남성", "male", "Male", "MALE", "M", "m"]
age_values = ["30대", "30", "thirties", "30s"]

for gender in gender_values:
    for age in age_values:
        query = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"metadata.gender": gender}},
                        {"term": {"metadata.age_group": age}}
                    ]
                }
            }
        }
        try:
            response = client.search(index="welcome_all", body=query)
            total = response['hits']['total']['value']
            if total > 0:
                print(f"  ✅ gender='{gender}', age_group='{age}': {total}건")
        except:
            pass

# 6. answer 필드 타입 확인 (매핑)
print("\n[6] s_welcome_2nd - qa_pairs.answer 필드 매핑")
print("-" * 80)
try:
    mapping = client.indices.get_mapping(index="s_welcome_2nd")
    qa_pairs_props = mapping['s_welcome_2nd']['mappings']['properties']['qa_pairs']['properties']
    if 'answer' in qa_pairs_props:
        answer_type = qa_pairs_props['answer']
        print(f"  answer 필드 타입: {json.dumps(answer_type, indent=2, ensure_ascii=False)}")
except Exception as e:
    print(f"에러: {e}")

# 7. match 쿼리로 answer='있다' 찾기 (text 타입이면 match가 작동)
print("\n[7] match 쿼리로 answer='있다' + q_text='보유차량여부'")
print("-" * 80)
query7 = {
    "size": 0,
    "query": {
        "nested": {
            "path": "qa_pairs",
            "query": {
                "bool": {
                    "must": [
                        {"match": {"qa_pairs.q_text": "보유차량여부"}},
                        {"match": {"qa_pairs.answer": "있다"}}  # match 쿼리 사용
                    ]
                }
            }
        }
    }
}

try:
    response = client.search(index="s_welcome_2nd", body=query7)
    total = response['hits']['total']['value']
    print(f"결과: {total}건")
    if total > 0:
        print("  ✅ match 쿼리가 작동합니다! answer는 text 타입입니다.")
except Exception as e:
    print(f"에러: {e}")

print("\n" + "=" * 80)
print("확인 완료!")
print("=" * 80)
