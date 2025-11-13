#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""수정된 필터 검증 스크립트"""
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
print("수정된 차량 필터 검증")
print("=" * 80)

BEHAVIOR_YES_TOKENS = [
    "있다", "있음", "있어요", "yes", "y", "보유", "보유함", "보유중", "한다", "합니다", "해요"
]

# 테스트 1: match 쿼리로 30대 남성 + 차량 있음
print("\n[테스트 1] 30대 남성 + 차량 있음 (match 쿼리)")
print("-" * 80)

answer_should = [
    {"match": {"qa_pairs.answer": kw}}
    for kw in BEHAVIOR_YES_TOKENS
]

query1 = {
    "size": 3,
    "query": {
        "bool": {
            "must": [
                {
                    "nested": {
                        "path": "qa_pairs",
                        "query": {
                            "bool": {
                                "must": [
                                    {"match": {"qa_pairs.q_text": "보유차량여부"}},
                                    {"bool": {"should": answer_should, "minimum_should_match": 1}}
                                ]
                            }
                        }
                    }
                }
            ],
            "filter": [
                {"match": {"metadata.gender": "남자"}},
                {"match": {"metadata.age_group": "30대"}}
            ]
        }
    },
    "_source": ["user_id", "metadata", "qa_pairs"]
}

try:
    response = client.search(index="welcome_all", body=query1)
    total = response['hits']['total']['value']
    print(f"✅ 결과: {total}건")

    if total > 0:
        print("\n샘플 결과:")
        for hit in response['hits']['hits'][:3]:
            user_id = hit['_source'].get('user_id')
            gender = hit['_source'].get('metadata', {}).get('gender')
            age = hit['_source'].get('metadata', {}).get('age_group')
            qa_pairs = hit['_source'].get('qa_pairs', [])
            vehicle_qa = [qa for qa in qa_pairs if '차량' in qa.get('q_text', '')]
            if vehicle_qa:
                print(f"  User: {user_id}, {gender}/{age}, Answer: {vehicle_qa[0].get('answer')}")
        print("\n🎉 성공! 차량 필터가 작동합니다!")
    else:
        print("❌ 실패: 여전히 0건입니다.")
except Exception as e:
    print(f"❌ 에러: {e}")

# 테스트 2: 간단한 버전 (answer='있다'만)
print("\n[테스트 2] 30대 남성 + answer='있다' (단순 match)")
print("-" * 80)

query2 = {
    "size": 0,
    "query": {
        "bool": {
            "must": [
                {
                    "nested": {
                        "path": "qa_pairs",
                        "query": {
                            "bool": {
                                "must": [
                                    {"match": {"qa_pairs.q_text": "보유차량여부"}},
                                    {"match": {"qa_pairs.answer": "있다"}}
                                ]
                            }
                        }
                    }
                }
            ],
            "filter": [
                {"match": {"metadata.gender": "남자"}},
                {"match": {"metadata.age_group": "30대"}}
            ]
        }
    }
}

try:
    response = client.search(index="welcome_all", body=query2)
    total = response['hits']['total']['value']
    print(f"결과: {total}건")
    if total > 0:
        print("✅ 단순 match도 작동합니다!")
except Exception as e:
    print(f"에러: {e}")

print("\n" + "=" * 80)
print("검증 완료!")
print("=" * 80)
