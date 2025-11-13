#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""OpenSearch 데이터 구조 확인용 스크립트"""
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
print("차량 필터 디버깅 - 단계별 테스트")
print("=" * 80)

# 테스트 0: 30대 남성 총 몇 명?
print("\n[테스트 0] 30대 남성 총 인원")
print("-" * 80)
query0 = {
    "size": 0,
    "query": {
        "bool": {
            "filter": [
                {"term": {"metadata.gender": "남자"}},
                {"term": {"metadata.age_group": "30대"}}
            ]
        }
    }
}

try:
    response = client.search(index="s_welcome_2nd", body=query0)
    total = response['hits']['total']['value']
    print(f"결과: {total}건")
except Exception as e:
    print(f"에러: {e}")

# 테스트 1: q_text가 "보유차량여부"인 문서 (match 쿼리)
print("\n[테스트 1] match 쿼리: q_text='보유차량여부'")
print("-" * 80)
query1 = {
    "size": 3,
    "query": {
        "nested": {
            "path": "qa_pairs",
            "query": {
                "match": {"qa_pairs.q_text": "보유차량여부"}
            }
        }
    },
    "_source": ["user_id", "metadata.gender", "metadata.age_group", "qa_pairs"]
}

try:
    response = client.search(index="s_welcome_2nd", body=query1)
    total = response['hits']['total']['value']
    print(f"결과: {total}건")
    if total > 0:
        for hit in response['hits']['hits'][:3]:
            user_id = hit['_source'].get('user_id')
            gender = hit['_source'].get('metadata', {}).get('gender')
            age = hit['_source'].get('metadata', {}).get('age_group')
            qa_pairs = hit['_source'].get('qa_pairs', [])
            vehicle_qa = [qa for qa in qa_pairs if '차량' in qa.get('q_text', '')]
            if vehicle_qa:
                print(f"  User: {user_id}, {gender}/{age}, Answer: {vehicle_qa[0].get('answer')}")
except Exception as e:
    print(f"에러: {e}")

# 테스트 2: answer가 "있다"인 문서 (term 쿼리)
print("\n[테스트 2] term 쿼리: answer='있다' + q_text='보유차량여부' (match)")
print("-" * 80)
query2 = {
    "size": 0,
    "query": {
        "nested": {
            "path": "qa_pairs",
            "query": {
                "bool": {
                    "must": [
                        {"match": {"qa_pairs.q_text": "보유차량여부"}},
                        {"term": {"qa_pairs.answer": "있다"}}
                    ]
                }
            }
        }
    }
}

try:
    response = client.search(index="s_welcome_2nd", body=query2)
    total = response['hits']['total']['value']
    print(f"결과: {total}건")
except Exception as e:
    print(f"에러: {e}")

# 테스트 3: 30대 남성 + 차량 있음 (현재 코드 로직)
print("\n[테스트 3] 30대 남성 + 차량 있음 (현재 코드 로직)")
print("-" * 80)

BEHAVIOR_YES_TOKENS = [
    "있다", "있음", "있어요", "yes", "y", "보유", "보유함", "보유중", "한다", "합니다", "해요"
]

answer_should = [
    {"bool": {"should": [
        {"term": {"qa_pairs.answer": kw}},
        {"match_phrase": {"qa_pairs.answer_text": kw}}
    ]}}
    for kw in BEHAVIOR_YES_TOKENS
]

query3 = {
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
                                    {"bool": {"should": answer_should, "minimum_should_match": 1}}
                                ]
                            }
                        }
                    }
                }
            ],
            "filter": [
                {"term": {"metadata.gender": "남자"}},
                {"term": {"metadata.age_group": "30대"}}
            ]
        }
    }
}

try:
    response = client.search(index="s_welcome_2nd", body=query3)
    total = response['hits']['total']['value']
    print(f"결과: {total}건")
    if total > 0:
        print("성공! 이 쿼리가 작동합니다!")
    else:
        print("실패: 0건 - 쿼리에 문제가 있습니다")
except Exception as e:
    print(f"에러: {e}")

# 테스트 4: 간단한 버전 - nested bool 없이
print("\n[테스트 4] 간단한 버전: nested + match q_text + term answer + 필터")
print("-" * 80)
query4 = {
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
                                    {"term": {"qa_pairs.answer": "있다"}}
                                ]
                            }
                        }
                    }
                }
            ],
            "filter": [
                {"term": {"metadata.gender": "남자"}},
                {"term": {"metadata.age_group": "30대"}}
            ]
        }
    }
}

try:
    response = client.search(index="s_welcome_2nd", body=query4)
    total = response['hits']['total']['value']
    print(f"결과: {total}건")
    if total > 0:
        print("성공! 간단한 버전이 작동합니다!")
except Exception as e:
    print(f"에러: {e}")

print("\n" + "=" * 80)
print("분석 완료! 위 결과를 확인하세요.")
print("=" * 80)
