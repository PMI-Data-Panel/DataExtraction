#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""직업 관련 데이터 검색 테스트"""
import sys
import os
from opensearchpy import OpenSearch
from rag_query_analyzer.config import get_config

# UTF-8 출력 설정
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

config = get_config()

# OpenSearch 연결
os_client = OpenSearch(
    hosts=[{'host': config.OPENSEARCH_HOST, 'port': config.OPENSEARCH_PORT}],
    http_auth=(config.OPENSEARCH_USER, config.OPENSEARCH_PASSWORD),
    use_ssl=config.OPENSEARCH_USE_SSL,
    verify_certs=config.OPENSEARCH_VERIFY_CERTS,
    ssl_assert_hostname=config.OPENSEARCH_SSL_ASSERT_HOSTNAME,
    ssl_show_warn=False,
    timeout=60
)

# 연결 테스트
try:
    info = os_client.info()
    print(f"✅ OpenSearch 연결 성공: {config.OPENSEARCH_HOST}:{config.OPENSEARCH_PORT}")
    print(f"   버전: {info['version']['number']}")
except Exception as e:
    print(f"❌ OpenSearch 연결 실패: {e}")
    print(f"   호스트: {config.OPENSEARCH_HOST}:{config.OPENSEARCH_PORT}")
    print(f"   SSL: {config.OPENSEARCH_USE_SSL}")
    sys.exit(1)

print("=" * 80)
print("직업 관련 데이터 검색")
print("=" * 80)

# 1. "미용" 키워드로 qa_pairs 검색
print("\n[1] qa_pairs에서 '미용' 검색:")
query1 = {
    "query": {
        "nested": {
            "path": "qa_pairs",
            "query": {
                "bool": {
                    "should": [
                        {"match": {"qa_pairs.answer": "미용"}},
                        {"match": {"qa_pairs.answer_text": "미용"}}
                    ]
                }
            },
            "inner_hits": {
                "size": 3
            }
        }
    },
    "size": 5
}

try:
    result = os_client.search(index="survey_responses_merged", body=query1)
    print(f"총 {result['hits']['total']['value']}건 발견")

    for hit in result['hits']['hits'][:3]:
        user_id = hit['_source'].get('user_id')
        print(f"\n  User ID: {user_id}")

        # inner_hits에서 매칭된 qa_pairs 확인
        inner_hits = hit.get('inner_hits', {}).get('qa_pairs', {}).get('hits', {}).get('hits', [])
        for ih in inner_hits:
            qa = ih['_source']
            print(f"    Q: {qa.get('q_text', 'N/A')}")
            print(f"    A: {qa.get('answer', qa.get('answer_text', 'N/A'))}")
except Exception as e:
    print(f"  에러: {e}")

# 2. "직업" 질문에서 "미용" 답변 검색
print("\n[2] '직업' 질문에서 '미용' 답변 검색:")
query2 = {
    "query": {
        "nested": {
            "path": "qa_pairs",
            "query": {
                "bool": {
                    "must": [
                        {"match": {"qa_pairs.q_text": "직업"}},
                        {
                            "bool": {
                                "should": [
                                    {"match": {"qa_pairs.answer": "미용"}},
                                    {"match": {"qa_pairs.answer_text": "미용"}}
                                ]
                            }
                        }
                    ]
                }
            },
            "inner_hits": {
                "size": 3
            }
        }
    },
    "size": 5
}

try:
    result = os_client.search(index="survey_responses_merged", body=query2)
    print(f"총 {result['hits']['total']['value']}건 발견")

    for hit in result['hits']['hits'][:3]:
        user_id = hit['_source'].get('user_id')
        metadata = hit['_source'].get('metadata', {})
        print(f"\n  User ID: {user_id}")
        print(f"  Metadata occupation: {metadata.get('occupation', 'N/A')}")
        print(f"  Metadata job_function: {metadata.get('job_function', 'N/A')}")

        # inner_hits에서 매칭된 qa_pairs 확인
        inner_hits = hit.get('inner_hits', {}).get('qa_pairs', {}).get('hits', {}).get('hits', [])
        for ih in inner_hits:
            qa = ih['_source']
            print(f"    Q: {qa.get('q_text', 'N/A')}")
            print(f"    A: {qa.get('answer', qa.get('answer_text', 'N/A'))}")
except Exception as e:
    print(f"  에러: {e}")

# 3. metadata에서 occupation 필드 확인
print("\n[3] metadata.occupation 필드 샘플:")
query3 = {
    "query": {"match_all": {}},
    "size": 10,
    "_source": ["user_id", "metadata.occupation", "metadata.job_function"]
}

try:
    result = os_client.search(index="survey_responses_merged", body=query3)
    for hit in result['hits']['hits'][:5]:
        metadata = hit['_source'].get('metadata', {})
        print(f"  occupation: {metadata.get('occupation', 'N/A')}, job_function: {metadata.get('job_function', 'N/A')}")
except Exception as e:
    print(f"  에러: {e}")

print("\n" + "=" * 80)
print("테스트 완료!")
print("=" * 80)
