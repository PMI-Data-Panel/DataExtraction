#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""qa_pairs에서 demographics를 검색하는 대체 전략 테스트"""
import json
import sys
from opensearchpy import OpenSearch
from rag_query_analyzer.config import get_config

# UTF-8 출력 설정
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

config = get_config()

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
print("qa_pairs 기반 demographics 검색 테스트")
print("=" * 80)

# 1. qa_pairs에서 성별='남성' 찾기
print("\n[1] qa_pairs에서 성별='남성' 찾기")
print("-" * 80)

query_gender = {
    "size": 3,
    "query": {
        "nested": {
            "path": "qa_pairs",
            "query": {
                "bool": {
                    "must": [
                        {"match": {"qa_pairs.q_text": "성별"}},
                        {"match": {"qa_pairs.answer": "남성"}}
                    ]
                }
            },
            "inner_hits": {
                "size": 10,
                "_source": ["q_text", "answer"]
            }
        }
    },
    "_source": ["user_id", "metadata"]
}

response = client.search(index=config.WELCOME_INDEX, body=query_gender)
total = response['hits']['total']['value']
print(f"결과: {total}건")

if total > 0:
    print("\n샘플:")
    for hit in response['hits']['hits'][:3]:
        user_id = hit['_source'].get('user_id', 'N/A')
        metadata = hit['_source'].get('metadata', {})
        inner_hits = hit.get('inner_hits', {}).get('qa_pairs', {}).get('hits', {}).get('hits', [])

        print(f"\n  User: {user_id}")
        print(f"  Metadata: {metadata}")
        print(f"  QA pairs:")
        for ih in inner_hits[:5]:
            q_text = ih['_source'].get('q_text', '')
            answer = ih['_source'].get('answer', '')
            print(f"    Q: '{q_text}' → A: '{answer}'")

# 2. qa_pairs에서 출생년도=1980년대 찾기 (30대)
print("\n\n[2] qa_pairs에서 출생년도=1980년대 찾기")
print("-" * 80)

query_age = {
    "size": 0,
    "query": {
        "nested": {
            "path": "qa_pairs",
            "query": {
                "bool": {
                    "must": [
                        {"match": {"qa_pairs.q_text": "출생년도"}},
                        {"bool": {
                            "should": [
                                {"match": {"qa_pairs.answer": "198"}},
                                {"wildcard": {"qa_pairs.answer": "198*"}}
                            ]
                        }}
                    ]
                }
            }
        }
    }
}

response = client.search(index=config.WELCOME_INDEX, body=query_age)
total = response['hits']['total']['value']
print(f"결과: {total}건")

# 3. 성별='남성' + 출생년도=1980년대 (하나의 bool 쿼리로)
print("\n\n[3] 성별='남성' + 출생년도=1980년대")
print("-" * 80)

query_combined = {
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
                                    {"match": {"qa_pairs.q_text": "성별"}},
                                    {"match": {"qa_pairs.answer": "남성"}}
                                ]
                            }
                        }
                    }
                },
                {
                    "nested": {
                        "path": "qa_pairs",
                        "query": {
                            "bool": {
                                "must": [
                                    {"match": {"qa_pairs.q_text": "출생년도"}},
                                    {"wildcard": {"qa_pairs.answer": "198*"}}
                                ]
                            }
                        }
                    }
                }
            ]
        }
    }
}

response = client.search(index=config.WELCOME_INDEX, body=query_combined)
total = response['hits']['total']['value']
print(f"결과: {total}건")

if total > 0:
    print("✅ qa_pairs 기반 demographics 필터링이 작동합니다!")
else:
    print("❌ qa_pairs에서도 demographics를 찾을 수 없습니다.")

# 4. 최종 테스트: 성별 + 연령 + 차량
print("\n\n[4] 성별='남성' + 출생년도=1980년대 + 차량='있다'")
print("-" * 80)

query_final = {
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
                                    {"match": {"qa_pairs.q_text": "성별"}},
                                    {"match": {"qa_pairs.answer": "남성"}}
                                ]
                            }
                        }
                    }
                },
                {
                    "nested": {
                        "path": "qa_pairs",
                        "query": {
                            "bool": {
                                "must": [
                                    {"match": {"qa_pairs.q_text": "출생년도"}},
                                    {"wildcard": {"qa_pairs.answer": "198*"}}
                                ]
                            }
                        }
                    }
                },
                {
                    "nested": {
                        "path": "qa_pairs",
                        "query": {
                            "bool": {
                                "must": [
                                    {"bool": {
                                        "should": [
                                            {"match": {"qa_pairs.q_text": "차량"}},
                                            {"match": {"qa_pairs.q_text": "보유차량"}},
                                            {"match": {"qa_pairs.q_text": "보유차량여부"}}
                                        ]
                                    }},
                                    {"match": {"qa_pairs.answer": "있다"}}
                                ]
                            }
                        }
                    }
                }
            ]
        }
    },
    "_source": ["user_id"]
}

response = client.search(index=config.WELCOME_INDEX, body=query_final)
total = response['hits']['total']['value']
print(f"결과: {total}건")

if total > 0:
    print(f"✅ 성공! qa_pairs 기반 필터로 {total}건 검색됨")
    print("\n샘플 user_id:")
    for hit in response['hits']['hits'][:3]:
        print(f"  - {hit['_source'].get('user_id')}")
else:
    print("❌ 여전히 0건")
    print("   → 데이터 재색인이 필요할 수 있습니다")

print("\n" + "=" * 80)
print("테스트 완료!")
print("=" * 80)
