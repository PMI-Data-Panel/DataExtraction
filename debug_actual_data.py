#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""30대 남성의 실제 qa_pairs 데이터 확인"""
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
print("30대 남성의 실제 qa_pairs 데이터 확인")
print("=" * 80)

# 1. metadata로 30대 남성 검색 (우리가 사용하는 쿼리와 동일)
print("\n[1] metadata로 30대 남성 검색 (샘플 5명)")
print("-" * 80)

query = {
    "size": 5,
    "query": {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "should": [
                            {"term": {"metadata.gender.keyword": "M"}},
                            {"match_phrase": {"metadata.gender": "M"}},
                            {"match": {"metadata.gender": {"query": "M", "operator": "and"}}},
                            {"term": {"metadata.gender.keyword": "남성"}},
                            {"match_phrase": {"metadata.gender": "남성"}},
                            {"match": {"metadata.gender": {"query": "남성", "operator": "and"}}}
                        ],
                        "minimum_should_match": 1
                    }
                },
                {
                    "bool": {
                        "should": [
                            {"term": {"metadata.age_group.keyword": "30s"}},
                            {"match_phrase": {"metadata.age_group": "30s"}},
                            {"match": {"metadata.age_group": {"query": "30s", "operator": "and"}}},
                            {"term": {"metadata.age_group.keyword": "30대"}},
                            {"match_phrase": {"metadata.age_group": "30대"}},
                            {"match": {"metadata.age_group": {"query": "30대", "operator": "and"}}}
                        ],
                        "minimum_should_match": 1
                    }
                }
            ]
        }
    },
    "_source": ["user_id", "metadata.gender", "metadata.age_group", "qa_pairs"]
}

response = client.search(index=config.WELCOME_INDEX, body=query)
print(f"검색된 30대 남성: {response['hits']['total']['value']}건")

for idx, hit in enumerate(response['hits']['hits'], 1):
    source = hit['_source']
    user_id = source.get('user_id', 'N/A')
    metadata = source.get('metadata', {})
    gender = metadata.get('gender', 'N/A')
    age_group = metadata.get('age_group', 'N/A')
    qa_pairs = source.get('qa_pairs', [])

    print(f"\n사람 {idx}: user_id={user_id}")
    print(f"  Gender: {gender}, Age: {age_group}")
    print(f"  qa_pairs 개수: {len(qa_pairs)}개")

    if not qa_pairs:
        print(f"  ❌ qa_pairs가 비어있습니다!")
        continue

    # 모든 질문 텍스트 출력
    print(f"\n  전체 질문 목록 (처음 20개):")
    for i, qa in enumerate(qa_pairs[:20], 1):
        q_text = qa.get('q_text', '')
        answer = qa.get('answer', '')
        print(f"    {i}. Q: '{q_text}' → A: '{answer}'")

    # 차량 관련 질문 찾기
    vehicle_keywords = ['차량', '차', '자동차', '보유']
    vehicle_qa = [qa for qa in qa_pairs if any(kw in qa.get('q_text', '') for kw in vehicle_keywords)]

    if vehicle_qa:
        print(f"\n  ✅ 차량 관련 질문 발견 ({len(vehicle_qa)}개):")
        for vq in vehicle_qa:
            print(f"     Q: '{vq.get('q_text')}' → A: '{vq.get('answer')}'")
    else:
        print(f"\n  ❌ 차량 관련 질문 없음")

# 2. q_text 필드의 매핑 확인
print("\n" + "=" * 80)
print("[2] qa_pairs.q_text 필드 매핑 확인")
print("-" * 80)

try:
    mapping = client.indices.get_mapping(index=config.WELCOME_INDEX)
    qa_pairs_mapping = mapping[config.WELCOME_INDEX]['mappings']['properties'].get('qa_pairs', {})

    print(f"qa_pairs 타입: {qa_pairs_mapping.get('type', 'N/A')}")

    if qa_pairs_mapping.get('type') == 'nested':
        properties = qa_pairs_mapping.get('properties', {})
        q_text_mapping = properties.get('q_text', {})
        answer_mapping = properties.get('answer', {})

        print(f"\nqa_pairs.q_text:")
        print(f"  - type: {q_text_mapping.get('type', 'N/A')}")
        print(f"  - fields: {list(q_text_mapping.get('fields', {}).keys())}")

        print(f"\nqa_pairs.answer:")
        print(f"  - type: {answer_mapping.get('type', 'N/A')}")
        print(f"  - fields: {list(answer_mapping.get('fields', {}).keys())}")
    else:
        print(f"⚠️ qa_pairs가 nested 타입이 아닙니다: {qa_pairs_mapping}")
except Exception as e:
    print(f"매핑 확인 실패: {e}")

# 3. 실제 차량 필터 쿼리 테스트 (30대 남성 조건 없이)
print("\n" + "=" * 80)
print("[3] 차량 필터만 단독 테스트 (demographics 제외)")
print("-" * 80)

vehicle_filter = {
    "nested": {
        "path": "qa_pairs",
        "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "should": [
                                {"match": {"qa_pairs.q_text": "차량여부"}},
                                {"match": {"qa_pairs.q_text": "차량 여부"}},
                                {"match": {"qa_pairs.q_text": "보유차량"}},
                                {"match": {"qa_pairs.q_text": "차량"}},
                                {"match": {"qa_pairs.q_text": "차량보유"}},
                                {"match": {"qa_pairs.q_text": "보유차량여부"}},
                                {"match": {"qa_pairs.q_text": "자동차"}},
                                {"match": {"qa_pairs.q_text": "차 보유"}}
                            ],
                            "minimum_should_match": 1
                        }
                    },
                    {
                        "bool": {
                            "should": [
                                {"match": {"qa_pairs.answer": "있다"}}
                            ],
                            "minimum_should_match": 1
                        }
                    }
                ]
            }
        }
    }
}

query_vehicle_only = {
    "size": 0,
    "query": vehicle_filter
}

response = client.search(index=config.WELCOME_INDEX, body=query_vehicle_only)
total = response['hits']['total']['value']
print(f"차량 필터만 적용: {total}건")

if total == 0:
    print("❌ 차량 필터 자체가 아무것도 매칭하지 않습니다!")
    print("   → q_text 또는 answer 매칭 문제")
else:
    print(f"✅ 차량 필터는 작동합니다 ({total}건)")

# 4. 더 간단한 차량 필터 테스트 (answer 조건 제외)
print("\n[4] q_text만 테스트 (answer 조건 제외)")
print("-" * 80)

query_qtext_only = {
    "size": 3,
    "query": {
        "nested": {
            "path": "qa_pairs",
            "query": {
                "match": {"qa_pairs.q_text": "차량"}
            },
            "inner_hits": {
                "size": 3,
                "_source": ["q_text", "answer"]
            }
        }
    }
}

response = client.search(index=config.WELCOME_INDEX, body=query_qtext_only)
total = response['hits']['total']['value']
print(f"q_text='차량' 매칭: {total}건")

if total > 0:
    print("\n샘플 결과:")
    for hit in response['hits']['hits'][:3]:
        user_id = hit['_source'].get('user_id', 'N/A')
        inner_hits = hit.get('inner_hits', {}).get('qa_pairs', {}).get('hits', {}).get('hits', [])
        print(f"\n  User: {user_id}")
        for ih in inner_hits:
            q_text = ih['_source'].get('q_text', '')
            answer = ih['_source'].get('answer', '')
            print(f"    Q: '{q_text}' → A: '{answer}'")

print("\n" + "=" * 80)
print("분석 완료!")
print("=" * 80)
