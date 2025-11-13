#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""차량 질문이 있는 사람들의 실제 데이터 확인"""
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
print("차량 질문이 있는 사람들의 실제 qa_pairs 확인")
print("=" * 80)

# 1. 차량 필터로 검색 (inner_hits 사용하지 않고 _source로 전체 가져오기)
print("\n[1] 차량 질문이 있는 사람 검색 (샘플 5명)")
print("-" * 80)

query = {
    "size": 5,
    "query": {
        "nested": {
            "path": "qa_pairs",
            "query": {
                "bool": {
                    "must": [
                        {"bool": {
                            "should": [
                                {"match": {"qa_pairs.q_text": "차량"}},
                                {"match": {"qa_pairs.q_text": "보유차량"}},
                                {"match": {"qa_pairs.q_text": "보유차량여부"}},
                                {"match": {"qa_pairs.q_text": "자동차"}}
                            ],
                            "minimum_should_match": 1
                        }},
                        {"match": {"qa_pairs.answer": "있다"}}
                    ]
                }
            }
        }
    },
    "_source": ["user_id", "metadata", "qa_pairs"]
}

response = client.search(index=config.WELCOME_INDEX, body=query)
total = response['hits']['total']['value']
print(f"차량 있는 사람: {total}건")

if total == 0:
    print("❌ 차량 필터로 검색된 사람이 없습니다!")
    print("   → answer='있다' 조건 제거하고 다시 시도...")

    # answer 조건 없이 다시 검색
    query_no_answer = {
        "size": 5,
        "query": {
            "nested": {
                "path": "qa_pairs",
                "query": {
                    "match": {"qa_pairs.q_text": "차량"}
                }
            }
        },
        "_source": ["user_id", "metadata", "qa_pairs"]
    }

    response = client.search(index=config.WELCOME_INDEX, body=query_no_answer)
    total = response['hits']['total']['value']
    print(f"\n차량 질문만 (answer 조건 없음): {total}건")

if total > 0:
    for idx, hit in enumerate(response['hits']['hits'], 1):
        source = hit['_source']
        user_id = source.get('user_id', 'N/A')
        metadata = source.get('metadata', {})
        qa_pairs = source.get('qa_pairs', [])

        print(f"\n{'='*60}")
        print(f"사람 {idx}: {user_id}")
        print(f"{'='*60}")
        print(f"Metadata:")
        print(f"  - gender: {metadata.get('gender', 'N/A')}")
        print(f"  - age_group: {metadata.get('age_group', 'N/A')}")
        print(f"  - birth_year: {metadata.get('birth_year', 'N/A')}")
        print(f"  - region: {metadata.get('region', 'N/A')}")

        print(f"\nqa_pairs ({len(qa_pairs)}개):")

        # 차량 관련 질문 찾기
        vehicle_qa = []
        for qa in qa_pairs:
            q_text = qa.get('q_text', '').lower()
            if any(kw in q_text for kw in ['차량', '차', '자동차', '보유']):
                vehicle_qa.append(qa)

        if vehicle_qa:
            print(f"  ✅ 차량 관련 질문 ({len(vehicle_qa)}개):")
            for vq in vehicle_qa:
                print(f"    Q: '{vq.get('q_text')}' → A: '{vq.get('answer')}'")
        else:
            print(f"  ❌ 차량 관련 질문 없음")

        # 성별 질문 찾기
        gender_qa = []
        for qa in qa_pairs:
            q_text = qa.get('q_text', '').lower()
            if '성별' in q_text:
                gender_qa.append(qa)

        if gender_qa:
            print(f"\n  성별 질문:")
            for gq in gender_qa:
                print(f"    Q: '{gq.get('q_text')}' → A: '{gq.get('answer')}'")
        else:
            print(f"\n  ❌ 성별 질문 없음")

        # 출생년도 질문 찾기
        birth_qa = []
        for qa in qa_pairs:
            q_text = qa.get('q_text', '').lower()
            if '출생' in q_text or '나이' in q_text or '연령' in q_text:
                birth_qa.append(qa)

        if birth_qa:
            print(f"  출생년도 질문:")
            for bq in birth_qa:
                print(f"    Q: '{bq.get('q_text')}' → A: '{bq.get('answer')}'")
        else:
            print(f"  ❌ 출생년도 질문 없음")

        # 전체 질문 목록 (처음 10개만)
        print(f"\n  전체 질문 목록 (처음 10개):")
        for i, qa in enumerate(qa_pairs[:10], 1):
            q_text = qa.get('q_text', '')
            answer = qa.get('answer', '')
            print(f"    {i}. Q: '{q_text}' → A: '{answer}'")

# 2. 집계로 차량 질문의 정확한 텍스트 확인
print("\n\n" + "=" * 80)
print("[2] 차량 관련 q_text 집계")
print("-" * 80)

agg_query = {
    "size": 0,
    "query": {
        "nested": {
            "path": "qa_pairs",
            "query": {
                "match": {"qa_pairs.q_text": "차량"}
            }
        }
    },
    "aggs": {
        "qa_nested": {
            "nested": {"path": "qa_pairs"},
            "aggs": {
                "vehicle_questions": {
                    "filter": {
                        "match": {"qa_pairs.q_text": "차량"}
                    },
                    "aggs": {
                        "q_texts": {
                            "terms": {
                                "field": "qa_pairs.q_text.keyword",
                                "size": 20
                            }
                        }
                    }
                }
            }
        }
    }
}

try:
    response = client.search(index=config.WELCOME_INDEX, body=agg_query)
    aggs = response['aggregations']['qa_nested']['vehicle_questions']['q_texts']['buckets']

    print(f"차량 관련 q_text 목록:")
    for bucket in aggs:
        print(f"  - '{bucket['key']}' ({bucket['doc_count']}건)")
except Exception as e:
    print(f"집계 실패: {e}")

print("\n" + "=" * 80)
print("분석 완료!")
print("=" * 80)
