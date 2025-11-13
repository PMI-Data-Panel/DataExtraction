#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""30대 남성과 차량 질문의 교집합 확인"""
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
print("30대 남성 + 차량 질문 교집합 분석")
print("=" * 80)

# 1. welcome_all에서 30대 남성
print("\n[1] welcome_all - 30대 남성")
print("-" * 80)
query1 = {
    "size": 0,
    "query": {
        "bool": {
            "filter": [
                {"match": {"metadata.gender": "남자"}},
                {"match": {"metadata.age_group": "30대"}}
            ]
        }
    }
}

try:
    response = client.search(index="welcome_all", body=query1)
    total = response['hits']['total']['value']
    print(f"결과: {total}건")
except Exception as e:
    print(f"에러: {e}")

# 2. welcome_all에서 "보유차량여부" 질문이 있는 사람
print("\n[2] welcome_all - '보유차량여부' 질문 있는 사람")
print("-" * 80)
query2 = {
    "size": 0,
    "query": {
        "nested": {
            "path": "qa_pairs",
            "query": {
                "match": {"qa_pairs.q_text": "보유차량여부"}
            }
        }
    }
}

try:
    response = client.search(index="welcome_all", body=query2)
    total = response['hits']['total']['value']
    print(f"결과: {total}건")
except Exception as e:
    print(f"에러: {e}")

# 3. 30대 남성 중에 "보유차량여부" 질문이 있는 사람
print("\n[3] 30대 남성 + '보유차량여부' 질문 (교집합)")
print("-" * 80)
query3 = {
    "size": 0,
    "query": {
        "bool": {
            "must": [
                {
                    "nested": {
                        "path": "qa_pairs",
                        "query": {
                            "match": {"qa_pairs.q_text": "보유차량여부"}
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
    response = client.search(index="welcome_all", body=query3)
    total = response['hits']['total']['value']
    print(f"결과: {total}건")
    if total == 0:
        print("❌ 30대 남성 중에 '보유차량여부' 질문이 있는 사람이 없습니다!")
        print("   → 데이터 구조 문제일 가능성이 높습니다.")
except Exception as e:
    print(f"에러: {e}")

# 4. 30대 남성 샘플 데이터 확인
print("\n[4] 30대 남성 샘플 데이터 (qa_pairs 확인)")
print("-" * 80)
query4 = {
    "size": 2,
    "query": {
        "bool": {
            "filter": [
                {"match": {"metadata.gender": "남자"}},
                {"match": {"metadata.age_group": "30대"}}
            ]
        }
    },
    "_source": ["user_id", "metadata", "qa_pairs"]
}

try:
    response = client.search(index="welcome_all", body=query4)
    for hit in response['hits']['hits']:
        user_id = hit['_source'].get('user_id')
        gender = hit['_source'].get('metadata', {}).get('gender')
        age = hit['_source'].get('metadata', {}).get('age_group')
        qa_pairs = hit['_source'].get('qa_pairs', [])

        print(f"\nUser: {user_id}")
        print(f"  Gender: {gender}, Age: {age}")
        print(f"  QA Pairs 개수: {len(qa_pairs)}")

        if qa_pairs:
            # 처음 3개 질문만 출력
            print(f"  질문 샘플:")
            for i, qa in enumerate(qa_pairs[:3], 1):
                q_text = qa.get('q_text', '')
                answer = qa.get('answer', '')
                print(f"    {i}. Q: {q_text}, A: {answer}")

            # 차량 관련 질문 찾기
            vehicle_qa = [qa for qa in qa_pairs if '차량' in qa.get('q_text', '')]
            if vehicle_qa:
                print(f"  ✅ 차량 관련 질문 있음: {vehicle_qa[0].get('q_text')}")
            else:
                print(f"  ❌ 차량 관련 질문 없음")
                # 전체 질문 목록 출력
                all_questions = [qa.get('q_text', '') for qa in qa_pairs]
                print(f"  전체 질문 ({len(all_questions)}개):")
                for q in all_questions[:10]:
                    print(f"    - {q}")
except Exception as e:
    print(f"에러: {e}")

# 5. s_welcome_2nd에서 30대 남성
print("\n[5] s_welcome_2nd - 30대 남성")
print("-" * 80)
try:
    response = client.search(index="s_welcome_2nd", body=query1)
    total = response['hits']['total']['value']
    print(f"결과: {total}건")
    if total == 0:
        print("❌ s_welcome_2nd에는 30대 남성 데이터가 없습니다!")
except Exception as e:
    print(f"에러: {e}")

print("\n" + "=" * 80)
print("분석 완료!")
print("=" * 80)
