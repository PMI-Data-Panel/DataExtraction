#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""s_welcome_1st와 s_welcome_2nd를 user_id 기준으로 통합하여 새 인덱스 생성"""
import json
import sys
from collections import defaultdict
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
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

NEW_INDEX = "welcome_unified"

print("=" * 80)
print("s_welcome_1st + s_welcome_2nd 통합 재색인")
print("=" * 80)

# 1. 새 인덱스 생성 (기존 인덱스가 있으면 삭제)
print(f"\n[1] 새 인덱스 생성: {NEW_INDEX}")
print("-" * 80)

if client.indices.exists(index=NEW_INDEX):
    print(f"기존 인덱스 {NEW_INDEX} 삭제...")
    client.indices.delete(index=NEW_INDEX)

# s_welcome_2nd의 매핑 가져오기
mapping_2nd = client.indices.get_mapping(index="s_welcome_2nd")
mapping = mapping_2nd["s_welcome_2nd"]["mappings"]

client.indices.create(index=NEW_INDEX, body={"mappings": mapping})
print(f"✅ 인덱스 {NEW_INDEX} 생성 완료")

# 2. s_welcome_1st 데이터 로드 (metadata 소스)
print(f"\n[2] s_welcome_1st 데이터 로드 (metadata 소스)")
print("-" * 80)

user_metadata = {}
scroll_size = 1000

response = client.search(
    index="s_welcome_1st",
    scroll="5m",
    size=scroll_size,
    body={
        "query": {"match_all": {}},
        "_source": ["user_id", "metadata", "qa_pairs"]
    }
)

scroll_id = response['_scroll_id']
total_1st = response['hits']['total']['value']
processed_1st = 0

print(f"총 {total_1st}건 로드 중...")

while True:
    hits = response['hits']['hits']
    if not hits:
        break

    for hit in hits:
        source = hit['_source']
        user_id = source.get('user_id')
        if user_id:
            user_metadata[user_id] = {
                'metadata': source.get('metadata', {}),
                'qa_pairs_1st': source.get('qa_pairs', [])
            }

    processed_1st += len(hits)
    print(f"  진행: {processed_1st}/{total_1st} ({processed_1st * 100 // total_1st}%)")

    response = client.scroll(scroll_id=scroll_id, scroll="5m")

client.clear_scroll(scroll_id=scroll_id)
print(f"✅ s_welcome_1st 로드 완료: {len(user_metadata)}명")

# 3. s_welcome_2nd와 통합하여 새 인덱스에 저장
print(f"\n[3] s_welcome_2nd와 통합하여 {NEW_INDEX}에 저장")
print("-" * 80)

response = client.search(
    index="s_welcome_2nd",
    scroll="5m",
    size=scroll_size,
    body={
        "query": {"match_all": {}},
        "_source": True
    }
)

scroll_id = response['_scroll_id']
total_2nd = response['hits']['total']['value']
processed_2nd = 0
unified_docs = []
batch_size = 500

print(f"총 {total_2nd}건 통합 중...")

matched_count = 0
unmatched_count = 0

while True:
    hits = response['hits']['hits']
    if not hits:
        break

    for hit in hits:
        source = hit['_source']
        user_id = source.get('user_id')

        if user_id and user_id in user_metadata:
            # 매칭됨: metadata와 qa_pairs 통합
            matched_count += 1
            unified_doc = {
                '_index': NEW_INDEX,
                '_id': hit['_id'],
                '_source': {
                    **source,
                    'metadata': user_metadata[user_id]['metadata'],  # 1st의 metadata 사용
                    'qa_pairs': user_metadata[user_id]['qa_pairs_1st'] + source.get('qa_pairs', [])  # 통합
                }
            }
        else:
            # 매칭 안됨: 2nd 데이터만 사용
            unmatched_count += 1
            unified_doc = {
                '_index': NEW_INDEX,
                '_id': hit['_id'],
                '_source': source
            }

        unified_docs.append(unified_doc)

        # 배치 저장
        if len(unified_docs) >= batch_size:
            success, errors = bulk(client, unified_docs, raise_on_error=False)
            if errors:
                print(f"  ⚠️ 배치 저장 중 {len(errors)}개 에러")
            unified_docs = []

    processed_2nd += len(hits)
    print(f"  진행: {processed_2nd}/{total_2nd} ({processed_2nd * 100 // total_2nd}%) - 매칭: {matched_count}, 미매칭: {unmatched_count}")

    response = client.scroll(scroll_id=scroll_id, scroll="5m")

# 남은 문서 저장
if unified_docs:
    success, errors = bulk(client, unified_docs, raise_on_error=False)
    if errors:
        print(f"  ⚠️ 마지막 배치 저장 중 {len(errors)}개 에러")

client.clear_scroll(scroll_id=scroll_id)
print(f"\n✅ 통합 완료!")
print(f"  - 매칭된 사용자: {matched_count}명")
print(f"  - 미매칭 사용자: {unmatched_count}명")

# 4. 검증: 30대 남성 + 차량 테스트
print(f"\n[4] 검증: 30대 남성 + 차량='있다' 테스트")
print("-" * 80)

test_query = {
    "size": 5,
    "query": {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "should": [
                            {"term": {"metadata.gender.keyword": "M"}},
                            {"match": {"metadata.gender": "남성"}}
                        ],
                        "minimum_should_match": 1
                    }
                },
                {
                    "bool": {
                        "should": [
                            {"term": {"metadata.age_group.keyword": "30s"}},
                            {"match": {"metadata.age_group": "30대"}}
                        ],
                        "minimum_should_match": 1
                    }
                },
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
            ]
        }
    },
    "_source": ["user_id", "metadata.gender", "metadata.age_group"]
}

response = client.search(index=NEW_INDEX, body=test_query)
test_count = response['hits']['total']['value']

print(f"결과: {test_count}건")

if test_count > 0:
    print(f"\n🎉 성공! 통합 인덱스가 제대로 작동합니다!")
    print(f"\n샘플 user_id:")
    for hit in response['hits']['hits'][:5]:
        user_id = hit['_source'].get('user_id')
        gender = hit['_source'].get('metadata', {}).get('gender')
        age_group = hit['_source'].get('metadata', {}).get('age_group')
        print(f"  - {user_id} ({gender}, {age_group})")

    print(f"\n다음 명령으로 설정을 변경하세요:")
    print(f"  WELCOME_INDEX='{NEW_INDEX}' (환경 변수 또는 .env 파일)")
else:
    print(f"❌ 여전히 0건입니다.")
    print(f"   → 1st와 2nd의 user_id가 겹치지 않을 수 있습니다.")

print("\n" + "=" * 80)
print("재색인 완료!")
print("=" * 80)
