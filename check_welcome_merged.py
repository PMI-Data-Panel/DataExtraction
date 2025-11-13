#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""welcome_merged 인덱스 검증"""
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
print("welcome_merged 인덱스 검증")
print("=" * 80)

# 1. 인덱스 존재 확인
print("\n[1] 인덱스 존재 확인")
print("-" * 80)

INDEX_NAME = "survey_responses_merged"

if not client.indices.exists(index=INDEX_NAME):
    print(f"❌ {INDEX_NAME} 인덱스가 존재하지 않습니다!")
    print("\n사용 가능한 인덱스:")
    indices = client.cat.indices(format='json', h='index')
    for idx in indices:
        print(f"  - {idx['index']}")
    sys.exit(1)

print(f"✅ {INDEX_NAME} 인덱스 존재함")

# 2. 문서 수 확인
print("\n[2] 문서 수 확인")
print("-" * 80)

stats = client.indices.stats(index=INDEX_NAME)
doc_count = stats['indices'][INDEX_NAME]['total']['docs']['count']
size = stats['indices'][INDEX_NAME]['total']['store']['size_in_bytes'] / (1024 * 1024)

print(f"총 문서 수: {doc_count:,}건")
print(f"인덱스 크기: {size:.1f} MB")

# 3. 샘플 데이터 구조 확인
print("\n[3] 샘플 데이터 구조 확인 (3명)")
print("-" * 80)

response = client.search(
    index=INDEX_NAME,
    body={
        "size": 3,
        "_source": ["user_id", "metadata", "qa_pairs"]
    }
)

for idx, hit in enumerate(response['hits']['hits'], 1):
    source = hit['_source']
    user_id = source.get('user_id', 'N/A')
    metadata = source.get('metadata', {})
    qa_pairs = source.get('qa_pairs', [])

    print(f"\n사람 {idx}: {user_id}")
    print(f"  metadata:")
    print(f"    - gender: {metadata.get('gender', 'N/A')}")
    print(f"    - age_group: {metadata.get('age_group', 'N/A')}")
    print(f"    - birth_year: {metadata.get('birth_year', 'N/A')}")
    print(f"    - region: {metadata.get('region', 'N/A')}")

    print(f"  qa_pairs: {len(qa_pairs)}개")

    # 성별 질문 확인
    gender_qa = [qa for qa in qa_pairs if '성별' in qa.get('q_text', '')]
    if gender_qa:
        print(f"    ✅ 성별 질문 있음: '{gender_qa[0].get('answer')}'")
    else:
        print(f"    ❌ 성별 질문 없음")

    # 출생년도 질문 확인
    birth_qa = [qa for qa in qa_pairs if '출생' in qa.get('q_text', '')]
    if birth_qa:
        print(f"    ✅ 출생년도 질문 있음: '{birth_qa[0].get('answer')}'")
    else:
        print(f"    ❌ 출생년도 질문 없음")

    # 차량 질문 확인
    vehicle_qa = [qa for qa in qa_pairs if '차량' in qa.get('q_text', '')]
    if vehicle_qa:
        print(f"    ✅ 차량 질문 있음: Q='{vehicle_qa[0].get('q_text')}', A='{vehicle_qa[0].get('answer')}'")
    else:
        print(f"    ❌ 차량 질문 없음")

# 4. 통합 검증: 30대 남성 확인
print("\n\n[4] 30대 남성 검색")
print("-" * 80)

query_30s_male = {
    "size": 0,
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
                }
            ]
        }
    }
}

response = client.search(index=INDEX_NAME, body=query_30s_male)
count_30s_male = response['hits']['total']['value']
print(f"30대 남성: {count_30s_male:,}명")

# 5. 차량='있다' 확인
print("\n[5] 차량='있다' 검색")
print("-" * 80)

query_vehicle = {
    "size": 0,
    "query": {
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
}

response = client.search(index=INDEX_NAME, body=query_vehicle)
count_vehicle = response['hits']['total']['value']
print(f"차량='있다': {count_vehicle:,}명")

# 6. 최종 테스트: 30대 남성 + 차량='있다'
print("\n[6] 최종 테스트: 30대 남성 + 차량='있다'")
print("-" * 80)

query_final = {
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

response = client.search(index=INDEX_NAME, body=query_final)
count_final = response['hits']['total']['value']

print(f"결과: {count_final:,}명")

if count_final > 0:
    print(f"\n🎉🎉🎉 성공! {INDEX_NAME}가 제대로 작동합니다!")
    print(f"\n샘플 결과 (5명):")
    for hit in response['hits']['hits']:
        user_id = hit['_source'].get('user_id')
        gender = hit['_source'].get('metadata', {}).get('gender')
        age_group = hit['_source'].get('metadata', {}).get('age_group')
        print(f"  - {user_id} ({gender}, {age_group})")

    print(f"\n✅ 이제 WELCOME_INDEX를 '{INDEX_NAME}'로 변경하면 됩니다!")
    print(f"   → .env 파일에서: WELCOME_INDEX={INDEX_NAME}")
else:
    print(f"\n❌ 여전히 0건입니다.")
    print(f"   통합이 제대로 안된 것 같습니다.")

print("\n" + "=" * 80)
print("검증 완료!")
print("=" * 80)
print(f"\n요약:")
print(f"  - 총 문서: {doc_count:,}건")
print(f"  - 30대 남성: {count_30s_male:,}명")
print(f"  - 차량='있다': {count_vehicle:,}명")
print(f"  - 30대 남성 + 차량='있다': {count_final:,}명")
