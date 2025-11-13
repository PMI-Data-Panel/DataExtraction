#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""사용 가능한 OpenSearch 인덱스 목록 확인"""
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
print("OpenSearch 인덱스 목록 및 통계")
print("=" * 80)

# 모든 인덱스 목록
indices = client.cat.indices(format='json', h='index,docs.count,store.size')

# welcome 관련 인덱스만 필터링
welcome_indices = [idx for idx in indices if 'welcome' in idx['index'].lower()]

print("\nWelcome 관련 인덱스:")
print("-" * 80)
for idx in welcome_indices:
    index_name = idx['index']
    doc_count = idx['docs.count']
    size = idx.get('store.size', 'N/A')
    print(f"\n인덱스: {index_name}")
    print(f"  문서 수: {doc_count}")
    print(f"  크기: {size}")

    # 각 인덱스에서 샘플 데이터 확인
    try:
        sample = client.search(
            index=index_name,
            body={
                "size": 1,
                "_source": ["user_id", "metadata", "qa_pairs"]
            }
        )

        if sample['hits']['total']['value'] > 0:
            source = sample['hits']['hits'][0]['_source']
            metadata = source.get('metadata', {})
            qa_pairs = source.get('qa_pairs', [])

            print(f"\n  샘플 데이터:")
            print(f"    - metadata.gender: {metadata.get('gender', 'N/A')}")
            print(f"    - metadata.age_group: {metadata.get('age_group', 'N/A')}")
            print(f"    - qa_pairs 개수: {len(qa_pairs)}개")

            if qa_pairs:
                # 차량 질문 확인
                vehicle_qa = [qa for qa in qa_pairs if '차량' in qa.get('q_text', '')]
                # 성별 질문 확인
                gender_qa = [qa for qa in qa_pairs if '성별' in qa.get('q_text', '')]

                print(f"    - 차량 질문: {'있음' if vehicle_qa else '없음'}")
                print(f"    - 성별 질문: {'있음' if gender_qa else '없음'}")

            # 30대 남성 + 차량 테스트
            test_query = {
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
                }
            }

            try:
                test_result = client.search(index=index_name, body=test_query)
                test_count = test_result['hits']['total']['value']
                print(f"\n  ✨ 30대 남성 + 차량='있다': {test_count}건")

                if test_count > 0:
                    print(f"     🎉 이 인덱스를 사용하면 됩니다!")
            except Exception as e:
                print(f"  테스트 실패: {e}")

    except Exception as e:
        print(f"  샘플 데이터 가져오기 실패: {e}")

print("\n" + "=" * 80)
print("현재 설정:")
print(f"  WELCOME_INDEX = {config.WELCOME_INDEX}")
print("=" * 80)
