#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""survey_responses_merged 인덱스 전체 구조 분석"""
import json
import sys
from collections import Counter
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

INDEX_NAME = "survey_responses_merged"

print("=" * 100)
print(f"survey_responses_merged 인덱스 전체 구조 분석")
print("=" * 100)

# 1. 기본 통계
print("\n[1] 기본 통계")
print("-" * 100)

try:
    stats = client.indices.stats(index=INDEX_NAME)
    doc_count = stats['indices'][INDEX_NAME]['total']['docs']['count']
    size_bytes = stats['indices'][INDEX_NAME]['total']['store']['size_in_bytes']
    size_mb = size_bytes / (1024 * 1024)

    print(f"총 문서 수: {doc_count:,}건")
    print(f"인덱스 크기: {size_mb:.2f} MB ({size_bytes:,} bytes)")
except Exception as e:
    print(f"통계 조회 실패: {e}")
    sys.exit(1)

# 2. 인덱스 매핑 구조
print("\n\n[2] 인덱스 매핑 구조")
print("-" * 100)

try:
    mapping = client.indices.get_mapping(index=INDEX_NAME)
    properties = mapping[INDEX_NAME]['mappings']['properties']

    print(f"최상위 필드 ({len(properties)}개):")
    for field_name, field_info in sorted(properties.items()):
        field_type = field_info.get('type', 'N/A')
        print(f"  - {field_name}: {field_type}")

        # nested 타입이면 내부 필드도 출력
        if field_type == 'nested':
            nested_props = field_info.get('properties', {})
            print(f"    내부 필드 ({len(nested_props)}개):")
            for nested_field, nested_info in sorted(nested_props.items()):
                nested_type = nested_info.get('type', 'N/A')
                print(f"      • {nested_field}: {nested_type}")

        # object 타입이면 내부 필드도 출력
        elif field_type == 'object' or 'properties' in field_info:
            obj_props = field_info.get('properties', {})
            print(f"    내부 필드 ({len(obj_props)}개):")
            for obj_field, obj_info in sorted(obj_props.items()):
                obj_type = obj_info.get('type', 'N/A')
                print(f"      • {obj_field}: {obj_type}")

except Exception as e:
    print(f"매핑 조회 실패: {e}")

# 3. Demographics 분포 (집계)
print("\n\n[3] Demographics 분포 (집계)")
print("-" * 100)

# Gender 분포
print("\n▶ Gender 분포:")
try:
    agg_query = {
        "size": 0,
        "aggs": {
            "gender_dist": {
                "terms": {
                    "field": "metadata.gender.keyword",
                    "size": 10
                }
            }
        }
    }
    response = client.search(index=INDEX_NAME, body=agg_query)
    buckets = response['aggregations']['gender_dist']['buckets']
    for bucket in buckets:
        print(f"  - {bucket['key']}: {bucket['doc_count']:,}건")
except Exception as e:
    print(f"  집계 실패: {e}")

# Age group 분포
print("\n▶ Age Group 분포:")
try:
    agg_query = {
        "size": 0,
        "aggs": {
            "age_dist": {
                "terms": {
                    "field": "metadata.age_group.keyword",
                    "size": 10,
                    "order": {"_key": "asc"}
                }
            }
        }
    }
    response = client.search(index=INDEX_NAME, body=agg_query)
    buckets = response['aggregations']['age_dist']['buckets']
    for bucket in buckets:
        print(f"  - {bucket['key']}: {bucket['doc_count']:,}건")
except Exception as e:
    print(f"  집계 실패: {e}")

# Region 분포 (상위 10개)
print("\n▶ Region 분포 (상위 10개):")
try:
    agg_query = {
        "size": 0,
        "aggs": {
            "region_dist": {
                "terms": {
                    "field": "metadata.region.keyword",
                    "size": 10
                }
            }
        }
    }
    response = client.search(index=INDEX_NAME, body=agg_query)
    buckets = response['aggregations']['region_dist']['buckets']
    for bucket in buckets:
        print(f"  - {bucket['key']}: {bucket['doc_count']:,}건")
except Exception as e:
    print(f"  집계 실패: {e}")

# 4. qa_pairs 질문 분석
print("\n\n[4] qa_pairs 질문 분석 (상위 20개)")
print("-" * 100)

try:
    agg_query = {
        "size": 0,
        "aggs": {
            "qa_nested": {
                "nested": {"path": "qa_pairs"},
                "aggs": {
                    "question_dist": {
                        "terms": {
                            "field": "qa_pairs.q_text.keyword",
                            "size": 20
                        }
                    }
                }
            }
        }
    }
    response = client.search(index=INDEX_NAME, body=agg_query)
    buckets = response['aggregations']['qa_nested']['question_dist']['buckets']

    print(f"\n전체 qa_pairs 개수: {response['aggregations']['qa_nested']['doc_count']:,}개")
    print(f"\n가장 많은 질문 (상위 20개):")
    for idx, bucket in enumerate(buckets, 1):
        print(f"  {idx:2d}. '{bucket['key']}': {bucket['doc_count']:,}건")
except Exception as e:
    print(f"집계 실패: {e}")

# 5. Behavioral 관련 질문 확인
print("\n\n[5] Behavioral 질문 존재 여부")
print("-" * 100)

behavioral_questions = {
    "흡연": ["흡연", "담배"],
    "차량": ["차량", "보유차량여부", "자동차"],
    "음주": ["주류", "음주", "술", "맥주", "소주"],
}

for category, keywords in behavioral_questions.items():
    print(f"\n▶ {category} 관련 질문:")
    for keyword in keywords:
        try:
            query = {
                "size": 0,
                "query": {
                    "nested": {
                        "path": "qa_pairs",
                        "query": {
                            "match": {"qa_pairs.q_text": keyword}
                        }
                    }
                }
            }
            response = client.search(index=INDEX_NAME, body=query)
            count = response['hits']['total']['value']
            print(f"  - '{keyword}' 포함: {count:,}명")
        except Exception as e:
            print(f"  - '{keyword}': 조회 실패 ({e})")

# 6. 샘플 문서 (3개)
print("\n\n[6] 샘플 문서 (3개)")
print("-" * 100)

try:
    query = {
        "size": 3,
        "query": {"match_all": {}},
        "_source": ["user_id", "metadata", "qa_pairs"]
    }
    response = client.search(index=INDEX_NAME, body=query)

    for idx, hit in enumerate(response['hits']['hits'], 1):
        source = hit['_source']
        print(f"\n샘플 {idx}:")
        print(f"  user_id: {source.get('user_id', 'N/A')}")

        metadata = source.get('metadata', {})
        print(f"  metadata:")
        print(f"    - gender: {metadata.get('gender', 'N/A')}")
        print(f"    - age_group: {metadata.get('age_group', 'N/A')}")
        print(f"    - birth_year: {metadata.get('birth_year', 'N/A')}")
        print(f"    - region: {metadata.get('region', 'N/A')}")

        qa_pairs = source.get('qa_pairs', [])
        print(f"  qa_pairs: {len(qa_pairs)}개")

        # 처음 5개 질문 출력
        if qa_pairs:
            print(f"  샘플 질문 (처음 5개):")
            for qa in qa_pairs[:5]:
                q_text = qa.get('q_text', '')
                answer = qa.get('answer', '')
                print(f"    • Q: '{q_text}' → A: '{answer}'")

        # 차량 질문 확인
        vehicle_qa = [qa for qa in qa_pairs if '차량' in qa.get('q_text', '').lower()]
        if vehicle_qa:
            print(f"  ✅ 차량 질문 있음: '{vehicle_qa[0].get('q_text')}' → '{vehicle_qa[0].get('answer')}'")

except Exception as e:
    print(f"샘플 조회 실패: {e}")

# 7. 데이터 통합 검증
print("\n\n[7] 데이터 통합 검증")
print("-" * 100)

# metadata가 있고 qa_pairs도 풍부한 문서 수
print("\n▶ 데이터 완성도:")
queries = {
    "Metadata 있음": {
        "bool": {
            "must": [
                {"exists": {"field": "metadata.gender"}},
                {"exists": {"field": "metadata.age_group"}}
            ]
        }
    },
    "qa_pairs 10개 이상": {
        "script": {
            "script": "doc['qa_pairs'].size() >= 10"
        }
    },
}

for desc, query_body in queries.items():
    try:
        response = client.search(
            index=INDEX_NAME,
            body={"size": 0, "query": query_body}
        )
        count = response['hits']['total']['value']
        pct = (count / doc_count * 100) if doc_count > 0 else 0
        print(f"  - {desc}: {count:,}건 ({pct:.1f}%)")
    except Exception as e:
        print(f"  - {desc}: 조회 실패 ({e})")

print("\n" + "=" * 100)
print("분석 완료!")
print("=" * 100)
