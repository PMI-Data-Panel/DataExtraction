"""인덱스 매핑 확인"""
import sys
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from opensearchpy import OpenSearch
from rag_query_analyzer.config import get_config
import json

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

print("="*80)
print("1. 인덱스 매핑 확인")
print("="*80)

mapping = client.indices.get_mapping(index="survey_responses_merged")
properties = mapping['survey_responses_merged']['mappings']['properties']

print("\nmetadata 필드 매핑:")
if 'metadata' in properties:
    metadata_props = properties['metadata']
    print(json.dumps(metadata_props, indent=2, ensure_ascii=False))
else:
    print("  ⚠️ metadata 필드가 없습니다!")

print("\n" + "="*80)
print("2. Aggregation 직접 테스트")
print("="*80)

# gender aggregation 테스트
queries = [
    ("metadata.gender", "성별 (text)"),
    ("metadata.gender.keyword", "성별 (keyword)"),
]

for field, desc in queries:
    print(f"\n{desc} - field: {field}")

    query = {
        "size": 0,
        "aggs": {
            "test": {
                "terms": {
                    "field": field,
                    "size": 10
                }
            }
        }
    }

    try:
        response = client.search(index="survey_responses_merged", body=query)
        buckets = response['aggregations']['test']['buckets']
        print(f"  결과: {len(buckets)}개 버킷")
        for bucket in buckets[:5]:
            print(f"    - {bucket['key']}: {bucket['doc_count']}건")
    except Exception as e:
        print(f"  ❌ 에러: {e}")

print("\n" + "="*80)
