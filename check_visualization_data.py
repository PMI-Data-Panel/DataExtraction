"""시각화 데이터 구조 확인"""
import sys
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from opensearchpy import OpenSearch
from rag_query_analyzer.config import get_config
from collections import Counter

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
print("1. Metadata 필드 샘플 확인")
print("="*80)

query = {
    "size": 5,
    "_source": ["metadata", "user_id"]
}

response = client.search(index="survey_responses_merged", body=query)
for i, hit in enumerate(response['hits']['hits'], 1):
    metadata = hit['_source'].get('metadata', {})
    print(f"\n샘플 {i}:")
    print(f"  user_id: {hit['_source'].get('user_id')}")
    print(f"  metadata.gender: {metadata.get('gender')}")
    print(f"  metadata.age_group: {metadata.get('age_group')}")
    print(f"  metadata.region: {metadata.get('region')}")
    print(f"  metadata.occupation: {metadata.get('occupation')}")

print("\n" + "="*80)
print("2. 술 관련 질문 텍스트 확인")
print("="*80)

query = {
    "size": 0,
    "aggs": {
        "qa_nested": {
            "nested": {"path": "qa_pairs"},
            "aggs": {
                "questions": {
                    "terms": {
                        "field": "qa_pairs.q_text.keyword",
                        "size": 100
                    }
                }
            }
        }
    }
}

response = client.search(index="survey_responses_merged", body=query)
questions = response['aggregations']['qa_nested']['questions']['buckets']

# 술 관련 질문 찾기
alcohol_questions = [q for q in questions if any(kw in q['key'] for kw in ['술', '음주', '음용'])]
print(f"\n술 관련 질문 ({len(alcohol_questions)}개):")
for q in alcohol_questions[:10]:
    print(f"  - '{q['key']}' ({q['doc_count']}건)")

print("\n" + "="*80)
print("3. 특정 술 질문의 답변 분포")
print("="*80)

if alcohol_questions:
    top_alcohol_q = alcohol_questions[0]['key']
    print(f"\n질문: '{top_alcohol_q}'")

    query = {
        "size": 0,
        "aggs": {
            "qa_nested": {
                "nested": {"path": "qa_pairs"},
                "aggs": {
                    "alcohol_filter": {
                        "filter": {
                            "term": {"qa_pairs.q_text.keyword": top_alcohol_q}
                        },
                        "aggs": {
                            "answers": {
                                "terms": {
                                    "field": "qa_pairs.answer.keyword",
                                    "size": 20
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    response = client.search(index="survey_responses_merged", body=query)
    answers = response['aggregations']['qa_nested']['alcohol_filter']['answers']['buckets']

    print(f"\n답변 분포 (상위 20개):")
    for ans in answers:
        print(f"  - '{ans['key']}': {ans['doc_count']}건")

print("\n" + "="*80)
