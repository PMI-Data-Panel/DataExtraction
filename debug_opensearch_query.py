"""OpenSearch 쿼리 디버깅 - uses_fast_delivery"""
import sys
import json
from opensearchpy import OpenSearch
from rag_query_analyzer.config import get_config

sys.path.insert(0, '.')
from api.search_api import build_behavioral_filters, BEHAVIORAL_KEYWORD_MAP

config = get_config()

try:
    client = OpenSearch(
        hosts=[{'host': config.OPENSEARCH_HOST, 'port': config.OPENSEARCH_PORT}],
        http_auth=(config.OPENSEARCH_USER, config.OPENSEARCH_PASSWORD),
        use_ssl=config.OPENSEARCH_USE_SSL,
        verify_certs=config.OPENSEARCH_VERIFY_CERTS,
        ssl_assert_hostname=config.OPENSEARCH_SSL_ASSERT_HOSTNAME,
        ssl_show_warn=False,
        timeout=30,
        max_retries=3,
        retry_on_timeout=True
    )
    info = client.info()
    print(f"✅ Connected to OpenSearch v{info['version']['number']}")
    print()
except Exception as e:
    print(f"❌ ERROR: Connection failed - {str(e)}", file=sys.stderr)
    sys.exit(1)

print("=" * 100)
print("🔍 Debugging drinks_coffee OpenSearch Query")
print("=" * 100)
print()

# 1. build_behavioral_filters() 함수로 필터 생성
behavioral_conditions = {
    'drinks_coffee': True
}

print("📝 Behavioral Conditions:")
print(json.dumps(behavioral_conditions, ensure_ascii=False, indent=2))
print()

filters = build_behavioral_filters(behavioral_conditions)

print(f"🔍 Generated {len(filters)} filter(s)")
print()

if filters:
    print("📋 Generated OpenSearch Filter:")
    print(json.dumps(filters[0], ensure_ascii=False, indent=2))
    print()

    # 2. 이 필터를 사용해서 실제 OpenSearch 검색 실행
    query = {
        "query": filters[0],
        "size": 10,
        "_source": ["user_id", "qa_pairs"]
    }

    print("🔍 Executing OpenSearch Query...")
    print()

    try:
        response = client.search(index="survey_responses_merged", body=query)
        total = response["hits"]["total"]["value"]

        print(f"✅ Search Results: {total}건")
        print()

        if total > 0:
            print("📌 Sample Results:")
            for i, hit in enumerate(response["hits"]["hits"][:3], 1):
                user_id = hit["_source"]["user_id"]
                qa_pairs = hit["_source"].get("qa_pairs", [])

                print(f"\n  [{i}] User: {user_id}")

                # uses_fast_delivery 관련 질문 찾기
                for qa in qa_pairs:
                    q_text = qa.get("q_text", "")
                    if "빠른 배송" in q_text or "당일" in q_text or "새벽" in q_text:
                        print(f"      Question: {q_text[:60]}...")
                        answer = qa.get("answer", "N/A")
                        if isinstance(answer, list):
                            print(f"      Answer: {answer[:2]}...")
                        else:
                            print(f"      Answer: {answer[:60]}...")
                        break
        else:
            print("❌ No results found!")
            print()
            print("🔍 Let's test if the question exists:")

            # 질문 존재 여부 확인
            test_query = {
                "query": {
                    "nested": {
                        "path": "qa_pairs",
                        "query": {
                            "match": {
                                "qa_pairs.q_text": "보유가전제품"
                            }
                        }
                    }
                },
                "size": 3,
                "_source": ["user_id", "qa_pairs"]
            }

            test_response = client.search(index="survey_responses_merged", body=test_query)
            test_total = test_response["hits"]["total"]["value"]

            print(f"   Question '보유가전제품' exists: {test_total}건")

            if test_total > 0:
                hit = test_response["hits"]["hits"][0]
                qa_pairs = hit["_source"].get("qa_pairs", [])

                for qa in qa_pairs:
                    if "보유가전제품" in qa.get("q_text", "") or "가전제품" in qa.get("q_text", ""):
                        print(f"   Sample Question: {qa.get('q_text', '')[:80]}...")
                        answer = qa.get("answer", "")
                        if isinstance(answer, list):
                            print(f"   Sample Answers: {answer[:3]}")
                        else:
                            print(f"   Sample Answer: {answer[:80]}...")
                        break

    except Exception as e:
        print(f"❌ ERROR: {str(e)}")

else:
    print("❌ No filters generated!")

client.close()
print()
print("=" * 100)
print("Debug Complete!")
print("=" * 100)
