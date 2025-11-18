"""Debug PARTIAL conditions to see why extraction fails"""
import sys
from opensearchpy import OpenSearch
from rag_query_analyzer.config import get_config

sys.path.insert(0, '.')
from api.search_api import extract_behavior_from_qa_pairs

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
    print(f"Connected to OpenSearch v{info['version']['number']}")
    print()
except Exception as e:
    print(f"ERROR: Connection failed - {str(e)}", file=sys.stderr)
    sys.exit(1)

# PARTIAL 조건들
partial_conditions = {
    'drinks_coffee': '보유가전제품',
    'has_subscription': '여러분은 할인, 캐시백, 멤버십 등 포인트 적립 혜택을 얼마나 신경 쓰시나요?',
    'uses_social_media': '여러분은 요즘 가장 많이 사용하는 앱은 무엇인가요?',
    'watches_movies_dramas': '여러분은 요즘 가장 많이 사용하는 앱은 무엇인가요?',
    'uses_financial_services': '여러분은 요즘 가장 많이 사용하는 앱은 무엇인가요?',
    'shops_fashion': '여러분은 본인을 위해 소비하는 것 중 가장 기분 좋아지는 소비는 무엇인가요?',
    'interested_in_cars': '보유차량여부',
    'uses_parcel_delivery': '빠른 배송(당일·새벽·직진 배송) 서비스를 주로 어떤 제품을 구매할 때 이용하시나요?'
}

for behavior_key, question in partial_conditions.items():
    print("=" * 100)
    print(f"Debugging: {behavior_key}")
    print(f"Question: {question}")
    print("=" * 100)

    # 해당 질문에 답변한 샘플 유저 1명 가져오기
    sample_query = {
        "size": 1,
        "query": {
            "nested": {
                "path": "qa_pairs",
                "query": {
                    "term": {"qa_pairs.q_text.keyword": question}
                }
            }
        },
        "_source": ["user_id", "qa_pairs"]
    }

    try:
        sample_response = client.search(index="survey_responses_merged", body=sample_query)

        if sample_response["hits"]["total"]["value"] > 0:
            hit = sample_response["hits"]["hits"][0]
            user_id = hit["_source"]["user_id"]
            qa_pairs = hit["_source"].get("qa_pairs", [])

            # 해당 질문의 답변 찾기
            answer = None
            for qa in qa_pairs:
                if qa.get("q_text") == question:
                    answer = qa.get("answer")
                    break

            print(f"Sample User: {user_id}")
            print(f"Answer: {answer}")
            print()

            # extraction 실행 (debug=True)
            result = extract_behavior_from_qa_pairs(qa_pairs, behavior_key, debug=True)

            print(f"Extraction Result: {result}")
            print()
        else:
            print("No sample found")
            print()

    except Exception as e:
        print(f"ERROR: {str(e)}")
        print()

client.close()
print("=" * 100)
print("Debug Complete!")
print("=" * 100)
