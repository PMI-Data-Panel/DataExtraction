"""Debug PARTIAL conditions with users who have positive answers"""
import sys
from opensearchpy import OpenSearch
from rag_query_analyzer.config import get_config

sys.path.insert(0, '.')
from api.search_api import extract_behavior_from_qa_pairs, BEHAVIORAL_KEYWORD_MAP

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

# PARTIAL 조건들과 각각의 positive 키워드
partial_tests = {
    'has_stress': {
        'question': '다음 중 가장 스트레스를 많이 느끼는 상황은 무엇인가요?',
        'positive_answer': '경제적 문제'
    },
    'drinks_coffee': {
        'question': '보유가전제품',
        'positive_answer': '커피 머신'
    },
    'uses_social_media': {
        'question': '여러분은 요즘 가장 많이 사용하는 앱은 무엇인가요?',
        'positive_answer': 'SNS 앱'
    },
    'watches_movies_dramas': {
        'question': '여러분은 요즘 가장 많이 사용하는 앱은 무엇인가요?',
        'positive_answer': '동영상 스트리밍 앱'
    },
    'uses_financial_services': {
        'question': '여러분은 요즘 가장 많이 사용하는 앱은 무엇인가요?',
        'positive_answer': '금융 앱'
    },
    'shops_fashion': {
        'question': '여러분은 본인을 위해 소비하는 것 중 가장 기분 좋아지는 소비는 무엇인가요?',
        'positive_answer': '옷/패션관련'
    }
}

for behavior_key, test_info in partial_tests.items():
    print("=" * 100)
    print(f"Testing: {behavior_key}")
    print(f"Question: {test_info['question']}")
    print(f"Looking for positive answer containing: {test_info['positive_answer']}")
    print("=" * 100)

    # 해당 질문에 positive 답변을 한 유저 찾기
    sample_query = {
        "size": 1,
        "query": {
            "bool": {
                "must": [
                    {
                        "nested": {
                            "path": "qa_pairs",
                            "query": {
                                "bool": {
                                    "must": [
                                        {"term": {"qa_pairs.q_text.keyword": test_info['question']}},
                                        {"match": {"qa_pairs.answer": test_info['positive_answer']}}
                                    ]
                                }
                            }
                        }
                    }
                ]
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
                if qa.get("q_text") == test_info['question']:
                    answer = qa.get("answer")
                    break

            print(f"✅ Found user with positive answer!")
            print(f"Sample User: {user_id}")
            print(f"Answer: {answer}")
            print()

            # extraction 실행 (debug=True)
            result = extract_behavior_from_qa_pairs(qa_pairs, behavior_key, debug=True)

            if result is True:
                print(f"✅ SUCCESS: Extraction returned True")
            elif result is False:
                print(f"⚠️ WARNING: Extraction returned False (expected True)")
            else:
                print(f"❌ FAIL: Extraction returned None (expected True)")
            print()
        else:
            print(f"❌ No user found with positive answer containing: {test_info['positive_answer']}")
            print()

    except Exception as e:
        print(f"ERROR: {str(e)}")
        print()

client.close()
print("=" * 100)
print("Debug Complete!")
print("=" * 100)
