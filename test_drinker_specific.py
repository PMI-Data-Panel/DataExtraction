"""Test drinker extraction specifically"""
import sys
from opensearchpy import OpenSearch
from opensearchpy.exceptions import ConnectionError, AuthenticationException
from rag_query_analyzer.config import get_config

sys.path.insert(0, '.')
from api.search_api import extract_behavior_from_qa_pairs

config = get_config()

print("🔌 OpenSearch 연결 설정:")
print(f"   Host: {config.OPENSEARCH_HOST}")
print(f"   Port: {config.OPENSEARCH_PORT}")
print(f"   User: {config.OPENSEARCH_USER}")
print(f"   SSL: {config.OPENSEARCH_USE_SSL}")
print()

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
    
    # 연결 테스트
    print("🔍 OpenSearch 연결 테스트 중...")
    info = client.info()
    print(f"✅ OpenSearch 연결 성공!")
    print(f"   버전: {info['version']['number']}")
    print()
except ConnectionError as e:
    print("❌ OpenSearch 연결 실패!", file=sys.stderr)
    print(f"   오류: {str(e)}", file=sys.stderr)
    print(file=sys.stderr)
    print("💡 해결 방법:", file=sys.stderr)
    print("   1. OpenSearch 서버가 실행 중인지 확인하세요", file=sys.stderr)
    print(f"   2. {config.OPENSEARCH_HOST}:{config.OPENSEARCH_PORT}에 접근 가능한지 확인하세요", file=sys.stderr)
    print("   3. .env 파일에서 OPENSEARCH_HOST, OPENSEARCH_PORT 설정을 확인하세요", file=sys.stderr)
    print("   4. 방화벽 설정을 확인하세요", file=sys.stderr)
    sys.exit(1)
except AuthenticationException as e:
    print("❌ OpenSearch 인증 실패!", file=sys.stderr)
    print(f"   오류: {str(e)}", file=sys.stderr)
    print(file=sys.stderr)
    print("💡 해결 방법:", file=sys.stderr)
    print("   .env 파일에서 OPENSEARCH_USER, OPENSEARCH_PASSWORD를 확인하세요", file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f"❌ 예상치 못한 오류 발생: {str(e)}", file=sys.stderr)
    print(f"   오류 타입: {type(e).__name__}", file=sys.stderr)
    sys.exit(1)

print("=" * 100)
print("Testing drinker extraction with exact question: '음용경험 술'")
print("=" * 100)
print()

# 1. "음용경험 술" 질문 찾기
query = {
    "size": 0,
    "query": {"match_all": {}},
    "aggs": {
        "qa_nested": {
            "nested": {"path": "qa_pairs"},
            "aggs": {
                "matching_questions": {
                    "filter": {
                        "term": {"qa_pairs.q_text.keyword": "음용경험 술"}
                    },
                    "aggs": {
                        "sample_answers": {
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

try:
    response = client.search(index="survey_responses_merged", body=query)
    count = response["aggregations"]["qa_nested"]["matching_questions"]["doc_count"]
    answers = response["aggregations"]["qa_nested"]["matching_questions"]["sample_answers"]["buckets"]

    print(f"Question '음용경험 술' found: {count} responses")
    print()
    print("Sample answers:")
    for ans in answers:
        print(f"  - {ans['key']} (count: {ans['doc_count']})")
    print()

except Exception as e:
    print(f"❌ 검색 실패: {str(e)}", file=sys.stderr)
    print(file=sys.stderr)
    print("💡 해결 방법:", file=sys.stderr)
    print("   1. survey_responses_merged 인덱스가 존재하는지 확인하세요", file=sys.stderr)
    print("   2. 인덱스 이름이 올바른지 확인하세요", file=sys.stderr)
    client.close()
    sys.exit(1)

# 2. 샘플 유저로 extraction 테스트
print("=" * 100)
print("Testing extraction on sample users")
print("=" * 100)
print()

sample_query = {
    "size": 5,
    "query": {
        "nested": {
            "path": "qa_pairs",
            "query": {
                "term": {"qa_pairs.q_text.keyword": "음용경험 술"}
            }
        }
    },
    "_source": ["user_id", "qa_pairs"]
}

try:
    sample_response = client.search(index="survey_responses_merged", body=sample_query)

    for hit in sample_response["hits"]["hits"]:
        user_id = hit["_source"]["user_id"]
        qa_pairs = hit["_source"].get("qa_pairs", [])

        # 음용경험 술 답변 찾기
        drink_answer = None
        for qa in qa_pairs:
            if qa.get("q_text") == "음용경험 술":
                drink_answer = qa.get("answer")
                break

        # extraction 실행
        result = extract_behavior_from_qa_pairs(qa_pairs, "drinker", debug=False)

        print(f"User: {user_id}")
        print(f"  Answer: {drink_answer}")
        print(f"  Extracted drinker: {result}")
        print()

except Exception as e:
    print(f"❌ 샘플 쿼리 실패: {str(e)}", file=sys.stderr)
    print(file=sys.stderr)
    print("💡 해결 방법:", file=sys.stderr)
    print("   1. survey_responses_merged 인덱스가 존재하는지 확인하세요", file=sys.stderr)
    print("   2. 인덱스에 데이터가 있는지 확인하세요", file=sys.stderr)

client.close()

print("=" * 100)
print("Test Complete!")
print("=" * 100)
