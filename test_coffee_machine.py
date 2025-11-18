"""커피 머신 보유 유저 찾기"""
import sys
from opensearchpy import OpenSearch
from opensearchpy.exceptions import ConnectionError, AuthenticationException
from rag_query_analyzer.config import get_config

sys.path.insert(0, '.')

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
print("🔍 커피 머신 보유 유저 검색")
print("=" * 100)
print()


# 1. "커피 머신" 키워드로 답변 검색
query = {
    "size": 5,
    "query": {
        "nested": {
            "path": "qa_pairs",
            "query": {
                "bool": {
                    "must": [
                        {"match": {"qa_pairs.q_text": "보유가전제품"}},
                        {"match": {"qa_pairs.answer": "커피 머신"}}
                    ]
                }
            }
        }
    },
    "_source": ["user_id", "qa_pairs"]
}

try:
    response = client.search(index="survey_responses_merged", body=query)
    total = response["hits"]["total"]["value"]

    print(f"✅ '커피 머신' 키워드 검색 결과: {total}건")
    print()

    if total > 0:
        print("📌 샘플 유저들:")
        for i, hit in enumerate(response["hits"]["hits"], 1):
            user_id = hit["_source"]["user_id"]
            qa_pairs = hit["_source"].get("qa_pairs", [])

            print(f"\n  [{i}] User: {user_id}")

            # 보유가전제품 질문 찾기
            for qa in qa_pairs:
                if "보유가전제품" in qa.get("q_text", ""):
                    answer = qa.get("answer", "")
                    if isinstance(answer, list):
                        print(f"      Answer (list): {answer[:5]}")
                        # 커피 머신 포함 여부 확인
                        coffee_items = [item for item in answer if "커피" in item]
                        if coffee_items:
                            print(f"      ✅ 커피 관련 항목: {coffee_items}")
                    else:
                        print(f"      Answer (str): {answer[:100]}...")
                    break
    else:
        print("❌ '커피 머신' 키워드로 검색된 유저 없음")
        print()
        print("🔍 다른 키워드로 시도:")

        # 2. "커피"만으로 검색
        query2 = {
            "size": 5,
            "query": {
                "nested": {
                    "path": "qa_pairs",
                    "query": {
                        "bool": {
                            "must": [
                                {"match": {"qa_pairs.q_text": "보유가전제품"}},
                                {"match": {"qa_pairs.answer": "커피"}}
                            ]
                        }
                    }
                }
            },
            "_source": ["user_id", "qa_pairs"]
        }

        response2 = client.search(index="survey_responses_merged", body=query2)
        total2 = response2["hits"]["total"]["value"]

        print(f"   '커피' 키워드 검색 결과: {total2}건")

        if total2 > 0:
            hit = response2["hits"]["hits"][0]
            user_id = hit["_source"]["user_id"]
            qa_pairs = hit["_source"].get("qa_pairs", [])

            print(f"   Sample User: {user_id}")

            for qa in qa_pairs:
                if "보유가전제품" in qa.get("q_text", ""):
                    answer = qa.get("answer", "")
                    if isinstance(answer, list):
                        print(f"   Answer (list): {answer[:5]}")
                        coffee_items = [item for item in answer if "커피" in item]
                        if coffee_items:
                            print(f"   ✅ 커피 관련 항목: {coffee_items}")
                    else:
                        print(f"   Answer (str): {answer[:100]}...")
                    break

except Exception as e:
    print(f"❌ 검색 실패: {str(e)}", file=sys.stderr)
    print(file=sys.stderr)
    print("💡 해결 방법:", file=sys.stderr)
    print("   1. survey_responses_merged 인덱스가 존재하는지 확인하세요", file=sys.stderr)
    print("   2. 인덱스 이름이 올바른지 확인하세요", file=sys.stderr)
    print("   3. 인덱스에 데이터가 있는지 확인하세요", file=sys.stderr)

client.close()
print()
print("=" * 100)
print("테스트 완료!")
print("=" * 100)

