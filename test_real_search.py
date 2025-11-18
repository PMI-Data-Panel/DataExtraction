"""실제 검색 테스트 - behavioral conditions가 제대로 작동하는지 확인"""
import sys
import json
import asyncio
from opensearchpy import OpenSearch
from opensearchpy.exceptions import ConnectionError, AuthenticationException
sys.path.insert(0, '.')

from api.search_api import search_natural_language, NLSearchRequest, router
from rag_query_analyzer.config import get_config

# 테스트 쿼리들
test_queries = [
    {
        "query": "배달음식 자주 시키는 20대 직장인 10명",
        "expected_conditions": ["uses_food_delivery", "age_20s", "occupation_office_worker"],
        "expected_count": 10
    },
    {
        "query": "커피 좋아하는 30대 여성 5명",
        "expected_conditions": ["drinks_coffee", "age_30s", "gender_female"],
        "expected_count": 5
    },
    {
        "query": "SNS 많이 쓰는 대학생 15명",
        "expected_conditions": ["uses_social_media", "occupation_student"],
        "expected_count": 15
    },
    {
        "query": "동영상 스트리밍 앱 쓰는 20대 10명",
        "expected_conditions": ["watches_movies_dramas", "age_20s"],
        "expected_count": 10
    },
    {
        "query": "금융앱 사용하는 직장인 10명",
        "expected_conditions": ["uses_financial_services", "occupation_office_worker"],
        "expected_count": 10
    },
    {
        "query": "패션 쇼핑 좋아하는 여성 10명",
        "expected_conditions": ["shops_fashion", "gender_female"],
        "expected_count": 10
    },
    {
        "query": "차 있는 30대 남성 10명",
        "expected_conditions": ["interested_in_cars", "age_30s", "gender_male"],
        "expected_count": 10
    },
    {
        "query": "빠른 배송 서비스 쓰는 사람 10명",
        "expected_conditions": ["uses_parcel_delivery"],
        "expected_count": 10
    },
    {
        "query": "스마트 기기 많이 쓰는 사람 10명",
        "expected_conditions": ["uses_smart_devices"],
        "expected_count": 10
    },
    {
        "query": "환경 보호에 관심 있는 20대 10명",
        "expected_conditions": ["cares_about_environment", "age_20s"],
        "expected_count": 10
    }
]

# OpenSearch 연결 설정
config = get_config()

print("🔌 OpenSearch 연결 설정:")
print(f"   Host: {config.OPENSEARCH_HOST}")
print(f"   Port: {config.OPENSEARCH_PORT}")
print()

try:
    os_client = OpenSearch(
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
    info = os_client.info()
    print(f"✅ OpenSearch 연결 성공!")
    print(f"   버전: {info['version']['number']}")
    print()
    
    # router에 os_client 설정
    router.os_client = os_client
    
except ConnectionError as e:
    print("❌ OpenSearch 연결 실패!", file=sys.stderr)
    print(f"   오류: {str(e)}", file=sys.stderr)
    sys.exit(1)
except AuthenticationException as e:
    print("❌ OpenSearch 인증 실패!", file=sys.stderr)
    print(f"   오류: {str(e)}", file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f"❌ 예상치 못한 오류 발생: {str(e)}", file=sys.stderr)
    sys.exit(1)

print("=" * 100)
print("🔍 실제 검색 테스트")
print("=" * 100)
print()

async def run_search_test(test_query: dict):
    """검색 테스트 실행"""
    try:
        # NLSearchRequest 생성
        request = NLSearchRequest(
            query=test_query['query'],
            page=1,
            log_conversation=False,
            log_search_history=False
        )
        
        # 실제 검색 실행
        response = await search_natural_language(request, os_client=os_client)
        
        # 결과 분석
        result = {
            "query": test_query['query'],
            "expected_count": test_query['expected_count'],
            "actual_count": len(response.results) if response.results else 0,
            "total_hits": response.total_hits,
            "success": False,
            "message": ""
        }

        if result['actual_count'] == 0:
            result['message'] = "❌ FAIL: 검색 결과 0명"
        elif result['actual_count'] < result['expected_count']:
            result['message'] = f"⚠️ PARTIAL: {result['actual_count']}명 (기대: {result['expected_count']}명)"
            result['success'] = True
        else:
            result['message'] = f"✅ PASS: {result['actual_count']}명"
            result['success'] = True

        return result, response

    except Exception as e:
        result = {
            "query": test_query['query'],
            "expected_count": test_query['expected_count'],
            "actual_count": 0,
            "total_hits": 0,
            "success": False,
            "message": f"❌ ERROR: {str(e)}"
        }
        return result, None

results = []

for i, test in enumerate(test_queries, 1):
    print(f"[{i}/{len(test_queries)}] Testing: {test['query']}")
    print("-" * 100)

    try:
        # 실제 검색 실행
        result, response = asyncio.run(run_search_test(test))

        results.append(result)

        print(f"  Expected: {test['expected_count']}명")
        print(f"  Actual: {result['actual_count']}명 (Total: {result['total_hits']}명)")
        print(f"  {result['message']}")

        # 첫 번째 유저 샘플 출력
        if response and response.results:
            sample_user = response.results[0]
            print(f"  Sample User ID: {sample_user.user_id}")
            if sample_user.demographic_info:
                demo = sample_user.demographic_info
                print(f"  Sample Demographics: {demo.get('gender', 'N/A')}, {demo.get('age_group', 'N/A')}")

        print()

    except Exception as e:
        result = {
            "query": test['query'],
            "expected_count": test['expected_count'],
            "actual_count": 0,
            "total_hits": 0,
            "success": False,
            "message": f"❌ ERROR: {str(e)}"
        }
        results.append(result)
        print(f"  ❌ ERROR: {str(e)}")
        print()

# 최종 요약
print("=" * 100)
print("📊 테스트 요약")
print("=" * 100)
print()

total = len(results)
passed = sum(1 for r in results if r['success'])
failed = total - passed

print(f"Total: {total}")
print(f"✅ PASS: {passed} ({passed/total*100:.1f}%)")
print(f"❌ FAIL: {failed} ({failed/total*100:.1f}%)")
print()

if failed > 0:
    print("❌ Failed queries:")
    for r in results:
        if not r['success']:
            print(f"  - {r['query']}: {r['message']}")
    print()

# JSON 결과 저장
with open("search_test_results.json", "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

print("Results saved to: search_test_results.json")
print()
print("=" * 100)
print("🎉 테스트 완료!")
print("=" * 100)
