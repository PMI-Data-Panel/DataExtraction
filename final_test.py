"""RAG Query Analyzer 전체 알고리즘 테스트"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import requests
import json

url = "http://localhost:8001/search/query"

# 테스트 쿼리
test_queries = [
    {"query": "20대 전문직", "description": "20대 전문직 검색"},
    {"query": "30대 사무직", "description": "30대 사무직 검색"},
]

print("=" * 80)
print("RAG Query Analyzer 전체 알고리즘 테스트")
print("=" * 80)

for test in test_queries:
    query_text = test["query"]
    desc = test["description"]

    print(f"\n{'='*80}")
    print(f"테스트: {desc}")
    print(f"쿼리: '{query_text}'")
    print("=" * 80)

    payload = {
        "query": query_text,
        "index_name": "s_welcome_2nd",
        "size": 5,
        "use_vector_search": False
    }

    try:
        response = requests.post(url, json=payload, timeout=60)

        if response.status_code == 200:
            result = response.json()

            print(f"\n[결과]")
            print(f"총 검색 결과: {result['total_hits']}건")
            if result.get('max_score'):
                print(f"최고 점수: {result['max_score']:.4f}")
            print(f"소요 시간: {result['took_ms']}ms")

            print(f"\n[RAG 쿼리 분석 결과]")
            analysis = result.get('query_analysis', {})
            print(f"  ✓ 의도(intent): {analysis.get('intent')}")
            print(f"  ✓ must_terms: {analysis.get('must_terms')}")
            print(f"  ✓ should_terms: {analysis.get('should_terms')}")
            print(f"  ✓ alpha(벡터 가중치): {analysis.get('alpha')}")
            print(f"  ✓ confidence: {analysis.get('confidence')}")

            if result['results']:
                print(f"\n[검색 결과 상위 {min(3, len(result['results']))}건]")
                for i, item in enumerate(result['results'][:3], 1):
                    print(f"\n{i}. User ID: {item['user_id']}")
                    print(f"   점수: {item['score']:.4f}")

                    # matched_qa_pairs 출력
                    matched_qa = item.get('matched_qa_pairs', [])
                    if matched_qa:
                        print(f"   매칭된 QA:")
                        for qa in matched_qa[:3]:
                            q_text = qa.get('q_text', '')
                            answer = qa.get('answer', '')
                            print(f"   - {q_text}: {answer[:80]}")
            else:
                print("\n[경고] 검색 결과가 없습니다!")

        else:
            print(f"\n[오류] HTTP {response.status_code}")
            print(response.text)

    except Exception as e:
        print(f"\n[오류] {e}")

print(f"\n{'='*80}")
print("테스트 완료!")
print("=" * 80)
