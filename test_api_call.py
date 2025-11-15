"""실제 API 호출 테스트"""
import sys
import json
import requests

if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# API 엔드포인트
url = "http://localhost:8000/search/nl"

# 테스트 쿼리
payload = {
    "query": "술 마신 경험이 있는 20대 남성",
    "use_vector_search": True,
    "index_name": "survey_responses_merged",
    "size": 10,
    "page": 1
}

print("=" * 80)
print(f"쿼리: '{payload['query']}'")
print("=" * 80)

try:
    print("\n🔄 API 호출 중...")
    response = requests.post(url, json=payload, timeout=60)

    if response.status_code == 200:
        result = response.json()

        print(f"\n✅ 검색 성공!")
        print(f"   총 결과: {result.get('total_hits', 0)}건")
        print(f"   실행 시간: {result.get('took_ms', 0)}ms")

        # Query Analysis 출력
        if 'query_analysis' in result:
            qa = result['query_analysis']
            print(f"\n📊 Query Analysis:")
            print(f"   Must terms: {qa.get('must_terms', [])}")
            print(f"   Should terms: {qa.get('should_terms', [])}")
            if 'behavioral_conditions' in qa:
                print(f"   Behavioral: {qa.get('behavioral_conditions', {})}")

        # 결과 샘플
        results = result.get('results', [])
        print(f"\n📋 결과 샘플 (상위 {min(5, len(results))}개):")
        for i, item in enumerate(results[:5], 1):
            user_id = item.get('user_id', 'unknown')
            score = item.get('score', 0)
            demo = item.get('demographic_info', {})

            print(f"\n{i}. user_id: {user_id}")
            print(f"   점수: {score:.4f}")
            print(f"   성별: {demo.get('gender', 'N/A')}, 연령: {demo.get('age', 'N/A')}")

            # QA 샘플
            qa_pairs = item.get('qa_pairs', [])
            if qa_pairs:
                print(f"   QA 개수: {len(qa_pairs)}개")

                # 술 관련 QA 찾기
                alcohol_qa = [qa for qa in qa_pairs
                             if any(kw in qa.get('q_text', '') for kw in ['술', '음주', '음용'])]
                if alcohol_qa:
                    print(f"   술 관련 QA:")
                    for qa in alcohol_qa[:2]:
                        print(f"      Q: {qa.get('q_text', '')[:40]}")
                        print(f"      A: {qa.get('answer', '')[:60]}")

    else:
        print(f"\n❌ 검색 실패: HTTP {response.status_code}")
        print(f"   응답: {response.text[:500]}")

except requests.exceptions.ConnectionError:
    print("\n❌ API 서버에 연결할 수 없습니다.")
    print("   서버가 실행 중인지 확인하세요: http://localhost:8000")
except Exception as e:
    print(f"\n❌ 오류 발생: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
