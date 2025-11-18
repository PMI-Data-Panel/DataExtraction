"""모든 Behavioral Conditions 테스트 스크립트"""
import sys
from opensearchpy import OpenSearch
from rag_query_analyzer.config import get_config

# API 모듈에서 키워드 맵과 추출 함수 임포트
sys.path.insert(0, '.')
from api.search_api import BEHAVIORAL_KEYWORD_MAP, extract_behavior_from_qa_pairs

# 설정 로드
config = get_config()

print("=" * 100)
print("🧪 Behavioral Conditions 전체 테스트")
print("=" * 100)
print()

# OpenSearch 연결
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

# 테스트 결과 저장
test_results = []

print("Testing all behavioral conditions...")
print()

for behavior_key, keyword_config in BEHAVIORAL_KEYWORD_MAP.items():
    print(f"Testing: {behavior_key}")
    print("-" * 100)

    question_keywords = keyword_config['question_keywords']
    positive_keywords = keyword_config['positive_keywords']
    negative_keywords = keyword_config['negative_keywords']

    # 1. 실제 설문 데이터에서 해당 질문 찾기
    found_questions = []

    for q_keyword in question_keywords:  # ✅ 모든 키워드 테스트
        # wildcard 쿼리로 질문 검색
        query = {
            "size": 0,
            "query": {"match_all": {}},
            "aggs": {
                "qa_nested": {
                    "nested": {"path": "qa_pairs"},
                    "aggs": {
                        "matching_questions": {
                            "filter": {
                                "wildcard": {"qa_pairs.q_text.keyword": f"*{q_keyword}*"}
                            },
                            "aggs": {
                                "questions": {
                                    "terms": {
                                        "field": "qa_pairs.q_text.keyword",
                                        "size": 5
                                    },
                                    "aggs": {
                                        "sample_answers": {
                                            "terms": {
                                                "field": "qa_pairs.answer.keyword",
                                                "size": 5
                                            }
                                        }
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
            questions = response["aggregations"]["qa_nested"]["matching_questions"]["questions"]["buckets"]

            if questions:
                for q_bucket in questions[:1]:  # 첫 번째 질문만
                    found_questions.append({
                        "question": q_bucket["key"],
                        "count": q_bucket["doc_count"],
                        "sample_answers": [a["key"] for a in q_bucket["sample_answers"]["buckets"][:3]]
                    })
                break  # 질문을 찾았으면 더 이상 검색하지 않음
        except Exception as e:
            pass

    # 2. 샘플 user 데이터로 실제 추출 테스트
    extraction_result = None
    sample_user_id = None

    if found_questions:
        # 해당 질문에 답변한 user 1명 가져오기
        first_question = found_questions[0]["question"]

        sample_query = {
            "size": 1,
            "query": {
                "nested": {
                    "path": "qa_pairs",
                    "query": {
                        "term": {"qa_pairs.q_text.keyword": first_question}
                    }
                }
            },
            "_source": ["user_id", "qa_pairs"]
        }

        try:
            sample_response = client.search(index="survey_responses_merged", body=sample_query)

            if sample_response["hits"]["total"]["value"] > 0:
                hit = sample_response["hits"]["hits"][0]
                sample_user_id = hit["_source"]["user_id"]
                qa_pairs = hit["_source"].get("qa_pairs", [])

                # 실제 추출 함수 실행
                extraction_result = extract_behavior_from_qa_pairs(qa_pairs, behavior_key, debug=False)
        except Exception as e:
            pass

    # 3. 결과 정리
    status = "❌ FAIL"
    reason = "No matching questions found"

    if found_questions:
        if extraction_result is not None:
            status = "✅ PASS"
            reason = f"Extracted: {extraction_result}"
        else:
            status = "⚠️ PARTIAL"
            reason = "Question found, but extraction failed"

    result = {
        "behavior_key": behavior_key,
        "status": status,
        "reason": reason,
        "found_questions": len(found_questions),
        "sample_question": found_questions[0]["question"] if found_questions else None,
        "sample_user": sample_user_id,
        "extraction_result": extraction_result
    }

    test_results.append(result)

    # 출력
    print(f"  Status: {status}")
    print(f"  Reason: {reason}")
    if found_questions:
        print(f"  Sample Question: {found_questions[0]['question'][:80]}...")
        print(f"  Sample Answers: {found_questions[0]['sample_answers'][:3]}")
    if sample_user_id:
        print(f"  Sample User: {sample_user_id}")
    print()

client.close()

# 최종 요약
print()
print("=" * 100)
print("📊 Test Summary")
print("=" * 100)
print()

total = len(test_results)
passed = sum(1 for r in test_results if r["status"] == "✅ PASS")
partial = sum(1 for r in test_results if r["status"] == "⚠️ PARTIAL")
failed = sum(1 for r in test_results if r["status"] == "❌ FAIL")

print(f"Total: {total}")
print(f"✅ PASS: {passed} ({passed/total*100:.1f}%)")
print(f"⚠️ PARTIAL: {partial} ({partial/total*100:.1f}%)")
print(f"❌ FAIL: {failed} ({failed/total*100:.1f}%)")
print()

# FAIL 항목 상세
if failed > 0:
    print("❌ Failed Conditions:")
    print("-" * 100)
    for r in test_results:
        if r["status"] == "❌ FAIL":
            print(f"  - {r['behavior_key']}: {r['reason']}")
    print()

# PARTIAL 항목 상세
if partial > 0:
    print("⚠️ Partial Conditions (needs keyword refinement):")
    print("-" * 100)
    for r in test_results:
        if r["status"] == "⚠️ PARTIAL":
            print(f"  - {r['behavior_key']}: {r['reason']}")
            if r['sample_question']:
                print(f"    Question: {r['sample_question'][:80]}...")
    print()

# PASS 항목 리스트
if passed > 0:
    print("✅ Passed Conditions:")
    print("-" * 100)
    for r in test_results:
        if r["status"] == "✅ PASS":
            print(f"  - {r['behavior_key']}: {r['extraction_result']}")
    print()

print("=" * 100)
print("🎉 Test Complete!")
print("=" * 100)

# JSON 결과 저장
import json
with open("behavioral_test_results.json", "w", encoding="utf-8") as f:
    json.dump(test_results, f, ensure_ascii=False, indent=2)
print()
print("Results saved to: behavioral_test_results.json")
