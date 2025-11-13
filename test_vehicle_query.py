#!/usr/bin/env python3
"""차량 필터 디버깅 스크립트"""
import json
from opensearchpy import OpenSearch

# OpenSearch 연결
client = OpenSearch(
    hosts=[{'host': '159.223.47.188', 'port': 9200}],
    http_auth=('admin', 'AVNS_1unywEqMDAepzpSW6vU'),
    use_ssl=False,
    verify_certs=False,
    ssl_show_warn=False
)

print("=" * 80)
print("차량 필터 디버깅 테스트")
print("=" * 80)

# 테스트 1: q_text가 정확히 "보유차량여부"인 문서 찾기 (term 쿼리)
print("\n[테스트 1] term 쿼리로 q_text='보유차량여부' 찾기")
print("-" * 80)
query1 = {
    "size": 3,
    "query": {
        "nested": {
            "path": "qa_pairs",
            "query": {
                "term": {"qa_pairs.q_text": "보유차량여부"}
            }
        }
    },
    "_source": ["user_id", "qa_pairs"]
}

try:
    response = client.search(index="s_welcome_2nd", body=query1)
    total = response['hits']['total']['value']
    print(f"✅ 결과: {total}건")
    if total > 0:
        for hit in response['hits']['hits'][:3]:
            user_id = hit['_source'].get('user_id')
            qa_pairs = hit['_source'].get('qa_pairs', [])
            vehicle_qa = [qa for qa in qa_pairs if qa.get('q_text') == '보유차량여부']
            if vehicle_qa:
                print(f"  User: {user_id}, Answer: {vehicle_qa[0].get('answer')}")
except Exception as e:
    print(f"❌ 에러: {e}")

# 테스트 2: match 쿼리로 q_text에 "보유차량" 포함된 것 찾기
print("\n[테스트 2] match 쿼리로 q_text에 '보유차량' 포함된 것 찾기")
print("-" * 80)
query2 = {
    "size": 0,
    "query": {
        "nested": {
            "path": "qa_pairs",
            "query": {
                "match": {"qa_pairs.q_text": "보유차량"}
            }
        }
    }
}

try:
    response = client.search(index="s_welcome_2nd", body=query2)
    total = response['hits']['total']['value']
    print(f"✅ 결과: {total}건")
except Exception as e:
    print(f"❌ 에러: {e}")

# 테스트 3: 현재 코드와 동일한 차량 필터 (match + term 조합)
print("\n[테스트 3] 현재 코드의 차량 필터 (30대 남성 + 차량 있음)")
print("-" * 80)

VEHICLE_QUESTION_KEYWORDS = [
    "보유차량", "차량여부", "차량 여부", "자동차", "차량", "차 보유", "차량보유"
]
BEHAVIOR_YES_TOKENS = [
    "있다", "있음", "있어요", "yes", "y", "보유", "보유함", "보유중", "한다", "합니다", "해요"
]

question_should = [
    {"match": {"qa_pairs.q_text": q}}
    for q in VEHICLE_QUESTION_KEYWORDS
]
answer_should = [
    {"bool": {"should": [
        {"term": {"qa_pairs.answer": kw}},
        {"match_phrase": {"qa_pairs.answer_text": kw}}
    ]}}
    for kw in BEHAVIOR_YES_TOKENS
]

query3 = {
    "size": 0,
    "query": {
        "bool": {
            "must": [
                {
                    "nested": {
                        "path": "qa_pairs",
                        "query": {
                            "bool": {
                                "must": [
                                    {"bool": {"should": question_should, "minimum_should_match": 1}},
                                    {"bool": {"should": answer_should, "minimum_should_match": 1}}
                                ]
                            }
                        }
                    }
                }
            ],
            "filter": [
                {"term": {"metadata.gender": "남자"}},
                {"term": {"metadata.age_group": "30대"}}
            ]
        }
    }
}

print(f"질문 키워드: {VEHICLE_QUESTION_KEYWORDS}")
print(f"답변 토큰: {BEHAVIOR_YES_TOKENS[:5]}...")

try:
    response = client.search(index="s_welcome_2nd", body=query3)
    total = response['hits']['total']['value']
    print(f"✅ 결과: {total}건")
except Exception as e:
    print(f"❌ 에러: {e}")

# 테스트 4: "보유차량여부" 추가한 버전
print("\n[테스트 4] '보유차량여부' 키워드 추가 버전")
print("-" * 80)

VEHICLE_QUESTION_KEYWORDS_FIXED = [
    "보유차량여부", "보유차량", "차량여부", "차량 여부", "자동차", "차량", "차 보유", "차량보유"
]

question_should_fixed = [
    {"match": {"qa_pairs.q_text": q}}
    for q in VEHICLE_QUESTION_KEYWORDS_FIXED
]

query4 = {
    "size": 0,
    "query": {
        "bool": {
            "must": [
                {
                    "nested": {
                        "path": "qa_pairs",
                        "query": {
                            "bool": {
                                "must": [
                                    {"bool": {"should": question_should_fixed, "minimum_should_match": 1}},
                                    {"bool": {"should": answer_should, "minimum_should_match": 1}}
                                ]
                            }
                        }
                    }
                }
            ],
            "filter": [
                {"term": {"metadata.gender": "남자"}},
                {"term": {"metadata.age_group": "30대"}}
            ]
        }
    }
}

print(f"수정된 질문 키워드: {VEHICLE_QUESTION_KEYWORDS_FIXED}")

try:
    response = client.search(index="s_welcome_2nd", body=query4)
    total = response['hits']['total']['value']
    print(f"✅ 결과: {total}건")
    if total > 0:
        print("🎉 성공! '보유차량여부'를 추가하니 검색됩니다!")
except Exception as e:
    print(f"❌ 에러: {e}")

print("\n" + "=" * 80)
print("디버깅 완료!")
print("=" * 80)
