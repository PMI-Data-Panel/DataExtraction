"""
RAG 시스템 간단 데모
실제 데이터 없이 시스템 동작을 테스트합니다.
"""
import os
import pandas as pd
import torch
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

print("="*70)
print(" RAG 설문 검색 시스템 - 간단 데모")
print("="*70 + "\n")

# 1. 임베딩 모델 테스트
print("1. KURE-v1 임베딩 모델 로드")
print("-"*70)

from sentence_transformers import SentenceTransformer

device = 'cuda' if torch.cuda.is_available() else 'cpu'
print(f"   장치: {device}")

embedding_model = SentenceTransformer("nlpai-lab/KURE-v1", device=device)
embedding_model.max_seq_length = 512

print(f"   모델 로드 완료!")
print(f"   차원: 1024, 최대 길이: 512 토큰\n")

# 2. 쿼리 복잡도 분석 테스트
print("2. 쿼리 복잡도 분석 테스트")
print("-"*70)

from rag_query_analyzer.core.query_complexity import get_complexity_analyzer

complexity_analyzer = get_complexity_analyzer()

test_queries = [
    "30대 남성",  # 단순
    "30대 남성 중 스트레스가 높은 사람",  # 중간
    "30대 남성과 40대 여성의 스트레스 수준을 비교하여 어떤 그룹이 더 높은지 분석"  # 복잡
]

for query in test_queries:
    complexity = complexity_analyzer.analyze(query)
    print(f"\n   쿼리: '{query}'")
    print(f"   복잡도: {complexity.total_score:.1f}점 ({complexity.level})")
    print(f"   LLM 사용: {'Yes' if complexity.use_llm else 'No'}")
    print(f"   이유: {', '.join(complexity.reasons[:2])}")

print()

# 3. Rule-based Analyzer 테스트
print("3. Rule-based Analyzer 테스트")
print("-"*70)

from rag_query_analyzer.analyzers.rule_analyzer import RuleBasedAnalyzer

rule_analyzer = RuleBasedAnalyzer()

query = "30대 남성 중 스트레스가 높은 사람"
analysis = rule_analyzer.analyze(query)

print(f"   쿼리: '{query}'")
print(f"   의도: {analysis.intent}")
print(f"   필수 키워드: {analysis.must_terms}")
print(f"   선택 키워드: {analysis.should_terms}")
print(f"   신뢰도: {analysis.confidence:.2f}")
print(f"   Alpha: {analysis.alpha:.2f}")

if analysis.expanded_keywords:
    print(f"   확장 키워드:")
    for key, values in list(analysis.expanded_keywords.items())[:2]:
        print(f"      {key} -> {', '.join(values)}")

print()

# 4. 임베딩 생성 테스트
print("4. 임베딩 생성 테스트 (배치)")
print("-"*70)

test_texts = [
    "서비스에 매우 만족합니다",
    "배송이 조금 느렸습니다",
    "가격이 적절합니다",
    "고객 응대가 친절했습니다"
]

print(f"   텍스트 개수: {len(test_texts)}")
print(f"   배치 인코딩 중...")

import time
start = time.time()
vectors = embedding_model.encode(test_texts, batch_size=4)
elapsed = time.time() - start

print(f"   완료! (소요 시간: {elapsed:.3f}초)")
print(f"   벡터 shape: {vectors.shape}")
print(f"   속도: {len(test_texts)/elapsed:.1f} texts/sec")

print()

# 5. 유사도 계산 테스트
print("5. 유사도 계산 테스트")
print("-"*70)

from scipy.spatial.distance import cosine

text1 = "서비스가 좋습니다"
text2 = "서비스에 만족합니다"
text3 = "배송이 느립니다"

v1 = embedding_model.encode(text1)
v2 = embedding_model.encode(text2)
v3 = embedding_model.encode(text3)

sim_12 = 1 - cosine(v1, v2)
sim_13 = 1 - cosine(v1, v3)

print(f"   '{text1}' <-> '{text2}'")
print(f"   유사도: {sim_12:.3f} (높음 - 같은 의미)\n")

print(f"   '{text1}' <-> '{text3}'")
print(f"   유사도: {sim_13:.3f} (낮음 - 다른 의미)")

print()

# 6. Claude Analyzer 테스트
print("6. Claude Analyzer 테스트 (LLM)")
print("-"*70)

from rag_query_analyzer.analyzers.claude_analyzer import ClaudeAnalyzer
from rag_query_analyzer.config import get_config

config = get_config()

try:
    claude_analyzer = ClaudeAnalyzer(config)

    query = "30대 남성과 40대 여성의 만족도를 비교"
    print(f"   쿼리: '{query}'")
    print(f"   Claude API 호출 중...")

    analysis = claude_analyzer.analyze(query)

    print(f"   의도: {analysis.intent}")
    print(f"   필수 키워드: {analysis.must_terms}")
    print(f"   신뢰도: {analysis.confidence:.2f}")

    if analysis.reasoning_steps:
        print(f"   추론 과정:")
        for step in analysis.reasoning_steps[:2]:
            print(f"      - {step}")

except Exception as e:
    print(f"   [SKIP] Claude API 오류: {e}")

print()

# 7. 데이터 분류 테스트
print("7. 질문 분류 테스트")
print("-"*70)

from rag_query_analyzer.data_processing import QuestionClassifier

classifier = QuestionClassifier()

test_data = [
    ("나이는?", "30대"),
    ("성별은?", "남성"),
    ("서비스 만족도는?", "매우 만족합니다. 특히 고객 응대가 좋았어요."),
]

for q_text, answer in test_data:
    q_type = classifier.classify(q_text, answer)
    field = classifier.get_demographic_field(q_text) if q_type == "객관식" else None

    print(f"   Q: {q_text}")
    print(f"   A: {answer}")
    print(f"   분류: {q_type}")
    if field:
        print(f"   필드: {field}")
    print()

print("="*70)
print(" 데모 완료!")
print("="*70)
print("\n다음 단계:")
print("  1. OpenSearch 실행: docker-compose up -d opensearch")
print("  2. 실제 데이터 준비: data/survey_welcome2.csv")
print("  3. API 서버 실행: python main.py")
print("  4. 검색 테스트: curl 'http://localhost:8000/intelligent-search/?query=30대%20남성'")
print()
