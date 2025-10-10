"""
RAG Query Analyzer 데모

사용법:
    python -m examples.demo
"""

import sys
import asyncio
from pathlib import Path

# 프로젝트 루트 추가
sys.path.append(str(Path(__file__).parent.parent))

from rag_query_analyzer import AdvancedRAGQueryAnalyzer, Config
from rag_query_analyzer.models import SearchResult
from rag_query_analyzer.utils import setup_logging


def create_sample_results(num: int = 5) -> list[SearchResult]:
    """테스트용 샘플 검색 결과 생성"""
    samples = [
        SearchResult(
            doc_id="doc1",
            score=0.95,
            summary="30대 기혼 남성, 서울 거주 직장인",
            answers={"age": "30대", "gender": "남성", "job": "직장인"}
        ),
        SearchResult(
            doc_id="doc2",
            score=0.88,
            summary="20대 미혼 여성, 부산 거주 대학생",
            answers={"age": "20대", "gender": "여성", "job": "학생"}
        ),
        SearchResult(
            doc_id="doc3",
            score=0.85,
            summary="40대 기혼 여성, 서울 거주 주부",
            answers={"age": "40대", "gender": "여성", "job": "주부"}
        ),
        SearchResult(
            doc_id="doc4",
            score=0.82,
            summary="50대 남성, 대전 거주 자영업",
            answers={"age": "50대", "gender": "남성", "job": "자영업"}
        ),
        SearchResult(
            doc_id="doc5",
            score=0.79,
            summary="30대 미혼 여성, 경기 거주 프리랜서",
            answers={"age": "30대", "gender": "여성", "job": "프리랜서"}
        )
    ]
    return samples[:num]


def main():
    """메인 데모 함수"""
    # 로깅 설정
    config = Config()
    setup_logging(config)
    
    print("🚀 RAG Query Analyzer 데모")
    print("=" * 70)
    
    # 분석기 초기화
    analyzer = AdvancedRAGQueryAnalyzer(config)
    
    print("✅ 시스템 초기화 완료")
    print("=" * 70)
    print("명령어: 'quit' (종료), 'stats' (통계)")
    print("=" * 70 + "\n")
    
    while True:
        try:
            # 사용자 입력
            user_input = input("🔍 검색 쿼리: ").strip()
            
            if user_input.lower() in ['quit', 'exit', '종료']:
                print("\n👋 프로그램을 종료합니다.")
                break
            
            if user_input.lower() == 'stats':
                stats = analyzer.get_statistics()
                print("\n📊 시스템 통계:")
                for key, value in stats.items():
                    print(f"  {key}: {value}")
                print()
                continue
            
            if not user_input:
                continue
            
            print("\n" + "=" * 70)
            print("📊 쿼리 분석 중...")
            
            # 메타데이터 예시
            metadata = {
                "survey_type": "만족도",
                "period": "2024년 4분기",
                "region_scope": "전국",
                "sample_size": 1000
            }
            
            # 쿼리 분석 (재작성 포함)
            analysis, rewritten = analyzer.analyze_with_rewriting(
                user_input,
                context="고객 만족도 조사",
                metadata=metadata
            )
            
            # 결과 출력
            print("\n" + analyzer.explain_analysis(analysis))
            
            # 재작성된 쿼리
            if rewritten:
                print(f"\n📝 재작성된 쿼리 ({len(rewritten)}개):")
                for i, rq in enumerate(rewritten[:3], 1):
                    print(f"  {i}. {rq[:60]}...")
            
            # Elasticsearch 쿼리 예시
            es_query = analyzer.build_search_query(analysis)
            print(f"\n🔧 ES 쿼리 구조: {list(es_query.keys())}")
            
            # 리랭킹 데모
            if config.ENABLE_RERANKING:
                print("\n🔄 리랭킹 데모:")
                sample_results = create_sample_results()
                reranked = analyzer.rerank_results(user_input, sample_results, top_k=3)
                
                print("  Top 3 결과:")
                for i, result in enumerate(reranked, 1):
                    print(f"    {i}. {result.summary} (점수: {result.get_final_score():.3f})")
            
            # 피드백 수집
            feedback = input("\n평가 (1-10, Enter로 건너뛰기): ").strip()
            if feedback and feedback.isdigit():
                score = int(feedback) / 10
                analyzer.log_performance(
                    user_input, 
                    analysis, 
                    reranked if config.ENABLE_RERANKING else [],
                    user_feedback=score
                )
                print("✅ 피드백 저장됨")
            
            print("=" * 70 + "\n")
            
        except KeyboardInterrupt:
            print("\n\n👋 프로그램을 종료합니다.")
            break
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}\n")

