"""
배치 처리 예제

사용법:
    python -m examples.batch_processing
"""

import sys
import asyncio
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from rag_query_analyzer import AdvancedRAGQueryAnalyzer, Config
from rag_query_analyzer.utils import setup_logging


async def batch_demo():
    """배치 처리 데모"""
    # 설정
    config = Config()
    config.ENABLE_ASYNC = True
    setup_logging(config)
    
    # 분석기 초기화
    analyzer = AdvancedRAGQueryAnalyzer(config)
    
    print("🚀 배치 처리 데모")
    print("=" * 70)
    
    # 테스트 쿼리들
    test_queries = [
        "30대 서울 거주 직장인의 만족도",
        "스트레스가 높은 20대 여성",
        "구매 의향이 있는 고소득층",
        "제품에 불만족하는 고객들",
        "자주 이용하는 충성 고객"
    ]
    
    print(f"📋 {len(test_queries)}개 쿼리 처리 시작...\n")
    
    # 배치 처리
    results = await analyzer.analyze_batch_async(
        test_queries,
        context="고객 만족도 조사"
    )
    
    # 결과 출력
    for query, analysis in zip(test_queries, results):
        print(f"쿼리: {query}")
        print(f"  • 의도: {analysis.intent}")
        print(f"  • 신뢰도: {analysis.confidence:.0%}")
        print(f"  • 키워드: {', '.join(analysis.must_terms[:3])}")
        print()
    
    print("=" * 70)
    print("✅ 배치 처리 완료!")
    
    # 통계
    stats = analyzer.get_statistics()
    print(f"\n📊 처리 통계:")
    print(f"  • 캐시 히트율: {stats.get('hit_rate', 0):.1%}")
    print(f"  • 처리 시간: {sum(r.execution_time for r in results):.2f}초")


# ========================================
# setup.py
# ========================================
"""
from setuptools import setup, find_packages

setup(
    name="rag-query-analyzer",
    version="1.0.0",
    description="Advanced RAG Query Analyzer for Survey Data",
    author="Your Name",
    packages=find_packages(),
    install_requires=[
        "anthropic>=0.3.0",
        "sentence-transformers>=2.0.0",
        "numpy>=1.20.0",
        "python-dotenv>=0.19.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "black>=22.0.0",
            "flake8>=4.0.0",
        ]
    },
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
            "rag-analyzer=examples.demo:main",
            "rag-batch=examples.batch_processing:main",
        ],
    },
)
"""


# ========================================
# 실행 예시
# ========================================
if __name__ == "__main__":
    # 데모 실행
    from examples.demo import main
    main()
    