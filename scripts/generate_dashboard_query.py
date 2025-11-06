"""
OpenSearch Dashboards Dev Tools용 쿼리 생성기

사용법:
    python generate_dashboard_query.py "20대 남성"
"""
import sys
import json
from rag_query_analyzer.analyzers.rule_analyzer import RuleBasedAnalyzer
from rag_query_analyzer.utils.remote_query_builder import RemoteOpenSearchQueryBuilder


def generate_dashboard_query(query_text: str):
    """Dashboards Dev Tools용 쿼리 생성"""

    print("=" * 60)
    print(f"검색어: {query_text}")
    print("=" * 60)

    # 1. 쿼리 분석
    analyzer = RuleBasedAnalyzer()
    analysis = analyzer.analyze(query_text)

    print(f"\n[분석 결과]")
    print(f"추출된 키워드: {analysis.must_terms}")
    print(f"검색 의도: {analysis.intent}")
    print(f"신뢰도: {analysis.confidence:.2%}")

    # 2. 쿼리 생성
    builder = RemoteOpenSearchQueryBuilder()
    os_query = builder.build_query(analysis, size=10)

    # 3. Dev Tools 형식으로 출력
    print("\n" + "=" * 60)
    print("아래 쿼리를 복사해서 OpenSearch Dashboards Dev Tools에 붙여넣으세요:")
    print("=" * 60)
    print()
    print(f"GET s_welcome_2nd/_search")
    print(json.dumps(os_query, indent=2, ensure_ascii=False))
    print()
    print("=" * 60)

    # 4. 간단한 버전도 생성
    print("\n[간단한 버전 - 인구통계만]")
    print("=" * 60)

    # 20대, 남성 등 인구통계만 추출
    demo_terms = [term for term in analysis.must_terms if term in ["20대", "30대", "40대", "50대", "남성", "여성", "기혼", "미혼"]]

    if demo_terms:
        simple_query = {
            "query": {
                "bool": {
                    "must": []
                }
            },
            "size": 10
        }

        for term in demo_terms:
            simple_query["query"]["bool"]["must"].append({
                "nested": {
                    "path": "qa_pairs",
                    "query": {
                        "match": {
                            "qa_pairs.answer": term
                        }
                    }
                }
            })

        print(f"\nGET s_welcome_2nd/_search")
        print(json.dumps(simple_query, indent=2, ensure_ascii=False))


def main():
    if len(sys.argv) < 2:
        print("사용법: python generate_dashboard_query.py <검색어>")
        print("\n예시:")
        print('  python generate_dashboard_query.py "20대 남성"')
        print('  python generate_dashboard_query.py "서울 거주 미혼 여성"')
        sys.exit(1)

    query_text = sys.argv[1]
    generate_dashboard_query(query_text)


if __name__ == "__main__":
    main()
