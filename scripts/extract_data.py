"""
원격 OpenSearch에서 데이터 추출 스크립트

사용 예시:
    python extract_data.py "20대 남성"
    python extract_data.py "서울 거주 미혼 여성"
    python extract_data.py "TV를 보유한 사람"
"""
import os
import sys
import json
import logging
from opensearchpy import OpenSearch
from dotenv import load_dotenv

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 환경 변수 로드
load_dotenv()

# query_analyzer import
from rag_query_analyzer.analyzers.main_analyzer import AdvancedRAGQueryAnalyzer
from rag_query_analyzer.analyzers.rule_analyzer import RuleBasedAnalyzer
from rag_query_analyzer.utils.remote_query_builder import RemoteOpenSearchQueryBuilder
from rag_query_analyzer.config import Config


class DataExtractor:
    """원격 OpenSearch 데이터 추출기"""

    def __init__(self):
        """초기화"""
        # 원격 OpenSearch 연결
        self.remote_host = os.getenv("OPENSEARCH_HOST", "159.223.47.188")
        self.remote_port = int(os.getenv("OPENSEARCH_PORT", "9200"))
        self.remote_user = os.getenv("OPENSEARCH_USERNAME", "admin")
        self.remote_password = os.getenv("OPENSEARCH_PASSWORD")

        logger.info(f"원격 OpenSearch 연결: {self.remote_host}:{self.remote_port}")

        self.client = OpenSearch(
            hosts=[{'host': self.remote_host, 'port': self.remote_port}],
            http_auth=(self.remote_user, self.remote_password),
            use_ssl=True,
            verify_certs=False,
            ssl_show_warn=False,
            timeout=60
        )

        # 연결 확인
        if not self.client.ping():
            raise ConnectionError("원격 OpenSearch 연결 실패")

        logger.info("[OK] 원격 OpenSearch 연결 성공")

        # Query Analyzer 초기화 (Rule-based만 사용 - API 키 불필요)
        self.analyzer = RuleBasedAnalyzer()
        self.query_builder = RemoteOpenSearchQueryBuilder()

        # 기본 인덱스
        self.index_name = "s_welcome_2nd"

    def search(self, query_text: str, size: int = 10) -> dict:
        """
        자연어 검색

        Args:
            query_text: 검색어 (예: "20대 남성")
            size: 반환할 문서 개수

        Returns:
            검색 결과
        """
        logger.info("=" * 60)
        logger.info(f"검색어: {query_text}")
        logger.info("=" * 60)

        # 1. 쿼리 분석
        logger.info("\n[1단계] 쿼리 분석 중...")
        analysis = self.analyzer.analyze(query_text)

        logger.info(f"  - 추출된 키워드: {analysis.must_terms}")
        logger.info(f"  - 검색 의도: {analysis.intent}")
        logger.info(f"  - 신뢰도: {analysis.confidence:.2%}")

        # 2. OpenSearch 쿼리 생성
        logger.info("\n[2단계] OpenSearch 쿼리 생성 중...")
        os_query = self.query_builder.build_query(analysis, size=size)

        logger.info(f"  - 생성된 쿼리:")
        logger.info(json.dumps(os_query, indent=2, ensure_ascii=False))

        # 3. 검색 실행
        logger.info("\n[3단계] 검색 실행 중...")
        try:
            response = self.client.search(
                index=self.index_name,
                body=os_query
            )

            total_hits = response['hits']['total']['value']
            logger.info(f"  - 검색 결과: {total_hits}건")

            return {
                "query": query_text,
                "analysis": analysis.to_dict(),
                "opensearch_query": os_query,
                "total_hits": total_hits,
                "results": response['hits']['hits'],
                "took_ms": response['took']
            }

        except Exception as e:
            logger.error(f"검색 실패: {e}")
            raise

    def extract_to_json(self, query_text: str, output_path: str = None, size: int = 100):
        """
        검색 결과를 JSON 파일로 저장

        Args:
            query_text: 검색어
            output_path: 저장 경로 (기본: ./output/search_results.json)
            size: 추출할 문서 개수
        """
        # 검색 실행
        result = self.search(query_text, size=size)

        # 출력 경로 설정
        if output_path is None:
            os.makedirs("./output", exist_ok=True)
            # 파일명에 검색어 포함
            safe_query = "".join(c for c in query_text if c.isalnum() or c in (' ', '_')).strip()
            safe_query = safe_query.replace(' ', '_')[:50]
            output_path = f"./output/search_{safe_query}.json"

        # JSON 저장
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        logger.info(f"\n[저장 완료] {output_path}")
        logger.info(f"  - 총 {result['total_hits']}건 중 {len(result['results'])}건 저장")

        return output_path

    def extract_to_csv(self, query_text: str, output_path: str = None, size: int = 100):
        """
        검색 결과를 CSV 파일로 저장

        Args:
            query_text: 검색어
            output_path: 저장 경로
            size: 추출할 문서 개수
        """
        import pandas as pd

        # 검색 실행
        result = self.search(query_text, size=size)

        # 데이터 평탄화 (flatten)
        rows = []
        for hit in result['results']:
            user_id = hit['_source']['user_id']
            timestamp = hit['_source'].get('timestamp', '')

            # 각 qa_pair를 행으로 변환
            for qa in hit['_source']['qa_pairs']:
                rows.append({
                    'user_id': user_id,
                    'timestamp': timestamp,
                    'q_code': qa.get('q_code', ''),
                    'q_type': qa.get('q_type', ''),
                    'q_text': qa.get('q_text', ''),
                    'answer': json.dumps(qa.get('answer', ''), ensure_ascii=False) if isinstance(qa.get('answer'), list) else qa.get('answer', ''),
                    'score': hit['_score']
                })

        # DataFrame 생성
        df = pd.DataFrame(rows)

        # 출력 경로 설정
        if output_path is None:
            os.makedirs("./output", exist_ok=True)
            safe_query = "".join(c for c in query_text if c.isalnum() or c in (' ', '_')).strip()
            safe_query = safe_query.replace(' ', '_')[:50]
            output_path = f"./output/search_{safe_query}.csv"

        # CSV 저장
        df.to_csv(output_path, index=False, encoding='utf-8-sig')

        logger.info(f"\n[저장 완료] {output_path}")
        logger.info(f"  - 총 {len(df)}개 행 저장")

        return output_path

    def print_results(self, query_text: str, size: int = 5):
        """
        검색 결과를 콘솔에 출력

        Args:
            query_text: 검색어
            size: 출력할 문서 개수
        """
        result = self.search(query_text, size=size)

        print("\n" + "=" * 60)
        print(f"검색 결과: {result['total_hits']}건")
        print("=" * 60)

        for idx, hit in enumerate(result['results'], 1):
            print(f"\n[{idx}] 사용자 ID: {hit['_source']['user_id']} (점수: {hit['_score']:.2f})")
            print(f"    타임스탬프: {hit['_source'].get('timestamp', 'N/A')}")

            # qa_pairs 중 일부만 출력
            qa_pairs = hit['_source']['qa_pairs']
            print(f"    응답 개수: {len(qa_pairs)}개")

            # 처음 3개만 출력
            for qa in qa_pairs[:3]:
                answer = qa.get('answer', '')
                if isinstance(answer, list):
                    answer = ', '.join(answer[:3]) + ('...' if len(answer) > 3 else '')
                print(f"      • {qa.get('q_text', '')}: {answer}")

            if len(qa_pairs) > 3:
                print(f"      ... (외 {len(qa_pairs)-3}개)")


def main():
    """메인 함수"""
    if len(sys.argv) < 2:
        print("사용법: python extract_data.py <검색어> [옵션]")
        print("\n예시:")
        print("  python extract_data.py \"20대 남성\"")
        print("  python extract_data.py \"서울 거주 미혼 여성\" --json")
        print("  python extract_data.py \"TV 보유\" --csv --size 50")
        print("\n옵션:")
        print("  --json        : JSON 파일로 저장")
        print("  --csv         : CSV 파일로 저장")
        print("  --size N      : 추출할 문서 개수 (기본: 10)")
        print("  --output PATH : 저장 경로")
        sys.exit(1)

    # 검색어
    query_text = sys.argv[1]

    # 옵션 파싱
    save_json = "--json" in sys.argv
    save_csv = "--csv" in sys.argv

    size = 10
    if "--size" in sys.argv:
        size_idx = sys.argv.index("--size")
        if size_idx + 1 < len(sys.argv):
            size = int(sys.argv[size_idx + 1])

    output_path = None
    if "--output" in sys.argv:
        output_idx = sys.argv.index("--output")
        if output_idx + 1 < len(sys.argv):
            output_path = sys.argv[output_idx + 1]

    # 추출기 초기화
    try:
        extractor = DataExtractor()

        # 저장 또는 출력
        if save_json:
            extractor.extract_to_json(query_text, output_path, size)
        elif save_csv:
            extractor.extract_to_csv(query_text, output_path, size)
        else:
            extractor.print_results(query_text, size)

    except Exception as e:
        logger.error(f"오류 발생: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
