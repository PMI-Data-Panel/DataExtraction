"""
데이터 형식 검증 스크립트

question_list.csv와 response_list.csv의 형식을 검증하고
OpenSearch 인덱싱에 문제가 없는지 확인합니다.
"""

import pandas as pd
import logging
from indexer.parser import parse_question_metadata, validate_metadata
from indexer.core import validate_response_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def validate_data_consistency(
    questions_meta: dict,
    response_file: str
) -> dict:
    """
    질문 메타데이터와 응답 데이터의 일관성을 검증합니다.

    Returns:
        검증 결과 딕셔너리
    """
    results = {
        "total_questions": len(questions_meta),
        "missing_in_responses": [],
        "extra_in_responses": [],
        "type_issues": [],
        "sample_data": {},
        "overall_status": "✅ 통과"
    }

    # 응답 데이터 로드
    logger.info(f"📊 응답 데이터 로딩: {response_file}")
    df = pd.read_csv(response_file, encoding="utf-8-sig", dtype=str)

    logger.info(f"   - 응답 수: {len(df):,}개")
    logger.info(f"   - 컬럼 수: {len(df.columns)}개")

    # 컬럼 확인
    response_columns = set(df.columns) - {'mb_sn'}
    question_codes = set(questions_meta.keys()) - {'mb_sn'}

    # 질문 메타데이터에는 있지만 응답에 없는 것
    missing = question_codes - response_columns
    if missing:
        results["missing_in_responses"] = list(missing)
        logger.warning(f"⚠️ 질문 메타데이터에는 있지만 응답 데이터에 없는 질문: {missing}")

    # 응답에는 있지만 질문 메타데이터에 없는 것
    extra = response_columns - question_codes
    if extra:
        results["extra_in_responses"] = list(extra)
        logger.warning(f"⚠️ 응답 데이터에는 있지만 질문 메타데이터에 없는 컬럼: {extra}")

    # 샘플 데이터 검증
    logger.info("\n📝 샘플 데이터 검증 (첫 5개 응답):")
    for idx, row in df.head(5).iterrows():
        user_id = row.get('mb_sn')
        logger.info(f"\n   사용자 {idx + 1} (ID: {user_id}):")

        sample_qa = []
        for q_code in list(question_codes)[:5]:  # 처음 5개 질문만
            if q_code in df.columns:
                answer = row.get(q_code)
                q_info = questions_meta.get(q_code, {})
                q_type = q_info.get('type', 'Unknown')

                if pd.notna(answer) and str(answer).strip():
                    logger.info(f"      {q_code} ({q_type}): {answer}")
                    sample_qa.append({
                        "q_code": q_code,
                        "q_type": q_type,
                        "answer": str(answer)
                    })

        if idx == 0:
            results["sample_data"] = {
                "user_id": user_id,
                "qa_pairs": sample_qa
            }

    # 타입별 통계
    logger.info("\n📊 데이터 타입별 통계:")
    type_stats = {}
    for q_code, q_info in questions_meta.items():
        if q_code == 'mb_sn':
            continue

        q_type = q_info.get('type', 'Unknown')
        type_stats[q_type] = type_stats.get(q_type, 0) + 1

    for q_type, count in type_stats.items():
        logger.info(f"   - {q_type}: {count}개")

    # MULTI 타입 검증
    logger.info("\n🔍 MULTI 타입 답변 형식 검증:")
    multi_questions = [q for q, info in questions_meta.items()
                      if info.get('type') == 'MULTI' and q in df.columns]

    for q_code in multi_questions[:3]:  # 처음 3개만
        sample_answers = df[q_code].dropna().head(3)
        logger.info(f"   {q_code}:")
        for answer in sample_answers:
            logger.info(f"      '{answer}'")
            # 쉼표로 구분되어 있는지 확인
            if ',' in str(answer):
                codes = str(answer).split(',')
                logger.info(f"         → {len(codes)}개 선택지")

    # 최종 판정
    if missing or extra:
        results["overall_status"] = "⚠️ 경고 (일부 불일치)"

    if results["type_issues"]:
        results["overall_status"] = "❌ 실패 (타입 오류)"

    return results


def main():
    """메인 검증 함수"""

    logger.info("=" * 60)
    logger.info("🔍 데이터 형식 검증 시작")
    logger.info("=" * 60)

    question_file = "./data/question_list.csv"
    response_file = "./data/response_list.csv"

    try:
        # 1. 질문 메타데이터 파싱
        logger.info("\n[1/3] 질문 메타데이터 파싱 중...")
        questions_meta = parse_question_metadata(question_file)

        # 2. 메타데이터 검증
        logger.info("\n[2/3] 메타데이터 검증 중...")
        if not validate_metadata(questions_meta):
            logger.error("❌ 메타데이터 검증 실패")
            return

        # 3. 응답 데이터 검증
        logger.info("\n[3/3] 응답 데이터 검증 중...")
        df = pd.read_csv(response_file, encoding="utf-8-sig", dtype=str)
        if not validate_response_data(df):
            logger.error("❌ 응답 데이터 검증 실패")
            return

        # 4. 일관성 검증
        logger.info("\n[보너스] 데이터 일관성 검증 중...")
        results = validate_data_consistency(questions_meta, response_file)

        # 결과 출력
        logger.info("\n" + "=" * 60)
        logger.info("📋 검증 결과 요약")
        logger.info("=" * 60)
        logger.info(f"   총 질문 수: {results['total_questions']}개")
        logger.info(f"   누락된 질문: {len(results['missing_in_responses'])}개")
        logger.info(f"   추가 컬럼: {len(results['extra_in_responses'])}개")
        logger.info(f"   최종 상태: {results['overall_status']}")

        if results['missing_in_responses']:
            logger.warning(f"\n   누락 목록: {results['missing_in_responses'][:10]}")

        if results['extra_in_responses']:
            logger.warning(f"\n   추가 목록: {results['extra_in_responses'][:10]}")

        logger.info("\n" + "=" * 60)
        logger.info("✅ 데이터 형식 검증 완료!")
        logger.info("=" * 60)

        # 샘플 OpenSearch 문서 시뮬레이션 (새로운 구조)
        logger.info("\n📄 OpenSearch 문서 샘플 (시뮬레이션 - 새 구조):")
        logger.info("=" * 60)

        sample_doc = {
            "user_id": results['sample_data'].get('user_id'),
            "timestamp": "2025-01-01T00:00:00",
            "qa_pairs": []
        }

        for qa in results['sample_data'].get('qa_pairs', [])[:5]:
            q_code = qa['q_code']
            q_info = questions_meta.get(q_code, {})
            answer_raw = qa['answer']

            if qa['q_type'] == 'MULTI':
                # MULTI: 각 선택지를 별도 qa_pair로 저장
                answers = [code.strip() for code in str(answer_raw).split(',') if code.strip()]
                for code in answers:
                    answer_text = q_info['options'].get(code, code)
                    if answer_text:
                        embedding_text = f"{q_info.get('text', '')} 질문에 '{answer_text}'라고 답변"
                        sample_doc['qa_pairs'].append({
                            "q_code": q_code,
                            "q_type": qa['q_type'],
                            "q_text": q_info.get('text', ''),
                            "answer_text": answer_text,
                            "embedding_text": embedding_text,
                            "answer_vector": "[1024차원 벡터 - 생략]"
                        })
            else:
                # SINGLE/Numeric/String: 단일 qa_pair로 저장
                answer_text = answer_raw
                if qa['q_type'] == 'SINGLE' and q_info.get('options'):
                    answer_text = q_info['options'].get(answer_raw, answer_raw)

                embedding_text = f"{q_info.get('text', '')} 질문에 '{answer_text}'라고 답변"
                sample_doc['qa_pairs'].append({
                    "q_code": q_code,
                    "q_type": qa['q_type'],
                    "q_text": q_info.get('text', ''),
                    "answer_text": answer_text,
                    "embedding_text": embedding_text,
                    "answer_vector": "[1024차원 벡터 - 생략]"
                })

        import json
        logger.info(json.dumps(sample_doc, indent=2, ensure_ascii=False))
        logger.info("=" * 60)

        # 구조 설명
        logger.info("\n📋 새로운 데이터 구조 특징:")
        logger.info("=" * 60)
        logger.info("✅ MULTI 타입: 각 선택지를 별도 qa_pairs 항목으로 저장")
        logger.info("✅ 필드명: answer_text (answer 대신)")
        logger.info("✅ embedding_text: '질문 ~ 답변' 형식으로 생성")
        logger.info("✅ answer_vector: 모든 답변에 대해 1024차원 임베딩 생성")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"🚨 검증 중 오류 발생: {e}", exc_info=True)
        return


if __name__ == "__main__":
    main()
