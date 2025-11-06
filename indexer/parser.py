"""
질문 메타데이터 파서

question_list.csv 파일을 파싱하여 질문 메타데이터를 추출합니다.

예상 CSV 형식:
변수명,문항,문항유형
mb_sn,패널ID,String
Q1,결혼여부,SINGLE
1,미혼,
2,기혼,
3,기타(사별/이혼 등),
Q2,자녀수,Numeric
...
"""

import pandas as pd
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


def parse_question_metadata(question_file: str) -> Dict[str, Dict[str, Any]]:
    """
    질문 메타데이터 CSV 파일을 파싱합니다.

    Args:
        question_file: 질문 메타데이터 CSV 파일 경로

    Returns:
        질문 메타데이터 딕셔너리
        {
            "Q1": {
                "text": "결혼여부",
                "type": "SINGLE",
                "options": {"1": "미혼", "2": "기혼", "3": "기타(사별/이혼 등)"}
            },
            "Q2": {
                "text": "자녀수",
                "type": "Numeric",
                "options": {}
            },
            ...
        }
    """
    logger.info(f"📖 질문 메타데이터 파싱 시작: {question_file}")

    try:
        # CSV 파일 읽기 (UTF-8 BOM 처리)
        df = pd.read_csv(question_file, encoding="utf-8-sig")

        # 컬럼명 정리 (공백 제거)
        df.columns = df.columns.str.strip()

        # 필수 컬럼 확인
        required_columns = ["변수명", "문항", "문항유형"]
        if not all(col in df.columns for col in required_columns):
            raise ValueError(f"필수 컬럼 누락: {required_columns}. 현재 컬럼: {list(df.columns)}")

        questions_meta = {}
        current_question = None

        for idx, row in df.iterrows():
            var_name = str(row["변수명"]).strip()
            q_text = str(row["문항"]).strip()
            q_type = str(row["문항유형"]).strip()

            # 빈 행 건너뛰기
            if var_name in ["", "nan", "None"] or pd.isna(var_name):
                continue

            # 질문 행인지 선택지 행인지 판단
            # 질문 행: 변수명이 Q로 시작하거나 mb_sn, q_type이 있음
            if var_name.startswith("Q") or var_name == "mb_sn" or q_type not in ["", "nan", "None"]:
                # 새로운 질문 시작
                current_question = var_name
                questions_meta[current_question] = {
                    "text": q_text if q_text not in ["", "nan", "None"] else var_name,
                    "type": q_type if q_type not in ["", "nan", "None"] else "String",
                    "options": {}
                }
                logger.debug(f"  질문 발견: {current_question} - {q_text} ({q_type})")

            else:
                # 선택지 행 (숫자로 시작)
                if current_question and var_name.isdigit():
                    option_code = var_name
                    option_text = q_text

                    if option_text not in ["", "nan", "None"]:
                        questions_meta[current_question]["options"][option_code] = option_text
                        logger.debug(f"    선택지 추가: {option_code} - {option_text}")

        # 통계 출력
        total_questions = len(questions_meta)
        questions_with_options = sum(1 for q in questions_meta.values() if q["options"])
        total_options = sum(len(q["options"]) for q in questions_meta.values())

        logger.info(f"✅ 질문 메타데이터 파싱 완료:")
        logger.info(f"   - 총 질문 수: {total_questions}개")
        logger.info(f"   - 선택지 있는 질문: {questions_with_options}개")
        logger.info(f"   - 총 선택지 수: {total_options}개")

        # 타입별 통계
        type_counts = {}
        for q in questions_meta.values():
            q_type = q["type"]
            type_counts[q_type] = type_counts.get(q_type, 0) + 1

        logger.info(f"   - 타입별 분포: {type_counts}")

        return questions_meta

    except Exception as e:
        logger.error(f"🚨 질문 메타데이터 파싱 실패: {e}", exc_info=True)
        raise


def validate_metadata(questions_meta: Dict[str, Dict[str, Any]]) -> bool:
    """
    파싱된 질문 메타데이터의 유효성을 검증합니다.

    Args:
        questions_meta: 질문 메타데이터 딕셔너리

    Returns:
        검증 성공 여부
    """
    logger.info("🔍 질문 메타데이터 검증 시작...")

    if not questions_meta:
        logger.error("🚨 질문 메타데이터가 비어있습니다.")
        return False

    issues = []

    # 각 질문별 검증
    for q_code, q_info in questions_meta.items():
        # 필수 필드 확인
        if "text" not in q_info or not q_info["text"]:
            issues.append(f"{q_code}: 질문 텍스트 없음")

        if "type" not in q_info or not q_info["type"]:
            issues.append(f"{q_code}: 질문 타입 없음")

        # 타입 검증
        valid_types = ["SINGLE", "MULTI", "Numeric", "String"]
        if q_info.get("type") not in valid_types:
            issues.append(f"{q_code}: 올바르지 않은 타입 '{q_info.get('type')}' (허용: {valid_types})")

        # SINGLE/MULTI 타입은 선택지가 있어야 함
        if q_info.get("type") in ["SINGLE", "MULTI"]:
            if "options" not in q_info or not q_info["options"]:
                issues.append(f"{q_code}: {q_info['type']} 타입이지만 선택지가 없음")

    if issues:
        logger.warning(f"⚠️ 메타데이터 검증 중 {len(issues)}개 문제 발견:")
        for issue in issues[:10]:  # 최대 10개만 출력
            logger.warning(f"   - {issue}")

        if len(issues) > 10:
            logger.warning(f"   ... 외 {len(issues) - 10}개 문제")

        # 경고만 하고 통과 (일부 문제는 허용)
        return True

    logger.info("✅ 질문 메타데이터 검증 완료: 문제 없음")
    return True
