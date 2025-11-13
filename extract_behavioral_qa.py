#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Behavioral 조건에 매칭된 QA 추출 함수"""
from typing import Dict, List, Any, Optional


def extract_behavioral_qa_pairs(
    source: Dict[str, Any],
    behavioral_conditions: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Behavioral 조건에 매칭된 qa_pairs 추출

    Args:
        source: OpenSearch 문서 _source
        behavioral_conditions: {"smoker": True, "has_vehicle": True, ...}

    Returns:
        매칭된 qa_pairs 리스트
        [
            {
                "condition_type": "smoker",
                "condition_value": True,
                "q_text": "귀하는 흡연을 하십니까?",
                "answer": "흡연함"
            },
            ...
        ]
    """
    qa_pairs = source.get('qa_pairs', [])
    if not qa_pairs or not behavioral_conditions:
        return []

    matched = []

    # 조건별 키워드 매핑
    CONDITION_KEYWORDS = {
        'smoker': ['흡연', '담배', '피우', '피움'],
        'has_vehicle': ['차량', '차', '자동차', '보유차량'],
        'alcohol_preference': ['주류', '음주', '술', '맥주', '소주', '와인', '막걸리'],
        'exercise_frequency': ['운동', '헬스', '체육'],
        'pet_ownership': ['반려동물', '펫', '애완동물', '강아지', '고양이'],
    }

    # 긍정 답변 패턴
    POSITIVE_ANSWERS = {
        '있다', '있음', '있어요', '보유', '보유함', '보유중',
        '한다', '합니다', '해요', '함', '하고있다',
        'yes', 'y', '예', '네'
    }

    # 부정 답변 패턴
    NEGATIVE_ANSWERS = {
        '없다', '없음', '없어요', '미보유', '않음', '안함',
        '하지않는다', '안합니다', '안해요',
        'no', 'n', '아니오'
    }

    for condition_type, condition_value in behavioral_conditions.items():
        # 이 조건에 해당하는 키워드들
        keywords = CONDITION_KEYWORDS.get(condition_type, [])
        if not keywords:
            continue

        # qa_pairs에서 이 조건에 해당하는 질문 찾기
        for qa in qa_pairs:
            q_text = qa.get('q_text', '').lower()
            answer = qa.get('answer', '')

            # 키워드 매칭
            if any(kw in q_text for kw in keywords):
                # Boolean 조건 (smoker, has_vehicle)
                if isinstance(condition_value, bool):
                    answer_lower = answer.lower()
                    is_positive = any(pos in answer_lower for pos in POSITIVE_ANSWERS)
                    is_negative = any(neg in answer_lower for neg in NEGATIVE_ANSWERS)

                    # 조건값과 답변이 일치하는지 확인
                    if condition_value and is_positive:
                        matched.append({
                            'condition_type': condition_type,
                            'condition_value': condition_value,
                            'q_text': qa.get('q_text', ''),
                            'answer': answer,
                            'confidence': 1.0
                        })
                        break  # 이 조건에 대해 하나만
                    elif not condition_value and is_negative:
                        matched.append({
                            'condition_type': condition_type,
                            'condition_value': condition_value,
                            'q_text': qa.get('q_text', ''),
                            'answer': answer,
                            'confidence': 1.0
                        })
                        break

                # String 조건 (alcohol_preference)
                else:
                    # 답변에 조건값이 포함되어 있으면 매칭
                    if str(condition_value).lower() in answer.lower():
                        matched.append({
                            'condition_type': condition_type,
                            'condition_value': condition_value,
                            'q_text': qa.get('q_text', ''),
                            'answer': answer,
                            'confidence': 0.8
                        })
                        break

    return matched


# 테스트
if __name__ == '__main__':
    # 샘플 데이터
    sample_source = {
        'user_id': 'w123',
        'qa_pairs': [
            {'q_text': '귀하의 성별은', 'answer': '남성'},
            {'q_text': '귀하는 흡연을 하십니까?', 'answer': '흡연함'},
            {'q_text': '보유차량여부', 'answer': '있다'},
            {'q_text': '주로 마시는 주류는?', 'answer': '맥주'}
        ]
    }

    conditions = {
        'smoker': True,
        'has_vehicle': True,
        'alcohol_preference': '맥주'
    }

    result = extract_behavioral_qa_pairs(sample_source, conditions)

    print("매칭된 QA pairs:")
    for item in result:
        print(f"  - {item['condition_type']}: {item['q_text']} → {item['answer']}")
