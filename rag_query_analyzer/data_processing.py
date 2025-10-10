import pandas as pd
import logging
import datetime
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

def process_survey_data(
    df_responses: pd.DataFrame,
    embedding_model,
    index_name: str
) -> List[Dict[str, Any]]:
    """
    응답 DataFrame을 사용자 단위의 Elasticsearch 문서로 변환하고 임베딩을 생성합니다.
    이 버전은 모든 답변이 전체 텍스트라고 가정하고, 별도의 질문 메타데이터를 사용하지 않습니다.
    """
    actions = []
    total_users = len(df_responses)
    logger.info(f"🔄 {total_users}명의 사용자 데이터 처리를 시작합니다. (단일 파일 모드)")

    for user_count, (_, row) in enumerate(df_responses.iterrows(), 1):
        if user_count % 100 == 0 or user_count == 1 or user_count == total_users:
            logger.info(f"🔄 사용자 데이터 처리 중... ({user_count}/{total_users})")

        user_id = row.get("mb_sn")
        if not user_id:
            user_id = f"user_{user_count}" # 'mb_sn'이 없는 경우 대체 ID 생성

        all_qa_pairs_for_user = []

        # 'mb_sn'을 제외한 모든 열을 질문-답변 쌍으로 처리
        for q_text, answer_text in row.items():
            if q_text == "mb_sn" or answer_text is None or pd.isna(answer_text):
                continue

            answer_text = str(answer_text).strip()
            if not answer_text:
                continue

            # QA 쌍 문서 생성
            qa_pair_doc = {
                "q_text": q_text,
                "answer_text": answer_text,
            }

            # 모든 답변에 대해 임베딩 생성
            embedding_text = f"{q_text} 문항에 '{answer_text}'라고 응답"
            qa_pair_doc["embedding_text"] = embedding_text
            qa_pair_doc["answer_vector"] = embedding_model.encode(embedding_text).tolist()

            all_qa_pairs_for_user.append(qa_pair_doc)

        if all_qa_pairs_for_user:
            final_user_document = {
                "user_id": user_id,
                "timestamp": datetime.datetime.now().isoformat(),
                "qa_pairs": all_qa_pairs_for_user,
            }
            actions.append({
                "_index": index_name,
                "_id": str(user_id),
                "_source": final_user_document
            })

    logger.info(f"✅ 총 {len(actions)}개의 사용자 문서를 생성했습니다.")
    return actions