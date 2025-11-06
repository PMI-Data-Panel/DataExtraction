"""
설문조사 데이터 처리 및 OpenSearch 색인 모듈
"""

import pandas as pd
from opensearchpy import OpenSearch
from opensearchpy.helpers import streaming_bulk
import datetime
import json
import logging
from typing import Tuple, Dict, Any, Generator, List

logger = logging.getLogger(__name__)


def validate_response_data(df: pd.DataFrame) -> bool:
    """
    응답 데이터프레임의 유효성을 검증합니다.

    Args:
        df: 응답 데이터프레임

    Returns:
        검증 성공 여부
    """
    required_columns = ['mb_sn']

    # 필수 컬럼 확인
    if not all(col in df.columns for col in required_columns):
        missing = [col for col in required_columns if col not in df.columns]
        logger.error(f"🚨 필수 컬럼 누락: {missing}")
        return False

    # mb_sn null 확인
    null_count = df['mb_sn'].isnull().sum()
    if null_count > 0:
        logger.warning(f"⚠️ mb_sn이 null인 행이 {null_count}개 있습니다. 해당 행은 건너뜁니다.")

    return True


def generate_user_documents(
    df_chunk: pd.DataFrame,
    questions_meta: Dict[str, Dict[str, Any]],
    index_name: str
) -> Generator[dict, None, None]:
    """
    데이터프레임 청크로부터 OpenSearch 문서를 생성합니다.

    ✅ MULTI 타입: 각 선택지를 별도의 qa_pairs 항목으로 저장
    ✅ SINGLE/Numeric/String 타입: 단일 qa_pairs 항목으로 저장
    ✅ 모든 답변에 대해 embedding_text 생성
    ✅ answer_vector는 나중에 배치 임베딩으로 생성

    Args:
        df_chunk: 응답 데이터 청크
        questions_meta: 질문 메타데이터
        index_name: 인덱스 이름

    Yields:
        OpenSearch bulk API용 액션 딕셔너리
    """
    for _, row in df_chunk.iterrows():
        user_id = row.get("mb_sn")

        # user_id 검증
        if not user_id or pd.isna(user_id):
            continue

        all_qa_pairs_for_user = []

        for q_code, raw_answer in row.items():
            # mb_sn 건너뛰기
            if q_code == "mb_sn":
                continue

            # null이나 빈 값 건너뛰기
            if raw_answer is None or pd.isna(raw_answer):
                continue

            # 질문 메타데이터 확인
            q_info = questions_meta.get(q_code)
            if not q_info:
                logger.debug(f"  ⚠️ 알 수 없는 질문 코드: {q_code}")
                continue

            q_text = q_info["text"]
            q_type = q_info["type"]

            # 답변 타입별 처리
            if q_type == "MULTI":
                # ✅ MULTI: 각 선택지를 별도 qa_pair로 저장
                answer_codes = str(raw_answer).split(",")

                for code in answer_codes:
                    code = code.strip()
                    if code and code != '':
                        answer_text = q_info["options"].get(code, f"알 수 없는 코드: {code}")
                        if answer_text and answer_text != f"알 수 없는 코드: {code}":
                            # embedding_text 생성
                            embedding_text = f"{q_text} 질문에 '{answer_text}'라고 답변"

                            qa_pair_doc = {
                                "q_code": q_code,
                                "q_type": q_type,
                                "q_text": q_text,
                                "answer_text": answer_text,
                                "embedding_text": embedding_text,
                                "answer_vector": None  # 나중에 배치 임베딩
                            }
                            all_qa_pairs_for_user.append(qa_pair_doc)

            elif q_type == "SINGLE":
                # ✅ SINGLE: 단일 qa_pair로 저장
                code = str(raw_answer).strip()
                if code and code != '':
                    answer_text = q_info["options"].get(code, raw_answer)
                    if answer_text:
                        # embedding_text 생성
                        embedding_text = f"{q_text} 질문에 '{answer_text}'라고 답변"

                        qa_pair_doc = {
                            "q_code": q_code,
                            "q_type": q_type,
                            "q_text": q_text,
                            "answer_text": answer_text,
                            "embedding_text": embedding_text,
                            "answer_vector": None  # 나중에 배치 임베딩
                        }
                        all_qa_pairs_for_user.append(qa_pair_doc)

            else:
                # ✅ Numeric, String: 단일 qa_pair로 저장
                answer_text = str(raw_answer).strip()
                if answer_text and answer_text != '':
                    # Numeric 타입: 정수인 경우 .0 제거 (2.0 → 2)
                    if q_type == "Numeric":
                        try:
                            # float로 변환 후 정수인지 확인
                            num_val = float(answer_text)
                            if num_val.is_integer():
                                answer_text = str(int(num_val))
                        except:
                            pass  # 변환 실패시 원본 유지

                    # embedding_text 생성
                    embedding_text = f"{q_text} 질문에 '{answer_text}'라고 답변"

                    qa_pair_doc = {
                        "q_code": q_code,
                        "q_type": q_type,
                        "q_text": q_text,
                        "answer_text": answer_text,
                        "embedding_text": embedding_text,
                        "answer_vector": None  # 나중에 배치 임베딩
                    }
                    all_qa_pairs_for_user.append(qa_pair_doc)

        if all_qa_pairs_for_user:
            final_user_document = {
                "user_id": str(user_id),
                "timestamp": datetime.datetime.now().isoformat(),
                "qa_pairs": all_qa_pairs_for_user,
            }

            yield {
                "_index": index_name,
                "_id": str(user_id),
                "_source": final_user_document
            }


def process_and_bulk_index(
    os_client: OpenSearch,
    questions_meta: Dict[str, Dict[str, Any]],
    response_file: str,
    index_name: str,
    embedding_model = None,
    chunk_size: int = 1000,
    bulk_chunk_size: int = 500
) -> Tuple[int, int]:
    """
    응답 CSV를 청크 단위로 읽고, 변환하며, OpenSearch에 스트리밍 방식으로 색인합니다.

    Args:
        os_client: OpenSearch 클라이언트
        questions_meta: 질문 메타데이터
        response_file: 응답 CSV 파일 경로
        index_name: 인덱스 이름
        embedding_model: 임베딩 모델 (KURE-v1 등)
        chunk_size: CSV 읽기 청크 크기 (메모리 효율성)
        bulk_chunk_size: bulk API 청크 크기 (네트워크 효율성)

    Returns:
        (성공 건수, 실패 건수) 튜플
    """

    # 파일 존재 확인
    try:
        # 먼저 전체 행 수 확인 (진행률 표시용)
        total_rows = sum(1 for _ in open(response_file, encoding="utf-8-sig")) - 1  # 헤더 제외
        logger.info(f"📊 처리할 총 응답 수: {total_rows:,}개")
    except FileNotFoundError:
        logger.error(f"🚨 응답 파일을 찾을 수 없습니다: {response_file}")
        raise
    except Exception as e:
        logger.error(f"🚨 파일 읽기 오류: {e}")
        raise

    total_success = 0
    total_failed = 0
    processed_count = 0
    failed_docs = []

    logger.info("⏳ 데이터 처리 및 색인을 시작합니다...")

    try:
        # 청크 단위로 CSV 읽기
        chunk_iterator = pd.read_csv(
            response_file,
            encoding="utf-8-sig",
            chunksize=chunk_size,
            dtype=str  # 모든 컬럼을 문자열로 읽음
        )

        for chunk_num, df_chunk in enumerate(chunk_iterator, 1):
            # NaN을 None으로 변환
            df_chunk = df_chunk.where(pd.notnull(df_chunk), None)

            # 데이터 검증 (첫 청크만)
            if chunk_num == 1:
                if not validate_response_data(df_chunk):
                    raise ValueError("데이터 검증 실패")

            # 1. 문서 생성 (임베딩 없이)
            actions = list(generate_user_documents(df_chunk, questions_meta, index_name))

            if not actions:
                continue

            # 2. 배치 임베딩 생성
            if embedding_model:
                _generate_batch_embeddings_for_actions(actions, embedding_model, batch_size=64)

            # 첫 번째 청크의 샘플 출력
            if chunk_num == 1:
                logger.info("\n--- 📄 첫 번째 사용자 문서 샘플 (임베딩 포함) ---")
                if actions:
                    logger.info(json.dumps(actions[0]["_source"], indent=2, ensure_ascii=False))
                logger.info("--------------------------------\n")

            # 3. bulk index
            for ok, response in streaming_bulk(
                os_client,
                actions,
                chunk_size=bulk_chunk_size,
                raise_on_error=False,
                raise_on_exception=False,
                request_timeout=60
            ):
                processed_count += 1

                if ok:
                    total_success += 1
                else:
                    total_failed += 1
                    # 실패한 문서 정보 기록 (최대 100개까지만)
                    if len(failed_docs) < 100:
                        failed_docs.append(response)

                    # 첫 10개 실패 케이스만 상세 로그
                    if total_failed <= 10:
                        logger.error(f"❌ 문서 색인 실패: {response}")

            # 진행률 표시 (청크 단위)
            progress = (processed_count / total_rows * 100) if total_rows > 0 else 0
            logger.info(f"청크 {chunk_num} 처리 완료... {processed_count:,}/{total_rows:,} ({progress:.1f}%) "
                  f"| 성공: {total_success:,} | 실패: {total_failed:,}")

        print("\n")  # 줄바꿈

        # 최종 결과
        logger.info("=" * 60)
        logger.info(f"🎉 색인 작업 완료!")
        logger.info(f"   ✅ 성공: {total_success:,}개")
        logger.info(f"   ❌ 실패: {total_failed:,}개")
        logger.info(f"   📊 총 처리: {processed_count:,}개")

        if total_failed > 0:
            logger.warning(f"\n⚠️ {total_failed}개 문서 색인 실패")
            if failed_docs:
                logger.warning("실패한 문서 샘플 (최대 5개):")
                for i, doc in enumerate(failed_docs[:5], 1):
                    logger.warning(f"  {i}. {doc}")

        logger.info("=" * 60)

        # 인덱스 refresh (검색 가능하도록)
        logger.info("🔄 인덱스 refresh 중...")
        os_client.indices.refresh(index=index_name)
        logger.info("✅ refresh 완료")

        return total_success, total_failed

    except Exception as e:
        logger.error(f"🚨 처리 중 예외 발생: {e}", exc_info=True)
        raise


def _generate_batch_embeddings_for_actions(
    actions: List[Dict],
    embedding_model,
    batch_size: int = 64
) -> None:
    """
    액션 리스트의 모든 qa_pairs에 대해 배치 임베딩을 생성합니다.

    Args:
        actions: OpenSearch bulk API용 액션 리스트
        embedding_model: 임베딩 모델 (KURE-v1 등)
        batch_size: 배치 크기
    """
    # 1. 모든 embedding_text 수집
    all_texts = []
    text_indices = []  # (action_idx, qa_pair_idx)

    for action_idx, action in enumerate(actions):
        qa_pairs = action["_source"].get("qa_pairs", [])
        for qa_idx, qa_pair in enumerate(qa_pairs):
            embedding_text = qa_pair.get("embedding_text")
            if embedding_text:
                all_texts.append(embedding_text)
                text_indices.append((action_idx, qa_idx))

    if not all_texts:
        logger.debug("📊 임베딩할 텍스트 없음")
        return

    logger.info(f"📊 {len(all_texts):,}개 텍스트 배치 임베딩 생성 중... (배치 크기: {batch_size})")

    try:
        # 배치 인코딩
        vectors = embedding_model.encode(
            all_texts,
            batch_size=batch_size,
            show_progress_bar=False,
            convert_to_tensor=False
        )

        # numpy array를 list로 변환
        if hasattr(vectors, 'tolist'):
            vectors = [v.tolist() if hasattr(v, 'tolist') else v for v in vectors]
        elif not isinstance(vectors, list):
            vectors = vectors.tolist()

        # 벡터 할당
        for (action_idx, qa_idx), vector in zip(text_indices, vectors):
            actions[action_idx]["_source"]["qa_pairs"][qa_idx]["answer_vector"] = vector

        logger.info(f"✅ 배치 임베딩 완료: {len(vectors):,}개")

    except Exception as e:
        logger.error(f"❌ 배치 임베딩 실패: {e}", exc_info=True)
        # Fallback: 임베딩 없이 진행 (answer_vector는 None으로 유지)
        logger.warning("⚠️ 임베딩 없이 색인을 진행합니다")


def verify_indexed_data(
    os_client: OpenSearch,
    index_name: str,
    sample_user_id: str = None
) -> None:
    """
    색인된 데이터를 검증합니다.

    Args:
        os_client: OpenSearch 클라이언트
        index_name: 인덱스 이름
        sample_user_id: 샘플로 조회할 사용자 ID (선택)
    """
    try:
        # 전체 문서 수 확인
        count = os_client.count(index=index_name)
        logger.info(f"📊 '{index_name}' 인덱스의 총 문서 수: {count['count']:,}개")

        # 샘플 문서 조회
        if sample_user_id:
            doc = os_client.get(index=index_name, id=sample_user_id)
            logger.info(f"\n--- 샘플 문서 (user_id: {sample_user_id}) ---")
            logger.info(json.dumps(doc['_source'], indent=2, ensure_ascii=False))
        else:
            # 랜덤 샘플 조회
            result = os_client.search(
                index=index_name,
                body={
                    "size": 1,
                    "query": {"match_all": {}}
                }
            )
            if result['hits']['hits']:
                doc = result['hits']['hits'][0]
                logger.info(f"\n--- 샘플 문서 (user_id: {doc['_id']}) ---")
                logger.info(json.dumps(doc['_source'], indent=2, ensure_ascii=False))

    except Exception as e:
        logger.error(f"⚠️ 데이터 검증 중 오류: {e}")
