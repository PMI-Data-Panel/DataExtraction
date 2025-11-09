import pandas as pd
import logging
import datetime
from typing import Dict, List, Any, Tuple, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class QuestionClassifier:
    """질문을 객관식/주관식으로 분류하고 메타데이터 관리"""

    def __init__(self, question_list_path: str = "./data/question_list.csv"):
        """
        question_list.csv 예상 형식:
        q_code,q_text,q_type,category,field_name
        Q1,나이대는?,객관식,인구통계,age_group
        Q2,성별은?,객관식,인구통계,gender
        Q3,서비스 만족도는?,주관식,만족도,satisfaction
        """
        self.question_map = {}
        self.demographic_fields = set()
        self.objective_questions = set()
        self.subjective_questions = set()

        if Path(question_list_path).exists():
            self._load_from_csv(question_list_path)
        else:
            logger.warning(f"⚠️ {question_list_path} 파일 없음. 휴리스틱 분류 사용")
            self.use_heuristic = True

    def _load_from_csv(self, path: str):
        """CSV에서 질문 메타데이터 로드"""
        try:
            df = pd.read_csv(path, encoding="utf-8-sig")

            for _, row in df.iterrows():
                q_text = row['q_text'].strip()
                q_type = row['q_type'].strip()

                self.question_map[q_text] = {
                    'q_code': row.get('q_code', ''),
                    'q_type': q_type,
                    'category': row.get('category', 'unknown'),
                    'field_name': row.get('field_name', '')
                }

                if q_type == '객관식':
                    self.objective_questions.add(q_text)
                    if row.get('category') == '인구통계':
                        self.demographic_fields.add(q_text)
                else:
                    self.subjective_questions.add(q_text)

            logger.info(f"✅ 질문 메타데이터 로드: 객관식 {len(self.objective_questions)}개, 주관식 {len(self.subjective_questions)}개")
            self.use_heuristic = False

        except Exception as e:
            logger.error(f"❌ question_list.csv 로드 실패: {e}")
            self.use_heuristic = True

    def classify(self, q_text: str, answer_text: str) -> str:
        """질문을 객관식/주관식으로 분류"""
        # 메타데이터 기반 분류
        if not self.use_heuristic and q_text in self.question_map:
            return self.question_map[q_text]['q_type']

        # 휴리스틱: 답변 길이와 패턴으로 판단
        answer_str = str(answer_text).strip()

        # 명확한 객관식 패턴
        objective_patterns = [
            '남성', '여성', '남자', '여자',
            '10대', '20대', '30대', '40대', '50대', '60대', '70대',
            '매우 그렇다', '그렇다', '보통', '아니다', '매우 아니다',
            '예', '아니오', 'Y', 'N'
        ]

        if answer_str in objective_patterns or len(answer_str) <= 15:
            return '객관식'

        # 긴 답변은 주관식
        if len(answer_str) > 50:
            return '주관식'

        # 쉼표나 마침표가 있으면 주관식
        if ',' in answer_str or '.' in answer_str or '。' in answer_str:
            return '주관식'

        # 기본값: 애매하면 주관식으로 (안전)
        return '주관식' if len(answer_str) > 20 else '객관식'

    def get_demographic_field(self, q_text: str) -> Optional[str]:
        """질문을 demographic 필드명으로 매핑"""
        # 메타데이터에서 먼저 찾기
        if q_text in self.question_map:
            field_name = self.question_map[q_text].get('field_name')
            if field_name:
                return field_name

        # 휴리스틱 매핑
        field_mapping = {
            '나이': 'age_group',
            '나이대': 'age_group',
            '연령': 'age_group',
            '성별': 'gender',
            '지역': 'region',
            '거주': 'region',
            '직업': 'occupation',
            '소득': 'income',
            '학력': 'education',
            '결혼': 'marital_status',
            '가구': 'household'
        }

        q_lower = q_text.lower()
        for keyword, field in field_mapping.items():
            if keyword in q_lower:
                return field

        return None

    def is_demographic(self, q_text: str) -> bool:
        """인구통계 질문인지 확인"""
        if not self.use_heuristic:
            return q_text in self.demographic_fields

        demo_keywords = ['나이', '연령', '성별', '지역', '거주', '직업', '소득', '학력']
        return any(kw in q_text for kw in demo_keywords)


def process_survey_data_hybrid(
    df_responses: pd.DataFrame,
    embedding_model,
    index_name: str,
    classifier: QuestionClassifier = None
) -> List[Dict[str, Any]]:
    """하이브리드 구조로 설문 데이터 변환

    구조:
    - demographics: 객관식 인구통계 데이터 (정확한 필터용)
    - subjective_responses: 주관식 답변만 (nested + 벡터)
    - all_subjective_text: 주관식 통합 텍스트 (키워드 검색용)
    """

    if classifier is None:
        classifier = QuestionClassifier()

    actions = []
    total_users = len(df_responses)

    logger.info(f"🔄 하이브리드 구조로 {total_users}명 데이터 처리 시작")

    stats = {
        'total_users': 0,
        'total_demographics': 0,
        'total_subjectives': 0,
        'skipped_users': 0
    }

    for user_count, (_, row) in enumerate(df_responses.iterrows(), 1):
        if user_count % 100 == 0 or user_count == 1 or user_count == total_users:
            logger.info(f"🔄 처리 중... ({user_count}/{total_users})")

        user_id = row.get("mb_sn")
        if not user_id or pd.isna(user_id):
            user_id = f"user_{user_count}"

        # 1. 데이터 분류
        demographics = {}
        other_objectives = {}  # 인구통계가 아닌 객관식
        subjective_responses = []
        all_subjective_texts = []

        for q_text, answer_text in row.items():
            if q_text == "mb_sn" or pd.isna(answer_text):
                continue

            answer_text = str(answer_text).strip()
            if not answer_text or answer_text.lower() in ['nan', 'none', '']:
                continue

            # 질문 분류
            q_type = classifier.classify(q_text, answer_text)

            if q_type == '객관식':
                # 인구통계인지 확인
                if classifier.is_demographic(q_text):
                    field_name = classifier.get_demographic_field(q_text)
                    if field_name:
                        demographics[field_name] = answer_text
                        stats['total_demographics'] += 1
                else:
                    # 인구통계가 아닌 객관식 (예: 만족도 등급)
                    other_objectives[q_text] = answer_text

            else:  # 주관식
                # 임베딩은 나중에 배치로 처리
                q_info = classifier.question_map.get(q_text, {})

                subjective_responses.append({
                    "q_text": q_text,
                    "q_code": q_info.get('q_code', q_text[:20]),
                    "q_category": q_info.get('category', 'unknown'),
                    "answer_text": answer_text,
                    "answer_vector": None,  # 나중에 배치로 생성
                    "answer_length": len(answer_text)
                })

                all_subjective_texts.append(answer_text)

        # 2. 문서 구성
        if not demographics and not subjective_responses:
            stats['skipped_users'] += 1
            continue

        final_document = {
            "user_id": str(user_id),
            "demographics": demographics,
            "other_objectives": other_objectives,  # 선택적
            "subjective_responses": subjective_responses,
            "all_subjective_text": " ".join(all_subjective_texts),
            "metadata": {
                "timestamp": datetime.datetime.now().isoformat(),
                "total_questions": len(row) - 1,
                "demographic_count": len(demographics),
                "objective_count": len(other_objectives),
                "subjective_count": len(subjective_responses),
                "avg_answer_length": (
                    sum(r['answer_length'] for r in subjective_responses) / len(subjective_responses)
                    if subjective_responses else 0
                )
            }
        }

        actions.append({
            "_index": index_name,
            "_id": str(user_id),
            "_source": final_document
        })

        stats['total_users'] += 1

    # 배치 임베딩 생성 (KURE-v1 최적화)
    logger.info(f"🔄 배치 임베딩 생성 중... ({stats['total_subjectives']}개)")
    _generate_batch_embeddings(actions, embedding_model)

    # 최종 통계
    logger.info(f"✅ 하이브리드 변환 완료:")
    logger.info(f"   - 처리된 사용자: {stats['total_users']}명")
    logger.info(f"   - 인구통계 필드: {stats['total_demographics']}개")
    logger.info(f"   - 주관식 답변: {stats['total_subjectives']}개")
    logger.info(f"   - 스킵된 사용자: {stats['skipped_users']}명")

    return actions


def _generate_batch_embeddings(actions: List[Dict], embedding_model, batch_size: int = 64):
    """
    모든 주관식 답변의 임베딩을 배치로 생성 (KURE-v1 최적화)

    장점:
    - GPU 활용 극대화
    - 속도 10-50배 향상
    - 메모리 효율적
    """
    # 1. 모든 텍스트 수집
    all_texts = []
    text_indices = []  # (action_idx, response_idx)

    for action_idx, action in enumerate(actions):
        subjective_responses = action["_source"].get("subjective_responses", [])
        for response_idx, response in enumerate(subjective_responses):
            if response["answer_vector"] is None:
                all_texts.append(response["answer_text"])
                text_indices.append((action_idx, response_idx))

    if not all_texts:
        logger.info("📊 임베딩할 텍스트 없음")
        return

    # 2. 배치 임베딩 생성
    logger.info(f"📊 {len(all_texts)}개 텍스트 배치 임베딩 생성 (배치 크기: {batch_size})")

    try:
        # hasattr로 encode 메서드 확인
        if hasattr(embedding_model, 'encode'):
            # 배치 인코딩
            vectors = embedding_model.encode(
                all_texts,
                batch_size=batch_size,
                show_progress_bar=True,
                convert_to_tensor=False
            )

            # numpy array를 list로 변환
            if hasattr(vectors, 'tolist'):
                vectors = [v.tolist() for v in vectors]
            elif isinstance(vectors, list):
                pass
            else:
                vectors = vectors.tolist()

        else:
            raise AttributeError("embedding_model에 encode 메서드가 없습니다")

    except Exception as e:
        logger.error(f"❌ 배치 임베딩 실패: {e}")
        # Fallback: 개별 인코딩
        logger.warning("⚠️ Fallback: 개별 인코딩으로 전환")
        vectors = []
        for text in all_texts:
            try:
                vec = embedding_model.encode(text)
                if hasattr(vec, 'tolist'):
                    vec = vec.tolist()
                vectors.append(vec)
            except Exception as e2:
                logger.error(f"❌ 개별 인코딩도 실패: {e2}")
                # 제로 벡터로 대체
                vectors.append([0.0] * 1024)

    # 3. 벡터 할당
    for (action_idx, response_idx), vector in zip(text_indices, vectors):
        actions[action_idx]["_source"]["subjective_responses"][response_idx]["answer_vector"] = vector

    logger.info(f"✅ 배치 임베딩 완료: {len(vectors)}개")


def analyze_survey_structure(df_responses: pd.DataFrame) -> Dict[str, Any]:
    """설문 데이터 구조 분석 (디버깅/모니터링용)"""

    analysis = {
        'total_rows': len(df_responses),
        'total_columns': len(df_responses.columns),
        'columns': list(df_responses.columns),
        'missing_values': {},
        'unique_values': {},
        'value_distributions': {}
    }

    for col in df_responses.columns:
        if col == 'mb_sn':
            continue

        # 결측값
        missing = df_responses[col].isna().sum()
        analysis['missing_values'][col] = missing

        # 고유값 개수
        unique = df_responses[col].nunique()
        analysis['unique_values'][col] = unique

        # 객관식 추정 (고유값이 20개 미만)
        if unique < 20:
            value_counts = df_responses[col].value_counts().head(10)
            analysis['value_distributions'][col] = value_counts.to_dict()

    return analysis
