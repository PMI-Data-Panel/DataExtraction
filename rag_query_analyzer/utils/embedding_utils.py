"""KURE-v1 임베딩 유틸리티"""
import logging
import torch
from typing import List, Union
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)


class KUREv1EmbeddingModel:
    """
    KURE-v1 한국어 임베딩 모델 래퍼

    특징:
    - 1024차원 dense vector
    - 최대 512 토큰 길이
    - 한국어 최적화
    - GPU 가속 지원
    """

    def __init__(
        self,
        model_name: str = "nlpai-lab/KURE-v1",
        device: str = None,
        batch_size: int = 32
    ):
        """
        Args:
            model_name: 모델 이름 (기본: KURE-v1)
            device: 장치 ('cuda', 'cpu', None=자동)
            batch_size: 배치 크기
        """
        self.model_name = model_name
        self.batch_size = batch_size

        # 장치 자동 선택
        if device is None:
            self.device = 'cuda' if torch.cuda.is_available() else 'cpu'
        else:
            self.device = device

        # 모델 로드
        logger.info(f"🔄 KURE-v1 모델 로딩 중... (장치: {self.device})")

        self.model = SentenceTransformer(
            model_name,
            device=self.device
        )

        # KURE-v1 설정
        self.model.max_seq_length = 512  # 최대 토큰 길이
        self.embedding_dim = 1024

        logger.info(f"✅ KURE-v1 로드 완료 (차원: {self.embedding_dim})")

        # 모델 워밍업
        self._warmup()

    def _warmup(self):
        """모델 워밍업 (첫 실행 지연 제거)"""
        try:
            dummy_text = "모델 워밍업 테스트"
            _ = self.model.encode(dummy_text, convert_to_numpy=True)
            logger.info("🔥 모델 워밍업 완료")
        except Exception as e:
            logger.warning(f"⚠️ 워밍업 실패 (무시 가능): {e}")

    def encode(
        self,
        texts: Union[str, List[str]],
        batch_size: int = None,
        show_progress: bool = False,
        normalize_embeddings: bool = True
    ) -> Union[List[float], List[List[float]]]:
        """
        텍스트를 임베딩 벡터로 변환

        Args:
            texts: 단일 텍스트 또는 텍스트 리스트
            batch_size: 배치 크기 (None이면 기본값)
            show_progress: 진행상황 표시
            normalize_embeddings: L2 정규화 여부

        Returns:
            임베딩 벡터 (단일 or 리스트)
        """
        if batch_size is None:
            batch_size = self.batch_size

        # 단일 텍스트 처리
        if isinstance(texts, str):
            vector = self.model.encode(
                texts,
                convert_to_numpy=False,
                normalize_embeddings=normalize_embeddings,
                show_progress_bar=False
            )
            return vector.tolist()

        # 배치 처리
        if len(texts) == 0:
            return []

        logger.debug(f"📊 배치 임베딩: {len(texts)}개 텍스트 (배치: {batch_size})")

        vectors = self.model.encode(
            texts,
            batch_size=batch_size,
            convert_to_numpy=False,
            normalize_embeddings=normalize_embeddings,
            show_progress_bar=show_progress
        )

        return [v.tolist() for v in vectors]

    def encode_batch_with_metadata(
        self,
        texts: List[str],
        metadata: List[dict] = None,
        batch_size: int = None
    ) -> List[dict]:
        """
        텍스트를 임베딩하고 메타데이터와 함께 반환

        Args:
            texts: 텍스트 리스트
            metadata: 각 텍스트에 대응하는 메타데이터 리스트
            batch_size: 배치 크기

        Returns:
            [{"text": ..., "vector": [...], "metadata": {...}}, ...]
        """
        vectors = self.encode(texts, batch_size=batch_size)

        results = []
        for i, (text, vector) in enumerate(zip(texts, vectors)):
            result = {
                "text": text,
                "vector": vector,
                "metadata": metadata[i] if metadata else {}
            }
            results.append(result)

        return results

    def get_similarity(
        self,
        text1: str,
        text2: str,
        metric: str = "cosine"
    ) -> float:
        """
        두 텍스트 간 유사도 계산

        Args:
            text1: 첫 번째 텍스트
            text2: 두 번째 텍스트
            metric: 유사도 메트릭 ('cosine', 'euclidean')

        Returns:
            유사도 점수
        """
        v1 = torch.tensor(self.encode(text1))
        v2 = torch.tensor(self.encode(text2))

        if metric == "cosine":
            # 코사인 유사도
            similarity = torch.nn.functional.cosine_similarity(
                v1.unsqueeze(0),
                v2.unsqueeze(0)
            ).item()
        elif metric == "euclidean":
            # 유클리디안 거리 (작을수록 유사)
            distance = torch.nn.functional.pairwise_distance(
                v1.unsqueeze(0),
                v2.unsqueeze(0)
            ).item()
            # 0-1 범위로 정규화
            similarity = 1 / (1 + distance)
        else:
            raise ValueError(f"지원하지 않는 메트릭: {metric}")

        return similarity

    def get_info(self) -> dict:
        """모델 정보 반환"""
        return {
            "model_name": self.model_name,
            "embedding_dim": self.embedding_dim,
            "max_seq_length": self.model.max_seq_length,
            "device": self.device,
            "batch_size": self.batch_size,
            "cuda_available": torch.cuda.is_available()
        }


# 전역 인스턴스 (싱글톤)
_embedding_model_instance = None


def get_embedding_model(
    model_name: str = "nlpai-lab/KURE-v1",
    device: str = None,
    batch_size: int = 32
) -> KUREv1EmbeddingModel:
    """임베딩 모델 싱글톤 인스턴스 반환"""
    global _embedding_model_instance

    if _embedding_model_instance is None:
        _embedding_model_instance = KUREv1EmbeddingModel(
            model_name=model_name,
            device=device,
            batch_size=batch_size
        )

    return _embedding_model_instance
