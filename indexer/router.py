"""
데이터 인덱서 FastAPI 라우터
"""

import logging
import os
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends, Query
from opensearchpy import OpenSearch
from pydantic import BaseModel, Field

from .parser import parse_question_metadata, validate_metadata
from .opensearch import (
    create_survey_index,
    update_index_refresh_interval,
    get_index_stats,
    force_merge_index
)
from .core import process_and_bulk_index, verify_indexed_data

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/indexer",
    tags=["Data Indexer"]
)


class IndexConfig(BaseModel):
    """인덱싱 설정"""
    index_name: str = Field(default="s_welcome_2nd", description="인덱스 이름")
    question_file: str = Field(default="./data/question_list.csv", description="질문 메타데이터 파일 경로")
    response_file: str = Field(default="./data/response_list.csv", description="응답 데이터 파일 경로")
    force_recreate: bool = Field(default=False, description="기존 인덱스 강제 재생성 여부")
    chunk_size: int = Field(default=1000, ge=100, le=10000, description="CSV 읽기 청크 크기")
    bulk_chunk_size: int = Field(default=500, ge=100, le=2000, description="Bulk API 청크 크기")
    number_of_shards: int = Field(default=3, ge=1, le=10, description="샤드 개수")
    number_of_replicas: int = Field(default=1, ge=0, le=3, description="복제본 개수")
    optimize_after_indexing: bool = Field(default=True, description="색인 후 최적화(force merge) 수행 여부")


class IndexResponse(BaseModel):
    """인덱싱 응답"""
    message: str
    index_name: str
    success_count: int
    failed_count: int
    total_docs: Optional[int] = None
    index_size_mb: Optional[float] = None


@router.get("/")
def read_root():
    """API 상태 확인"""
    return {
        "message": "Survey Data Indexer API 실행 중",
        "version": "2.0",
        "endpoints": [
            "/indexer/index-survey-data",
            "/indexer/index/{index_name}"
        ]
    }


@router.post("/index-survey-data", response_model=IndexResponse)
def index_survey_data_by_user(
    config: IndexConfig,
    os_client: OpenSearch = Depends(lambda: router.os_client),
):
    """
    설문 데이터 전체를 색인하는 메인 로직 실행.

    - 기존 인덱스 삭제/유지 옵션
    - 청크 단위 처리로 메모리 효율성 향상
    - 스트리밍 bulk로 안정적인 색인
    - 배치 임베딩 생성
    - 색인 후 자동 최적화
    """
    try:
        # OpenSearch 연결 확인
        if not os_client or not os_client.ping():
            raise HTTPException(
                status_code=503,
                detail="OpenSearch 서버에 연결할 수 없습니다. 서버 상태를 확인하세요."
            )

        # 임베딩 모델 확인
        embedding_model = getattr(router, 'embedding_model', None)
        if not embedding_model:
            logger.warning("⚠️ 임베딩 모델이 없습니다. 임베딩 없이 색인을 진행합니다.")

        logger.info("\n" + "=" * 60)
        logger.info("🚀 설문 데이터 색인 작업 시작")
        logger.info("=" * 60)
        logger.info(f"   인덱스: {config.index_name}")
        logger.info(f"   질문 파일: {config.question_file}")
        logger.info(f"   응답 파일: {config.response_file}")
        logger.info(f"   강제 재생성: {config.force_recreate}")
        logger.info(f"   청크 크기: {config.chunk_size}")
        logger.info(f"   Bulk 청크: {config.bulk_chunk_size}")
        logger.info(f"   임베딩 모델: {'사용' if embedding_model else '미사용'}")
        logger.info("=" * 60 + "\n")

        # 파일 존재 확인
        if not os.path.exists(config.question_file):
            raise HTTPException(
                status_code=404,
                detail=f"질문 파일을 찾을 수 없습니다: {config.question_file}"
            )
        if not os.path.exists(config.response_file):
            raise HTTPException(
                status_code=404,
                detail=f"응답 파일을 찾을 수 없습니다: {config.response_file}"
            )

        # 1단계: 인덱스 생성
        logger.info("\n[1/4] 📝 인덱스 생성 중...")
        create_survey_index(
            os_client=os_client,
            index_name=config.index_name,
            force_recreate=config.force_recreate,
            number_of_shards=config.number_of_shards,
            number_of_replicas=config.number_of_replicas
        )

        # 2단계: 질문 메타데이터 파싱
        logger.info("\n[2/4] 📖 질문 메타데이터 파싱 중...")
        questions_meta = parse_question_metadata(config.question_file)

        # 메타데이터 검증
        if not validate_metadata(questions_meta):
            raise HTTPException(
                status_code=400,
                detail="질문 메타데이터 검증 실패"
            )

        # 3단계: 데이터 처리 및 색인
        logger.info("\n[3/4] 🔄 데이터 처리 및 색인 중...")
        success_count, failed_count = process_and_bulk_index(
            os_client=os_client,
            questions_meta=questions_meta,
            response_file=config.response_file,
            index_name=config.index_name,
            embedding_model=embedding_model,
            chunk_size=config.chunk_size,
            bulk_chunk_size=config.bulk_chunk_size
        )

        # 4단계: 색인 후 최적화
        logger.info("\n[4/4] ⚙️ 색인 후 최적화 중...")

        # refresh_interval을 기본값으로 복구
        update_index_refresh_interval(os_client, config.index_name, "1s")

        # force merge (선택사항)
        if config.optimize_after_indexing:
            force_merge_index(os_client, config.index_name)

        # 인덱스 통계 조회
        stats = get_index_stats(os_client, config.index_name)

        logger.info("\n" + "=" * 60)
        logger.info("✅ 모든 작업 완료!")
        logger.info("=" * 60 + "\n")

        return IndexResponse(
            message="데이터 색인 작업 완료",
            index_name=config.index_name,
            success_count=success_count,
            failed_count=failed_count,
            total_docs=stats['doc_count'] if stats else None,
            index_size_mb=stats['size_mb'] if stats else None
        )

    except HTTPException:
        raise
    except FileNotFoundError as e:
        logger.error(f"🚨 파일을 찾을 수 없습니다: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        logger.error(f"🚨 데이터 검증 오류: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"🚨 처리 중 예상치 못한 오류 발생: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"처리 중 오류 발생: {str(e)}"
        )


@router.delete("/index/{index_name}")
def delete_index(
    index_name: str,
    confirm: bool = Query(False, description="삭제 확인"),
    os_client: OpenSearch = Depends(lambda: router.os_client),
):
    """
    인덱스를 삭제합니다. (주의: 복구 불가능)
    """
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail="인덱스 삭제를 확인하려면 confirm=true를 설정하세요."
        )

    try:
        if not os_client.indices.exists(index=index_name):
            raise HTTPException(
                status_code=404,
                detail=f"인덱스를 찾을 수 없습니다: {index_name}"
            )

        os_client.indices.delete(index=index_name)
        logger.info(f"🗑️ '{index_name}' 인덱스를 삭제했습니다.")

        return {
            "message": f"'{index_name}' 인덱스가 삭제되었습니다.",
            "index_name": index_name
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"🚨 인덱스 삭제 중 오류: {e}")
        raise HTTPException(status_code=500, detail=str(e))
