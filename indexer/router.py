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


