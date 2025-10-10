import os
import logging
import pandas as pd
from fastapi import FastAPI, HTTPException, Depends
from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer

# RAG Analyzer 모듈에서 핵심 컴포넌트들을 가져옵니다.
from rag_query_analyzer.config import get_config, Config
from rag_query_analyzer.analyzers.main_analyzer import AdvancedRAGQueryAnalyzer
from rag_query_analyzer.models import SearchResult
from rag_query_analyzer.data_processing import process_survey_data
from rag_query_analyzer.utils.elasticsearch import create_index_if_not_exists, bulk_index_data

# --- 로깅 설정 ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- 설정 및 전역 객체 초기화 ---
try:
    config = get_config()
    app = FastAPI(
        title="Advanced RAG Survey Search API",
        description="지능형 쿼리 분석과 하이브리드 검색, 리랭킹을 지원하는 설문조사 검색 API",
        version="2.0.0 (Auto-indexing)"
    )

    # Elasticsearch 클라이언트 초기화
    es_client = Elasticsearch(config.ES_HOST, request_timeout=30)

    # 임베딩 모델 로딩
    logger.info(f"임베딩 모델 로딩 중: {config.EMBEDDING_MODEL}")
    embedding_model = SentenceTransformer(config.EMBEDDING_MODEL)

    # 쿼리 분석기 초기화
    logger.info("쿼리 분석기 초기화 중...")
    analyzer = AdvancedRAGQueryAnalyzer(config)

except Exception as e:
    logger.critical(f"🚨 애플리케이션 초기화 실패: {e}", exc_info=True)
    raise

# --- FastAPI 시작 이벤트 ---
@app.on_event("startup")
async def startup_event():
    """애플리케이션 시작 시 데이터 자동 색인"""
    index_name = "survey_responses" # 고정된 인덱스 이름
    response_file = "./data/survey_welcome2.csv"

    logger.info("--- 🚀 애플리케이션 시작: 자동 색인을 준비합니다. (단일 파일 모드) ---")
    
    try:
        if not es_client.ping():
            logger.error("🚨 Elasticsearch 서버에 연결할 수 없습니다. 색인을 건너뜁니다.")
            return

        # 인덱스가 이미 존재하는지 확인
        if es_client.indices.exists(index=index_name):
            # 문서 개수 확인으로 이미 색인되었는지 추정
            count = es_client.count(index=index_name)['count']
            if count > 0:
                logger.info(f"👍 '{index_name}' 인덱스에 이미 {count}개의 문서가 존재합니다. 색인을 건너뜁니다.")
                return
            else:
                logger.info(f"🗑️ '{index_name}' 인덱스가 비어있어 새로 색인합니다.")
        
        logger.info(f"📂 데이터 파일 확인: {response_file}")
        if not os.path.exists(response_file):
            logger.warning(f"🚨 data 폴더에 {os.path.basename(response_file)} 파일이 없습니다.")
            logger.warning("   - 프로젝트 루트에 'data' 폴더를 생성하고 파일을 넣어주세요.")
            return

        # 데이터 처리 및 색인 실행
        create_index_if_not_exists(es_client, index_name)
        df_responses = pd.read_csv(response_file, encoding="utf-8-sig")
        df_responses = df_responses.astype(object).where(pd.notnull(df_responses), None)

        actions = process_survey_data(df_responses, embedding_model, index_name)
        
        if not actions:
            logger.warning("⚠️ 처리할 데이터가 없습니다.")
            return

        success, failed = bulk_index_data(es_client, actions)
        logger.info(f"🎉 자동 색인 완료! 성공: {success}, 실패: {len(failed)}")

    except Exception as e:
        logger.error(f"🚨 시작 시 데이터 색인 실패: {e}", exc_info=True)

# --- API 엔드포인트 ---

@app.get("/", summary="API 환영 메시지")
def read_root():
    return {"message": "Advanced RAG Survey Search API (Auto-indexing)에 오신 것을 환영합니다!"}


@app.get("/intelligent-search/", summary="지능형 설문 검색")
def intelligent_search(
    query: str,
    index_name: str = "survey_responses", # 기본 인덱스 이름 고정
    context: str = "",
    cfg: Config = Depends(get_config)
):
    """
    자연어 쿼리를 사용하여 설문 데이터를 지능적으로 검색합니다.
    - **query**: 검색할 자연어 쿼리 (예: "30대 남성 중 스트레스에 만족하는 사람")
    - **index_name**: 검색할 대상 인덱스 (기본값: survey_responses)
    - **context**: 검색에 도움이 될 추가적인 맥락 정보
    """
    if not es_client.indices.exists(index=index_name):
        raise HTTPException(status_code=404, detail=f"인덱스 '{index_name}'를 찾을 수 없습니다. 서버 시작 시 색인이 완료되었는지 확인하세요.")

    try:
        # 1. 쿼리 분석
        analysis = analyzer.analyze_query(query, context)

        # 2. 쿼리 임베딩 생성
        query_vector = embedding_model.encode(query).tolist()

        # 3. Elasticsearch 쿼리 빌드
        es_query = analyzer.build_search_query(
            analysis, query_vector, size=cfg.INITIAL_SEARCH_SIZE
        )
        
        # 4. 1차 검색 실행
        response = es_client.search(
            index=index_name, 
            body=es_query
        )
        
        initial_hits = response["hits"]["hits"]
        search_results = [
            SearchResult(
                doc_id=hit["_id"],
                score=hit["_score"],
                summary=hit.get("_source", {}).get("user_id", ""), # 간단한 요약
                answers=hit.get("_source", {})
            )
            for hit in initial_hits
        ]

        # 5. 리랭킹
        final_results = analyzer.rerank_results(query, search_results)

        return {
            "query": query,
            "query_analysis": analysis.to_dict(),
            "search_stats": {
                "initial_candidates": len(search_results),
                "final_results": len(final_results),
                "reranking_enabled": analyzer.reranker is not None
            },
            "results": [res.to_dict() for res in final_results]
        }

    except Exception as e:
        logger.error(f"지능형 검색 오류: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"검색 중 오류 발생: {e}")

@app.get("/system-status/", summary="시스템 상태 확인")
def system_status(cfg: Config = Depends(get_config)):
    """
    RAG 시스템의 주요 구성 요소 상태를 확인합니다.
    """
    return {
        "status": {
            "elasticsearch_connection": "connected" if es_client.ping() else "disconnected",
            "query_analyzer_initialized": "ok" if analyzer else "failed",
            "embedding_model_loaded": cfg.EMBEDDING_MODEL,
            "reranker_enabled": cfg.ENABLE_RERANKING,
            "reranker_model_loaded": cfg.RERANKER_MODEL if cfg.ENABLE_RERANKING else "N/A",
            "cache_enabled": cfg.ENABLE_CACHE,
            "claude_model": cfg.CLAUDE_MODEL
        }
    }

if __name__ == "__main__":
    import uvicorn
    logger.info("🚀 FastAPI 서버를 시작합니다...")
    uvicorn.run(app, host="0.0.0.0", port=8000)