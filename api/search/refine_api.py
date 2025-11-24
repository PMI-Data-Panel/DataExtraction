"""재질의 API - 이전 검색 결과를 기반으로 LLM 재분석

이 모듈은 Redis에 저장된 이전 검색 결과의 user_id들을 가져와서
해당 사용자들의 데이터만으로 LLM이 재분석하는 기능을 제공합니다.
"""
import json
import logging
import re
from typing import List, Dict, Any, Optional, Tuple
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from opensearchpy import OpenSearch

from ..search_api import _utc_now_iso

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/search/refine",
    tags=["Search Refine"]
)

# 런타임 의존성 (main_api.py에서 주입됨)
router.redis_client = None  # type: ignore[attr-defined]
router.os_client = None  # type: ignore[attr-defined]
router.anthropic_client = None  # type: ignore[attr-defined]
router.config = None  # type: ignore[attr-defined]
router.conversation_history_prefix = None  # type: ignore[attr-defined]


class RefineQueryRequest(BaseModel):
    """재질의 요청"""
    session_id: str = Field(..., description="세션 ID (이전 검색 결과를 가져올 세션)")
    query: str = Field(..., description="재질의 질문 (예: '이 사람들의 공통점은?', '이 중에서 흡연자는?')")
    max_user_ids: int = Field(default=10, ge=1, le=50, description="분석할 최대 user_id 수 (기본값: 10)")
    llm_instructions: Optional[str] = Field(
        default=None,
        description="LLM 분석 시 추가 지침"
    )


class RefineQueryResponse(BaseModel):
    """재질의 응답"""
    session_id: str
    previous_query: Optional[str] = None
    previous_top_user_ids: List[str] = Field(default_factory=list)
    analyzed_user_ids: List[str] = Field(default_factory=list)
    user_data_count: int = 0
    llm_analysis: Dict[str, Any] = Field(default_factory=dict)
    took_ms: int


def _get_top_user_ids_from_session(session_id: str) -> Tuple[Optional[str], List[str]]:
    """Redis에서 세션의 이전 검색 결과에서 top_user_ids 추출
    
    Returns:
        (이전_질문, top_user_ids_리스트)
    """
    redis_client = getattr(router, "redis_client", None)
    if not redis_client:
        # 디버깅: router 속성 확인
        logger.error("❌ Redis 클라이언트가 없습니다.")
        logger.error(f"   router.redis_client: {getattr(router, 'redis_client', 'NOT_SET')}")
        logger.error(f"   router 객체: {router}")
        logger.error(f"   router 속성들: {[attr for attr in dir(router) if not attr.startswith('_')]}")
        return None, []
    
    conversation_prefix = getattr(router, "conversation_history_prefix", "chat:session")
    conversation_key = f"{conversation_prefix}:{session_id}"
    
    logger.info(f"🔍 Redis 세션 조회: key={conversation_key}, prefix={conversation_prefix}, session_id={session_id}")
    
    try:
        # Redis 연결 확인
        try:
            redis_client.ping()
            logger.info(f"✅ Redis 연결 확인 성공")
        except Exception as ping_error:
            logger.error(f"❌ Redis 연결 실패: {ping_error}")
            return None, []
        
        # Redis에서 최근 메시지들 가져오기 (최대 50개)
        raw_items = redis_client.lrange(conversation_key, -50, -1)
        logger.info(f"📊 Redis 조회 결과: key={conversation_key}, items_count={len(raw_items) if raw_items else 0}")
        
        if not raw_items:
            # 디버깅: 모든 키 확인
            try:
                all_keys = redis_client.keys(f"{conversation_prefix}:*")
                logger.warning(f"⚠️ 세션 {session_id}의 대화 히스토리를 찾을 수 없습니다.")
                logger.warning(f"   조회한 키: {conversation_key}")
                logger.warning(f"   Redis에 존재하는 유사 키들: {all_keys[:10] if all_keys else '없음'}")
            except Exception as key_error:
                logger.error(f"❌ Redis 키 조회 실패: {key_error}")
            return None, []
        
        previous_query = None
        top_user_ids = []
        
        # 역순으로 순회하여 가장 최근 assistant 응답에서 top_user_ids 추출
        for item in reversed(raw_items):
            try:
                payload = json.loads(item)
                role = payload.get("role")
                content = payload.get("content")
                
                if role == "assistant" and content:
                    # content가 문자열이면 JSON 파싱 시도
                    if isinstance(content, str):
                        try:
                            content = json.loads(content)
                        except:
                            pass
                    
                    # top_user_ids 추출
                    if isinstance(content, dict):
                        ids = content.get("top_user_ids", [])
                        if ids:
                            top_user_ids = ids
                            break
                
                elif role == "user" and content and not previous_query:
                    # 가장 최근 user 질문 저장
                    if isinstance(content, str):
                        previous_query = content
                    else:
                        previous_query = str(content)
            
            except Exception as e:
                logger.debug(f"메시지 파싱 실패: {e}")
                continue
        
        return previous_query, top_user_ids
    
    except Exception as e:
        logger.error(f"Redis에서 세션 데이터 조회 실패: {e}")
        return None, []


def _fetch_user_data_from_opensearch(
    user_ids: List[str],
    index_name: str = "survey_responses_merged",
    os_client: Optional[OpenSearch] = None
) -> List[Dict[str, Any]]:
    """OpenSearch에서 user_id들의 상세 데이터 조회"""
    if not os_client:
        os_client = getattr(router, "os_client", None)
    
    if not os_client or not os_client.ping():
        raise HTTPException(
            status_code=503,
            detail="OpenSearch 서버에 연결할 수 없습니다."
        )
    
    if not user_ids:
        return []
    
    try:
        # mget으로 여러 문서 한 번에 조회
        mget_body = [{"_index": index_name, "_id": uid} for uid in user_ids]
        response = os_client.mget(
            body={"docs": mget_body},
            _source=True,
            ignore=[404]
        )
        
        user_data = []
        for doc in response.get("docs", []):
            if doc.get("found"):
                source = doc.get("_source", {})
                user_id = doc.get("_id")
                if user_id and source:
                    user_data.append({
                        "user_id": user_id,
                        **source
                    })
        
        return user_data
    
    except Exception as e:
        logger.error(f"OpenSearch에서 user 데이터 조회 실패: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"데이터 조회 실패: {str(e)}"
        )


def _prepare_data_for_llm(user_data: List[Dict[str, Any]], max_chars: int = 15000) -> List[Dict[str, Any]]:
    """LLM에 전달할 데이터 준비 (토큰 제한 고려)"""
    prepared = []
    total_chars = 0
    
    for data in user_data:
        # 핵심 정보만 추출
        item = {
            "user_id": data.get("user_id"),
            "demographic_info": data.get("demographic_info") or data.get("metadata", {}),
            "behavioral_info": data.get("behavioral_info", {}),
            "qa_pairs": (data.get("qa_pairs") or [])[:5],  # 최대 5개만
        }
        
        serialized = json.dumps(item, ensure_ascii=False)
        if total_chars + len(serialized) > max_chars:
            break
        
        prepared.append(item)
        total_chars += len(serialized)
    
    return prepared


def _call_llm_for_refinement(
    previous_query: Optional[str],
    new_query: str,
    user_data: List[Dict[str, Any]],
    instructions: Optional[str] = None
) -> Dict[str, Any]:
    """LLM에 재질의 요청"""
    anthropic_client = getattr(router, "anthropic_client", None)
    if not anthropic_client:
        raise HTTPException(
            status_code=503,
            detail="Anthropic 클라이언트가 설정되지 않았습니다."
        )
    
    config = getattr(router, "config", None)
    if not config:
        from rag_query_analyzer.config import get_config
        config = get_config()
    
    model_name = getattr(config, "CLAUDE_MODEL", "claude-3-5-sonnet-20241022")
    
    # 프롬프트 구성 (핵심 5줄)
    context_parts = []
    
    context_parts.append("당신은 설문조사 데이터 분석 전문가입니다. 주어진 사용자 데이터를 분석하여 질문에 대한 상세한 답변을 문장 형태로 작성해주세요.")
    if previous_query:
        context_parts.append(f"이전 검색 질문: {previous_query}")
    context_parts.append(f"새로운 질문: {new_query}")
    context_parts.append(f"사용자 데이터: {json.dumps(user_data, ensure_ascii=False, indent=2)}")
    context_parts.append("요구사항: 데이터 패턴과 공통점을 구체적으로 설명하고, 비율이나 분포를 포함한 자연스러운 문장으로 200-600자 이내로 작성해주세요.")
    
    prompt = "\n".join(context_parts)
    
    # LLM 호출
    try:
        message = anthropic_client.messages.create(
            model=model_name,
            max_tokens=3000,
            temperature=0.3,
            messages=[{"role": "user", "content": prompt}],
        )
        
        content = ""
        if message and getattr(message, "content", None):
            parts = getattr(message, "content", [])
            if parts:
                first = parts[0]
                content = getattr(first, "text", "") or ""
        
        # ⭐ 줄바꿈 문자 처리: \n\n을 공백으로 치환하여 JSON 응답에서 깔끔하게 표시
        if content:
            # 연속된 줄바꿈(\n\n, \n\n\n 등)을 하나의 공백으로 치환
            content = re.sub(r'\n+', ' ', content)
            # 연속된 공백을 하나로 정리
            content = re.sub(r'\s+', ' ', content)
            # 앞뒤 공백 제거
            content = content.strip()
        
        return {
            "model": model_name,
            "generated_at": _utc_now_iso(),
            "analysis": content,
            "user_count": len(user_data),
        }
    
    except Exception as e:
        logger.error(f"LLM 재질의 실패: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"LLM 분석 실패: {str(e)}"
        )


@router.post("/query", response_model=RefineQueryResponse, summary="재질의 - 이전 검색 결과 기반 LLM 재분석")
async def refine_query(
    request: RefineQueryRequest,
    os_client: OpenSearch = Depends(lambda: router.os_client),
):
    """
    이전 검색 결과의 user_id들을 가져와서 해당 사용자들의 데이터만으로 LLM 재분석
    
    흐름:
    1. Redis에서 session_id의 이전 검색 결과에서 top_user_ids 추출
    2. survey_responses_merged 인덱스에서 해당 user_id들의 상세 데이터 조회
    3. 상위 N개(기본 10개)만 선택
    4. LLM에 재질의 프롬프트 전달하여 분석
    5. 결과 반환
    
    예시:
    - 이전 검색: "30대 남성"
    - 재질의: "이 사람들의 공통점은?"
    - 재질의: "이 중에서 흡연자는 몇 명인가요?"
    """
    import time
    start_time = time.time()
    
    try:
        # 1. Redis에서 이전 검색 결과의 top_user_ids 가져오기
        logger.info(f"🔍 세션 {request.session_id}에서 이전 검색 결과 조회 중...")
        previous_query, top_user_ids = _get_top_user_ids_from_session(request.session_id)
        
        if not top_user_ids:
            raise HTTPException(
                status_code=404,
                detail=f"세션 {request.session_id}에서 이전 검색 결과를 찾을 수 없습니다. 먼저 검색을 수행해주세요."
            )
        
        logger.info(f"  ✅ 이전 질문: {previous_query}")
        logger.info(f"  ✅ 발견된 user_id: {len(top_user_ids)}개")
        
        # 2. 상위 N개만 선택
        selected_user_ids = top_user_ids[:request.max_user_ids]
        logger.info(f"  ✅ 분석 대상: {len(selected_user_ids)}개 (상위 {request.max_user_ids}개)")
        
        # 3. OpenSearch에서 user_id들의 상세 데이터 조회
        logger.info(f"📊 survey_responses_merged에서 {len(selected_user_ids)}개 user_id 데이터 조회 중...")
        user_data = _fetch_user_data_from_opensearch(
            user_ids=selected_user_ids,
            index_name="survey_responses_merged",
            os_client=os_client
        )
        
        if not user_data:
            raise HTTPException(
                status_code=404,
                detail=f"user_id {selected_user_ids}에 해당하는 데이터를 찾을 수 없습니다."
            )
        
        logger.info(f"  ✅ 조회된 데이터: {len(user_data)}개")
        
        # 4. LLM에 전달할 데이터 준비
        prepared_data = _prepare_data_for_llm(user_data, max_chars=15000)
        logger.info(f"  ✅ LLM 전달 데이터: {len(prepared_data)}개 (토큰 제한 고려)")
        
        # 5. LLM 재분석
        logger.info(f"🤖 LLM 재분석 중...")
        llm_result = _call_llm_for_refinement(
            previous_query=previous_query,
            new_query=request.query,
            user_data=prepared_data,
            instructions=request.llm_instructions
        )
        
        took_ms = int((time.time() - start_time) * 1000)
        
        logger.info(f"✅ 재질의 완료: {took_ms}ms")
        
        return RefineQueryResponse(
            session_id=request.session_id,
            previous_query=previous_query,
            previous_top_user_ids=top_user_ids,
            analyzed_user_ids=selected_user_ids,
            user_data_count=len(user_data),
            llm_analysis=llm_result,
            took_ms=took_ms,
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"재질의 중 오류: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"재질의 실패: {str(e)}"
        )

