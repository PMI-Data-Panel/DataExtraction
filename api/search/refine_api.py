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
    max_user_ids: int = Field(default=20, ge=1, le=50, description="분석할 최대 user_id 수 (기본값: 20)")
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


def _prepare_data_for_llm(
    user_data: List[Dict[str, Any]],
    query: str = "",
    max_chars: int = 50000
) -> List[Dict[str, Any]]:
    """LLM에 전달할 데이터 준비 (Query 관련 필터링 + 토큰 제한 고려)

    ⭐ Query 관련 필터링 로직:
    1. Query에서 한글/영문 키워드 추출 (2글자 이상)
    2. 각 qa_pair를 키워드 매칭 점수로 평가
    3. 키워드 있는 경우: 상위 10개 관련 qa_pairs (점수 > 0) + 기본 3개 (점수 = 0) 선택
    4. 키워드 없는 경우: 모든 qa_pairs 포함 (토큰 제한 내에서)

    장점:
    - 정확성: Query에 필요한 정보만 전달 (키워드 있는 경우)
    - 완전성: 모든 사용자 응답 포함 (키워드 없는 경우)
    - 효율성: 토큰 제한(50000 chars) 고려
    - 확장성: 복잡한 질문도 대응 가능
    """
    # 1. Query에서 한글/영문 키워드 추출 (2글자 이상)
    keywords = []
    if query:
        # 한글, 영문, 숫자 단어 추출
        words = re.findall(r'[가-힣]+|[a-zA-Z]+|[0-9]+', query)
        keywords = [w.lower() for w in words if len(w) > 1]
        logger.info(f"  📌 Query 키워드 추출: {keywords}")

    prepared = []
    total_chars = 0

    for data in user_data:
        # 2. qa_pairs 필터링 및 점수화
        qa_pairs = data.get("qa_pairs") or []

        if keywords and qa_pairs:
            scored_qa = []
            for qa in qa_pairs:
                q_text = (qa.get("q_text") or "").lower()
                answer = str(qa.get("answer") or "").lower()
                combined = f"{q_text} {answer}"

                # 매칭 키워드 개수로 점수 계산
                score = sum(1 for kw in keywords if kw in combined)
                scored_qa.append((score, qa))

            # 점수 내림차순 정렬
            scored_qa.sort(key=lambda x: x[0], reverse=True)

            # 상위 10개 (점수 > 0) + 하위 3개 (점수 = 0)
            relevant = [qa for score, qa in scored_qa if score > 0][:10]
            basic = [qa for score, qa in scored_qa if score == 0][:3]
            selected_qa = relevant + basic

            if relevant:
                logger.debug(f"  ✅ User {data.get('user_id')}: 관련 qa {len(relevant)}개, 기본 qa {len(basic)}개")
        else:
            # 키워드 없으면 모든 qa_pairs 포함
            selected_qa = qa_pairs
            logger.debug(f"  ✅ User {data.get('user_id')}: 모든 qa_pairs {len(selected_qa)}개 포함")

        # 3. 핵심 정보만 추출
        item = {
            "user_id": data.get("user_id"),
            "demographic_info": data.get("demographic_info") or data.get("metadata", {}),
            "behavioral_info": data.get("behavioral_info", {}),
            "qa_pairs": selected_qa,
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
    
    # 프롬프트 구성 (핵심만, 2-3줄 답변)
    context_parts = []
    
    context_parts.append("설문조사 데이터를 분석하여 질문에 답변해주세요.")
    if previous_query:
        context_parts.append(f"이전 질문: {previous_query}")
        context_parts.append("⚠️ 이전 질문과 중복되지 않는 새로운 관점이나 정보를 제공해주세요.")
    context_parts.append(f"질문: {new_query}")
    context_parts.append(f"데이터: {json.dumps(user_data, ensure_ascii=False, indent=2)}")
    context_parts.append("\n⚠️ 필수 답변 형식 (절대 준수):")
    context_parts.append("- 비율은 무조건 퍼센트(%)로만 표현하세요.")
    context_parts.append("- '5명 중 2명', '전체 10명의 응답자', '3명이', '2명이'와 같은 '명' 단위 표현을 절대 사용하지 마세요.")
    context_parts.append("- '전체 N명', 'N명의 응답자', 'N명이', 'N명 중'과 같은 모든 숫자+명 조합을 사용하지 마세요.")
    context_parts.append("- 올바른 예시: '40%의 응답자가...', '약 30%가...', '절반 이상이...'")
    context_parts.append("- 틀린 예시: '전체 5명 중...', '2명이...', '10명의 응답자 중...'")
    context_parts.append("\n핵심만 2-3줄로 간단히 작성해주세요.")
    
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
        
        # 4. LLM에 전달할 데이터 준비 (Query 관련 필터링 적용!)
        prepared_data = _prepare_data_for_llm(user_data, query=request.query, max_chars=50000)
        logger.info(f"  ✅ LLM 전달 데이터: {len(prepared_data)}개 (Query 관련 필터링 + 토큰 제한 고려)")
        
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

