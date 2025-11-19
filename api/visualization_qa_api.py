"""새로운 시각화 인덱스(survey_qa_analysis)용 API 라우터"""
import logging
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from opensearchpy import OpenSearch

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/visualization/qa",
    tags=["Visualization QA"]
)

# 런타임에 주입될 OpenSearch 클라이언트
router.os_client = None


# ============= Response Models =============

class AnswerDistribution(BaseModel):
    """답변 분포"""
    answer: str = Field(..., description="답변 내용")
    count: int = Field(..., description="응답 수")
    percentage: float = Field(..., description="전체 대비 비율 (%)")


class QuestionDistributionResponse(BaseModel):
    """질문별 답변 분포 응답"""
    question_field: str = Field(..., description="질문 필드명 (예: q_marriage)")
    total_responses: int = Field(..., description="전체 응답 수")
    answer_distribution: List[AnswerDistribution] = Field(..., description="답변별 분포")


class FilteredStatsResponse(BaseModel):
    """필터링된 통계 응답"""
    total_count: int = Field(..., description="필터링된 총 문서 수")
    gender_distribution: List[Dict[str, Any]] = Field(..., description="성별 분포")
    age_group_distribution: List[Dict[str, Any]] = Field(..., description="나이대 분포")
    region_distribution: List[Dict[str, Any]] = Field(..., description="지역 분포")


class QuestionListResponse(BaseModel):
    """질문 목록 응답"""
    questions: List[Dict[str, str]] = Field(..., description="질문 필드 목록 (필드명, 설명)")


class QuestionSummary(BaseModel):
    """질문 요약 정보"""
    question_field: str = Field(..., description="질문 필드명")
    question_description: str = Field(..., description="질문 설명")
    total_responses: int = Field(..., description="전체 응답 수")
    top_answers: List[AnswerDistribution] = Field(..., description="상위 답변 (최대 5개)")


class DashboardResponse(BaseModel):
    """대시보드 전체 데이터 응답"""
    total_users: int = Field(..., description="전체 사용자 수")
    metadata_summary: Dict[str, Any] = Field(..., description="메타데이터 요약 (성별, 나이대, 지역 분포)")
    question_summaries: List[QuestionSummary] = Field(..., description="주요 질문들의 답변 경향 요약")


def get_os_client():
    """OpenSearch 클라이언트 가져오기"""
    if router.os_client is None:
        raise HTTPException(status_code=500, detail="OpenSearch client not initialized")
    return router.os_client


@router.get("/questions", response_model=QuestionListResponse, summary="질문 필드 목록 조회")
async def get_question_fields(
    index_name: str = Query(default="survey_qa_analysis", description="인덱스 이름"),
    os_client: OpenSearch = Depends(get_os_client)
):
    """
    사용 가능한 질문 필드 목록을 반환합니다.
    """
    try:
        # 인덱스 매핑에서 q_* 필드 추출
        mapping = os_client.indices.get_mapping(index=index_name)
        index_mapping = mapping[index_name]["mappings"]["properties"]
        
        questions = []
        question_mapping = {
            "q_gender": "성별",
            "q_birth_year": "출생년도",
            "q_region": "지역",
            "q_sub_region": "세부 지역",
            "q_marriage": "결혼여부",
            "q_children_count": "자녀수",
            "q_family_count": "가족수",
            "q_education": "최종학력",
            "q_job": "직업",
            "q_job_role": "직무",
            "q_personal_income": "월평균 개인소득",
            "q_household_income": "월평균 가구소득",
            "q_appliances": "보유가전제품",
            "q_phone_brand": "보유 휴대폰 브랜드",
            "q_phone_model": "보유 휴대폰 모델",
            "q_car_owned": "보유차량여부",
            "q_car_brand": "자동차 제조사",
            "q_car_model": "자동차 모델",
            "q_smoke_type": "흡연경험",
            "q_smoke_brand": "흡연경험 담배브랜드",
            "q_drink_type": "음용경험 술",
        }
        
        # 실제 존재하는 q_* 필드만 추가
        for field_name in index_mapping.keys():
            if field_name.startswith("q_"):
                description = question_mapping.get(field_name, field_name.replace("q_", "").replace("_", " "))
                questions.append({
                    "field": field_name,
                    "description": description
                })
        
        return QuestionListResponse(questions=questions)
    
    except Exception as e:
        logger.error(f"Error getting question fields: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/question/{question_field}", response_model=QuestionDistributionResponse, summary="질문별 답변 분포 조회")
async def get_question_distribution(
    question_field: str,  # Path parameter는 Field() 사용 불가
    index_name: str = Query(default="survey_qa_analysis", description="인덱스 이름"),
    os_client: OpenSearch = Depends(get_os_client)
):
    """
    특정 질문에 대한 답변 분포를 반환합니다.
    
    예시:
    - /visualization/qa/question/q_marriage
    - /visualization/qa/question/q_education
    - /visualization/qa/question/q_appliances
    """
    try:
        # 집계 쿼리
        query = {
            "size": 0,
            "aggs": {
                "answer_distribution": {
                    "terms": {
                        "field": f"{question_field}.keyword",
                        "size": 100,  # 최대 100개 답변
                        "order": {"_count": "desc"}
                    }
                }
            }
        }
        
        response = os_client.search(index=index_name, body=query)
        
        total = response["hits"]["total"]["value"]
        aggs = response.get("aggregations", {})
        buckets = aggs.get("answer_distribution", {}).get("buckets", [])
        
        answer_distribution = []
        for bucket in buckets:
            count = bucket["doc_count"]
            percentage = round((count / total * 100), 2) if total > 0 else 0
            
            answer_distribution.append(AnswerDistribution(
                answer=bucket["key"],
                count=count,
                percentage=percentage
            ))
        
        return QuestionDistributionResponse(
            question_field=question_field,
            total_responses=total,
            answer_distribution=answer_distribution
        )
    
    except Exception as e:
        logger.error(f"Error getting question distribution: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/filtered-stats", response_model=FilteredStatsResponse, summary="필터링된 통계 조회")
async def get_filtered_stats(
    index_name: str = Query(default="survey_qa_analysis", description="인덱스 이름"),
    gender: Optional[str] = Query(None, description="성별 필터 (예: 남성, 여성)"),
    age_group: Optional[str] = Query(None, description="나이대 필터 (예: 20대, 30대)"),
    region: Optional[str] = Query(None, description="지역 필터 (예: 서울, 부산)"),
    question_field: Optional[str] = Query(None, description="질문 필드명 (예: q_marriage)"),
    question_value: Optional[str] = Query(None, description="질문 답변 값 (question_field와 함께 사용)"),
    os_client: OpenSearch = Depends(get_os_client)
):
    """
    필터 조건에 맞는 통계를 반환합니다.
    
    예시:
    - /visualization/qa/filtered-stats?gender=남성&age_group=30대
    - /visualization/qa/filtered-stats?question_field=q_marriage&question_value=기혼
    """
    try:
        # 필터 쿼리 구성
        must_clauses = []
        
        if gender:
            must_clauses.append({"term": {"meta_gender.keyword": gender}})
        
        if age_group:
            must_clauses.append({"term": {"meta_age_group.keyword": age_group}})
        
        if region:
            must_clauses.append({"term": {"meta_region.keyword": region}})
        
        if question_field and question_value:
            must_clauses.append({"term": {f"{question_field}.keyword": question_value}})
        
        query = {
            "size": 0,
            "query": {
                "bool": {
                    "must": must_clauses
                }
            } if must_clauses else {"match_all": {}},
            "aggs": {
                "gender_dist": {
                    "terms": {"field": "meta_gender.keyword", "size": 10}
                },
                "age_group_dist": {
                    "terms": {"field": "meta_age_group.keyword", "size": 10}
                },
                "region_dist": {
                    "terms": {"field": "meta_region.keyword", "size": 20}
                }
            }
        }
        
        response = os_client.search(index=index_name, body=query)
        
        total = response["hits"]["total"]["value"]
        
        # 집계 결과 변환
        aggs = response.get("aggregations", {})
        
        gender_dist = [
            {"label": b["key"], "value": b["doc_count"], "percentage": round((b["doc_count"] / total * 100), 2)}
            for b in aggs.get("gender_dist", {}).get("buckets", [])
        ]
        
        age_group_dist = [
            {"label": b["key"], "value": b["doc_count"], "percentage": round((b["doc_count"] / total * 100), 2)}
            for b in aggs.get("age_group_dist", {}).get("buckets", [])
        ]
        
        region_dist = [
            {"label": b["key"], "value": b["doc_count"], "percentage": round((b["doc_count"] / total * 100), 2)}
            for b in aggs.get("region_dist", {}).get("buckets", [])
        ]
        
        return FilteredStatsResponse(
            total_count=total,
            gender_distribution=gender_dist,
            age_group_distribution=age_group_dist,
            region_distribution=region_dist
        )
    
    except Exception as e:
        logger.error(f"Error getting filtered stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cross-analysis", summary="교차 분석")
async def get_cross_analysis(
    question_field1: str = Query(..., description="첫 번째 질문 필드 (예: q_marriage)"),
    question_field2: str = Query(..., description="두 번째 질문 필드 (예: q_education)"),
    index_name: str = Query(default="survey_qa_analysis", description="인덱스 이름"),
    os_client: OpenSearch = Depends(get_os_client)
):
    """
    두 질문 간의 교차 분석을 반환합니다.
    
    예시:
    - /visualization/qa/cross-analysis?question_field1=q_marriage&question_field2=q_education
    """
    try:
        query = {
            "size": 0,
            "aggs": {
                "field1_dist": {
                    "terms": {
                        "field": f"{question_field1}.keyword",
                        "size": 20
                    },
                    "aggs": {
                        "field2_dist": {
                            "terms": {
                                "field": f"{question_field2}.keyword",
                                "size": 20
                            }
                        }
                    }
                }
            }
        }
        
        response = os_client.search(index=index_name, body=query)
        
        aggs = response.get("aggregations", {})
        result = []
        for bucket1 in aggs.get("field1_dist", {}).get("buckets", []):
            field1_value = bucket1["key"]
            field2_dist = []
            
            for bucket2 in bucket1["field2_dist"]["buckets"]:
                field2_dist.append({
                    "label": bucket2["key"],
                    "value": bucket2["doc_count"],
                    "percentage": round((bucket2["doc_count"] / bucket1["doc_count"] * 100), 2)
                })
            
            result.append({
                "field1_value": field1_value,
                "field1_count": bucket1["doc_count"],
                "field2_distribution": field2_dist
            })
        
        return {
            "question_field1": question_field1,
            "question_field2": question_field2,
            "cross_analysis": result
        }
    
    except Exception as e:
        logger.error(f"Error getting cross analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dashboard", response_model=DashboardResponse, summary="전체 데이터 답변 경향 대시보드")
async def get_dashboard(
    index_name: str = Query(default="survey_qa_analysis", description="인덱스 이름"),
    question_fields: Optional[str] = Query(
        None, 
        description="조회할 질문 필드 목록 (쉼표로 구분, 예: q_marriage,q_education,q_job). 없으면 주요 질문 자동 선택"
    ),
    top_n: int = Query(default=5, description="각 질문별 상위 답변 개수"),
    os_client: OpenSearch = Depends(get_os_client)
):
    """
    전체 데이터의 답변 경향을 한 번에 조회합니다.
    
    - 전체 사용자 수
    - 메타데이터 분포 (성별, 나이대, 지역)
    - 주요 질문들의 답변 경향 (상위 답변만)
    
    예시:
    - /visualization/qa/dashboard
    - /visualization/qa/dashboard?question_fields=q_marriage,q_education,q_job&top_n=3
    """
    try:
        # 1. 전체 사용자 수 및 메타데이터 분포 조회
        metadata_query = {
            "size": 0,
            "aggs": {
                "gender_dist": {
                    "terms": {"field": "meta_gender.keyword", "size": 10}
                },
                "age_group_dist": {
                    "terms": {"field": "meta_age_group.keyword", "size": 10}
                },
                "region_dist": {
                    "terms": {"field": "meta_region.keyword", "size": 20}
                }
            }
        }
        
        metadata_response = os_client.search(index=index_name, body=metadata_query)
        total_users = metadata_response["hits"]["total"]["value"]
        metadata_aggs = metadata_response.get("aggregations", {})
        
        metadata_summary = {
            "gender_distribution": [
                {"label": b["key"], "value": b["doc_count"], "percentage": round((b["doc_count"] / total_users * 100), 2)}
                for b in metadata_aggs.get("gender_dist", {}).get("buckets", [])
            ],
            "age_group_distribution": [
                {"label": b["key"], "value": b["doc_count"], "percentage": round((b["doc_count"] / total_users * 100), 2)}
                for b in metadata_aggs.get("age_group_dist", {}).get("buckets", [])
            ],
            "region_distribution": [
                {"label": b["key"], "value": b["doc_count"], "percentage": round((b["doc_count"] / total_users * 100), 2)}
                for b in metadata_aggs.get("region_dist", {}).get("buckets", [])
            ]
        }
        
        # 2. 조회할 질문 필드 결정
        if question_fields:
            # 사용자가 지정한 필드 사용
            fields_to_query = [f.strip() for f in question_fields.split(",") if f.strip()]
        else:
            # 주요 질문 필드 자동 선택
            mapping = os_client.indices.get_mapping(index=index_name)
            index_mapping = mapping[index_name]["mappings"]["properties"]
            
            # 주요 질문 필드 우선순위
            priority_fields = [
                "q_marriage", "q_education", "q_job", "q_appliances",
                "q_phone_brand", "q_car_owned", "q_car_brand",
                "q_smoke_type", "q_drink_type", "q_personal_income"
            ]
            
            fields_to_query = [
                field for field in priority_fields 
                if field in index_mapping
            ][:10]  # 최대 10개만
        
        # 3. 각 질문 필드의 답변 분포 조회
        question_summaries = []
        question_mapping = {
            "q_gender": "성별",
            "q_birth_year": "출생년도",
            "q_region": "지역",
            "q_sub_region": "세부 지역",
            "q_marriage": "결혼여부",
            "q_children_count": "자녀수",
            "q_family_count": "가족수",
            "q_education": "최종학력",
            "q_job": "직업",
            "q_job_role": "직무",
            "q_personal_income": "월평균 개인소득",
            "q_household_income": "월평균 가구소득",
            "q_appliances": "보유가전제품",
            "q_phone_brand": "보유 휴대폰 브랜드",
            "q_phone_model": "보유 휴대폰 모델",
            "q_car_owned": "보유차량여부",
            "q_car_brand": "자동차 제조사",
            "q_car_model": "자동차 모델",
            "q_smoke_type": "흡연경험",
            "q_smoke_brand": "흡연경험 담배브랜드",
            "q_drink_type": "음용경험 술",
        }
        
        for field in fields_to_query:
            try:
                query = {
                    "size": 0,
                    "aggs": {
                        "answer_distribution": {
                            "terms": {
                                "field": f"{field}.keyword",
                                "size": top_n,
                                "order": {"_count": "desc"}
                            }
                        }
                    }
                }
                
                response = os_client.search(index=index_name, body=query)
                total = response["hits"]["total"]["value"]
                aggs = response.get("aggregations", {})
                buckets = aggs.get("answer_distribution", {}).get("buckets", [])
                
                top_answers = []
                for bucket in buckets:
                    count = bucket["doc_count"]
                    percentage = round((count / total * 100), 2) if total > 0 else 0
                    top_answers.append(AnswerDistribution(
                        answer=bucket["key"],
                        count=count,
                        percentage=percentage
                    ))
                
                question_summaries.append(QuestionSummary(
                    question_field=field,
                    question_description=question_mapping.get(field, field.replace("q_", "").replace("_", " ")),
                    total_responses=total,
                    top_answers=top_answers
                ))
            except Exception as e:
                logger.warning(f"Error getting distribution for {field}: {e}")
                continue
        
        return DashboardResponse(
            total_users=total_users,
            metadata_summary=metadata_summary,
            question_summaries=question_summaries
        )
    
    except Exception as e:
        logger.error(f"Error getting dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/multiple-questions", summary="여러 질문의 답변 분포를 한 번에 조회")
async def get_multiple_question_distributions(
    question_fields: str = Query(..., description="질문 필드 목록 (쉼표로 구분, 예: q_marriage,q_education,q_job)"),
    index_name: str = Query(default="survey_qa_analysis", description="인덱스 이름"),
    top_n: int = Query(default=10, description="각 질문별 상위 답변 개수"),
    os_client: OpenSearch = Depends(get_os_client)
):
    """
    여러 질문의 답변 분포를 한 번에 조회합니다.
    
    예시:
    - /visualization/qa/multiple-questions?question_fields=q_marriage,q_education,q_job
    """
    try:
        fields_to_query = [f.strip() for f in question_fields.split(",") if f.strip()]
        
        if not fields_to_query:
            raise HTTPException(status_code=400, detail="질문 필드를 최소 1개 이상 지정해야 합니다.")
        
        # 여러 질문을 한 번에 집계 (multi_terms 또는 각각 별도 집계)
        # 성능을 위해 각 질문별로 별도 쿼리 실행
        results = {}
        
        for field in fields_to_query:
            try:
                query = {
                    "size": 0,
                    "aggs": {
                        "answer_distribution": {
                            "terms": {
                                "field": f"{field}.keyword",
                                "size": top_n,
                                "order": {"_count": "desc"}
                            }
                        }
                    }
                }
                
                response = os_client.search(index=index_name, body=query)
                total = response["hits"]["total"]["value"]
                aggs = response.get("aggregations", {})
                buckets = aggs.get("answer_distribution", {}).get("buckets", [])
                
                answer_distribution = []
                for bucket in buckets:
                    count = bucket["doc_count"]
                    percentage = round((count / total * 100), 2) if total > 0 else 0
                    answer_distribution.append({
                        "answer": bucket["key"],
                        "count": count,
                        "percentage": percentage
                    })
                
                results[field] = {
                    "question_field": field,
                    "total_responses": total,
                    "answer_distribution": answer_distribution
                }
            except Exception as e:
                logger.warning(f"Error getting distribution for {field}: {e}")
                results[field] = {
                    "question_field": field,
                    "error": str(e)
                }
        
        return {
            "index_name": index_name,
            "questions": results
        }
    
    except Exception as e:
        logger.error(f"Error getting multiple question distributions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

