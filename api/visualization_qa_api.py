"""새로운 시각화 인덱스(survey_qa_analysis)용 API 라우터"""
import logging
from typing import List, Dict, Any
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


class QuestionStatistics(BaseModel):
    """질문별 통계"""
    question_field: str = Field(..., description="질문 필드명")
    question_description: str = Field(..., description="질문 설명")
    total_responses: int = Field(..., description="전체 응답 수")
    answer_distribution: List[AnswerDistribution] = Field(..., description="답변별 분포")


class AllStatisticsResponse(BaseModel):
    """전체 통계 응답"""
    index_name: str = Field(..., description="인덱스 이름")
    total_users: int = Field(..., description="전체 사용자 수")
    statistics: Dict[str, QuestionStatistics] = Field(..., description="질문별 통계 (필드명을 키로 사용)")


def get_os_client():
    """OpenSearch 클라이언트 가져오기"""
    if router.os_client is None:
        raise HTTPException(status_code=500, detail="OpenSearch client not initialized")
    return router.os_client


@router.get("/all-statistics", response_model=AllStatisticsResponse, summary="전체 데이터 통계 조회")
async def get_all_statistics(
    index_name: str = Query(default="survey_qa_analysis", description="인덱스 이름"),
    top_n: int = Query(default=100, description="각 질문별 상위 답변 개수 (최대 100개)"),
    os_client: OpenSearch = Depends(get_os_client)
):
    """
    모든 질문 필드에 대한 통계를 한 번에 조회합니다.
    
    다음 필드들의 통계를 모두 반환합니다:
    - q_gender (성별)
    - q_birth_year (출생년도)
    - q_region (지역)
    - q_sub_region (세부 지역)
    - q_marriage (결혼여부)
    - q_children_count (자녀수)
    - q_family_count (가족수)
    - q_education (최종학력)
    - q_job (직업)
    - q_job_role (직무)
    - q_personal_income (월평균 개인소득)
    - q_household_income (월평균 가구소득)
    - q_appliances (보유가전제품)
    - q_phone_brand (보유 휴대폰 브랜드)
    - q_phone_model (보유 휴대폰 모델)
    - q_car_owned (보유차량여부)
    - q_car_brand (자동차 제조사)
    - q_car_model (자동차 모델)
    - q_smoke_type (흡연경험)
    - q_smoke_brand (흡연경험 담배브랜드)
    - q_drink_type (음용경험 술)
    
    예시:
    - /visualization/qa/all-statistics
    - /visualization/qa/all-statistics?top_n=50
    """
    try:
        # 전체 사용자 수 조회
        count_query = {
            "size": 0,
            "track_total_hits": True
        }
        count_response = os_client.search(index=index_name, body=count_query)
        total_users = count_response["hits"]["total"]["value"]
        
        # 통계를 조회할 모든 질문 필드 목록
        question_fields = [
            "q_gender",
            "q_birth_year",
            "q_region",
            "q_sub_region",
            "q_marriage",
            "q_children_count",
            "q_family_count",
            "q_education",
            "q_job",
            "q_job_role",
            "q_personal_income",
            "q_household_income",
            "q_appliances",
            "q_phone_brand",
            "q_phone_model",
            "q_car_owned",
            "q_car_brand",
            "q_car_model",
            "q_smoke_type",
            "q_smoke_brand",
            "q_drink_type",
        ]
        
        # 질문 필드 설명 매핑
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
        
        # 각 질문 필드별 통계 조회
        statistics = {}
        
        for field in question_fields:
            try:
                query = {
                    "size": 0,
                    "track_total_hits": True,  # 정확한 총 개수 반환
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
                    answer_distribution.append(AnswerDistribution(
                        answer=bucket["key"],
                        count=count,
                        percentage=percentage
                    ))
                
                statistics[field] = QuestionStatistics(
                    question_field=field,
                    question_description=question_mapping.get(field, field.replace("q_", "").replace("_", " ")),
                    total_responses=total,
                    answer_distribution=answer_distribution
                )
                
            except Exception as e:
                logger.warning(f"Error getting distribution for {field}: {e}")
                # 에러가 발생해도 빈 통계로 추가
                statistics[field] = QuestionStatistics(
                    question_field=field,
                    question_description=question_mapping.get(field, field.replace("q_", "").replace("_", " ")),
                    total_responses=0,
                    answer_distribution=[]
                )
                continue
        
        return AllStatisticsResponse(
            index_name=index_name,
            total_users=total_users,
            statistics=statistics
        )
    
    except Exception as e:
        logger.error(f"Error getting all statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))
