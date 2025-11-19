"""시각화 API 라우터"""
import logging
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from opensearchpy import OpenSearch

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/visualization",
    tags=["Visualization"]
)

# 런타임에 주입될 OpenSearch 클라이언트
router.os_client = None


# ============= Response Models =============

class ChartDataPoint(BaseModel):
    """차트 데이터 포인트"""
    label: str = Field(..., description="레이블 (예: '20대', '남성')")
    value: int = Field(..., description="값 (카운트)")
    percentage: Optional[float] = Field(None, description="전체 대비 비율 (%)")


class DemographicsResponse(BaseModel):
    """인구통계 분포 응답"""
    index_name: str
    total_docs: int
    age_distribution: List[ChartDataPoint]
    gender_distribution: List[ChartDataPoint]
    birth_year_distribution: List[ChartDataPoint]


class BehavioralResponse(BaseModel):
    """행동 패턴 통계 응답"""
    index_name: str
    total_docs: int
    smoker_distribution: List[ChartDataPoint]
    vehicle_distribution: List[ChartDataPoint]


class UserInfoResponse(BaseModel):
    """사용자 전체 정보 통계 응답"""
    index_name: str
    total_docs: int
    gender_distribution: List[ChartDataPoint]
    age_distribution: List[ChartDataPoint]
    region_distribution: List[ChartDataPoint]
    marital_status_distribution: List[ChartDataPoint]
    family_size_distribution: List[ChartDataPoint]
    occupation_distribution: List[ChartDataPoint]
    income_distribution: List[ChartDataPoint]
    vehicle_distribution: List[ChartDataPoint]
    smoker_distribution: List[ChartDataPoint]
    drinker_distribution: List[ChartDataPoint]



def calculate_percentage(count: int, total: int) -> float:
    """퍼센티지 계산"""
    if total == 0:
        return 0.0
    return round((count / total) * 100, 2)


def get_os_client():
    """OpenSearch 클라이언트 가져오기"""
    if router.os_client is None:
        raise HTTPException(status_code=500, detail="OpenSearch client not initialized")
    return router.os_client

@router.get("/user-info/{index_name}", response_model=UserInfoResponse, summary="사용자 전체 정보 통계")
async def get_user_info_statistics(
    index_name: str,
    os_client: OpenSearch = Depends(get_os_client)
):
    """
    사용자 전체 정보 통계 데이터 반환
    
    - 성별 분포 (metadata.gender)
    - 나이대 분포 (metadata.age_group)
    - 지역 분포 (metadata.region)
    - 결혼여부 분포 (qa_pairs 또는 metadata)
    - 가족수 분포 (qa_pairs)
    - 직업 분포 (metadata.occupation)
    - 월평균 개인소득 분포 (qa_pairs)
    - 보유차량여부 분포 (qa_pairs)
    - 흡연 여부 분포 (qa_pairs)
    - 음주 여부 분포 (qa_pairs)
    """
    try:
        query = {
            "size": 0,
            "aggs": {
                # 성별 분포 (metadata) - gender는 이미 keyword 타입
                "gender_dist": {
                    "terms": {
                        "field": "metadata.gender",
                        "size": 10
                    }
                },
                # 나이대 분포 (metadata) - age_group은 이미 keyword 타입
                "age_group_dist": {
                    "terms": {
                        "field": "metadata.age_group",
                        "size": 20
                    }
                },
                # 지역 분포 (metadata) - region은 이미 keyword 타입
                "region_dist": {
                    "terms": {
                        "field": "metadata.region",
                        "size": 50
                    }
                },
                # 직업 분포 (metadata) - occupation은 이미 keyword 타입
                "occupation_dist": {
                    "terms": {
                        "field": "metadata.occupation",
                        "size": 50
                    }
                },
                # qa_pairs nested 집계
                "qa_nested": {
                    "nested": {"path": "qa_pairs"},
                    "aggs": {
                        # 흡연 경험 집계
                        "smoker_filter": {
                            "filter": {
                                "term": {"qa_pairs.q_text.keyword": "흡연경험"}
                            },
                            "aggs": {
                                "smoker_answers": {
                                    "terms": {
                                        "field": "qa_pairs.answer.keyword",
                                        "size": 10
                                    }
                                }
                            }
                        },
                        # 차량 보유 집계
                        "vehicle_filter": {
                            "filter": {
                                "term": {"qa_pairs.q_text.keyword": "보유차량여부"}
                            },
                            "aggs": {
                                "vehicle_answers": {
                                    "terms": {
                                        "field": "qa_pairs.answer.keyword",
                                        "size": 10
                                    }
                                }
                            }
                        },
                        # 음주 경험 집계
                        "drinker_filter": {
                            "filter": {
                                "term": {"qa_pairs.q_text.keyword": "음용경험 술"}
                            },
                            "aggs": {
                                "drinker_answers": {
                                    "terms": {
                                        "field": "qa_pairs.answer.keyword",
                                        "size": 10
                                    }
                                }
                            }
                        },
                        # 결혼여부 집계
                        "marital_filter": {
                            "filter": {
                                "bool": {
                                    "should": [
                                        {"term": {"qa_pairs.q_text.keyword": "결혼여부"}},
                                        {"term": {"qa_pairs.q_text.keyword": "혼인상태"}},
                                        {"wildcard": {"qa_pairs.q_text.keyword": "*결혼*"}}
                                    ]
                                }
                            },
                            "aggs": {
                                "marital_answers": {
                                    "terms": {
                                        "field": "qa_pairs.answer.keyword",
                                        "size": 20
                                    }
                                }
                            }
                        },
                        # 가족수 집계
                        "family_filter": {
                            "filter": {
                                "bool": {
                                    "should": [
                                        {"term": {"qa_pairs.q_text.keyword": "가족수"}},
                                        {"term": {"qa_pairs.q_text.keyword": "가구원수"}},
                                        {"term": {"qa_pairs.q_text.keyword": "가구원 수"}},
                                        {"wildcard": {"qa_pairs.q_text.keyword": "*가족 수*"}},
                                        {"wildcard": {"qa_pairs.q_text.keyword": "*가족수*"}}
                                    ],
                                    "must_not": [
                                        {"wildcard": {"qa_pairs.q_text.keyword": "*소득*"}},
                                        {"wildcard": {"qa_pairs.q_text.keyword": "*수입*"}}
                                    ]
                                }
                            },
                            "aggs": {
                                "family_answers": {
                                    "terms": {
                                        "field": "qa_pairs.answer.keyword",
                                        "size": 20
                                    }
                                }
                            }
                        },
                        # 월평균 개인소득 집계
                        "income_filter": {
                            "filter": {
                                "bool": {
                                    "should": [
                                        {"term": {"qa_pairs.q_text.keyword": "월평균 개인소득"}},
                                        {"wildcard": {"qa_pairs.q_text.keyword": "*소득*"}},
                                        {"wildcard": {"qa_pairs.q_text.keyword": "*수입*"}}
                                    ]
                                }
                            },
                            "aggs": {
                                "income_answers": {
                                    "terms": {
                                        "field": "qa_pairs.answer.keyword",
                                        "size": 30
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        response = os_client.search(index=index_name, body=query)
        total_docs = response["hits"]["total"]["value"]
        aggs = response["aggregations"]

        # 🔍 디버깅: aggregation 결과 로깅
        logger.info(f"Total docs: {total_docs}")
        logger.info(f"Gender agg: {aggs.get('gender_dist', {})}")
        logger.info(f"Age agg: {aggs.get('age_group_dist', {})}")

        # 성별 분포 처리
        gender_buckets = aggs["gender_dist"]["buckets"]
        gender_total = sum(bucket["doc_count"] for bucket in gender_buckets)
        gender_map = {"M": "남성", "F": "여성", "남성": "남성", "여성": "여성", "미정": "미정"}
        gender_distribution = [
            ChartDataPoint(
                label=gender_map.get(bucket["key"], bucket["key"]),
                value=bucket["doc_count"],
                percentage=calculate_percentage(bucket["doc_count"], gender_total if gender_total > 0 else total_docs)
            )
            for bucket in gender_buckets
        ]

        # 나이대 분포 처리
        age_buckets = aggs["age_group_dist"]["buckets"]
        age_total = sum(bucket["doc_count"] for bucket in age_buckets)
        age_distribution = [
            ChartDataPoint(
                label=bucket["key"],
                value=bucket["doc_count"],
                percentage=calculate_percentage(bucket["doc_count"], age_total if age_total > 0 else total_docs)
            )
            for bucket in age_buckets
        ]

        # 지역 분포 처리
        region_buckets = aggs["region_dist"]["buckets"]
        region_total = sum(bucket["doc_count"] for bucket in region_buckets)
        region_distribution = [
            ChartDataPoint(
                label=bucket["key"],
                value=bucket["doc_count"],
                percentage=calculate_percentage(bucket["doc_count"], region_total if region_total > 0 else total_docs)
            )
            for bucket in region_buckets
        ]

        # 직업 분포 처리
        occupation_buckets = aggs["occupation_dist"]["buckets"]
        occupation_total = sum(bucket["doc_count"] for bucket in occupation_buckets)
        occupation_distribution = [
            ChartDataPoint(
                label=bucket["key"],
                value=bucket["doc_count"],
                percentage=calculate_percentage(bucket["doc_count"], occupation_total if occupation_total > 0 else total_docs)
            )
            for bucket in occupation_buckets
        ]

        # 흡연 분포 처리
        smoker_buckets = aggs["qa_nested"]["smoker_filter"]["smoker_answers"]["buckets"]
        smoker_total = sum(bucket["doc_count"] for bucket in smoker_buckets)
        
        smoker_count = 0
        non_smoker_count = 0
        
        for bucket in smoker_buckets:
            answer = bucket["key"]
            count = bucket["doc_count"]
            
            if any(keyword in answer for keyword in ["일반 담배", "전자담배", "그냥 담배", "연초"]):
                smoker_count += count
            elif any(keyword in answer for keyword in ["피우지 않", "안 피운", "비흡연"]):
                non_smoker_count += count
            else:
                non_smoker_count += count
        
        smoker_distribution = [
            ChartDataPoint(
                label="흡연",
                value=smoker_count,
                percentage=calculate_percentage(smoker_count, smoker_total) if smoker_total > 0 else 0.0
            ),
            ChartDataPoint(
                label="비흡연",
                value=non_smoker_count,
                percentage=calculate_percentage(non_smoker_count, smoker_total) if smoker_total > 0 else 0.0
            )
        ]

        # 차량 보유 분포 처리
        vehicle_buckets = aggs["qa_nested"]["vehicle_filter"]["vehicle_answers"]["buckets"]
        vehicle_total = sum(bucket["doc_count"] for bucket in vehicle_buckets)
        
        vehicle_map = {
            "있다": "보유",
            "없다": "미보유",
            "": "미보유"
        }
        
        vehicle_distribution = [
            ChartDataPoint(
                label=vehicle_map.get(bucket["key"], bucket["key"]),
                value=bucket["doc_count"],
                percentage=calculate_percentage(bucket["doc_count"], vehicle_total) if vehicle_total > 0 else 0.0
            )
            for bucket in vehicle_buckets
        ]

        # 음주 분포 처리
        drinker_buckets = aggs["qa_nested"]["drinker_filter"]["drinker_answers"]["buckets"]
        drinker_total = sum(bucket["doc_count"] for bucket in drinker_buckets)

        drinker_count = 0
        non_drinker_count = 0

        # ⭐ 실제 답변: "맥주", "소주", "와인", "최근 1년 이내 술을 마시지 않음" 등
        NON_DRINKER_KEYWORDS = [
            "최근 1년 이내 술을 마시지 않음",
            "마시지 않음",
            "술을 마시지 않음",
            "안 마셔",
            "안마셔",
            "비음주",
            "금주",
            "음주 경험 없음"
        ]

        for bucket in drinker_buckets:
            answer = bucket["key"]
            count = bucket["doc_count"]

            # 비음주 키워드가 있으면 비음주자
            if any(keyword in answer for keyword in NON_DRINKER_KEYWORDS):
                non_drinker_count += count
            else:
                # 나머지는 모두 음주자 (맥주, 소주, 와인, 양주 등)
                drinker_count += count
        
        drinker_distribution = [
            ChartDataPoint(
                label="음주",
                value=drinker_count,
                percentage=calculate_percentage(drinker_count, drinker_total) if drinker_total > 0 else 0.0
            ),
            ChartDataPoint(
                label="비음주",
                value=non_drinker_count,
                percentage=calculate_percentage(non_drinker_count, drinker_total) if drinker_total > 0 else 0.0
            )
        ]

        # 결혼여부 분포 처리
        marital_buckets = aggs["qa_nested"]["marital_filter"]["marital_answers"]["buckets"]
        marital_total = sum(bucket["doc_count"] for bucket in marital_buckets)
        
        marital_status_distribution = [
            ChartDataPoint(
                label=bucket["key"],
                value=bucket["doc_count"],
                percentage=calculate_percentage(bucket["doc_count"], marital_total) if marital_total > 0 else 0.0
            )
            for bucket in marital_buckets
        ]

        # 가족수 분포 처리
        family_buckets = aggs["qa_nested"]["family_filter"]["family_answers"]["buckets"]

        # ⚠️ 소득 관련 답변 필터링 (만약 섞여있다면)
        # "5명 이상"은 유지하고, "월 XXX만원 이상"만 제거
        family_buckets_filtered = [
            bucket for bucket in family_buckets
            if not any(keyword in bucket["key"] for keyword in ["월 ", "만원", "~"])
        ]

        family_total = sum(bucket["doc_count"] for bucket in family_buckets_filtered)

        family_size_distribution = [
            ChartDataPoint(
                label=bucket["key"],
                value=bucket["doc_count"],
                percentage=calculate_percentage(bucket["doc_count"], family_total) if family_total > 0 else 0.0
            )
            for bucket in family_buckets_filtered
        ]

        # 월평균 개인소득 분포 처리
        income_buckets = aggs["qa_nested"]["income_filter"]["income_answers"]["buckets"]
        income_total = sum(bucket["doc_count"] for bucket in income_buckets)
        
        income_distribution = [
            ChartDataPoint(
                label=bucket["key"],
                value=bucket["doc_count"],
                percentage=calculate_percentage(bucket["doc_count"], income_total) if income_total > 0 else 0.0
            )
            for bucket in income_buckets
        ]

        return UserInfoResponse(
            index_name=index_name,
            total_docs=total_docs,
            gender_distribution=gender_distribution,
            age_distribution=age_distribution,
            region_distribution=region_distribution,
            marital_status_distribution=marital_status_distribution,
            family_size_distribution=family_size_distribution,
            occupation_distribution=occupation_distribution,
            income_distribution=income_distribution,
            vehicle_distribution=vehicle_distribution,
            smoker_distribution=smoker_distribution,
            drinker_distribution=drinker_distribution
        )

    except Exception as e:
        logger.error(f"Error getting user info statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))



