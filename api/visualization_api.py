"""시각화 API 라우터"""
import logging
import json
import base64
import uuid
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from opensearchpy import OpenSearch
import requests
from requests.auth import HTTPBasicAuth
from rag_query_analyzer.config import get_config

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


# 필드 키워드 → OpenSearch 필드 경로 매핑
FIELD_MAPPING = {
    "연령": "metadata.age_group",
    "나이": "metadata.age_group",
    "age": "metadata.age_group",
    "age_group": "metadata.age_group",
    "성별": "metadata.gender",
    "gender": "metadata.gender",
    "직업": "metadata.occupation",
    "occupation": "metadata.occupation",
    "지역": "metadata.region",
    "region": "metadata.region",
    "결혼": "qa_pairs.answer.keyword",
    "결혼여부": "qa_pairs.answer.keyword",
    "marital": "qa_pairs.answer.keyword",
    "가족수": "qa_pairs.answer.keyword",
    "family": "qa_pairs.answer.keyword",
    "소득": "qa_pairs.answer.keyword",
    "income": "qa_pairs.answer.keyword",
    "흡연": "qa_pairs.answer.keyword",
    "smoker": "qa_pairs.answer.keyword",
    "음주": "qa_pairs.answer.keyword",
    "drinker": "qa_pairs.answer.keyword",
    "차량": "qa_pairs.answer.keyword",
    "vehicle": "qa_pairs.answer.keyword",
}

# qa_pairs 필드의 질문 텍스트 매핑
QA_QUESTION_MAPPING = {
    "흡연": "흡연경험",
    "smoker": "흡연경험",
    "음주": "음용경험 술",
    "drinker": "음용경험 술",
    "차량": "보유차량여부",
    "vehicle": "보유차량여부",
    "결혼": "결혼여부",
    "marital": "결혼여부",
    "가족수": "가족수",
    "family": "가족수",
    "소득": "월평균 개인소득",
    "income": "월평균 개인소득",
}


def _ensure_index_pattern(
    base_url: str,
    index_name: str,
    auth: HTTPBasicAuth,
    verify_certs: bool,
    os_client: OpenSearch
) -> Optional[str]:
    """
    인덱스 패턴이 존재하는지 확인하고, 없으면 생성
    
    Args:
        base_url: Dashboards base URL
        index_name: 인덱스 이름
        auth: 인증 정보
        verify_certs: SSL 인증서 검증 여부
        os_client: OpenSearch 클라이언트 (인덱스 존재 확인용)
        
    Returns:
        인덱스 패턴 ID 또는 None
    """
    try:
        # 먼저 OpenSearch에 인덱스가 실제로 존재하는지 확인
        try:
            if not os_client.indices.exists(index=index_name):
                logger.warning(f"⚠️ OpenSearch에 인덱스가 존재하지 않음: {index_name}")
                return None
        except Exception as e:
            logger.warning(f"⚠️ OpenSearch 인덱스 확인 실패: {e}")
            # 인덱스 확인 실패해도 계속 진행 (인덱스가 존재할 수도 있음)
        
        # 인덱스 패턴 ID는 보통 인덱스 이름과 동일
        pattern_id = index_name
        
        # 인덱스 패턴 존재 확인
        check_url = f"{base_url}/api/saved_objects/index-pattern/{pattern_id}"
        headers = {"kbn-xsrf": "true"}
        
        response = requests.get(
            check_url,
            auth=auth,
            headers=headers,
            verify=verify_certs,
            timeout=10
        )
        
        if response.status_code == 200:
            logger.debug(f"✅ 인덱스 패턴 이미 존재: {pattern_id}")
            # 인덱스 패턴이 존재하더라도 실제 인덱스와 연결되어 있는지 확인
            try:
                pattern_data = response.json()
                # 패턴이 올바르게 설정되어 있는지 확인
                if pattern_data.get("attributes", {}).get("title") == index_name:
                    return pattern_id
            except:
                pass
        
        # 인덱스 패턴이 없으면 생성
        logger.info(f"📝 인덱스 패턴 생성 시도: {pattern_id} (인덱스: {index_name})")
        create_url = f"{base_url}/api/saved_objects/index-pattern/{pattern_id}"
        
        # 인덱스 패턴 속성 생성
        pattern_object = {
            "attributes": {
                "title": index_name,
                "timeFieldName": None
            }
        }
        
        create_response = requests.post(
            create_url,
            json=pattern_object,
            auth=auth,
            headers={**headers, "Content-Type": "application/json"},
            verify=verify_certs,
            timeout=10
        )
        
        if create_response.status_code in [200, 201]:
            logger.info(f"✅ 인덱스 패턴 생성 성공: {pattern_id}")
            # 생성 후 다시 확인하여 반환
            verify_response = requests.get(
                check_url,
                auth=auth,
                headers=headers,
                verify=verify_certs,
                timeout=10
            )
            if verify_response.status_code == 200:
                return pattern_id
            else:
                logger.warning(f"⚠️ 인덱스 패턴 생성 후 확인 실패")
                return None
        else:
            error_text = create_response.text
            logger.warning(f"⚠️ 인덱스 패턴 생성 실패: {create_response.status_code} - {error_text}")
            
            # 이미 존재하는 경우 (409 Conflict)
            if create_response.status_code == 409:
                logger.info(f"ℹ️ 인덱스 패턴이 이미 존재함 (409), 기존 패턴 사용: {pattern_id}")
                return pattern_id
            
            return None
            
    except requests.exceptions.RequestException as e:
        logger.warning(f"⚠️ 인덱스 패턴 API 요청 실패: {e}")
        return None
    except Exception as e:
        logger.warning(f"⚠️ 인덱스 패턴 확인/생성 중 오류: {e}")
        return None


def _create_dashboards_visualization(
    index_name: str,
    field_path: str,
    field_keyword: str,
    search_body: Dict[str, Any],
    os_client: OpenSearch
) -> Optional[str]:
    """
    OpenSearch Dashboards Saved Objects API를 사용해서 시각화를 동적으로 생성하고 URL 반환
    
    Args:
        index_name: 인덱스 이름
        field_path: 필드 경로
        field_keyword: 필드 키워드
        search_body: OpenSearch 쿼리 body
        
    Returns:
        Dashboards 시각화 URL 또는 None
    """
    try:
        config = get_config()
        
        # Dashboards 호스트가 설정되지 않았으면 None 반환
        if not config.OPENSEARCH_DASHBOARDS_HOST:
            logger.debug("Dashboards 호스트가 설정되지 않아 시각화를 생성하지 않습니다")
            return None
        
        # 프로토콜 결정
        protocol = "https" if config.OPENSEARCH_DASHBOARDS_USE_SSL else "http"
        base_url = f"{protocol}://{config.OPENSEARCH_DASHBOARDS_HOST}:{config.OPENSEARCH_DASHBOARDS_PORT}"
        
        # 인증 정보
        auth = HTTPBasicAuth(config.OPENSEARCH_USERNAME, config.OPENSEARCH_PASSWORD)
        
        # 인덱스 패턴 확인 및 생성
        pattern_id = _ensure_index_pattern(
            base_url,
            index_name,
            auth,
            config.OPENSEARCH_VERIFY_CERTS,
            os_client
        )
        
        if not pattern_id:
            logger.warning(f"⚠️ 인덱스 패턴을 사용할 수 없어 기본 URL만 반환합니다")
            return f"{base_url}/app/visualize#/create?type=histogram&indexPattern={index_name}"
        
        # 시각화 ID 생성 (고유한 ID)
        viz_id = f"auto-{field_keyword.lower().replace(' ', '-')}-{uuid.uuid4().hex[:8]}"
        
        # OpenSearch aggregation 구조에서 필드 경로 추출
        aggs = search_body.get("aggs", {})
        chart_data_agg = aggs.get("chart_data", {})
        
        # terms aggregation인 경우
        if "terms" in chart_data_agg:
            agg_field = chart_data_agg["terms"].get("field", field_path)
        elif "nested" in chart_data_agg:
            # nested aggregation인 경우
            nested_aggs = chart_data_agg.get("aggs", {})
            filtered_agg = nested_aggs.get("filtered", {})
            values_agg = filtered_agg.get("aggs", {}).get("values", {})
            agg_field = values_agg.get("terms", {}).get("field", field_path)
        else:
            agg_field = field_path
        
        # 시각화 객체 생성 (Vertical Bar Chart)
        visualization_object = {
            "type": "visualization",
            "id": viz_id,
            "attributes": {
                "title": f"{field_keyword} Distribution",
                "visState": json.dumps({
                    "title": f"{field_keyword} Distribution",
                    "type": "histogram",
                    "params": {
                        "grid": {"categoryLines": False, "style": {"color": "#eee"}},
                        "categoryAxes": [{
                            "id": "CategoryAxis-1",
                            "type": "category",
                            "position": "bottom",
                            "show": True,
                            "style": {},
                            "scale": {"type": "linear"},
                            "labels": {"show": True, "truncate": 100},
                            "title": {}
                        }],
                        "valueAxes": [{
                            "id": "ValueAxis-1",
                            "name": "LeftAxis-1",
                            "type": "value",
                            "position": "left",
                            "show": True,
                            "style": {},
                            "scale": {"type": "linear", "mode": "normal"},
                            "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                            "title": {"text": "Count"}
                        }],
                        "seriesParams": [{
                            "show": True,
                            "type": "histogram",
                            "mode": "stacked",
                            "data": {"label": "Count", "id": "1"},
                            "valueAxis": "ValueAxis-1",
                            "drawLinesBetweenPoints": True,
                            "showCircles": True
                        }],
                        "addTooltip": True,
                        "addLegend": True,
                        "legendPosition": "right",
                        "times": [],
                        "addTimeMarker": False
                    },
                    "aggs": [{
                        "id": "1",
                        "enabled": True,
                        "type": "count",
                        "schema": "metric",
                        "params": {}
                    }, {
                        "id": "2",
                        "enabled": True,
                        "type": "terms",
                        "schema": "segment",
                        "params": {
                            "field": agg_field,
                            "size": search_body.get("aggs", {}).get("chart_data", {}).get("terms", {}).get("size", 20),
                            "order": "desc",
                            "orderBy": "1"
                        }
                    }]
                }),
                "uiStateJSON": "{}",
                "description": f"Auto-generated visualization for {field_keyword} distribution",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "index": pattern_id,  # 인덱스 패턴 ID 사용
                        "query": search_body.get("query", {"match_all": {}}),
                        "filter": []
                    })
                }
            }
        }
        
        # Saved Objects API 엔드포인트
        api_url = f"{base_url}/api/saved_objects/visualization/{viz_id}"
        
        # 헤더
        headers = {
            "Content-Type": "application/json",
            "kbn-xsrf": "true"
        }
        
        # 시각화 생성 요청
        logger.info(f"📊 Dashboards 시각화 생성 시도: {viz_id}")
        response = requests.post(
            api_url,
            json=visualization_object,
            auth=auth,
            headers=headers,
            verify=config.OPENSEARCH_VERIFY_CERTS,
            timeout=10
        )
        
        if response.status_code in [200, 201]:
            # 시각화 URL 생성
            visualize_url = f"{base_url}/app/visualize#/edit/{viz_id}"
            logger.info(f"✅ Dashboards 시각화 생성 성공: {visualize_url}")
            return visualize_url
        else:
            logger.warning(f"⚠️ Dashboards 시각화 생성 실패: {response.status_code} - {response.text}")
            # 실패 시 기본 Visualize Editor URL 반환
            return f"{base_url}/app/visualize#/create?type=histogram&indexPattern={index_name}"
        
    except requests.exceptions.RequestException as e:
        logger.warning(f"⚠️ Dashboards API 요청 실패: {e}")
        # 실패 시 기본 Visualize Editor URL 반환
        try:
            config = get_config()
            if config.OPENSEARCH_DASHBOARDS_HOST:
                protocol = "https" if config.OPENSEARCH_DASHBOARDS_USE_SSL else "http"
                base_url = f"{protocol}://{config.OPENSEARCH_DASHBOARDS_HOST}:{config.OPENSEARCH_DASHBOARDS_PORT}"
                return f"{base_url}/app/visualize#/create?type=histogram&indexPattern={index_name}"
        except:
            pass
        return None
    except Exception as e:
        logger.warning(f"⚠️ Dashboards 시각화 생성 중 오류: {e}")
        return None


def _get_field_path(field_keyword: str) -> tuple[str, bool, Optional[str]]:
    """
    필드 키워드를 OpenSearch 필드 경로로 변환
    
    Returns:
        (field_path, is_nested, q_text_keyword): 필드 경로, nested aggregation 필요 여부, 질문 텍스트 키워드
    """
    field_lower = field_keyword.lower()
    
    # 직접 매핑 확인
    if field_keyword in FIELD_MAPPING:
        field_path = FIELD_MAPPING[field_keyword]
        is_nested = field_path.startswith("qa_pairs")
        q_text_keyword = QA_QUESTION_MAPPING.get(field_keyword) if is_nested else None
        return field_path, is_nested, q_text_keyword
    
    # 소문자 매핑 확인
    if field_lower in FIELD_MAPPING:
        field_path = FIELD_MAPPING[field_lower]
        is_nested = field_path.startswith("qa_pairs")
        q_text_keyword = QA_QUESTION_MAPPING.get(field_lower) if is_nested else None
        return field_path, is_nested, q_text_keyword
    
    # 기본값: metadata 필드로 가정
    return f"metadata.{field_keyword}", False, None


class AggregationRequest(BaseModel):
    """시각화 요청 (키워드 기반)"""
    index_name: str = Field(default="survey_responses_merged", description="인덱스 이름")
    field: str = Field(..., description="시각화할 필드 키워드 (예: '연령', '직업', '성별', '지역', '흡연', '음주', '차량')")
    filter_query: Optional[Dict[str, Any]] = Field(default=None, description="필터링 쿼리 (선택사항)")
    size: int = Field(default=20, ge=1, le=100, description="반환할 버킷 수")
    order: str = Field(default="desc", description="정렬 순서 (desc: 내림차순, asc: 오름차순)")


class AggregationResponse(BaseModel):
    """시각화 응답 (OpenSearch aggregation 형식)"""
    field: str
    field_path: str
    index_name: str
    total_docs: int
    aggregations: Dict[str, Any] = Field(..., description="OpenSearch aggregation 결과 (그래프 데이터)")
    dashboards_url: Optional[str] = Field(None, description="OpenSearch Dashboards 시각화 URL (그래프 바로 보기)")
    took_ms: int


@router.post("/aggregation", response_model=AggregationResponse, summary="키워드 기반 시각화 데이터 조회")
async def execute_aggregation(
    request: AggregationRequest,
    os_client: OpenSearch = Depends(get_os_client)
):
    """
    키워드를 입력하면 자동으로 aggregation 쿼리를 생성하여 시각화 데이터를 반환합니다.
    
    지원하는 필드:
    - 연령/나이/age: metadata.age_group
    - 성별/gender: metadata.gender
    - 직업/occupation: metadata.occupation
    - 지역/region: metadata.region
    - 흡연/smoker: qa_pairs (흡연경험 질문)
    - 음주/drinker: qa_pairs (음용경험 술 질문)
    - 차량/vehicle: qa_pairs (보유차량여부 질문)
    - 결혼/marital: qa_pairs (결혼여부 질문)
    - 가족수/family: qa_pairs (가족수 질문)
    - 소득/income: qa_pairs (월평균 개인소득 질문)
    
    예시:
    ```json
    {
        "field": "연령",
        "size": 20,
        "order": "desc"
    }
    ```
    """
    try:
        field_path, is_nested, q_text_keyword = _get_field_path(request.field)
        
        if not field_path:
            raise HTTPException(
                status_code=400,
                detail=f"지원하지 않는 필드 키워드입니다: {request.field}"
            )
        
        # 쿼리 구성
        search_body = {
            "size": 0,
            "query": request.filter_query if request.filter_query else {"match_all": {}}
        }
        
        # Aggregation 구성
        if is_nested and q_text_keyword:
            # qa_pairs nested aggregation
            search_body["aggs"] = {
                "chart_data": {
                    "nested": {
                        "path": "qa_pairs"
                    },
                    "aggs": {
                        "filtered": {
                            "filter": {
                                "term": {"qa_pairs.q_text.keyword": q_text_keyword}
                            },
                            "aggs": {
                                "values": {
                                    "terms": {
                                        "field": field_path,
                                        "size": request.size,
                                        "order": {"_count": request.order}
                                    }
                                }
                            }
                        }
                    }
                }
            }
            agg_path = ["chart_data", "filtered", "values"]
        else:
            # metadata 직접 aggregation
            search_body["aggs"] = {
                "chart_data": {
                    "terms": {
                        "field": field_path,
                        "size": request.size,
                        "order": {"_count": request.order}
                    }
                }
            }
            agg_path = ["chart_data"]
        
        logger.info(f"📊 시각화 쿼리 실행: field='{request.field}' → {field_path}")
        
        # OpenSearch 쿼리 실행
        response = os_client.search(index=request.index_name, body=search_body)
        
        total_docs = response["hits"]["total"]["value"]
        aggregations = response.get("aggregations", {})
        took_ms = response.get("took", 0)
        
        logger.info(f"✅ 시각화 완료: total_docs={total_docs}, took={took_ms}ms")
        
        # Dashboards 시각화 생성 및 URL 생성
        dashboards_url = _create_dashboards_visualization(
            request.index_name,
            field_path,
            request.field,
            search_body,
            os_client
        )
        
        return AggregationResponse(
            field=request.field,
            field_path=field_path,
            index_name=request.index_name,
            total_docs=total_docs,
            aggregations=aggregations,
            dashboards_url=dashboards_url,
            took_ms=took_ms
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 시각화 실행 중 오류: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"시각화 실행 실패: {str(e)}"
        )


