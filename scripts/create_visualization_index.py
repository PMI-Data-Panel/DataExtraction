"""
OpenSearch 스크롤 API를 사용하여 질문/답변 경향 분석용 인덱스 생성

기존 survey_responses_merged 인덱스에서 데이터를 읽어서
시각화에 최적화된 새로운 인덱스를 생성합니다.
"""

from __future__ import annotations

import argparse
import json
import sys
import re
import os
from collections import defaultdict
from typing import Dict, List, Any, Optional
from datetime import datetime

# Windows 콘솔 인코딩 설정
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk


DEFAULT_HOST = "159.223.47.188"
DEFAULT_PORT = 9200
DEFAULT_USER = "admin"
DEFAULT_PASSWORD = "AVNS_1unywEqMDAepzpSW6vU"
SOURCE_INDEX = "survey_responses_merged"
TARGET_INDEX = "survey_qa_analysis"


def build_client(host: str, port: int, username: str, password: str) -> OpenSearch:
    """OpenSearch 클라이언트 생성"""
    return OpenSearch(
        hosts=[{"host": host, "port": port}],
        http_auth=(username, password),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
        timeout=60,
        max_retries=3,
        retry_on_timeout=True,
    )


def normalize_answer(answer: Any) -> Any:
    """답변을 정규화 (리스트는 그대로 유지, 단일 값은 문자열)"""
    if isinstance(answer, list):
        # 빈 값 제거
        filtered = [str(item).strip() for item in answer if item and str(item).strip()]
        return filtered if filtered else None
    if answer is None:
        return None
    result = str(answer).strip()
    return result if result else None


def text_to_field_name(text: str) -> str:
    """질문 텍스트를 영문 필드명으로 변환"""
    # 기본 매핑 (일반적인 질문들)
    mapping = {
        # 인구통계 질문
        "귀하의 성별은": "gender",
        "귀하의 출생년도는 어떻게 되십니까?": "birth_year",
        "회원님께서 현재 살고 계신 지역은 어디인가요?": "region",
        # 지역별 세부 지역 질문 (주요 지역들)
        "그렇다면, 서울의 어느 구에 살고 계신가요?": "sub_region",
        "그렇다면, 부산의 어느 구에 살고 계신가요?": "sub_region",
        "그렇다면, 경기의 어느 구에 살고 계신가요?": "sub_region",
        "그렇다면, 인천의 어느 구에 살고 계신가요?": "sub_region",
        "그렇다면, 대구의 어느 구에 살고 계신가요?": "sub_region",
        "그렇다면, 대전의 어느 구에 살고 계신가요?": "sub_region",
        "그렇다면, 광주의 어느 구에 살고 계신가요?": "sub_region",
        "그렇다면, 울산의 어느 구에 살고 계신가요?": "sub_region",
        "그렇다면, 강원의 어느 구에 살고 계신가요?": "sub_region",
        "그렇다면, 충북의 어느 구에 살고 계신가요?": "sub_region",
        "그렇다면, 충남의 어느 구에 살고 계신가요?": "sub_region",
        "그렇다면, 전북의 어느 구에 살고 계신가요?": "sub_region",
        "그렇다면, 전남의 어느 구에 살고 계신가요?": "sub_region",
        "그렇다면, 경북의 어느 구에 살고 계신가요?": "sub_region",
        "그렇다면, 경남의 어느 구에 살고 계신가요?": "sub_region",
        "그렇다면, 제주의 어느 구에 살고 계신가요?": "sub_region",
        "그렇다면, 기타 / 해외의 어느 구에 살고 계신가요?": "sub_region",
        
        # 가족/인적사항
        "결혼여부": "marriage",
        "자녀수": "children_count",
        "가족수": "family_count",
        
        # 교육/직업
        "최종학력": "education",
        "직업": "job",
        "직무": "job_role",
        
        # 소득
        "월평균 개인소득": "personal_income",
        "월평균 가구소득": "household_income",
        
        # 가전제품
        "보유가전제품": "appliances",
        
        # 휴대폰
        "보유 휴대폰 단말기 브랜드": "phone_brand",
        "보유 휴대폰 모델명": "phone_model",
        
        # 자동차
        "보유차량여부": "car_owned",
        "자동차 제조사": "car_brand",
        "자동차 모델": "car_model",
        
        # 흡연
        "흡연경험": "smoke_type",
        "흡연경험 담배브랜드": "smoke_brand",
        
        # 음주
        "음용경험 술": "drink_type",
    }
    
    # 매핑에 있으면 사용
    if text in mapping:
        return mapping[text]
    
    # 자동 변환: 한글을 영문으로 변환
    # 간단한 키워드 기반 변환
    text_lower = text.lower()
    
    # 인구통계 질문
    if "성별" in text:
        return "gender"
    elif "출생년도" in text or "출생" in text:
        return "birth_year"
    elif "지역" in text and "구" not in text:
        return "region"
    elif "구에 살고" in text or "어느 구" in text or ("그렇다면" in text and "어느" in text and "살고" in text):
        # 모든 지역의 세부 지역은 sub_region으로 통일
        return "sub_region"
    
    # 가족/인적사항
    elif "결혼" in text:
        return "marriage"
    elif "자녀" in text:
        return "children_count"
    elif "가족" in text:
        return "family_count"
    
    # 교육/직업
    elif "학력" in text or "학원" in text:
        return "education"
    elif "직업" in text and "직무" not in text:
        return "job"
    elif "직무" in text:
        return "job_role"
    
    # 소득
    elif "소득" in text:
        if "개인" in text:
            return "personal_income"
        elif "가구" in text:
            return "household_income"
        return "income"
    
    # 가전제품
    elif "가전" in text or ("제품" in text and "휴대폰" not in text and "자동차" not in text):
        return "appliances"
    
    # 휴대폰
    elif "휴대폰" in text or "스마트폰" in text:
        if "브랜드" in text or "제조사" in text:
            return "phone_brand"
        elif "모델" in text:
            return "phone_model"
        return "phone"
    
    # 자동차
    elif "차량" in text or "자동차" in text:
        if "여부" in text:
            return "car_owned"
        elif "제조사" in text or "브랜드" in text:
            return "car_brand"
        elif "모델" in text:
            return "car_model"
        return "car"
    
    # 흡연
    elif "흡연" in text or "담배" in text:
        if "브랜드" in text:
            return "smoke_brand"
        return "smoke_type"
    
    # 음주
    elif "음용" in text or ("술" in text and "주류" not in text):
        return "drink_type"
    
    # 기본 변환: 한글 제거하고 영문 키워드 추출 또는 해시 사용
    # 질문 텍스트의 해시를 사용하여 고유한 필드명 생성
    field_hash = str(abs(hash(text)))[:8]
    return f"q_{field_hash}"


def create_analysis_index(client: OpenSearch, index_name: str, force_recreate: bool = False):
    """시각화 분석용 인덱스 생성
    
    ⚠️ 주의: 기존 인덱스는 절대 삭제하지 않습니다.
    force_recreate=True일 때만 대상 인덱스(target_index)를 삭제합니다.
    원본 인덱스(source_index)는 절대 건드리지 않습니다.
    """
    
    if client.indices.exists(index=index_name):
        if force_recreate:
            print(f"\n{'='*60}")
            print(f"⚠️  경고: 기존 인덱스 '{index_name}'를 삭제하려고 합니다!")
            print(f"{'='*60}")
            print(f"이 작업은 되돌릴 수 없습니다.")
            print(f"계속하려면 10초 이내에 Ctrl+C를 눌러 취소하세요...")
            import time
            for i in range(10, 0, -1):
                print(f"  삭제까지 {i}초...", end='\r')
                time.sleep(1)
            print(f"\n{'='*60}")
            print(f"⚠️ 기존 인덱스 '{index_name}' 삭제 중...")
            client.indices.delete(index=index_name)
            print(f"✅ 인덱스 '{index_name}' 삭제 완료")
        else:
            print(f"ℹ️ 인덱스 '{index_name}'가 이미 존재합니다.")
            print(f"   기존 인덱스를 유지하고 데이터를 추가합니다.")
            print(f"   인덱스를 재생성하려면 --force-recreate 플래그를 사용하세요.")
            return
    
    print(f"✨ 인덱스 '{index_name}' 생성 중...")
    
    # 인덱스 매핑 정의 (동적 매핑 허용)
    mappings = {
        "properties": {
            # 기본 필드
            "user_id": {
                "type": "keyword"
            },
            "timestamp": {
                "type": "date"
            },
            
            # 메타데이터 필드
            "meta_gender": {
                "type": "keyword"
            },
            "meta_age": {
                "type": "integer"
            },
            "meta_age_group": {
                "type": "keyword"
            },
            "meta_region": {
                "type": "keyword"
            },
            "meta_sub_region": {
                "type": "keyword"
            },
            "meta_panel": {
                "type": "keyword"
            },
            "meta_birth_year": {
                "type": "keyword"
            },
            "meta_survey_datetime": {
                "type": "date"
            }
        },
        # 동적 매핑: q_* 필드들은 자동으로 매핑됨
        "dynamic": True,
        "dynamic_templates": [
            {
                # 문자열 필드 (단일 답변)
                "question_string_fields": {
                    "match": "q_*",
                    "match_mapping_type": "string",
                    "mapping": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword"
                            }
                        }
                    }
                }
            },
            {
                # 배열 필드 (다중 선택 답변)
                "question_array_fields": {
                    "match": "q_*",
                    "match_mapping_type": "object",
                    "mapping": {
                        "type": "keyword"
                    }
                }
            }
        ]
    }
    
    # 인덱스 설정
    settings = {
        "number_of_shards": 3,
        "number_of_replicas": 1,
        "refresh_interval": "30s",
        "analysis": {
            "analyzer": {
                "standard": {
                    "type": "standard"
                }
            }
        }
    }
    
    body = {
        "settings": settings,
        "mappings": mappings
    }
    
    try:
        client.indices.create(index=index_name, body=body)
        print(f"✅ 인덱스 '{index_name}' 생성 완료")
    except Exception as e:
        print(f"❌ 인덱스 생성 실패: {e}")
        raise


def scroll_all_documents(client: OpenSearch, index_name: str, batch_size: int = 1000):
    """스크롤 API를 사용하여 모든 문서 읽기"""
    print(f"📖 인덱스 '{index_name}'에서 데이터 읽기 시작...")
    
    scroll_time = "5m"
    total_docs = 0
    
    try:
        # 초기 검색 (match_all)
        response = client.search(
            index=index_name,
            body={"query": {"match_all": {}}},
            scroll=scroll_time,
            size=batch_size
        )
        
        scroll_id = response.get('_scroll_id')
        hits = response['hits']['hits']
        total_hits = response['hits']['total']['value']
        
        print(f"📊 총 문서 수: {total_hits:,}건")
        
        while hits:
            for hit in hits:
                yield hit['_source']
                total_docs += 1
            
            if total_docs % 10000 == 0:
                print(f"  진행: {total_docs:,} / {total_hits:,} ({total_docs*100//total_hits}%)")
            
            # 다음 배치
            response = client.scroll(
                scroll_id=scroll_id,
                scroll=scroll_time
            )
            scroll_id = response.get('_scroll_id')
            hits = response['hits']['hits']
        
        # 스크롤 정리
        if scroll_id:
            client.clear_scroll(scroll_id=scroll_id)
        
        print(f"✅ 데이터 읽기 완료: {total_docs:,}건")
        
    except Exception as e:
        print(f"❌ 스크롤 읽기 실패: {e}")
        raise


def transform_data(documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """원본 데이터를 시각화용 구조로 변환 (사용자별 문서)"""
    print("🔄 데이터 변환 중...")
    
    transformed_docs = []
    processed_users = 0
    
    for doc in documents:
        user_id = doc.get("user_id", "")
        timestamp = doc.get("timestamp", "")
        metadata = doc.get("metadata", {})
        qa_pairs = doc.get("qa_pairs", [])
        
        if not user_id:
            continue
        
        # 새 문서 생성
        new_doc = {
            "user_id": user_id,
            "timestamp": timestamp,
        }
        
        # [1] 메타데이터를 flat하게 변환
        if metadata:
            new_doc["meta_gender"] = metadata.get("gender", "")
            new_doc["meta_age"] = metadata.get("age")
            new_doc["meta_age_group"] = metadata.get("age_group", "")
            new_doc["meta_region"] = metadata.get("region", "")
            new_doc["meta_sub_region"] = metadata.get("sub_region", "")
            new_doc["meta_panel"] = metadata.get("panel", "")
            new_doc["meta_birth_year"] = metadata.get("birth_year", "")
            if metadata.get("survey_datetime"):
                new_doc["meta_survey_datetime"] = metadata.get("survey_datetime")
        
        # [2] 질문-답변 쌍을 영문 키로 필드로 승격
        for qa in qa_pairs:
            q_text = qa.get("q_text", "").strip()
            if not q_text:
                continue
            
            answer = qa.get("answer", "")
            answer_normalized = normalize_answer(answer)
            
            # 답변이 없으면 건너뛰기
            if answer_normalized is None:
                continue
            
            # 질문 텍스트를 영문 필드명으로 변환
            field_name = text_to_field_name(q_text)
            field_key = f"q_{field_name}"
            
            # 다중 선택인 경우 배열로, 단일 선택인 경우 문자열로 저장
            if isinstance(answer_normalized, list):
                new_doc[field_key] = answer_normalized
            else:
                new_doc[field_key] = answer_normalized
        
        transformed_docs.append(new_doc)
        processed_users += 1
        
        if processed_users % 10000 == 0:
            print(f"  변환 진행: {processed_users:,}명")
    
    print(f"✅ 변환 완료: {processed_users:,}개 문서 생성")
    return transformed_docs


def bulk_index_documents(client: OpenSearch, index_name: str, documents: List[Dict[str, Any]], batch_size: int = 1000):
    """Bulk API를 사용하여 문서 인덱싱"""
    print(f"📝 인덱스 '{index_name}'에 문서 저장 중...")
    
    actions = []
    total_indexed = 0
    
    for i, doc in enumerate(documents):
        # 문서 ID 생성 (user_id 사용)
        doc_id = doc.get('user_id', f"doc_{i}")
        
        action = {
            "_index": index_name,
            "_id": doc_id,
            "_source": doc
        }
        actions.append(action)
        
        # 배치 단위로 인덱싱
        if len(actions) >= batch_size:
            try:
                success, failed = bulk(client, actions, raise_on_error=False)
                total_indexed += success
                if failed:
                    print(f"  ⚠️ {len(failed)}개 문서 인덱싱 실패")
                actions = []
                
                if total_indexed % 10000 == 0:
                    print(f"  진행: {total_indexed:,} / {len(documents):,} ({total_indexed*100//len(documents)}%)")
            except Exception as e:
                print(f"  ❌ 배치 인덱싱 실패: {e}")
                actions = []
    
    # 남은 문서 인덱싱
    if actions:
        try:
            success, failed = bulk(client, actions, raise_on_error=False)
            total_indexed += success
            if failed:
                print(f"  ⚠️ {len(failed)}개 문서 인덱싱 실패")
        except Exception as e:
            print(f"  ❌ 마지막 배치 인덱싱 실패: {e}")
    
    print(f"✅ 인덱싱 완료: {total_indexed:,} / {len(documents):,}건")
    
    # 인덱스 새로고침
    client.indices.refresh(index=index_name)
    print("✅ 인덱스 새로고침 완료")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="OpenSearch 스크롤 API를 사용하여 시각화용 인덱스 생성"
    )
    parser.add_argument("--host", default=DEFAULT_HOST, help="OpenSearch host")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="OpenSearch port")
    parser.add_argument("--user", default=DEFAULT_USER, help="OpenSearch username")
    parser.add_argument("--password", default=DEFAULT_PASSWORD, help="OpenSearch password")
    parser.add_argument(
        "--source-index",
        default=SOURCE_INDEX,
        help=f"원본 인덱스 이름 (default: {SOURCE_INDEX})"
    )
    parser.add_argument(
        "--target-index",
        default=TARGET_INDEX,
        help=f"대상 인덱스 이름 (default: {TARGET_INDEX})"
    )
    parser.add_argument(
        "--force-recreate",
        action="store_true",
        help="⚠️ 대상 인덱스(target_index)가 있으면 삭제하고 재생성 (원본 인덱스는 절대 건드리지 않음)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="스크롤 배치 크기 (default: 1000)"
    )
    
    args = parser.parse_args()
    
    try:
        # 클라이언트 생성
        print("🔌 OpenSearch 연결 중...")
        client = build_client(args.host, args.port, args.user, args.password)
        
        # 연결 확인
        info = client.info()
        print(f"✅ OpenSearch 연결 성공: v{info['version']['number']}")
        
        # 원본 인덱스 확인 (절대 건드리지 않음)
        if not client.indices.exists(index=args.source_index):
            print(f"❌ 원본 인덱스 '{args.source_index}'가 존재하지 않습니다.")
            return 1
        
        print(f"✅ 원본 인덱스 '{args.source_index}' 확인 완료 (읽기 전용)")
        print(f"📝 대상 인덱스: '{args.target_index}'")
        
        # 원본과 대상이 같은지 확인 (실수 방지)
        if args.source_index == args.target_index:
            print(f"\n❌ 오류: 원본 인덱스와 대상 인덱스가 같습니다!")
            print(f"   원본 인덱스 '{args.source_index}'는 절대 수정할 수 없습니다.")
            print(f"   다른 대상 인덱스 이름을 지정하세요.")
            return 1
        
        # 새 인덱스 생성 (대상 인덱스만)
        create_analysis_index(client, args.target_index, args.force_recreate)
        
        # 스크롤로 모든 문서 읽기
        documents = list(scroll_all_documents(client, args.source_index, args.batch_size))
        
        if not documents:
            print("⚠️ 읽을 문서가 없습니다.")
            return 0
        
        # 데이터 변환
        transformed_docs = transform_data(documents)
        
        if not transformed_docs:
            print("⚠️ 변환된 문서가 없습니다.")
            return 0
        
        # 새 인덱스에 저장
        bulk_index_documents(client, args.target_index, transformed_docs, args.batch_size)
        
        # 최종 통계
        stats = client.indices.stats(index=args.target_index)
        doc_count = stats['indices'][args.target_index]['total']['docs']['count']
        print(f"\n📊 최종 통계:")
        print(f"  - 인덱스: {args.target_index}")
        print(f"  - 문서 수: {doc_count:,}건")
        print(f"  - 원본 사용자 수: {len(documents):,}명")
        print(f"  - 변환된 문서 수: {len(transformed_docs):,}건")
        
        print("\n✅ 작업 완료!")
        return 0
        
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 중단되었습니다.")
        return 1
    except Exception as exc:
        print(f"❌ 작업 실패: {exc}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

