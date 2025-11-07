# 조인된 인덱스 사용 가이드

## 개요

`welcome_1st`와 `welcome_2nd`를 사전 조인하여 통합 인덱스(`s_users_joined`)를 생성합니다.

## 주요 장점

1. **RRF 결합 정확도 상승**
   - Qdrant/OpenSearch 모두 같은 `user_id` 기반 데이터 구조로 접근 가능
   - RRF에서 "동일 사용자 문서"를 쉽게 병합 가능
   - score aggregation 시 중복 매칭 이슈 감소

2. **필터링 효율 향상**
   - must 필터를 age/job 성분 모두 한 번에 적용 가능
   - 예: `"metadata.age_group": "30대" AND "metadata.occupation": "사무직"`
   - 두 인덱스 각각 조회 후 매칭할 필요 없음

3. **검색 속도 개선**
   - 조인 비용이 미리 사라지므로, 요청 시 latency 감소 (특히 대용량 데이터일 때)
   - 실시간 검색 API에서는 JOIN + RRF 구조보다 JOIN 사전결합이 훨씬 빠름

4. **결과 일관성 확보**
   - 나이/직업 필드가 하나의 문서 단위로 묶여 있으면,
   - 필터 누락/불일치(예: 동일 user_id지만 age_group만 있는 문서) 문제가 사라짐

## 사용 방법

### 1. 조인된 인덱스 생성

```bash
# 기본 설정으로 생성 (s_welcome_1st + s_welcome_2nd → s_users_joined)
python scripts/create_joined_index.py

# 커스텀 설정으로 생성
python scripts/create_joined_index.py \
    --source-1st s_welcome_1st \
    --source-2nd s_welcome_2nd \
    --target s_users_joined \
    --batch-size 1000
```

### 2. 조인된 인덱스 구조

```json
{
  "user_id": "w123456789",
  "metadata": {
    "age_group": "30대",        // welcome_1st에서
    "gender": "남성",            // welcome_1st에서
    "birth_year": "1990",        // welcome_1st에서
    "region": "서울",            // welcome_1st에서
    "occupation": "office",      // welcome_2nd에서
    "job_category": "사무직"     // welcome_2nd에서
  },
  "qa_pairs": [
    // welcome_1st의 qa_pairs + welcome_2nd의 qa_pairs 병합
  ],
  "timestamp": "2025-11-06T16:30:49.080338",
  "source_indices": ["s_welcome_1st", "s_welcome_2nd"]
}
```

### 3. 검색 API에서 조인된 인덱스 사용

검색 시 `index_name`에 조인된 인덱스를 포함하거나, 기본값으로 사용:

```json
{
  "query": "30대 사무직 50명 데이터를 보여줘",
  "index_name": "s_users_joined,survey_*",  // 조인된 인덱스 우선 사용
  "use_vector_search": true
}
```

### 4. 필터 최적화

조인된 인덱스를 사용하면 필터가 단순해집니다:

**이전 (분리된 인덱스):**
```json
{
  "bool": {
    "must": [
      {
        "bool": {
          "should": [
            {"term": {"metadata.age_group.keyword": "30대"}},
            {"nested": {"path": "qa_pairs", "query": {...}}}
          ]
        }
      },
      {
        "bool": {
          "should": [
            {"term": {"metadata.occupation.keyword": "office"}},
            {"nested": {"path": "qa_pairs", "query": {...}}}
          ]
        }
      }
    ]
  }
}
```

**이후 (조인된 인덱스):**
```json
{
  "bool": {
    "must": [
      {"term": {"metadata.age_group.keyword": "30대"}},
      {"term": {"metadata.occupation.keyword": "office"}}
    ]
  }
}
```

## 성능 비교

### 이전 방식 (분리된 인덱스)
- OpenSearch 쿼리: 복잡한 nested 쿼리 + should 조건
- RRF 후처리: 각 결과마다 `welcome_1st`와 `welcome_2nd` 조회 (N+2 쿼리)
- 총 쿼리 수: 1 (검색) + N*2 (후처리) = **1 + 2N**

### 조인된 인덱스 방식
- OpenSearch 쿼리: 단순한 term 필터
- RRF 후처리: 추가 조회 불필요
- 총 쿼리 수: 1 (검색) = **1**

**예시: 50개 결과 반환 시**
- 이전: 1 + 50*2 = **101개 쿼리**
- 조인: **1개 쿼리**

## 주의사항

1. **인덱스 업데이트**: 원본 인덱스(`s_welcome_1st`, `s_welcome_2nd`)가 업데이트되면 조인된 인덱스도 재생성 필요
2. **저장 공간**: 조인된 인덱스는 원본 두 인덱스의 합보다 약간 큼 (중복 필드 제거)
3. **인덱싱 시간**: 초기 조인 인덱스 생성에는 시간이 소요됨 (대량 데이터의 경우)

## 마이그레이션 계획

1. ✅ 조인 인덱스 생성 스크립트 작성
2. ⏳ 조인된 인덱스 사용하도록 검색 로직 수정
3. ⏳ `DemographicEntity.to_opensearch_filter()` 최적화
4. ⏳ 성능 테스트 및 비교
5. ⏳ 프로덕션 배포

