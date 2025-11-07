# 검색 아키텍처 개선: 분리 유지 + RRF + Lazy Join

## 문제점 분석

### 현재 상황
- `welcome_1st` = 설문조사1 참여자 subset
- `welcome_2nd` = 설문조사2 참여자 subset
- 일부 유저는 두 설문 모두 참여, 일부는 한 설문만 참여
- 전체 유저 정보를 대표하는 인덱스는 없음

### 사전 조인(Pre-join)의 문제점
- Inner join만 가능 → 교집합만 포함
- 한 설문만 참여한 유저 누락
- **Recall 급격히 감소** 위험

## 개선된 아키텍처

### 핵심 전략: 분리 유지 + RRF + Lazy Join

```
1. welcome_1st 검색 (OpenSearch + Qdrant) → RRF 결합
2. welcome_2nd 검색 (OpenSearch + Qdrant) → RRF 결합
3. 두 인덱스의 RRF 결과를 다시 RRF로 결합
4. 필터 적용 시 user_id 기준으로 Lazy Join
```

### 장점

1. **Recall 최대화**
   - 한 설문만 참여한 유저도 검색 결과에 포함
   - 각 인덱스를 독립적으로 검색하여 누락 최소화

2. **정확한 필터링**
   - 필터 적용 시 user_id 기준으로 필요한 인덱스만 조회
   - "30대" 필터 → welcome_1st에서만 확인
   - "사무직" 필터 → welcome_2nd에서만 확인
   - 두 필터 모두 → 두 인덱스 모두 확인 (Lazy Join)

3. **성능 최적화**
   - 각 인덱스를 병렬로 검색 가능
   - 필요한 경우에만 추가 조회 (Lazy Join)
   - RRF로 자연스러운 랭킹 결합

## 구현 상세

### 1. 인덱스별 독립 검색

```python
# welcome_1st 검색
welcome_1st_keyword_results = search_opensearch(
    index="s_welcome_1st",
    query=query_with_filters,
    size=size * 2
)
welcome_1st_vector_results = search_qdrant(
    collection="s_welcome_1st",
    query_vector=query_vector,
    limit=size * 2
)
welcome_1st_rrf = calculate_rrf_score(
    welcome_1st_keyword_results,
    welcome_1st_vector_results,
    k=60
)

# welcome_2nd 검색
welcome_2nd_keyword_results = search_opensearch(
    index="s_welcome_2nd",
    query=query_with_filters,
    size=size * 2
)
welcome_2nd_vector_results = search_qdrant(
    collection="s_welcome_2nd",
    query_vector=query_vector,
    limit=size * 2
)
welcome_2nd_rrf = calculate_rrf_score(
    welcome_2nd_keyword_results,
    welcome_2nd_vector_results,
    k=60
)
```

### 2. 인덱스 간 RRF 결합

```python
# 두 인덱스의 RRF 결과를 다시 RRF로 결합
final_rrf = calculate_rrf_score(
    welcome_1st_rrf,
    welcome_2nd_rrf,
    k=60
)
```

### 3. Lazy Join 필터링

```python
# 필터 적용 시 user_id 기준으로 필요한 인덱스만 조회
for doc in final_rrf:
    user_id = doc.get('user_id')
    
    # "30대" 필터가 있으면 welcome_1st에서 확인
    if has_age_filter:
        welcome_1st_doc = os_client.get(index='s_welcome_1st', id=user_id, ignore=[404])
        if welcome_1st_doc.get('found'):
            age_group = welcome_1st_doc['_source'].get('metadata', {}).get('age_group')
            if age_group != "30대":
                continue  # 필터 미충족
    
    # "사무직" 필터가 있으면 welcome_2nd에서 확인
    if has_occupation_filter:
        welcome_2nd_doc = os_client.get(index='s_welcome_2nd', id=user_id, ignore=[404])
        if welcome_2nd_doc.get('found'):
            occupation = welcome_2nd_doc['_source'].get('metadata', {}).get('occupation')
            if occupation != "office":
                continue  # 필터 미충족
    
    # 모든 필터를 만족하면 결과에 포함
    filtered_results.append(doc)
```

## 성능 비교

### 이전 방식 (통합 검색)
- OpenSearch: `index="*"` 한 번 검색
- Qdrant: 모든 컬렉션 검색
- RRF: 한 번 결합
- 후처리: 각 결과마다 welcome_1st/welcome_2nd 조회 (N+2 쿼리)

### 개선된 방식 (분리 검색 + RRF)
- OpenSearch: welcome_1st, welcome_2nd 각각 검색 (병렬 가능)
- Qdrant: welcome_1st, welcome_2nd 각각 검색 (병렬 가능)
- RRF: 인덱스별 RRF → 최종 RRF (2단계)
- 후처리: 필터가 있는 경우에만 필요한 인덱스 조회 (Lazy Join)

## 필터 전략

### 단일 필터 (예: "30대"만)
- welcome_1st에서만 검색
- welcome_2nd는 검색하지 않음
- **Recall 최대화**: welcome_1st의 모든 "30대" 유저 포함

### 복합 필터 (예: "30대 사무직")
- welcome_1st에서 "30대" 검색
- welcome_2nd에서 "사무직" 검색
- RRF로 결합
- 후처리에서 user_id 기준으로 두 조건 모두 확인 (Lazy Join)
- **정확도 최대화**: 두 조건을 모두 만족하는 유저만 포함

## 마이그레이션 계획

1. ✅ 아키텍처 설계
2. ⏳ 인덱스별 독립 검색 구현
3. ⏳ 인덱스 간 RRF 결합 구현
4. ⏳ Lazy Join 필터링 구현
5. ⏳ 성능 테스트 및 비교
6. ⏳ 프로덕션 배포

