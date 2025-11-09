# SurveyPilot 팀 역할 분담 및 일정 계획

## 👥 팀 구성

### 백엔드 팀 (3명)
- **Backend A** (시니어): 검색 엔진 & RRF 담당
- **Backend B**: Celery & Redis 담당
- **Backend C**: LLM & API 통합 담당

### 프론트엔드 팀 (2명)
- **Frontend A** (시니어): 검색 & 무한 스크롤 담당
- **Frontend B**: 시각화 & 상세보기 담당

---

## 📅 전체 일정 (4주)

```
Week 1: 핵심 기능 구현
Week 2: 데이터 처리 & 시각화
Week 3: LLM 통합 & 고도화
Week 4: 통합 테스트 & 배포
```

---

## 🗓️ Week 1: 핵심 검색 기능 (5일)

### Backend A - 검색 엔진 & RRF 🔍

#### Day 1-2: OpenSearch + Qdrant 검색 구현
```python
# 담당 작업
□ OpenSearch 검색 함수 구현
  - search_opensearch(query, filters, size)
  - 키워드 매칭
  - 필터링 (region, age_group, etc)

□ Qdrant 검색 함수 구현
  - search_qdrant(query, size)
  - 벡터 유사도 검색
  - 임베딩 생성

□ 유닛 테스트 작성
  - 각 검색 함수 테스트
  - 결과 포맷 검증

# 산출물
- services/opensearch_service.py
- services/qdrant_service.py
- tests/test_search_services.py
```

#### Day 3-4: RRF 병합 로직 구현
```python
# 담당 작업
□ RRF 알고리즘 구현
  - rrf_merge(os_results, qdrant_results, k=60)
  - 스코어 정규화
  - 중복 제거

□ 병렬 검색 최적화
  - asyncio 활용
  - 동시 실행 (OpenSearch + Qdrant)

□ 성능 테스트
  - 1000건 검색 시간 측정
  - 병렬 vs 순차 비교

# 산출물
- services/rrf_service.py
- tests/test_rrf_merge.py
- docs/RRF_ALGORITHM.md
```

#### Day 5: 통합 및 문서화
```python
# 담당 작업
□ 전체 검색 파이프라인 통합
  - search_pipeline(query, filters, size)
  - 에러 핸들링

□ API 문서 작성
  - 함수 docstring
  - 사용 예시

□ Backend B에게 인터페이스 전달
  - 함수 시그니처
  - 반환 데이터 포맷

# 산출물
- services/search_pipeline.py
- docs/SEARCH_API_SPEC.md
```

---

### Backend B - Celery & Redis 🔄

#### Day 1-2: Celery 환경 구축
```python
# 담당 작업
□ Celery 프로젝트 설정
  - celery_app.py 생성
  - Redis broker 연결
  - Worker 설정

□ 기본 Task 구조 작성
  - @celery_app.task 데코레이터
  - 상태 업데이트 로직
  - 에러 핸들링

□ 로컬 테스트
  - Worker 실행 확인
  - Task 큐잉 테스트

# 산출물
- celery_app.py
- tasks/__init__.py
- docker-compose.yml (Celery + Redis)
```

#### Day 3-4: Redis 캐싱 시스템 구현
```python
# 담당 작업
□ Redis 캐시 클라이언트 구현
  - RedisSearchCache 클래스
  - cache_search_results()
  - get_scroll_results()

□ 데이터 구조 설계
  - Sorted Set (스코어 정렬)
  - Hash (상세 데이터)
  - 메타데이터 저장

□ TTL 관리
  - 자동 만료 설정
  - 메모리 관리

# 산출물
- services/redis_cache.py
- tests/test_redis_cache.py
- docs/REDIS_SCHEMA.md
```

#### Day 5: Celery Task 통합
```python
# 담당 작업
□ 검색 Task 구현
  - search_with_rrf_task()
  - Backend A의 함수 통합
  - 진행 상태 업데이트

□ 결과 Redis 저장
  - RRF 결과를 Redis에 캐싱
  - query_hash 생성

□ 통합 테스트
  - 전체 파이프라인 테스트
  - 성능 측정

# 산출물
- tasks/search_tasks.py
- tests/test_celery_integration.py
```

---

### Backend C - FastAPI 기본 API ⚡

#### Day 1-2: FastAPI 프로젝트 구조
```python
# 담당 작업
□ FastAPI 프로젝트 초기화
  - main.py
  - routers/ 구조
  - 환경 설정 (config.py)

□ CORS 설정
  - 프론트엔드 연결 준비
  - 미들웨어 설정

□ 기본 헬스체크 API
  - GET /health
  - GET /api/status

# 산출물
- main.py
- routers/__init__.py
- config.py
- requirements.txt
```

#### Day 3-4: 검색 API 엔드포인트 구현
```python
# 담당 작업
□ 비동기 검색 시작 API
  - POST /api/search/async
  - Celery Task 호출
  - task_id 반환

□ 작업 상태 확인 API
  - GET /api/search/task/{task_id}
  - Celery result 조회
  - 진행률 반환

□ Request/Response 모델
  - Pydantic 스키마 정의
  - 입력 검증

# 산출물
- routers/search.py
- schemas/search_schemas.py
- tests/test_search_api.py
```

#### Day 5: 무한 스크롤 API
```python
# 담당 작업
□ 스크롤 API 구현
  - POST /api/search/scroll
  - Redis 캐시 조회
  - 페이지네이션

□ API 문서 자동화
  - FastAPI Swagger UI
  - 예시 Request/Response

□ 프론트엔드 팀에게 API 스펙 전달
  - Postman Collection
  - API 사용 가이드

# 산출물
- routers/search.py (완성)
- docs/API_DOCUMENTATION.md
- postman/SurveyPilot.postman_collection.json
```

---

### Frontend A - 검색 UI & 무한 스크롤 🎨

#### Day 1-2: 프로젝트 초기 설정
```typescript
// 담당 작업
□ React + TypeScript 프로젝트 생성
  - Vite 또는 CRA
  - TypeScript 설정

□ 라이브러리 설치
  - axios (API 통신)
  - react-query (데이터 페칭)
  - tailwindcss (스타일링)

□ 프로젝트 구조 설계
  - components/
  - pages/
  - services/
  - types/

// 산출물
- package.json
- tsconfig.json
- src/ 폴더 구조
```

#### Day 3-4: 검색 UI 구현
```typescript
// 담당 작업
□ 검색 입력 컴포넌트
  - SearchBar.tsx
  - 쿼리 입력
  - 검색 버튼

□ 검색 상태 관리
  - 로딩 상태
  - 에러 처리
  - task_id 저장

□ 상태 폴링 구현
  - 3초마다 /api/search/task 호출
  - 완료 시 결과 표시

// 산출물
- components/SearchBar.tsx
- hooks/useSearch.ts
- services/searchAPI.ts
```

#### Day 5: 무한 스크롤 구현
```typescript
// 담당 작업
□ 검색 결과 리스트
  - SearchResults.tsx
  - ResultCard.tsx
  - 20건씩 표시

□ Intersection Observer
  - 스크롤 감지
  - 자동 로딩

□ 로딩/에러 UI
  - LoadingSpinner.tsx
  - ErrorMessage.tsx

// 산출물
- components/SearchResults.tsx
- components/ResultCard.tsx
- hooks/useInfiniteScroll.ts
```

---

### Frontend B - 레이아웃 & 기본 UI 🖼️

#### Day 1-2: 전체 레이아웃 구현
```typescript
// 담당 작업
□ 메인 레이아웃
  - MainLayout.tsx
  - Header, Footer
  - 네비게이션

□ 페이지 라우팅
  - React Router
  - SearchPage.tsx

□ 다크모드 테마
  - 네이비 블루 컬러
  - 글래스모피즘 효과

// 산출물
- components/Layout/MainLayout.tsx
- components/Layout/Header.tsx
- styles/theme.ts
```

#### Day 3-4: 기본 컴포넌트 제작
```typescript
// 담당 작업
□ 재사용 컴포넌트
  - Button.tsx
  - Input.tsx
  - Card.tsx
  - Badge.tsx

□ 로딩/에러 컴포넌트
  - LoadingSpinner.tsx
  - ErrorBoundary.tsx

□ Storybook 설정 (선택)
  - 컴포넌트 문서화

// 산출물
- components/common/
- components/LoadingSpinner.tsx
- components/ErrorBoundary.tsx
```

#### Day 5: 통합 및 스타일링
```typescript
// 담당 작업
□ Frontend A와 통합
  - 검색 페이지 레이아웃
  - 컴포넌트 합성

□ 반응형 디자인
  - 모바일 최적화
  - 태블릿 대응

□ 애니메이션
  - 페이드 인/아웃
  - 스크롤 효과

// 산출물
- pages/SearchPage.tsx (완성)
- styles/animations.css
```

---

## 🗓️ Week 2: 데이터 처리 & 시각화 (5일)

### Backend A - 집계 및 필터링 📊

#### Day 1-2: 집계 함수 구현
```python
# 담당 작업
□ 메타데이터 집계
  - calculate_aggregations()
  - region, sub_region, age_group
  - Counter 활용

□ 필터링 로직
  - filter_results_by_meta()
  - 동적 필터 적용

□ 성능 최적화
  - 집계 캐싱
  - Redis에 저장

# 산출물
- services/aggregation_service.py
- tests/test_aggregations.py
```

#### Day 3-4: 시각화 API 구현
```python
# 담당 작업
□ 집계 데이터 조회 API
  - GET /api/visualization/{query_hash}
  - 집계 데이터 반환

□ 필터링 API
  - POST /api/visualization/filter
  - 드릴다운 지원

□ 최적화
  - 응답 속도 개선
  - 캐싱 전략

# 산출물
- routers/visualization.py
- schemas/visualization_schemas.py
```

#### Day 5: 통계 분석 추가
```python
# 담당 작업
□ 고급 통계
  - 평균, 중앙값, 표준편차
  - 상위/하위 분석

□ 트렌드 분석
  - 시계열 데이터 (선택)
  - 비율 계산

# 산출물
- services/statistics_service.py
```

---

### Backend B - 상세보기 & 데이터 조회 📋

#### Day 1-2: 사용자 상세 API
```python
# 담당 작업
□ 전체 설문 조회
  - GET /api/user/{user_id}/full
  - OpenSearch에서 조회
  - 전체 Q&A 반환

□ 매칭된 질문만 조회
  - GET /api/user/{user_id}/matched
  - Redis 캐시 활용

□ 데이터 포맷팅
  - 읽기 쉬운 구조
  - 하이라이트 처리

# 산출물
- routers/user.py
- services/user_service.py
```

#### Day 3-4: 데이터 내보내기 (CSV/Excel)
```python
# 담당 작업
□ CSV 내보내기
  - export_to_csv_task()
  - Celery 비동기 처리
  - pandas 활용

□ Excel 내보내기
  - export_to_excel_task()
  - openpyxl 활용

□ 파일 다운로드 API
  - GET /api/export/{file_id}
  - S3 또는 로컬 저장

# 산출물
- tasks/export_tasks.py
- routers/export.py
```

#### Day 5: 배치 작업 최적화
```python
# 담당 작업
□ 스트리밍 방식 내보내기
  - 메모리 효율화
  - 1000건씩 처리

□ 진행 상태 표시
  - 퍼센트 업데이트
  - 예상 완료 시간

# 산출물
- services/streaming_export.py
```

---

### Backend C - WebSocket 알림 🔔

#### Day 1-3: WebSocket 서버 구현
```python
# 담당 작업
□ WebSocket 엔드포인트
  - ws://api/ws/{client_id}
  - FastAPI WebSocket

□ 연결 관리
  - ConnectionManager 클래스
  - 클라이언트 추적

□ Celery 이벤트 리스닝
  - Task 완료 시 알림
  - 실시간 푸시

# 산출물
- routers/websocket.py
- services/connection_manager.py
```

#### Day 4-5: 알림 시스템 통합
```python
# 담당 작업
□ 검색 완료 알림
  - "검색 완료! 850건 발견"
  - query_hash 전달

□ 내보내기 완료 알림
  - "파일 생성 완료"
  - 다운로드 링크

□ 에러 알림
  - 실패 시 사용자에게 알림

# 산출물
- services/notification_service.py
- tests/test_websocket.py
```

---

### Frontend A - 시각화 차트 📊

#### Day 1-3: Chart.js 통합
```typescript
// 담당 작업
□ Chart.js 설치 및 설정
  - react-chartjs-2
  - 기본 설정

□ 차트 컴포넌트 구현
  - BarChart.tsx (지역별)
  - PieChart.tsx (연령대)
  - DonutChart.tsx (세부지역)

□ 데이터 바인딩
  - API 연동
  - 동적 업데이트

// 산출물
- components/Charts/BarChart.tsx
- components/Charts/PieChart.tsx
- components/Charts/DonutChart.tsx
```

#### Day 4-5: 시각화 대시보드
```typescript
// 담당 작업
□ 시각화 패널 구현
  - Visualization.tsx
  - 4개 차트 그리드

□ 인터랙티브 기능
  - 차트 클릭 → 필터링
  - 드릴다운

□ 반응형 차트
  - 모바일 최적화
  - 자동 리사이징

// 산출물
- components/Visualization.tsx
- hooks/useVisualization.ts
```

---

### Frontend B - 상세보기 모달 📄

#### Day 1-3: 모달 컴포넌트
```typescript
// 담당 작업
□ 기본 모달 구조
  - Modal.tsx (재사용)
  - Overlay, 닫기 버튼

□ 사용자 상세 모달
  - UserDetailModal.tsx
  - 기본 정보 표시
  - 매칭된 Q&A

□ 전체 설문 펼치기
  - Accordion 방식
  - 30개 질문 표시

// 산출물
- components/Modal.tsx
- components/UserDetailModal.tsx
```

#### Day 4-5: 상세보기 기능 완성
```typescript
// 담당 작업
□ API 연동
  - /api/user/{id}/full 호출
  - 로딩 상태 관리

□ 하이라이트 표시
  - 매칭된 키워드 강조
  - 스코어 배지

□ 스타일링
  - 읽기 쉬운 레이아웃
  - 인쇄 가능한 형식

// 산출물
- components/UserDetailModal.tsx (완성)
- hooks/useUserDetail.ts
```

---

## 🗓️ Week 3: LLM 통합 & 고도화 (5일)

### Backend A - 성능 최적화 ⚡

#### Day 1-3: 검색 최적화
```python
# 담당 작업
□ 병렬 처리 최적화
  - asyncio 개선
  - 동시성 제어

□ 캐싱 전략 고도화
  - LRU 캐시
  - 부분 캐싱

□ 인덱스 최적화
  - OpenSearch 설정
  - Force Merge

# 산출물
- docs/OPTIMIZATION_GUIDE.md
- 성능 테스트 리포트
```

#### Day 4-5: 모니터링 구축
```python
# 담당 작업
□ 로깅 시스템
  - 구조화된 로그
  - 레벨별 분류

□ 성능 메트릭
  - 검색 시간 추적
  - 메모리 사용량

□ 알림 설정
  - 에러 알림
  - 성능 저하 알림

# 산출물
- utils/logger.py
- utils/metrics.py
```

---

### Backend B - 데이터 파이프라인 🔄

#### Day 1-2: 데이터 검증
```python
# 담당 작업
□ 입력 검증
  - 쿼리 유효성 검사
  - 필터 검증

□ 결과 검증
  - 데이터 무결성
  - 중복 제거

# 산출물
- validators/query_validator.py
- validators/result_validator.py
```

#### Day 3-5: 배치 작업 관리
```python
# 담당 작업
□ 작업 큐 관리
  - 우선순위 큐
  - 재시도 로직

□ 실패 처리
  - Dead Letter Queue
  - 알림 전송

□ 작업 스케줄링
  - 정기 작업
  - Celery Beat

# 산출물
- tasks/batch_tasks.py
- config/celery_config.py
```

---

### Backend C - LLM 통합 🤖

#### Day 1-2: Claude API 연동
```python
# 담당 작업
□ LLM 서비스 구현
  - LLMAnalyzer 클래스
  - Anthropic API 연동

□ 프롬프트 엔지니어링
  - 분석 프롬프트
  - 재질의 프롬프트

□ 에러 핸들링
  - Rate limit 처리
  - 타임아웃 관리

# 산출물
- services/llm_service.py
- prompts/analysis_prompts.py
```

#### Day 3-4: LLM API 엔드포인트
```python
# 담당 작업
□ 자동 분석 API
  - POST /api/llm/analyze
  - Celery Task로 비동기 처리

□ 재질의 API
  - POST /api/llm/chat
  - 대화 히스토리 관리

□ 대화 조회 API
  - GET /api/llm/conversation/{id}

# 산출물
- routers/llm.py
- tasks/llm_tasks.py
- schemas/llm_schemas.py
```

#### Day 5: LLM 최적화
```python
# 담당 작업
□ 응답 스트리밍
  - Server-Sent Events (SSE)
  - 실시간 응답

□ 컨텍스트 압축
  - 상위 100건만 사용
  - 토큰 절약

□ 비용 최적화
  - 캐싱 전략
  - 중복 요청 방지

# 산출물
- services/llm_optimizer.py
```

---

### Frontend A - LLM 채팅 UI 💬

#### Day 1-3: 채팅 인터페이스
```typescript
// 담당 작업
□ 채팅 컴포넌트
  - LLMChat.tsx
  - 메시지 리스트
  - 입력창

□ 메시지 렌더링
  - 사용자/AI 구분
  - 타임스탬프
  - 마크다운 지원

□ 실시간 타이핑 효과
  - 스트리밍 응답
  - 애니메이션

// 산출물
- components/LLMChat.tsx
- components/ChatMessage.tsx
- hooks/useLLMChat.ts
```

#### Day 4-5: LLM 분석 패널
```typescript
// 담당 작업
□ 자동 분석 표시
  - AnalysisPanel.tsx
  - 검색 완료 시 자동 표시

□ 키 파인딩 카드
  - 주요 인사이트 강조
  - 아이콘 & 배지

□ 추천 질문 버튼
  - "이런 것도 물어보세요"
  - 클릭 시 자동 입력

// 산출물
- components/AnalysisPanel.tsx
- components/KeyFinding.tsx
```

---

### Frontend B - 고급 필터링 🎛️

#### Day 1-3: 필터 컴포넌트
```typescript
// 담당 작업
□ 필터 사이드바
  - FilterPanel.tsx
  - 체크박스/라디오 버튼

□ 다중 필터 지원
  - 지역, 연령, 직업
  - AND/OR 조건

□ 필터 상태 관리
  - Redux 또는 Zustand
  - URL 쿼리 동기화

// 산출물
- components/FilterPanel.tsx
- hooks/useFilters.ts
- store/filterStore.ts
```

#### Day 4-5: 고급 기능
```typescript
// 담당 작업
□ 필터 프리셋
  - 자주 쓰는 조합 저장
  - 빠른 적용

□ 필터 리셋
  - 초기화 버튼
  - 단계별 되돌리기

□ 필터 결과 미리보기
  - 적용 전 개수 표시

// 산출물
- components/FilterPreset.tsx
- utils/filterUtils.ts
```

---

## 🗓️ Week 4: 통합 테스트 & 배포 (5일)

### 전체 팀 - 통합 테스트 🧪

#### Day 1-2: 기능 테스트

**Backend 팀**
```python
# Backend A
□ E2E 검색 테스트
  - 전체 파이프라인
  - 성능 측정

# Backend B
□ 동시성 테스트
  - 10개 동시 검색
  - Redis 부하 테스트

# Backend C
□ API 통합 테스트
  - 모든 엔드포인트
  - Postman/pytest
```

**Frontend 팀**
```typescript
// Frontend A
□ 사용자 시나리오 테스트
  - 검색 → 스크롤 → 상세
  - LLM 대화

// Frontend B
□ UI/UX 테스트
  - 반응형 확인
  - 크로스 브라우저
```

#### Day 3: 버그 픽스 & 최적화

**전체 팀**
- 발견된 버그 수정
- 성능 개선
- 코드 리뷰

---

### Backend 팀 - 배포 준비 🚀

#### Day 4: 인프라 설정

**Backend A**
```bash
# Docker 이미지 빌드
□ Dockerfile 작성
□ docker-compose.yml 완성
□ 환경 변수 설정
```

**Backend B**
```bash
# Celery Worker 배포
□ Worker Dockerfile
□ Redis 연결 확인
□ 프로세스 모니터링
```

**Backend C**
```bash
# FastAPI 배포
□ Gunicorn/Uvicorn 설정
□ Nginx 리버스 프록시
□ SSL 인증서
```

#### Day 5: 프로덕션 배포

**전체 Backend 팀**
```bash
# DigitalOcean 배포
□ Droplet 생성
□ 서비스 배포
□ 헬스체크 확인
□ 모니터링 설정
```

---

### Frontend 팀 - 배포 & 문서화 📚

#### Day 4: 프로덕션 빌드

**Frontend A**
```bash
# 빌드 최적화
□ 코드 스플리팅
□ 레이지 로딩
□ 번들 크기 최적화
```

**Frontend B**
```bash
# 정적 파일 최적화
□ 이미지 압축
□ CSS 최적화
□ 캐싱 전략
```

#### Day 5: 배포 & 문서

**Frontend A**
```bash
# Vercel/Netlify 배포
□ 환경 변수 설정
□ API 엔드포인트 연결
□ 도메인 연결
```

**Frontend B**
```markdown
# 사용자 가이드 작성
□ README.md
□ 기능 설명서
□ 스크린샷/GIF
```

---

## 📋 산출물 체크리스트

### Week 1 산출물
```
Backend:
✅ services/opensearch_service.py
✅ services/qdrant_service.py
✅ services/rrf_service.py
✅ celery_app.py
✅ services/redis_cache.py
✅ tasks/search_tasks.py
✅ routers/search.py
✅ docs/API_DOCUMENTATION.md

Frontend:
✅ components/SearchBar.tsx
✅ components/SearchResults.tsx
✅ components/Layout/MainLayout.tsx
✅ hooks/useSearch.ts
✅ hooks/useInfiniteScroll.ts
```

### Week 2 산출물
```
Backend:
✅ services/aggregation_service.py
✅ routers/visualization.py
✅ routers/user.py
✅ tasks/export_tasks.py
✅ routers/websocket.py

Frontend:
✅ components/Charts/*.tsx
✅ components/Visualization.tsx
✅ components/UserDetailModal.tsx
✅ hooks/useVisualization.ts
```

### Week 3 산출물
```
Backend:
✅ services/llm_service.py
✅ routers/llm.py
✅ tasks/llm_tasks.py
✅ utils/logger.py
✅ validators/*.py

Frontend:
✅ components/LLMChat.tsx
✅ components/AnalysisPanel.tsx
✅ components/FilterPanel.tsx
✅ store/filterStore.ts
```

### Week 4 산출물
```
Backend:
✅ Dockerfile
✅ docker-compose.yml
✅ nginx.conf
✅ 배포 스크립트

Frontend:
✅ 프로덕션 빌드
✅ 배포 설정
✅ README.md
✅ 사용자 가이드
```

---

## 💬 커뮤니케이션 계획

### 일일 스탠드업 (15분)
- **시간**: 매일 오전 10시
- **참석**: 전체 팀 (5명)
- **내용**:
  - 어제 완료한 작업
  - 오늘 할 작업
  - 블로커/이슈

### 주간 리뷰 (1시간)
- **시간**: 매주 금요일 오후 5시
- **참석**: 전체 팀
- **내용**:
  - 주간 목표 달성도
  - 다음 주 계획
  - 회고

### 백엔드-프론트엔드 동기화 (30분)
- **시간**: 매주 월/수요일 오후 3시
- **참석**: Backend C (API 담당) + Frontend A (시니어)
- **내용**:
  - API 인터페이스 확인
  - 데이터 포맷 협의
  - 통합 이슈 해결

---

## 🎯 주요 마일스톤

### Week 1 마일스톤 (Day 5)
```
✅ 검색 기능 동작
  - Celery로 비동기 검색
  - RRF 병합
  - Redis 캐싱
  - 무한 스크롤

✅ 기본 UI 완성
  - 검색 입력
  - 결과 리스트
  - 로딩 상태
```

### Week 2 마일스톤 (Day 10)
```
✅ 시각화 완성
  - 4개 차트
  - 필터링
  - 상세보기 모달

✅ 데이터 내보내기
  - CSV/Excel
  - 비동기 처리
```

### Week 3 마일스톤 (Day 15)
```
✅ LLM 통합 완료
  - 자동 분석
  - 재질의
  - 실시간 채팅

✅ 고급 기능
  - 필터링
  - 최적화
```

### Week 4 마일스톤 (Day 20)
```
✅ 배포 완료
  - 백엔드 서버
  - 프론트엔드
  - 모니터링

✅ 문서화
  - API 문서
  - 사용자 가이드
```

---

## ⚠️ 리스크 관리

### 기술적 리스크

| 리스크 | 확률 | 영향 | 대응 방안 |
|--------|------|------|----------|
| RRF 성능 저하 | 중 | 높음 | Backend A가 사전 벤치마크 |
| Redis 메모리 부족 | 중 | 중 | 부분 캐싱 전략 |
| LLM API 비용 | 낮 | 중 | 캐싱 & 요청 제한 |
| 동시성 이슈 | 중 | 높음 | Backend B가 부하 테스트 |

### 일정 리스크

| 리스크 | 확률 | 영향 | 대응 방안 |
|--------|------|------|----------|
| Week 1 지연 | 중 | 높음 | 핵심 기능 우선순위 |
| LLM 통합 복잡도 | 높 | 중 | Week 3 여유 시간 확보 |
| 배포 이슈 | 중 | 중 | Week 4 Day 1-3에 사전 테스트 |

---

## 📊 예상 작업 시간 (인일)

### Backend 팀 (3명 × 4주 = 60인일)
```
Backend A (검색/RRF):      20인일
Backend B (Celery/Redis):  20인일
Backend C (API/LLM):       20인일
```

### Frontend 팀 (2명 × 4주 = 40인일)
```
Frontend A (검색/LLM UI):  20인일
Frontend B (시각화/상세):   20인일
```

### 총 작업량: 100인일

---

## ✅ 성공 기준

### Week 1
- ✅ 검색 1000건 10초 이내
- ✅ 무한 스크롤 20ms 이내
- ✅ API 응답률 99%

### Week 2
- ✅ 차트 렌더링 1초 이내
- ✅ 모달 로딩 500ms 이내
- ✅ CSV 내보내기 30초 이내

### Week 3
- ✅ LLM 분석 15초 이내
- ✅ 재질의 5초 이내
- ✅ 필터링 즉시 반영

### Week 4
- ✅ 배포 성공
- ✅ 모든 테스트 통과
- ✅ 문서화 완료

---

## 🎉 결론

이 계획은 **현실적이고 실행 가능**합니다.

**핵심 포인트**:
1. 명확한 역할 분담
2. 주간 단위 마일스톤
3. 일일 커뮤니케이션
4. 리스크 관리

**첫 주가 가장 중요합니다!**
Week 1 마일스톤 달성 시 나머지는 순조롭게 진행될 것입니다.

**화이팅! 🚀**