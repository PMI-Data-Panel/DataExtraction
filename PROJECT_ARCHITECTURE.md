# RAG 기반 설문 데이터 검색 시스템 - 아키텍처 문서

## 📋 프로젝트 개요

이 프로젝트는 **자연어 쿼리**를 받아 **설문조사 패널 데이터**를 검색하는 RAG(Retrieval-Augmented Generation) 기반 시스템입니다. 사용자가 "30대 직장인 중 흡연자 100명"과 같은 자연어로 검색하면, 시스템이 이를 분석하여 OpenSearch에서 관련 데이터를 찾아 반환합니다.

---

## 🏗️ 시스템 아키텍처

```
┌─────────────────────────────────────────────────────────────┐
│                    FastAPI Application                      │
│                  (api/main_api.py)                          │
└─────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
┌───────▼────────┐  ┌──────▼──────┐  ┌─────────▼────────┐
│  Search API    │  │ Visualization│  │   Indexer      │
│ (search_api.py)│  │     API      │  │   (router.py)   │
└───────┬────────┘  └─────────────┘  └─────────────────┘
        │
        ├─── Query Analysis (RAG Analyzer)
        ├─── Hybrid Search (Keyword + Vector)
        ├─── Behavioral Conditions Extraction
        └─── Result Filtering & Ranking
```

---

## 🔄 주요 검색 흐름

### 1. 요청 수신 (`/search/nl` 엔드포인트)

```python
POST /search/nl
{
  "query": "30대 직장인 중 흡연자 100명",
  "size": 100,
  "use_vector_search": true
}
```

### 2. 쿼리 분석 단계 (Multi-Stage Analysis)

#### 2.1. Claude Analyzer (1단계: LLM 기반 분석)
- **역할**: 자연어 쿼리에서 의도, 키워드, 필터 추출
- **출력**:
  - `intent`: 검색 의도
  - `must_terms`: 필수 키워드
  - `should_terms`: 선택적 키워드
  - `demographic_entities`: 인구통계 정보 (나이, 성별, 직업 등)
  - `behavioral_conditions`: 행동 패턴 조건 (흡연, 음주 등)

#### 2.2. Semantic Analyzer
- **역할**: 의미적 키워드 확장 및 동의어 처리
- **기능**:
  - 동의어 확장 (Qdrant 기반 동적 확장)
  - 의미적 유사어 검색

#### 2.3. Rule-Based Analyzer
- **역할**: 규칙 기반 패턴 매칭
- **기능**: 하드코딩된 패턴으로 빠른 추출

#### 2.4. Demographic Extractor
- **역할**: 인구통계 정보 정확 추출
- **추출 항목**:
  - `gender`: 성별
  - `age_group`: 연령대
  - `region`: 지역
  - `occupation`: 직업
  - `marital_status`: 결혼 여부

### 3. Behavioral Conditions 2단계 분류

#### 3.1. 1단계: Behavior Key 추출
- LLM이 쿼리에서 **어떤 질문**과 관련있는지만 추출
- 예: "흡연자" → `{"smoker": true}`
- 예: "유럽 여행 가는 사람" → `{"travels": true}`

#### 3.2. 2단계: Answer Value 분류
- `answer_values`가 있는 패턴에 대해 **구체적 답변 값** 분류
- 예: "유럽 여행" → `{"travels": "유럽"}`
- 예: "OTT 2개 이상" → `{"ott_services": ["2개", "3개", "4개 이상"]}` (다중 선택)
- **LLM 함수**: `classify_answer_value_with_llm()`

### 4. 하이브리드 검색 (Hybrid Search)

#### 4.1. 키워드 검색 (Keyword Search)
- OpenSearch의 `bool` 쿼리 사용
- `must`: 필수 키워드 (nested qa_pairs에서 검색)
- `should`: 선택적 키워드
- `must_not`: 제외 키워드

#### 4.2. 벡터 검색 (Vector Search)
- SentenceTransformer로 쿼리 임베딩 생성
- OpenSearch의 `kNN` 쿼리 사용
- 의미적 유사도 기반 검색

#### 4.3. RRF (Reciprocal Rank Fusion)
- 두 검색 결과를 RRF 알고리즘으로 결합
- `alpha` 파라미터로 키워드/벡터 가중치 조정
- 기본값: `alpha=0.5` (균형)

### 5. 필터링 단계

#### 5.1. Demographic Filters
- OpenSearch `term` 필터로 정확 매칭
- 필드: `metadata.gender`, `metadata.age_group` 등

#### 5.2. Behavioral Filters
- Nested 필터 사용 (`qa_pairs` 배열 내 검색)
- 질문 키워드 + 답변 값 매칭
- 다중 값 지원 (OR 조건)

#### 5.3. Panel Data Cache 필터링 (메모리 기반)
- 서버 시작 시 전체 데이터를 메모리에 로드
- Pandas DataFrame으로 벡터화된 필터링
- 초고속 필터링 (OpenSearch보다 빠름)

### 6. 결과 후처리

#### 6.1. Reranking
- 검색 결과 재정렬
- 키워드 매칭 점수, 벡터 유사도, 필터 매칭도 종합

#### 6.2. QA Pairs 추출
- 각 사용자 결과에서 관련 Q&A 쌍 추출
- `matched_qa_pairs`: 검색 키워드와 매칭된 Q&A
- `behavioral_qa_pairs`: Behavioral 조건과 매칭된 Q&A

#### 6.3. LLM Summary (선택적)
- Claude로 검색 결과 요약 생성
- 사용자가 요청한 경우에만 실행

### 7. 캐싱

#### 7.1. 메모리 캐시 (TTLCache)
- 검색 결과 캐싱 (5분 TTL)
- 동일 쿼리 재검색 시 즉시 반환

#### 7.2. Redis 캐시
- 압축된 검색 결과 저장
- 대화 히스토리 저장
- 검색 이력 저장

#### 7.3. LLM 쿼리 캐시
- Behavioral 조건 추출 결과 캐싱
- 동일 쿼리 재분석 방지

---

## 📦 주요 모듈 구조

### 1. `rag_query_analyzer/` - 쿼리 분석 모듈

#### `analyzers/`
- **`main_analyzer.py`**: 메인 분석기 (여러 분석기 통합)
- **`claude_analyzer.py`**: Claude API 기반 분석
- **`semantic_analyzer.py`**: 의미적 분석
- **`demographic_extractor.py`**: 인구통계 추출
- **`rule_analyzer.py`**: 규칙 기반 분석

#### `core/`
- **`query_optimizer.py`**: 쿼리 최적화 (과거 성능 학습)
- **`query_rewriter.py`**: 쿼리 재작성
- **`semantic_model.py`**: 의미 모델

#### `utils/`
- **`opensearch_query_builder.py`**: OpenSearch 쿼리 빌더
- **`synonym_expander.py`**: 동의어 확장기
- **`reranker.py`**: 결과 재정렬

### 2. `api/search_api.py` - 검색 API

#### 주요 함수:
- **`search_natural_language()`**: 메인 검색 엔드포인트
- **`extract_behavioral_conditions_llm()`**: Behavioral 조건 추출
- **`classify_answer_value_with_llm()`**: 2단계 답변 분류
- **`build_behavioral_filters()`**: Behavioral 필터 생성
- **`extract_all_behaviors_batch()`**: 배치 행동 패턴 추출

#### 주요 클래스:
- **`PanelDataCache`**: 메모리 기반 패널 데이터 캐시
- **`SearchRequest`**: 검색 요청 모델
- **`SearchResponse`**: 검색 응답 모델

### 3. `connectors/` - 외부 서비스 연결

- **`hybrid_searcher.py`**: 하이브리드 검색 빌더
- **`data_fetcher.py`**: OpenSearch 데이터 페처
- **`opensearch_cloud.py`**: OpenSearch 클라이언트
- **`qdrant_helper.py`**: Qdrant 벡터 DB 헬퍼

### 4. `constants/behavior_maps.py` - Behavioral 패턴 정의

- **`BEHAVIORAL_KEYWORD_MAP`**: 모든 행동 패턴 키워드 맵
- 각 패턴별:
  - `question_keywords`: 질문 키워드
  - `answer_values`: 가능한 답변 값
  - `question_text`: 질문 텍스트

---

## 🔍 분석 모듈 상세

### 1. Claude Analyzer (`claude_analyzer.py`)

**역할**: LLM 기반 자연어 이해

**프로세스**:
1. 사용자 쿼리를 Claude에 전달
2. 구조화된 JSON 응답 받기
3. 의도, 키워드, 필터 추출

**출력 예시**:
```json
{
  "intent": "demographic_and_behavioral_search",
  "must_terms": ["직장인", "흡연자"],
  "should_terms": [],
  "demographic_entities": [
    {"type": "age_group", "value": "30대"},
    {"type": "occupation", "value": "사무직"}
  ],
  "behavioral_conditions": {
    "smoker": true
  }
}
```

### 2. Semantic Analyzer (`semantic_analyzer.py`)

**역할**: 의미적 키워드 확장

**기능**:
- 동의어 확장 (예: "커피" → "아메리카노", "에스프레소")
- 의미적 유사어 검색
- Qdrant 기반 동적 동의어 확장

### 3. Demographic Extractor (`demographic_extractor.py`)

**역할**: 인구통계 정보 정확 추출

**추출 항목**:
- `gender`: 남성, 여성
- `age_group`: 20대, 30대, 40대 등
- `region`: 서울, 경기, 부산 등
- `occupation`: 사무직, 전문직, 서비스직 등
- `marital_status`: 기혼, 미혼

**정규화**: 동의어를 표준 값으로 변환

### 4. Behavioral Conditions Extractor

#### 4.1. 1단계: `extract_behavioral_conditions_llm()`
- 쿼리에서 **어떤 질문**과 관련있는지 추출
- `BEHAVIORAL_KEYWORD_MAP`의 키만 반환 (모두 `true`)

#### 4.2. 2단계: `classify_answer_value_with_llm()`
- 각 `behavior_key`에 대해 **구체적 답변 값** 분류
- 단일 선택: `"유럽"` (string)
- 다중 선택: `["2개", "3개", "4개 이상"]` (list)

**예시**:
```python
# 1단계 결과
{"travels": true, "has_pet": true}

# 2단계 결과
{"travels": "유럽", "has_pet": "반려동물을 키우는 중이다"}
```

### 5. Query Optimizer (`query_optimizer.py`)

**역할**: 과거 성능 데이터 기반 최적화

**기능**:
- 쿼리 성능 로깅
- 자동 평가 (결과 품질 측정)
- 최적 `alpha` 파라미터 추천
- 유사 쿼리 기반 파라미터 조정

---

## 🚀 성능 최적화

### 1. 메모리 캐싱
- **PanelDataCache**: 전체 패널 데이터를 메모리에 로드
- Pandas DataFrame으로 벡터화된 필터링
- OpenSearch 쿼리보다 **10-100배 빠름**

### 2. 다중 캐시 레이어
- 메모리 캐시 (TTLCache) → Redis 캐시 → OpenSearch
- 동일 쿼리 재검색 시 즉시 반환

### 3. 병렬 처리
- Scroll API 병렬 처리 (8개 슬라이스)
- 비동기 I/O (asyncio)

### 4. 압축 캐싱
- Redis에 gzip 압축 저장
- 네트워크 대역폭 절약

---

## 📊 데이터 구조

### OpenSearch 인덱스 구조

```json
{
  "user_id": "user_123",
  "metadata": {
    "gender": "남성",
    "age_group": "30대",
    "region": "서울",
    "occupation": "사무직"
  },
  "qa_pairs": [
    {
      "question": "현재 흡연하시나요?",
      "answer": "예"
    },
    {
      "question": "주로 어떤 음료를 드시나요?",
      "answer": "커피"
    }
  ],
  "text": "전체 텍스트 (검색용)",
  "timestamp": "2024-01-01T00:00:00Z"
}
```

### Behavioral 패턴 구조

```python
BEHAVIORAL_KEYWORD_MAP = {
    "smoker": {
        "question_keywords": {"흡연", "담배", "니코틴"},
        "answer_values": {
            "예": ["예", "네", "흡연"],
            "아니오": ["아니오", "비흡연"]
        },
        "question_text": "현재 흡연하시나요?"
    },
    "travels": {
        "question_keywords": {"여행", "해외", "여행지"},
        "answer_values": {
            "유럽": ["유럽", "프랑스", "독일"],
            "아시아": ["일본", "중국", "태국"]
        },
        "question_text": "최근 해외 여행을 다녀오셨나요?"
    }
}
```

---

## 🔧 주요 설정

### 환경 변수
- `OPENSEARCH_URL`: OpenSearch 클러스터 URL
- `QDRANT_URL`: Qdrant 벡터 DB URL
- `REDIS_URL`: Redis 캐시 URL
- `CLAUDE_API_KEY`: Anthropic Claude API 키
- `ENABLE_CLAUDE_ANALYZER`: Claude 분석기 활성화 여부

### 설정 파일
- `rag_query_analyzer/config.py`: 전역 설정
- `constants/behavior_maps.py`: Behavioral 패턴 정의

---

## 📈 모니터링

### 로깅
- 검색 쿼리 로깅
- 성능 메트릭 (응답 시간, 캐시 히트율)
- LLM 호출 로깅

### Redis 저장
- 대화 히스토리 (`conversation:{session_id}`)
- 검색 이력 (`search_history:{user_id}`)

---

## 🎯 주요 기능 요약

1. **자연어 검색**: "30대 직장인 중 흡연자 100명" → 구조화된 검색
2. **하이브리드 검색**: 키워드 + 벡터 검색 결합
3. **2단계 Behavioral 분류**: 질문 추출 → 답변 값 분류
4. **메모리 캐싱**: 초고속 필터링
5. **다중 캐시 레이어**: 메모리 → Redis → OpenSearch
6. **LLM 통합**: Claude 기반 자연어 이해
7. **동의어 확장**: Qdrant 기반 동적 확장
8. **쿼리 최적화**: 과거 성능 기반 자동 조정

---

## 🔄 검색 흐름 다이어그램

```
사용자 쿼리 입력
    ↓
[1] Claude Analyzer (의도, 키워드 추출)
    ↓
[2] Demographic Extractor (인구통계 추출)
    ↓
[3] Behavioral 1단계 (질문 추출)
    ↓
[4] Behavioral 2단계 (답변 값 분류)
    ↓
[5] 하이브리드 검색 (키워드 + 벡터)
    ↓
[6] 필터링 (Demographic + Behavioral)
    ↓
[7] Reranking
    ↓
[8] 결과 후처리 (QA Pairs 추출)
    ↓
[9] LLM Summary (선택적)
    ↓
최종 응답 반환
```

---

## 📝 참고사항

- **OpenSearch 버전**: 2.10+ (RRF 지원)
- **Python 버전**: 3.8+
- **주요 라이브러리**: FastAPI, OpenSearch, SentenceTransformers, Anthropic, Pandas

