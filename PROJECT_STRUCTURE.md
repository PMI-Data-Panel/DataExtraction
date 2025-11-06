# 프로젝트 구조 (리팩토링 완료)

## 📁 디렉토리 구조

```
DataExtraction/
├── 📦 connectors/              # 클라우드 데이터베이스 연결
│   ├── __init__.py
│   ├── opensearch_cloud.py     # OpenSearch 연결 및 인덱스 관리
│   ├── hybrid_searcher.py      # 하이브리드 검색 쿼리 빌더 (RRF)
│   ├── data_fetcher.py         # 통합 데이터 페처
│   └── qdrant_cloud.py         # Qdrant 연결 (향후 구현)
│
├── 🌐 api/                     # FastAPI 엔드포인트
│   ├── __init__.py
│   ├── main_api.py             # 메인 애플리케이션 (초기화, 기본 엔드포인트)
│   ├── search_api.py           # 검색 API (/search/*)
│   └── visualization_api.py    # 시각화 API (/visualization/*)
│
├── 🔧 rag_query_analyzer/      # 쿼리 분석 엔진
│   ├── __init__.py
│   ├── config.py               # 설정 관리
│   ├── data_processing.py      # 데이터 전처리
│   │
│   ├── 🧠 analyzers/           # 쿼리 분석기
│   │   ├── __init__.py
│   │   ├── base.py             # 베이스 클래스
│   │   ├── rule_analyzer.py    # 규칙 기반 분석
│   │   ├── semantic_analyzer.py # 의미 기반 분석
│   │   ├── claude_analyzer.py  # LLM 기반 분석 (Claude)
│   │   └── main_analyzer.py    # 통합 분석기
│   │
│   ├── 🎯 core/                # 핵심 컴포넌트
│   │   ├── __init__.py
│   │   ├── query_complexity.py # 쿼리 복잡도 평가
│   │   ├── query_expander.py   # 쿼리 확장
│   │   ├── query_optimizer.py  # 쿼리 최적화
│   │   ├── query_rewriter.py   # 쿼리 재작성
│   │   ├── semantic_model.py   # 도메인 의미 모델
│   │   └── cache.py            # 캐싱
│   │
│   ├── 📊 models/              # 데이터 모델
│   │   ├── __init__.py
│   │   ├── query.py            # QueryAnalysis, SearchResult
│   │   ├── entities.py         # 엔티티 정의
│   │   └── logs.py             # 로깅 모델
│   │
│   └── 🛠️ utils/               # 유틸리티
│       ├── __init__.py
│       ├── logger.py           # 로깅 설정
│       ├── reranker.py         # 결과 재순위화
│       └── embedding_utils.py  # 임베딩 유틸리티
│
├── 📦 indexer/                 # 데이터 인덱싱
│   ├── __init__.py
│   ├── router.py               # FastAPI 라우터 (인덱싱 엔드포인트)
│   ├── core.py                 # 코어 인덱싱 로직
│   ├── opensearch.py           # 인덱스 스키마 관리
│   └── parser.py               # CSV 파싱
│
├── 📜 scripts/                 # 유틸리티 스크립트
│   ├── __init__.py
│   ├── sync_cloud_data.py      # 클라우드 데이터 동기화
│   ├── test_cloud_connection.py # 연결 테스트
│   ├── check_mapping.py        # 매핑 확인
│   ├── check_remote_opensearch.py # 원격 연결 확인
│   ├── create_dummy_data.py    # 더미 데이터 생성
│   ├── extract_data.py         # 데이터 추출
│   ├── generate_dashboard_query.py # 대시보드 쿼리 생성
│   ├── quick_test.py           # 빠른 테스트
│   ├── reset_index.py          # 인덱스 초기화
│   ├── test_indexer.py         # 인덱서 테스트
│   ├── test_setup.py           # 설정 검증
│   └── validate_data_format.py # 데이터 형식 검증
│
├── 📄 data/                    # 데이터 파일
│   ├── question_list.csv       # 질문 메타데이터
│   └── response_list.csv       # 응답 데이터
│
├── 📝 문서
│   ├── README_OPENSEARCH.md
│   ├── KURE-v1_SETUP_GUIDE.md
│   ├── OPENSEARCH_DASHBOARDS_GUIDE.md
│   ├── FULL_TEST.md
│   ├── QUICK_START.md
│   └── PROJECT_STRUCTURE.md    # 이 파일
│
├── main.py                     # 메인 실행 파일 (api/main_api.py 호출)
├── start_server.py             # 서버 시작 스크립트
├── demo.py                     # 데모 스크립트
├── requirements.txt            # Python 패키지 의존성
├── .env                        # 환경 변수 (gitignored)
├── .env.example                # 환경 변수 템플릿
├── docker-compose.yml          # Docker 설정
└── Dockerfile                  # 컨테이너 정의
```

## 🔄 주요 변경 사항

### 1. **connectors/** (신규 생성)
- **목적**: OpenSearch, Qdrant 등 클라우드 데이터베이스 연결 통합 관리
- **이전 위치**: `rag_query_analyzer/utils/opensearch_*.py`
- **주요 파일**:
  - `opensearch_cloud.py`: OpenSearch Cloud + CRAG + AWS OpenSearch Service 지원
  - `qdrant_cloud.py`: Qdrant 벡터 DB 연결 (http://104.248.144.17:6333)
  - `hybrid_searcher.py`: RRF 기반 하이브리드 검색 쿼리 빌더
  - `data_fetcher.py`: 데이터 조회 통합 인터페이스

### 2. **api/** (신규 생성)
- **목적**: FastAPI 엔드포인트를 기능별로 분리
- **이전**: `main.py`에 모든 엔드포인트가 집중
- **주요 파일**:
  - `main_api.py`: 애플리케이션 초기화 및 기본 엔드포인트
  - `search_api.py`: 검색 관련 엔드포인트 (`/search/*`)
  - `visualization_api.py`: 시각화 엔드포인트 (`/visualization/*`)

### 3. **scripts/** (정리)
- **목적**: 유틸리티 스크립트를 한 곳에 정리
- **이전**: 루트 디렉토리에 20+ 스크립트 파일 산재
- **신규 스크립트**:
  - `sync_cloud_data.py`: 클라우드 데이터 동기화
  - `test_cloud_connection.py`: 연결 테스트

### 4. **rag_query_analyzer/utils/** (축소)
- **제거된 파일**: `opensearch_utils.py`, `opensearch_query_builder.py`, `remote_query_builder.py`
- **이동 위치**: `connectors/` 모듈로 이동
- **유지된 파일**: 범용 유틸리티만 유지 (`logger.py`, `reranker.py`, `embedding_utils.py`)

## 🚀 실행 방법

### 개발 서버 시작
```bash
# 방법 1: start_server.py 사용 (포트 자동 감지)
python start_server.py

# 방법 2: main.py 직접 실행
python main.py

# 방법 3: uvicorn 직접 실행
python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### 스크립트 실행
```bash
# 연결 테스트
python scripts/test_cloud_connection.py

# 데이터 동기화 (향후 구현)
python scripts/sync_cloud_data.py --local-index s_welcome_2nd --direction local-to-remote

# 인덱스 초기화
python scripts/reset_index.py
```

## 📡 API 엔드포인트

### 기본 엔드포인트
- `GET /` - API 환영 메시지
- `GET /health` - 헬스 체크
- `GET /system-status` - 시스템 상태 확인

### 인덱싱 (`/indexer`)
- `POST /indexer/index-survey-data` - 설문 데이터 색인
- `DELETE /indexer/index/{index_name}` - 인덱스 삭제

### 검색 (`/search`)
- `POST /search/query` - 검색 쿼리 실행
- `POST /search/similar` - 유사 문서 검색 (향후 구현)
- `GET /search/stats/{index_name}` - 검색 통계

### 시각화 (`/visualization`) - 향후 구현
- `GET /visualization/demographics/{index_name}` - 인구통계 분포
- `GET /visualization/word-cloud/{index_name}` - 워드 클라우드
- `GET /visualization/sentiment/{index_name}` - 감정 분석

### 문서
- `GET /docs` - Swagger UI
- `GET /redoc` - ReDoc

## 🔧 설정 파일

### `.env` (환경 변수)
```env
# OpenSearch 설정
OPENSEARCH_HOST=localhost
OPENSEARCH_PORT=9200
OPENSEARCH_USER=admin
OPENSEARCH_PASSWORD=Admin@1234
OPENSEARCH_USE_SSL=false

# 임베딩 모델
EMBEDDING_MODEL=dragonkue/KURE-v1
EMBEDDING_DIM=1024

# kNN 설정
HNSW_M=16
HNSW_EF_CONSTRUCTION=512
VECTOR_ENGINE=nmslib
```

## 📦 의존성

### 핵심 패키지
- **FastAPI**: 웹 프레임워크
- **opensearch-py**: OpenSearch 클라이언트
- **sentence-transformers**: KURE-v1 임베딩 모델
- **pandas**: 데이터 처리
- **torch**: ML 모델 지원
- **python-dotenv**: 환경 변수 관리

### 설치
```bash
pip install -r requirements.txt
```

## 🧪 테스트

### 연결 테스트
```bash
python scripts/test_cloud_connection.py --service opensearch
```

### API 테스트
```bash
# 서버 시작 후
curl http://localhost:8000/health
curl http://localhost:8000/system-status
```

### 검색 테스트
```bash
curl -X POST "http://localhost:8000/search/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "20대 남성의 AI에 대한 생각",
    "index_name": "s_welcome_2nd",
    "size": 10
  }'
```

## 📚 추가 문서

- [OpenSearch 설정](README_OPENSEARCH.md)
- [KURE-v1 모델 설정](KURE-v1_SETUP_GUIDE.md)
- [전체 테스트 가이드](FULL_TEST.md)
- [빠른 시작](QUICK_START.md)
- [OpenSearch Dashboards 가이드](OPENSEARCH_DASHBOARDS_GUIDE.md)

## 🔮 향후 계획

### Phase 1: 검색 기능 강화
- [ ] 고급 필터링 (복합 조건)
- [ ] 결과 집계 및 통계
- [ ] 유사 문서 검색

### Phase 2: 시각화
- [ ] 인구통계 분포 차트
- [ ] 워드 클라우드
- [ ] 감정 분석 대시보드
- [ ] 트렌드 분석

### Phase 3: 클라우드 통합
- [x] Qdrant 연동 (완료: http://104.248.144.17:6333)
- [x] 원격 OpenSearch 연결 (완료: 159.223.47.188:9200)
- [ ] AWS OpenSearch Service 지원 (구현 완료, 테스트 대기)
- [ ] 멀티 클라우드 동기화

### Phase 4: 성능 최적화
- [ ] 캐싱 전략 고도화
- [ ] 쿼리 최적화
- [ ] 배치 처리 개선
