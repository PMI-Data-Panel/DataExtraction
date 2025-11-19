# 시각화 가이드

새로 생성된 `survey_qa_analysis` 인덱스를 사용한 시각화 방법입니다.

## API 엔드포인트

### 1. 질문 필드 목록 조회

사용 가능한 모든 질문 필드를 조회합니다.

```http
GET /visualization/qa/questions
```

**응답 예시:**
```json
{
  "questions": [
    {"field": "q_marriage", "description": "결혼여부"},
    {"field": "q_education", "description": "최종학력"},
    {"field": "q_job", "description": "직업"},
    {"field": "q_appliances", "description": "보유가전제품"},
    ...
  ]
}
```

### 2. 질문별 답변 분포 조회

특정 질문에 대한 답변 분포를 조회합니다.

```http
GET /visualization/qa/question/{question_field}
```

**예시:**
```http
GET /visualization/qa/question/q_marriage
GET /visualization/qa/question/q_education
GET /visualization/qa/question/q_appliances
```

**응답 예시:**
```json
{
  "question_field": "q_marriage",
  "total_responses": 33427,
  "answer_distribution": [
    {
      "answer": "기혼",
      "count": 20000,
      "percentage": 59.85
    },
    {
      "answer": "미혼",
      "count": 13427,
      "percentage": 40.15
    }
  ]
}
```

### 3. 필터링된 통계 조회

인구통계나 질문 답변으로 필터링한 통계를 조회합니다.

```http
GET /visualization/qa/filtered-stats?gender=남성&age_group=30대
GET /visualization/qa/filtered-stats?question_field=q_marriage&question_value=기혼
```

**쿼리 파라미터:**
- `gender`: 성별 (남성, 여성)
- `age_group`: 나이대 (20대, 30대, 40대, 50대 등)
- `region`: 지역 (서울, 부산, 경기 등)
- `question_field`: 질문 필드명 (예: q_marriage)
- `question_value`: 질문 답변 값 (예: 기혼)

**응답 예시:**
```json
{
  "total_count": 5000,
  "gender_distribution": [
    {"label": "남성", "value": 3000, "percentage": 60.0},
    {"label": "여성", "value": 2000, "percentage": 40.0}
  ],
  "age_group_distribution": [
    {"label": "30대", "value": 5000, "percentage": 100.0}
  ],
  "region_distribution": [
    {"label": "서울", "value": 2000, "percentage": 40.0},
    {"label": "경기", "value": 1500, "percentage": 30.0},
    ...
  ]
}
```

### 4. 교차 분석

두 질문 간의 교차 분석을 수행합니다.

```http
GET /visualization/qa/cross-analysis?question_field1=q_marriage&question_field2=q_education
```

**응답 예시:**
```json
{
  "question_field1": "q_marriage",
  "question_field2": "q_education",
  "cross_analysis": [
    {
      "field1_value": "기혼",
      "field1_count": 20000,
      "field2_distribution": [
        {"label": "대학교 졸업", "value": 12000, "percentage": 60.0},
        {"label": "고등학교 졸업", "value": 5000, "percentage": 25.0},
        ...
      ]
    },
    {
      "field1_value": "미혼",
      "field1_count": 13427,
      "field2_distribution": [
        {"label": "대학교 졸업", "value": 10000, "percentage": 74.5},
        ...
      ]
    }
  ]
}
```

## 프론트엔드 사용 예시

### React/Vue 예시

```javascript
// 질문 목록 가져오기
const fetchQuestions = async () => {
  const response = await fetch('https://34.87.184.111/visualization/qa/questions');
  const data = await response.json();
  return data.questions;
};

// 결혼여부 분포 가져오기
const fetchMarriageDistribution = async () => {
  const response = await fetch('https://34.87.184.111/visualization/qa/question/q_marriage');
  const data = await response.json();
  return data.answer_distribution;
};

// 필터링된 통계 가져오기
const fetchFilteredStats = async (filters) => {
  const params = new URLSearchParams(filters);
  const response = await fetch(`https://34.87.184.111/visualization/qa/filtered-stats?${params}`);
  const data = await response.json();
  return data;
};
```

### Chart.js를 사용한 시각화 예시

```javascript
// 파이 차트 예시
const renderPieChart = async (questionField) => {
  const response = await fetch(`https://34.87.184.111/visualization/qa/question/${questionField}`);
  const data = await response.json();
  
  const chartData = {
    labels: data.answer_distribution.map(item => item.answer),
    datasets: [{
      data: data.answer_distribution.map(item => item.count),
      backgroundColor: ['#FF6384', '#36A2EB', '#FFCE56', ...]
    }]
  };
  
  new Chart(ctx, {
    type: 'pie',
    data: chartData
  });
};

// 막대 차트 예시
const renderBarChart = async (questionField) => {
  const response = await fetch(`https://34.87.184.111/visualization/qa/question/${questionField}`);
  const data = await response.json();
  
  const chartData = {
    labels: data.answer_distribution.map(item => item.answer),
    datasets: [{
      label: '응답 수',
      data: data.answer_distribution.map(item => item.count),
      backgroundColor: '#36A2EB'
    }]
  };
  
  new Chart(ctx, {
    type: 'bar',
    data: chartData
  });
};
```

## 주요 질문 필드

- `q_marriage`: 결혼여부
- `q_education`: 최종학력
- `q_job`: 직업
- `q_job_role`: 직무
- `q_personal_income`: 월평균 개인소득
- `q_household_income`: 월평균 가구소득
- `q_appliances`: 보유가전제품 (배열)
- `q_phone_brand`: 보유 휴대폰 브랜드
- `q_phone_model`: 보유 휴대폰 모델
- `q_car_owned`: 보유차량여부
- `q_car_brand`: 자동차 제조사
- `q_car_model`: 자동차 모델
- `q_smoke_type`: 흡연경험 (배열)
- `q_smoke_brand`: 흡연경험 담배브랜드 (배열)
- `q_drink_type`: 음용경험 술 (배열)

## 인구통계 필터 필드

- `meta_gender`: 성별
- `meta_age_group`: 나이대
- `meta_region`: 지역
- `meta_sub_region`: 세부 지역

## API 문서

실행 중인 서버에서 다음 URL로 API 문서를 확인할 수 있습니다:

- Swagger UI: `https://34.87.184.111/docs`
- ReDoc: `https://34.87.184.111/redoc`

## 주의사항

1. **배열 필드**: `q_appliances`, `q_smoke_type`, `q_drink_type` 등은 배열 형태이므로 집계 시 주의가 필요합니다.
2. **인덱스 이름**: 기본값은 `survey_qa_analysis`입니다. 다른 인덱스를 사용하려면 `index_name` 파라미터를 지정하세요.
3. **HTTPS**: 프로덕션 환경에서는 HTTPS를 사용하세요 (`https://34.87.184.111`).

