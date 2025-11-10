# redis_celery/celeryconfig.py

from kombu import Queue, Exchange

# ==========================================
# Worker 설정 (프로덕션 환경 목표)
# ==========================================

# Worker 동시성 설정 (Docker Compose에서 큐별로 명시하므로, 여기서만 설정하고 command에서 덮어쓸 수 있습니다)
worker_concurrency = 8  
worker_pool = 'prefork'

# 메모리 관리 (200MB 초과 시 재시작)
worker_max_memory_per_child = 200000 
worker_max_tasks_per_child = 1000  

# ==========================================
# Task 라우팅 (우선순위 큐 정의)
# ==========================================

task_default_exchange = 'tasks'
task_default_exchange_type = 'topic'

# 큐 정의: Docker Compose의 Worker 수에 맞게 분리
task_queues = (
    # 고우선순위: 단일 인덱스 검색 (빠른 응답)
    Queue('search_high', Exchange('tasks'), routing_key='search.high', priority=10),
    
    # 중간우선순위: 전체 인덱스 오케스트레이터
    Queue('search_medium', Exchange('tasks'), routing_key='search.medium', priority=5),
    
    # 저우선순위: RRF 결합 및 캐싱 (백그라운드)
    Queue('search_low', Exchange('tasks'), routing_key='search.low', priority=1),
)

# Task 라우팅 규칙: tasks.py에 정의된 Task 이름을 큐에 연결
task_routes = {
    # 단일 인덱스 검색 (고우선순위)
    'tasks.search_single_index': {
        'queue': 'search_high',
        'routing_key': 'search.high',
    },
    # 전체 검색 오케스트레이터 (중간)
    'tasks.parallel_hybrid_search_orchestrator': {
        'queue': 'search_medium',
        'routing_key': 'search.medium',
    },
    # RRF 결합 (저우선순위)
    'tasks.combine_results': {
        'queue': 'search_low',
        'routing_key': 'search.low',
    },
}

result_backend_transport_options = {
    'master_name': 'mymaster',  # Redis Sentinel (고가용성)
    'socket_keepalive': True,
    'socket_keepalive_options': {
        'TCP_KEEPIDLE': 60,
        'TCP_KEEPINTVL': 10,
        'TCP_KEEPCNT': 3
    }
}
# ==========================================
# 타임아웃 및 모니터링 설정
# ==========================================

task_soft_time_limit = 120  # 2분 경고
task_time_limit = 180       # 3분 강제 종료
result_expires = 3600

# ✅ Chord 콜백이 실패해도 메인 Task 결과는 유지
task_track_started = True
task_acks_late = True  # Task 완료 후 ACK (재시도 안전)

worker_send_task_events = True
task_send_sent_event = True