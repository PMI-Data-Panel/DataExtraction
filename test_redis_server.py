"""
서버 Redis 직접 연결 테스트

서버 IP(34.87.184.111)로 Redis에 직접 연결하여 테스트합니다.
"""
import redis

# 서버 Redis에 직접 연결
server_ip = "34.87.184.111"
server_port = 6379
server_db = 0

print("=" * 60)
print("🔍 서버 Redis 직접 연결 테스트")
print("=" * 60)
print(f"\n📋 연결 정보:")
print(f"   서버 IP: {server_ip}")
print(f"   포트: {server_port}")
print(f"   DB: {server_db}")
print()

try:
    client = redis.Redis(
        host=server_ip,
        port=server_port,
        db=server_db,
        decode_responses=True,
        socket_connect_timeout=5
    )
    
    # 연결 테스트
    print("🔌 연결 시도 중...")
    response = client.ping()
    print(f"✅ Redis 연결 성공: {response}")
    
    # 서버 정보 출력
    server_info = client.info('server')
    memory_info = client.info('memory')
    clients_info = client.info('clients')
    
    print(f"\n📊 서버 정보:")
    print(f"   Redis 버전: {server_info.get('redis_version', 'N/A')}")
    print(f"   메모리 사용량: {memory_info.get('used_memory_human', 'N/A')}")
    print(f"   연결된 클라이언트 수: {clients_info.get('connected_clients', 'N/A')}")
    
    # 간단한 읽기/쓰기 테스트
    print(f"\n🧪 읽기/쓰기 테스트...")
    test_key = 'test_key'
    test_value = 'Hello from local!'
    
    # 쓰기
    client.set(test_key, test_value, ex=10)  # 10초 후 만료
    print(f"   ✅ 쓰기 성공: {test_key} = {test_value}")
    
    # 읽기
    value = client.get(test_key)
    if value == test_value:
        print(f"   ✅ 읽기 성공: {test_key} = {value}")
    else:
        print(f"   ⚠️ 읽기 불일치: 예상={test_value}, 실제={value}")
    
    # 삭제
    client.delete(test_key)
    print(f"   ✅ 삭제 성공: {test_key}")
    
    print("\n" + "=" * 60)
    print("✅ 모든 테스트 통과!")
    print("=" * 60)
    print(f"\n💡 이 IP로 연결하려면 .env 파일에 다음을 추가하세요:")
    print(f"   REDIS_HOST={server_ip}")
    print(f"   REDIS_PORT={server_port}")
    print(f"   REDIS_URL=redis://{server_ip}:{server_port}/{server_db}")
    
except redis.ConnectionError as e:
    print(f"❌ Redis 연결 실패: {e}")
    print(f"   에러 타입: {type(e).__name__}")
    print("\n💡 확인 사항:")
    print("   1. 서버 IP가 올바른지 확인")
    print("   2. 포트 6379가 열려있는지 확인")
    print("   3. 방화벽 설정 확인")
    print("   4. 네트워크 연결 확인")
except Exception as e:
    print(f"❌ 오류 발생: {e}")
    print(f"   에러 타입: {type(e).__name__}")



