"""
Redis 연결 테스트 스크립트

서버의 Redis 캐시 연결 상태를 확인합니다.
"""
import os
import sys
import redis
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

def test_redis_connection():
    """Redis 연결 테스트"""
    print("=" * 60)
    print("🔍 Redis 연결 테스트")
    print("=" * 60)
    
    # 환경 변수에서 Redis 설정 읽기
    redis_host = os.getenv("REDIS_HOST", "redis_cache")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_db = int(os.getenv("REDIS_DB", "0"))
    redis_url = os.getenv("REDIS_URL", f"redis://{redis_host}:{redis_port}/{redis_db}")
    
    print(f"\n📋 Redis 설정:")
    print(f"   REDIS_HOST: {redis_host}")
    print(f"   REDIS_PORT: {redis_port}")
    print(f"   REDIS_DB: {redis_db}")
    print(f"   REDIS_URL: {redis_url}")
    print()
    
    # 방법 1: REDIS_URL로 연결 시도
    print("🔌 방법 1: REDIS_URL로 연결 시도...")
    try:
        client1 = redis.Redis.from_url(redis_url, decode_responses=True, socket_connect_timeout=5)
        result1 = client1.ping()
        if result1:
            print("   ✅ 연결 성공!")
            print(f"   서버 정보: {client1.info('server')}")
            print(f"   메모리 사용량: {client1.info('memory')['used_memory_human']}")
            print(f"   연결된 클라이언트 수: {client1.info('clients')['connected_clients']}")
        else:
            print("   ❌ PING 실패")
    except Exception as e:
        print(f"   ❌ 연결 실패: {e}")
        print(f"   에러 타입: {type(e).__name__}")
        client1 = None
    print()
    
    # 방법 2: 호스트/포트로 직접 연결 시도
    print("🔌 방법 2: 호스트/포트로 직접 연결 시도...")
    try:
        client2 = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=True,
            socket_connect_timeout=5
        )
        result2 = client2.ping()
        if result2:
            print("   ✅ 연결 성공!")
        else:
            print("   ❌ PING 실패")
    except Exception as e:
        print(f"   ❌ 연결 실패: {e}")
        print(f"   에러 타입: {type(e).__name__}")
        client2 = None
    print()
    
    # 방법 3: localhost로 연결 시도 (로컬 환경용)
    if redis_host != "localhost":
        print("🔌 방법 3: localhost로 연결 시도 (로컬 환경용)...")
        try:
            client3 = redis.Redis(
                host="localhost",
                port=redis_port,
                db=redis_db,
                decode_responses=True,
                socket_connect_timeout=5
            )
            result3 = client3.ping()
            if result3:
                print("   ✅ 연결 성공! (localhost에서 실행 중)")
            else:
                print("   ❌ PING 실패")
        except Exception as e:
            print(f"   ❌ 연결 실패: {e}")
            client3 = None
        print()
    
    # 방법 4: 127.0.0.1로 연결 시도
    if redis_host not in ["127.0.0.1", "localhost"]:
        print("🔌 방법 4: 127.0.0.1로 연결 시도...")
        try:
            client4 = redis.Redis(
                host="127.0.0.1",
                port=redis_port,
                db=redis_db,
                decode_responses=True,
                socket_connect_timeout=5
            )
            result4 = client4.ping()
            if result4:
                print("   ✅ 연결 성공! (127.0.0.1에서 실행 중)")
            else:
                print("   ❌ PING 실패")
        except Exception as e:
            print(f"   ❌ 연결 실패: {e}")
            client4 = None
        print()
    
    # 방법 5: 서버 IP로 직접 연결 시도 (34.87.184.111)
    server_ip = "34.87.184.111"
    print(f"🔌 방법 5: 서버 IP로 직접 연결 시도 ({server_ip})...")
    try:
        client5 = redis.Redis(
            host=server_ip,
            port=6379,
            db=0,
            decode_responses=True,
            socket_connect_timeout=5
        )
        result5 = client5.ping()
        if result5:
            print("   ✅ 연결 성공! (서버 IP에서 실행 중)")
            print(f"   서버 정보: {client5.info('server')}")
            print(f"   메모리 사용량: {client5.info('memory')['used_memory_human']}")
            print(f"   연결된 클라이언트 수: {client5.info('clients')['connected_clients']}")
            
            # 읽기/쓰기 테스트
            test_key = "test:server_connection"
            test_value = "Hello from local!"
            client5.set(test_key, test_value, ex=10)
            read_value = client5.get(test_key)
            if read_value == test_value:
                print(f"   ✅ 읽기/쓰기 테스트 성공: {test_key} = {read_value}")
            client5.delete(test_key)
        else:
            print("   ❌ PING 실패")
            client5 = None
    except Exception as e:
        print(f"   ❌ 연결 실패: {e}")
        print(f"   에러 타입: {type(e).__name__}")
        client5 = None
    print()
    
    # 테스트: 간단한 읽기/쓰기
    if client1:
        print("🧪 읽기/쓰기 테스트...")
        try:
            test_key = "test:connection"
            test_value = "Hello Redis!"
            
            # 쓰기
            client1.set(test_key, test_value, ex=10)  # 10초 후 만료
            print(f"   ✅ 쓰기 성공: {test_key} = {test_value}")
            
            # 읽기
            read_value = client1.get(test_key)
            if read_value == test_value:
                print(f"   ✅ 읽기 성공: {test_key} = {read_value}")
            else:
                print(f"   ⚠️ 읽기 불일치: 예상={test_value}, 실제={read_value}")
            
            # 삭제
            client1.delete(test_key)
            print(f"   ✅ 삭제 성공: {test_key}")
            
        except Exception as e:
            print(f"   ❌ 읽기/쓰기 실패: {e}")
        print()
    
    # 요약
    print("=" * 60)
    print("📊 테스트 요약")
    print("=" * 60)
    
    if client1 and client1.ping():
        print("✅ Redis 연결 정상!")
        print(f"   사용 가능한 연결 방법: REDIS_URL ({redis_url})")
        return True
    elif client2 and client2.ping():
        print("✅ Redis 연결 정상!")
        print(f"   사용 가능한 연결 방법: 호스트/포트 ({redis_host}:{redis_port})")
        return True
    else:
        print("❌ Redis 연결 실패!")
        print("\n💡 해결 방법:")
        print("   1. Redis 서버가 실행 중인지 확인: docker ps | grep redis")
        print("   2. 환경 변수 확인: echo $REDIS_HOST $REDIS_PORT")
        print("   3. 네트워크 확인: 같은 Docker 네트워크에 있는지 확인")
        print("   4. 방화벽 확인: 포트 6379가 열려있는지 확인")
        return False

if __name__ == "__main__":
    success = test_redis_connection()
    sys.exit(0 if success else 1)

