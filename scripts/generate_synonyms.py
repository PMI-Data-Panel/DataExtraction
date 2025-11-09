"""
동의어 사전 생성 스크립트 (1회만 실행)

실행 방법:
    python scripts/generate_synonyms.py

비용: $0.5 (Claude Haiku 기준, 50개 term)
"""
import json
import asyncio
from anthropic import AsyncAnthropic
import os
from typing import List, Dict
import sys
from pathlib import Path

# 프로젝트 루트를 경로에 추가
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Anthropic API 키 설정
client = AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

async def expand_term_with_llm(term: str, category: str) -> List[str]:
    """
    LLM으로 동의어 확장
    
    Args:
        term: 확장할 용어 (예: "30대")
        category: 카테고리 (age, gender, occupation)
    """
    
    # 카테고리별 프롬프트 커스터마이징
    context_map = {
        "age": "연령대",
        "gender": "성별",
        "occupation": "직업",
        "region": "지역",
        "education": "학력",
        "marital_status": "결혼여부"
    }
    
    context = context_map.get(category, "항목")
    
    prompt = f"""당신은 한국 설문조사 데이터 전문가입니다.
설문조사 응답에서 "{term}"과 동일한 의미로 사용되는 표현들을 찾아주세요.

맥락: {context} 관련 응답

요구사항:
1. 정확한 동의어만 포함 (유사어 제외)
2. 설문 응답에서 실제 사용되는 표현
3. 최대 7개
4. 원본 term 포함
5. JSON 배열 형식

예시:
입력: "30대" (연령대)
출력: ["30대", "삼십대", "30-39세", "30~39세"]

입력: "남성" (성별)
출력: ["남성", "남자", "남", "M", "male"]

입력: "사무직" (직업)
출력: ["사무직", "사무원", "사무 종사자", "회사원", "화이트칼라"]

입력: "{term}" ({context})
출력:"""

    try:
        message = await client.messages.create(
            model="claude-3-5-haiku-20241022",  # 가장 저렴
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}]
        )
        
        response_text = message.content[0].text.strip()
        
        # JSON 파싱 시도
        try:
            # JSON 배열만 추출 (앞뒤 텍스트 제거)
            start = response_text.find('[')
            end = response_text.rfind(']') + 1
            if start >= 0 and end > start:
                json_str = response_text[start:end]
                synonyms = json.loads(json_str)
                
                # 검증
                if not isinstance(synonyms, list):
                    raise ValueError("Not a list")
                
                # 원본 term이 없으면 추가
                if term not in synonyms:
                    synonyms.insert(0, term)
                
                return synonyms
            else:
                raise ValueError("No JSON array found")
        
        except (json.JSONDecodeError, ValueError) as e:
            print(f"⚠️  JSON 파싱 실패: {term} - {e}")
            print(f"   응답: {response_text}")
            return [term]  # 실패 시 원본만
    
    except Exception as e:
        print(f"❌ LLM 호출 실패: {term} - {e}")
        return [term]

async def generate_synonym_dictionary():
    """동의어 사전 생성"""
    
    # 핵심 용어 정의
    terms_by_category = {
        "age": [
            "10대", "20대", "30대", "40대", "50대", "60대", "70대",
            "10대 미만", "80대 이상"
        ],
        "gender": [
            "남성", "여성", "남자", "여자"
        ],
        "occupation": [
            "사무직", "전문직", "서비스직", "학생", "주부", 
            "자영업", "경영관리직", "생산직", "판매직",
            "프리랜서", "무직", "은퇴", "대학생", "대학원생"
        ],
        "region": [
            "서울", "부산", "대구", "인천", "광주", "대전", "울산",
            "경기", "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주"
        ],
        "education": [
            "초등학교", "중학교", "고등학교", "대학교", "대학원",
            "무학", "박사"
        ],
        "marital_status": [
            "미혼", "기혼", "이혼", "사별"
        ]
    }
    
    synonym_dict = {}
    total_terms = sum(len(terms) for terms in terms_by_category.values())
    processed = 0
    
    print(f"📚 동의어 사전 생성 시작 (총 {total_terms}개 term)")
    print("=" * 60)
    
    # 카테고리별 처리
    for category, terms in terms_by_category.items():
        print(f"\n🔹 {category.upper()} ({len(terms)}개)")
        
        for term in terms:
            processed += 1
            print(f"   [{processed}/{total_terms}] {term}...", end=" ", flush=True)
            
            synonyms = await expand_term_with_llm(term, category)
            synonym_dict[term] = synonyms
            
            print(f"✅ {len(synonyms)}개")
            
            # API 레이트 리밋 방지
            await asyncio.sleep(0.5)
    
    # JSON 파일로 저장
    output_path = PROJECT_ROOT / "config" / "synonyms.json"
    output_path.parent.mkdir(exist_ok=True)
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(synonym_dict, f, ensure_ascii=False, indent=2)
    
    print("\n" + "=" * 60)
    print(f"✅ 동의어 사전 생성 완료!")
    print(f"📁 저장 위치: {output_path}")
    print(f"📊 총 {len(synonym_dict)}개 term")
    print(f"💰 예상 비용: ${processed * 0.01:.2f}")
    
    # 통계
    total_synonyms = sum(len(syns) for syns in synonym_dict.values())
    avg_synonyms = total_synonyms / len(synonym_dict) if len(synonym_dict) > 0 else 0
    print(f"📈 평균 동의어 수: {avg_synonyms:.1f}개")

async def preview_sample():
    """샘플 미리보기"""
    print("\n🔍 샘플 생성 (3개)")
    print("=" * 60)
    
    samples = [
        ("30대", "age"),
        ("사무직", "occupation"),
        ("서울", "region")
    ]
    
    for term, category in samples:
        synonyms = await expand_term_with_llm(term, category)
        print(f"\n{term} ({category}):")
        print(f"  → {synonyms}")
        await asyncio.sleep(0.5)

if __name__ == "__main__":
    print("🚀 동의어 사전 생성기")
    print("=" * 60)
    
    if len(sys.argv) > 1 and sys.argv[1] == "--preview":
        # 미리보기 모드
        asyncio.run(preview_sample())
    else:
        # 전체 생성
        print("⚠️  주의: Claude API를 사용합니다 (비용 발생)")
        print(f"   예상 비용: ~$0.50")
        print()
        
        response = input("계속하시겠습니까? (y/N): ")
        if response.lower() == 'y':
            asyncio.run(generate_synonym_dictionary())
        else:
            print("❌ 취소됨")

