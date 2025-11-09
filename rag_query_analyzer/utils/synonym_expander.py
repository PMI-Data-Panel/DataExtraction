"""
동의어 확장기 (정적 사전 + Qdrant 동적 확장)
"""
import json
import logging
from typing import List, Dict, Optional, Set
from pathlib import Path
from functools import lru_cache

logger = logging.getLogger(__name__)

class StaticSynonymExpander:
    """
    정적 동의어 확장기
    
    특징:
    - 오프라인 사전 기반 (무료, 빠름)
    - LLM 호출 없음
    - 사전에 없는 term은 원본 반환
    """
    
    def __init__(self, synonym_file: Optional[str] = None):
        if synonym_file is None:
            # 기본 경로: 프로젝트 루트/config/synonyms.json
            project_root = Path(__file__).parent.parent.parent
            synonym_file = project_root / "config" / "synonyms.json"
        
        self.synonym_file = Path(synonym_file)
        self.synonyms: Dict[str, List[str]] = {}
        self._load_synonyms()
    
    def _load_synonyms(self):
        """동의어 사전 로드"""
        if not self.synonym_file.exists():
            logger.warning(f"⚠️  동의어 사전 없음: {self.synonym_file}")
            logger.info(f"   생성 방법: python scripts/generate_synonyms.py")
            return
        
        try:
            with open(self.synonym_file, 'r', encoding='utf-8') as f:
                self.synonyms = json.load(f)
            
            total_terms = len(self.synonyms)
            total_synonyms = sum(len(syns) for syns in self.synonyms.values())
            avg_synonyms = total_synonyms / total_terms if total_terms > 0 else 0
            
            logger.info(f"✅ 동의어 사전 로드 완료")
            logger.info(f"   - Terms: {total_terms}개")
            logger.info(f"   - 평균 동의어: {avg_synonyms:.1f}개")
        
        except Exception as e:
            logger.error(f"❌ 동의어 사전 로드 실패: {e}")
            self.synonyms = {}
    
    def expand(self, term: str) -> List[str]:
        """
        동의어 확장
        
        Args:
            term: 확장할 용어
        
        Returns:
            동의어 리스트 (사전에 없으면 [term])
        """
        if term in self.synonyms:
            logger.debug(f"✅ {term} → {self.synonyms[term]}")
            return self.synonyms[term]
        else:
            logger.debug(f"⚠️  사전에 없음: {term} (원본 사용)")
            return [term]
    
    def expand_multiple(self, terms: List[str]) -> List[str]:
        """
        여러 term 확장 (중복 제거)
        
        Args:
            terms: 확장할 용어 리스트
        
        Returns:
            확장된 동의어 리스트 (중복 제거)
        """
        all_synonyms = []
        for term in terms:
            all_synonyms.extend(self.expand(term))
        
        # 중복 제거하면서 순서 유지
        seen = set()
        unique = []
        for syn in all_synonyms:
            if syn not in seen:
                seen.add(syn)
                unique.append(syn)
        
        return unique
    
    def has_term(self, term: str) -> bool:
        """사전에 term이 있는지 확인"""
        return term in self.synonyms
    
    def add_custom_synonym(self, term: str, synonyms: List[str]):
        """
        커스텀 동의어 추가 (런타임)
        
        사전 파일은 수정하지 않음 (메모리만)
        """
        self.synonyms[term] = synonyms
        logger.info(f"➕ 커스텀 동의어 추가: {term} → {synonyms}")
    
    def get_stats(self) -> Dict:
        """통계 반환"""
        total_terms = len(self.synonyms)
        total_synonyms = sum(len(syns) for syns in self.synonyms.values())
        
        return {
            "total_terms": total_terms,
            "total_synonyms": total_synonyms,
            "avg_synonyms": total_synonyms / total_terms if total_terms > 0 else 0,
            "loaded_from": str(self.synonym_file)
        }

class HybridSynonymExpander:
    """
    하이브리드 동의어 확장기
    
    특징:
    1. 정적 사전 우선 확인 (빠름, 무료)
    2. 정적 사전에 없으면 Qdrant 동적 확장 (유사 벡터 검색)
    3. 동적 확장 결과 캐싱 (성능 최적화)
    """
    
    def __init__(
        self,
        static_expander: StaticSynonymExpander = None,
        qdrant_client = None,
        embedding_model = None,
        cache_size: int = 1000
    ):
        """
        Args:
            static_expander: 정적 동의어 확장기
            qdrant_client: Qdrant 클라이언트 (동적 확장용)
            embedding_model: 임베딩 모델 (동적 확장용)
            cache_size: 캐시 크기 (LRU)
        """
        self.static_expander = static_expander or StaticSynonymExpander()
        self.qdrant_client = qdrant_client
        self.embedding_model = embedding_model
        self.dynamic_cache: Dict[str, List[str]] = {}  # 동적 확장 결과 캐시
        self.cache_size = cache_size
        
        # 동적 확장 설정
        self.dynamic_enabled = qdrant_client is not None and embedding_model is not None
        self.dynamic_limit = 5  # Qdrant에서 가져올 최대 동의어 수
        self.similarity_threshold = 0.7  # 최소 유사도 임계값
        
        if self.dynamic_enabled:
            logger.info("✅ HybridSynonymExpander: Qdrant 동적 확장 활성화")
        else:
            logger.info("⚠️  HybridSynonymExpander: Qdrant 동적 확장 비활성화 (정적 사전만 사용)")
    
    def expand(self, term: str, use_dynamic: bool = True) -> List[str]:
        """
        동의어 확장 (정적 + 동적)
        
        Args:
            term: 확장할 용어
            use_dynamic: Qdrant 동적 확장 사용 여부
        
        Returns:
            동의어 리스트
        """
        # 1. 정적 사전 확인
        static_synonyms = self.static_expander.expand(term)
        
        # 정적 사전에 있으면 바로 반환
        if len(static_synonyms) > 1 or (len(static_synonyms) == 1 and static_synonyms[0] != term):
            logger.debug(f"✅ 정적 사전: {term} → {static_synonyms}")
            return static_synonyms
        
        # 2. 동적 확장 비활성화 또는 정적 사전에 있으면 정적 결과만 반환
        if not use_dynamic or not self.dynamic_enabled:
            return static_synonyms
        
        # 3. 캐시 확인
        if term in self.dynamic_cache:
            cached = self.dynamic_cache[term]
            logger.debug(f"💾 캐시 히트: {term} → {cached}")
            # 정적 + 동적 병합
            all_synonyms = list(set(static_synonyms + cached))
            return all_synonyms
        
        # 4. Qdrant 동적 확장
        try:
            dynamic_synonyms = self._expand_with_qdrant(term)
            
            # 캐시에 저장 (LRU 방식)
            if len(self.dynamic_cache) >= self.cache_size:
                # 가장 오래된 항목 제거 (간단한 FIFO)
                oldest_key = next(iter(self.dynamic_cache))
                del self.dynamic_cache[oldest_key]
            
            self.dynamic_cache[term] = dynamic_synonyms
            logger.info(f"🔄 Qdrant 동적 확장: {term} → {dynamic_synonyms}")
            
            # 정적 + 동적 병합
            all_synonyms = list(set(static_synonyms + dynamic_synonyms))
            return all_synonyms
        
        except Exception as e:
            logger.warning(f"⚠️  Qdrant 동적 확장 실패: {term} - {e}")
            # 실패 시 정적 결과만 반환
            return static_synonyms
    
    def _expand_with_qdrant(self, term: str) -> List[str]:
        """
        Qdrant에서 유사한 답변 텍스트를 찾아 동의어로 확장
        
        Args:
            term: 확장할 용어
        
        Returns:
            동의어 리스트
        """
        if not self.qdrant_client or not self.embedding_model:
            return []
        
        try:
            # 1. term을 임베딩 벡터로 변환
            query_vector = self.embedding_model.encode(term).tolist()
            
            # 2. 모든 Qdrant 컬렉션에서 검색
            collections = self.qdrant_client.get_collections()
            all_synonyms: Set[str] = set()
            
            for collection in collections.collections:
                try:
                    # Qdrant에서 유사 벡터 검색
                    results = self.qdrant_client.search(
                        collection_name=collection.name,
                        query_vector=query_vector,
                        limit=self.dynamic_limit * 2,  # 더 많이 가져와서 필터링
                        score_threshold=self.similarity_threshold,
                        with_payload=True,
                        with_vectors=False
                    )
                    
                    # payload에서 answer_text 추출
                    for result in results:
                        payload = result.payload
                        if payload:
                            # qa_pairs에서 answer_text 추출
                            qa_pairs = payload.get('qa_pairs', [])
                            if isinstance(qa_pairs, list):
                                for qa in qa_pairs:
                                    if isinstance(qa, dict):
                                        answer_text = qa.get('answer_text') or qa.get('answer')
                                        if answer_text and isinstance(answer_text, str):
                                            # 원본 term과 유사한 답변만 추가
                                            # (너무 긴 답변은 제외)
                                            if len(answer_text) <= 50 and answer_text != term:
                                                all_synonyms.add(answer_text)
                            
                            # metadata에서도 추출 (직업, 성별 등)
                            metadata = payload.get('metadata', {})
                            for key in ['occupation', 'gender', 'age_group']:
                                value = metadata.get(key)
                                if value and isinstance(value, str) and value != term:
                                    all_synonyms.add(value)
                
                except Exception as e:
                    logger.debug(f"⚠️  컬렉션 {collection.name} 검색 실패: {e}")
                    continue
            
            # 상위 N개만 반환 (유사도 순)
            synonyms_list = list(all_synonyms)[:self.dynamic_limit]
            logger.debug(f"🔍 Qdrant 검색 결과: {term} → {len(synonyms_list)}개 동의어")
            return synonyms_list
        
        except Exception as e:
            logger.error(f"❌ Qdrant 동적 확장 오류: {term} - {e}")
            return []
    
    def expand_multiple(self, terms: List[str], use_dynamic: bool = True) -> List[str]:
        """여러 term 확장 (중복 제거)"""
        all_synonyms = []
        for term in terms:
            all_synonyms.extend(self.expand(term, use_dynamic=use_dynamic))
        
        # 중복 제거하면서 순서 유지
        seen = set()
        unique = []
        for syn in all_synonyms:
            if syn not in seen:
                seen.add(syn)
                unique.append(syn)
        
        return unique
    
    def clear_cache(self):
        """동적 확장 캐시 초기화"""
        self.dynamic_cache.clear()
        logger.info("🗑️  동적 확장 캐시 초기화 완료")
    
    def get_stats(self) -> Dict:
        """통계 반환"""
        static_stats = self.static_expander.get_stats()
        return {
            **static_stats,
            "dynamic_enabled": self.dynamic_enabled,
            "dynamic_cache_size": len(self.dynamic_cache),
            "cache_size_limit": self.cache_size
        }

# 싱글톤 인스턴스 (전역)
_expander_instance: Optional[HybridSynonymExpander] = None

def get_synonym_expander(
    qdrant_client = None,
    embedding_model = None
) -> HybridSynonymExpander:
    """
    싱글톤 HybridSynonymExpander 가져오기
    
    Args:
        qdrant_client: Qdrant 클라이언트 (동적 확장용, 선택)
        embedding_model: 임베딩 모델 (동적 확장용, 선택)
    
    Returns:
        HybridSynonymExpander 인스턴스
    """
    global _expander_instance
    if _expander_instance is None:
        _expander_instance = HybridSynonymExpander(
            qdrant_client=qdrant_client,
            embedding_model=embedding_model
        )
    return _expander_instance

