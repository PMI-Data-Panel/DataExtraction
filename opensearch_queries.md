# OpenSearch DevTools 쿼리 모음

## 0. ⭐ _source 전체 보기 (가장 기본)
```json
GET /survey_responses_merged/_search
{
  "size": 1,
  "query": {
    "match_all": {}
  }
}
```

## 0-1. ⭐ _source 전체 보기 (여러 개)
```json
GET /survey_responses_merged/_search
{
  "size": 10,
  "query": {
    "match_all": {}
  }
}
```

## 0-2. ⭐ _source에서 특정 필드만 보기
```json
GET /survey_responses_merged/_search
{
  "size": 10,
  "query": {
    "match_all": {}
  },
  "_source": ["user_id", "qa_pairs", "metadata"]
}
```

## 0-3. ⭐ _source에서 qa_pairs만 보기
```json
GET /survey_responses_merged/_search
{
  "size": 10,
  "query": {
    "match_all": {}
  },
  "_source": {
    "includes": ["qa_pairs.*"]
  }
}
```

## 1. 샘플 문서 1개 가져오기 (전체 구조 확인)
```json
GET /survey_responses_merged/_search
{
  "size": 1,
  "query": {
    "match_all": {}
  }
}
```

## 2. 샘플 문서 5개 가져오기 (qa_pairs 구조 확인)
```json
GET /survey_responses_merged/_search
{
  "size": 5,
  "query": {
    "match_all": {}
  },
  "_source": ["user_id", "qa_pairs", "metadata"]
}
```

## 3. 모든 질문 목록 추출 (q_text만)
```json
GET /survey_responses_merged/_search
{
  "size": 0,
  "aggs": {
    "unique_questions": {
      "terms": {
        "field": "qa_pairs.q_text.keyword",
        "size": 1000
      }
    }
  }
}
```

## 4. OTT 관련 질문 찾기
```json
GET /survey_responses_merged/_search
{
  "size": 10,
  "query": {
    "nested": {
      "path": "qa_pairs",
      "query": {
        "match": {
          "qa_pairs.q_text": "OTT"
        }
      }
    }
  },
  "_source": ["user_id", "qa_pairs.q_text", "qa_pairs.answer_text", "qa_pairs.answer"]
}
```

## 5. 모든 필드명 확인 (qa_pairs 내부 구조)
```json
GET /survey_responses_merged/_search
{
  "size": 1,
  "query": {
    "match_all": {}
  },
  "_source": {
    "includes": ["qa_pairs.*"]
  }
}
```

## 6. qa_pairs의 모든 필드 확인 (answer, answer_text 등)
```json
GET /survey_responses_merged/_search
{
  "size": 3,
  "query": {
    "match_all": {}
  },
  "_source": ["user_id", "qa_pairs"]
}
```

## 7. 특정 질문에 대한 모든 답변 찾기
```json
GET /survey_responses_merged/_search
{
  "size": 100,
  "query": {
    "nested": {
      "path": "qa_pairs",
      "query": {
        "match": {
          "qa_pairs.q_text": "OTT"
        }
      }
    }
  },
  "_source": ["user_id", "qa_pairs.q_text", "qa_pairs.answer_text", "qa_pairs.answer", "metadata"]
}
```

## 8. qa_pairs의 모든 가능한 필드 확인 (첫 번째 문서의 qa_pairs만)
```json
GET /survey_responses_merged/_search
{
  "size": 1,
  "query": {
    "match_all": {}
  }
}
```

## 9. 질문별 답변 개수 집계
```json
GET /survey_responses_merged/_search
{
  "size": 0,
  "aggs": {
    "questions": {
      "nested": {
        "path": "qa_pairs"
      },
      "aggs": {
        "question_texts": {
          "terms": {
            "field": "qa_pairs.q_text.keyword",
            "size": 500
          }
        }
      }
    }
  }
}
```

## 10. answer 필드가 있는지 확인
```json
GET /survey_responses_merged/_search
{
  "size": 5,
  "query": {
    "match_all": {}
  },
  "_source": {
    "includes": ["user_id", "qa_pairs.*"]
  }
}
```

## 11. 전체 문서 수 확인
```json
GET /survey_responses_merged/_count
{
  "query": {
    "match_all": {}
  }
}
```

## 12. 인덱스 매핑 확인 (qa_pairs 구조 확인)
```json
GET /survey_responses_merged/_mapping
```

## 13. Scroll API로 모든 문서 가져오기 (Python 스크립트용)
```json
# 첫 번째 요청
POST /survey_responses_merged/_search?scroll=5m
{
  "size": 1000,
  "query": {
    "match_all": {}
  },
  "_source": ["user_id", "qa_pairs", "metadata"]
}

# 이후 요청 (scroll_id는 첫 번째 응답에서 받음)
POST /_search/scroll
{
  "scroll": "5m",
  "scroll_id": "YOUR_SCROLL_ID_HERE"
}
```

## 14. answer와 answer_text 필드 모두 확인
```json
GET /survey_responses_merged/_search
{
  "size": 10,
  "query": {
    "match_all": {}
  },
  "_source": {
    "includes": ["user_id", "qa_pairs.q_text", "qa_pairs.answer*", "qa_pairs.answer_text"]
  }
}
```

## 15. 모든 고유 질문 목록 (간단 버전)
```json
GET /survey_responses_merged/_search
{
  "size": 0,
  "aggs": {
    "all_questions": {
      "nested": {
        "path": "qa_pairs"
      },
      "aggs": {
        "question_list": {
          "terms": {
            "field": "qa_pairs.q_text.keyword",
            "size": 10000
          }
        }
      }
    }
  }
}
```

## 16. 특정 질문에 대한 모든 답변 보기 (예: OTT 서비스 질문)
```json
GET /survey_responses_merged/_search
{
  "size": 100,
  "query": {
    "nested": {
      "path": "qa_pairs",
      "query": {
        "term": {
          "qa_pairs.q_text.keyword": "여러분이 현재 이용 중인 OTT 서비스는 몇 개인가요?"
        }
      }
    }
  },
  "_source": ["user_id", "qa_pairs.q_text", "qa_pairs.answer_text", "qa_pairs.answer", "metadata"]
}
```

## 17. 질문별 답변 집계 (답변 텍스트별 개수)
```json
GET /survey_responses_merged/_search
{
  "size": 0,
  "aggs": {
    "questions": {
      "nested": {
        "path": "qa_pairs"
      },
      "aggs": {
        "question_list": {
          "terms": {
            "field": "qa_pairs.q_text.keyword",
            "size": 10
          },
          "aggs": {
            "answers": {
              "terms": {
                "field": "qa_pairs.answer_text.keyword",
                "size": 20
              }
            }
          }
        }
      }
    }
  }
}
```

## 18. 특정 질문의 모든 답변 텍스트만 보기 (답변 집계)
```json
GET /survey_responses_merged/_search
{
  "size": 0,
  "query": {
    "nested": {
      "path": "qa_pairs",
      "query": {
        "term": {
          "qa_pairs.q_text.keyword": "여러분이 현재 이용 중인 OTT 서비스는 몇 개인가요?"
        }
      }
    }
  },
  "aggs": {
    "answers": {
      "nested": {
        "path": "qa_pairs"
      },
      "aggs": {
        "filtered_question": {
          "filter": {
            "term": {
              "qa_pairs.q_text.keyword": "여러분이 현재 이용 중인 OTT 서비스는 몇 개인가요?"
            }
          },
          "aggs": {
            "answer_list": {
              "terms": {
                "field": "qa_pairs.answer_text.keyword",
                "size": 100
              }
            }
          }
        }
      }
    }
  }
}
```

## 19. 여러 질문의 답변을 한 번에 보기 (Top 5 질문)
```json
GET /survey_responses_merged/_search
{
  "size": 0,
  "aggs": {
    "questions": {
      "nested": {
        "path": "qa_pairs"
      },
      "aggs": {
        "top_questions": {
          "terms": {
            "field": "qa_pairs.q_text.keyword",
            "size": 5
          },
          "aggs": {
            "sample_answers": {
              "top_hits": {
                "size": 10,
                "_source": {
                  "includes": ["qa_pairs.answer_text", "qa_pairs.answer", "user_id"]
                }
              }
            },
            "all_answers": {
              "terms": {
                "field": "qa_pairs.answer_text.keyword",
                "size": 50
              }
            }
          }
        }
      }
    }
  }
}
```

## 20. 질문별로 답변 샘플과 전체 답변 집계 함께 보기
```json
GET /survey_responses_merged/_search
{
  "size": 0,
  "aggs": {
    "questions": {
      "nested": {
        "path": "qa_pairs"
      },
      "aggs": {
        "question_list": {
          "terms": {
            "field": "qa_pairs.q_text.keyword",
            "size": 10,
            "order": {
              "_count": "desc"
            }
          },
          "aggs": {
            "answer_samples": {
              "top_hits": {
                "size": 5,
                "_source": {
                  "includes": ["qa_pairs.answer_text", "qa_pairs.answer", "user_id", "metadata"]
                }
              }
            },
            "answer_distribution": {
              "terms": {
                "field": "qa_pairs.answer_text.keyword",
                "size": 20
              }
            }
          }
        }
      }
    }
  }
}
```

## 21. ⭐ 특정 질문이 포함된 문서의 _source 전체 보기
```json
GET /survey_responses_merged/_search
{
  "size": 10,
  "query": {
    "nested": {
      "path": "qa_pairs",
      "query": {
        "match": {
          "qa_pairs.q_text": "OTT"
        }
      }
    }
  }
}
```

## 22. ⭐ 특정 질문의 정확한 텍스트로 _source 보기
```json
GET /survey_responses_merged/_search
{
  "size": 10,
  "query": {
    "nested": {
      "path": "qa_pairs",
      "query": {
        "term": {
          "qa_pairs.q_text.keyword": "여러분이 현재 이용 중인 OTT 서비스는 몇 개인가요?"
        }
      }
    }
  }
}
```

## 23. ⭐ _source에서 qa_pairs의 모든 필드 보기 (answer, answer_text 등)
```json
GET /survey_responses_merged/_search
{
  "size": 5,
  "query": {
    "match_all": {}
  },
  "_source": {
    "includes": ["user_id", "qa_pairs.*", "metadata.*"]
  }
}
```

## 24. ⭐ 특정 user_id의 _source 전체 보기
```json
GET /survey_responses_merged/_search
{
  "size": 1,
  "query": {
    "term": {
      "user_id": "특정_user_id_여기"
    }
  }
}
```

## 25. ⭐ _source를 예쁘게 보기 (pretty 옵션)
```json
GET /survey_responses_merged/_search?pretty
{
  "size": 3,
  "query": {
    "match_all": {}
  }
}
```

