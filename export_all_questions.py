"""All questions export to JSON"""
import json
import sys
from opensearchpy import OpenSearch
from rag_query_analyzer.config import get_config

config = get_config()

try:
    client = OpenSearch(
        hosts=[{'host': config.OPENSEARCH_HOST, 'port': config.OPENSEARCH_PORT}],
        http_auth=(config.OPENSEARCH_USER, config.OPENSEARCH_PASSWORD),
        use_ssl=config.OPENSEARCH_USE_SSL,
        verify_certs=config.OPENSEARCH_VERIFY_CERTS,
        ssl_assert_hostname=config.OPENSEARCH_SSL_ASSERT_HOSTNAME,
        ssl_show_warn=False,
        timeout=30,
        max_retries=3,
        retry_on_timeout=True
    )
    info = client.info()
except Exception as e:
    print(f"ERROR: Connection failed - {str(e)}", file=sys.stderr)
    sys.exit(1)

# 모든 질문 타입과 각 질문별 답변 형식 조회
query = {
    "size": 0,
    "query": {"match_all": {}},
    "aggs": {
        "qa_nested": {
            "nested": {"path": "qa_pairs"},
            "aggs": {
                "questions": {
                    "terms": {
                        "field": "qa_pairs.q_text.keyword",
                        "size": 1000
                    },
                    "aggs": {
                        "answers": {
                            "terms": {
                                "field": "qa_pairs.answer.keyword",
                                "size": 100
                            }
                        }
                    }
                }
            }
        }
    }
}

try:
    response = client.search(index="survey_responses_merged", body=query)
except Exception as e:
    print(f"ERROR: Search failed - {str(e)}", file=sys.stderr)
    client.close()
    sys.exit(1)

# 모든 질문과 답변 추출
all_questions = response["aggregations"]["qa_nested"]["questions"]["buckets"]

output = {
    "total_questions": len(all_questions),
    "questions": [
        {
            "question": q["key"],
            "response_count": q["doc_count"],
            "answer_types": [
                {
                    "answer": a["key"],
                    "count": a["doc_count"]
                }
                for a in q["answers"]["buckets"]
            ]
        }
        for q in all_questions
    ]
}

filename = "all_questions_and_answers.json"
with open(filename, 'w', encoding='utf-8') as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

print(f"Success: {len(all_questions)} questions saved to {filename}")

client.close()
