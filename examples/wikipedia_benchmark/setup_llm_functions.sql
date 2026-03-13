-- LLM classifier functions for Wikipedia benchmark
-- Requires: pgai extension + Ollama with a pulled model

-- Real classifier: calls Ollama via pgai
CREATE OR REPLACE FUNCTION wiki_llm_classify(
    content TEXT,
    question TEXT,
    model_name TEXT DEFAULT 'qwen3.5:4b',
    max_content_length INTEGER DEFAULT 2000
) RETURNS BOOLEAN AS $$
DECLARE
    response_text TEXT;
BEGIN
    SELECT (ai.ollama_generate(
        model_name,
        format('Answer only YES or NO: %s

Text: %s', question, LEFT(content, max_content_length)),
        system_prompt => 'You are a classifier. Answer only YES or NO. Nothing else.',
        chat_options => '{"num_ctx": 4096, "temperature": 0.0}'
    ) ->> 'response') INTO response_text;
    RETURN response_text ILIKE 'yes%';
END;
$$ LANGUAGE plpgsql;

-- Mock classifier: deterministic hash-based, no LLM needed
-- ~30% TRUE rate based on content+question hash
CREATE OR REPLACE FUNCTION wiki_llm_classify_mock(
    content TEXT,
    question TEXT,
    model_name TEXT DEFAULT 'mock',
    max_content_length INTEGER DEFAULT 2000
) RETURNS BOOLEAN AS $$
BEGIN
    RETURN (abs(hashtext(LEFT(content, max_content_length) || question)) % 10) < 3;
END;
$$ LANGUAGE plpgsql;
