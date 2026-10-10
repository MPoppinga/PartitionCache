-- LLM classifier functions for Wikipedia benchmark
-- Supports any OpenAI-compatible API (OpenRouter, LM Studio, vLLM, Ollama, etc.)
--
-- Configuration via PostgreSQL GUC settings:
--   wiki_llm.base_url    - API base URL (default: http://localhost:1234/v1)
--   wiki_llm.api_key     - API key (default: 'no-key-required')
--   wiki_llm.model       - Model name (default: 'qwen-3.5:9b')
--   wiki_llm.extra_body  - Extra JSON body params (default: '{}')
--   wiki_llm.max_retries - Max retry attempts on rate limit (default: '10')
--
-- Examples:
--   -- LM Studio (local)
--   ALTER SYSTEM SET wiki_llm.base_url = 'http://hex:1234/v1';
--   ALTER SYSTEM SET wiki_llm.model = 'qwen-3.5:9b';
--
--   -- OpenRouter (cloud)
--   ALTER SYSTEM SET wiki_llm.base_url = 'https://openrouter.ai/api/v1';
--   ALTER SYSTEM SET wiki_llm.api_key = 'sk-or-v1-...';
--   ALTER SYSTEM SET wiki_llm.model = 'z-ai/glm-4.7-flash';
--   ALTER SYSTEM SET wiki_llm.extra_body = '{"reasoning": {"effort": "none"}}';
--
--   -- Ollama (local)
--   ALTER SYSTEM SET wiki_llm.base_url = 'http://localhost:11434/v1';
--   ALTER SYSTEM SET wiki_llm.model = 'qwen3:8b';

-- Real classifier: calls any OpenAI-compatible API via pgai
CREATE OR REPLACE FUNCTION wiki_llm_classify(
    content TEXT,
    question TEXT,
    model_name TEXT DEFAULT NULL,
    max_content_length INTEGER DEFAULT 20000
) RETURNS BOOLEAN AS $$
DECLARE
    response JSONB;
    response_text TEXT;
    retry_count INTEGER := 0;
    max_retries INTEGER;
    _model TEXT;
    _base_url TEXT;
    _api_key TEXT;
    _extra_body JSONB;
BEGIN
    -- Read configuration from GUC settings with defaults
    _model := COALESCE(model_name, current_setting('wiki_llm.model', true), 'qwen-3.5:9b');
    _base_url := COALESCE(current_setting('wiki_llm.base_url', true), 'http://localhost:1234/v1');
    _api_key := COALESCE(current_setting('wiki_llm.api_key', true), 'no-key-required');
    _extra_body := COALESCE(current_setting('wiki_llm.extra_body', true), '{}')::jsonb;
    max_retries := COALESCE(current_setting('wiki_llm.max_retries', true), '10')::integer;

    LOOP
        BEGIN
            SELECT ai.openai_chat_complete(
                _model,
                jsonb_build_array(
                    jsonb_build_object('role', 'user', 'content',
                        format('Answer only YES or NO. Nothing else. %s

Text: %s', question, LEFT(content, max_content_length)))
                ),
                api_key => _api_key,
                temperature => 0.0,
                max_tokens => 10,
                extra_body => _extra_body,
                client_config => ai.openai_client_config(base_url => _base_url)
            ) INTO response;
            response_text := response->'choices'->0->'message'->>'content';
            RETURN response_text ILIKE 'yes%';
        EXCEPTION WHEN OTHERS THEN
            IF SQLERRM LIKE '%429%' OR SQLERRM LIKE '%Rate limit%' OR SQLERRM LIKE '%rate-limited%' THEN
                retry_count := retry_count + 1;
                IF retry_count > max_retries THEN
                    RAISE;
                END IF;
                RAISE NOTICE 'Rate limited (attempt %/%), waiting %s...', retry_count, max_retries, (10 + retry_count * 5)::text;
                PERFORM pg_sleep(10 + retry_count * 5);
            ELSE
                RAISE;
            END IF;
        END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Mock classifier: deterministic hash-based, no LLM needed
-- ~30% TRUE rate based on content+question hash
CREATE OR REPLACE FUNCTION wiki_llm_classify_mock(
    content TEXT,
    question TEXT,
    model_name TEXT DEFAULT 'mock',
    max_content_length INTEGER DEFAULT 20000
) RETURNS BOOLEAN AS $$
BEGIN
    RETURN (abs(hashtext(LEFT(content, max_content_length) || question)) % 10) < 3;
END;
$$ LANGUAGE plpgsql;
