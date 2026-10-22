-- Flight 3: Science categories + heavy edits + many references + LLM
-- Q3_2: Living scientists with Nobel-level recognition (3 LLM questions)
SELECT a.article_id
FROM wikipedia_articles a
WHERE EXISTS (SELECT 1 FROM unnest(a.categories) c
              WHERE c ILIKE '%scien%' OR c ILIKE '%physic%'
                 OR c ILIKE '%chemi%' OR c ILIKE '%biolog%')
  AND a.edit_count > 100
  AND a.num_references > 20
  AND wiki_llm_classify(a.content, 'Is this article about a scientist or researcher who is still alive?')
  AND wiki_llm_classify(a.content, 'Has this person received a major international award such as a Nobel Prize, Fields Medal, or similar honor?')
  AND wiki_llm_classify(a.content, 'Did this person make their primary contributions after 1970?')
