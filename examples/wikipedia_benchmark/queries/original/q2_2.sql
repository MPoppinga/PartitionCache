-- Flight 2: Science category + high edits + recent + LLM
-- Q2_2: Notable scientists with awards (3 LLM questions)
SELECT a.article_id
FROM wikipedia_articles a
WHERE EXISTS (SELECT 1 FROM unnest(a.categories) c WHERE c ILIKE '%scienc%')
  AND a.edit_count > 50
  AND a.creation_year >= 2005
  AND wiki_llm_classify(a.content, 'Is this a biography of a scientist or researcher?')
  AND wiki_llm_classify(a.content, 'Has this person received a major international award or recognition?')
  AND wiki_llm_classify(a.content, 'Did this person make their primary contributions after 1950?')
