-- Flight 1: History category + high edits + recent + LLM
-- Q1_3: Interconnected historical events (2 LLM questions)
SELECT a.article_id
FROM wikipedia_articles a
WHERE EXISTS (SELECT 1 FROM unnest(a.categories) c WHERE c ILIKE '%histor%')
  AND a.edit_count > 50
  AND a.creation_year >= 2005
  AND wiki_llm_classify(a.content, 'Does the article mention at least two other significant historical events by name?')
  AND wiki_llm_classify(a.content, 'Did this event take place before the year 1900?')
