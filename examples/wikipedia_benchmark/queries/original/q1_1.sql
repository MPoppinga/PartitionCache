-- Flight 1: History category + high edits + recent + LLM
-- Q1_1: Women in historic events (2 LLM questions, both must be true)
SELECT a.article_id
FROM wikipedia_articles a
WHERE EXISTS (SELECT 1 FROM unnest(a.categories) c WHERE c ILIKE '%histor%')
  AND a.edit_count > 50
  AND a.creation_year >= 2005
  AND wiki_llm_classify(a.content, 'Was a woman a central figure in the events described?')
  AND wiki_llm_classify(a.content, 'Did this person or event lead to lasting social change?')
