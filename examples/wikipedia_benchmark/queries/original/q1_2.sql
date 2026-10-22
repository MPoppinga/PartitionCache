-- Flight 1: History category + high edits + recent + LLM
-- Q1_2: Military conflicts with diplomatic aftermath (3 LLM questions)
SELECT a.article_id
FROM wikipedia_articles a
WHERE EXISTS (SELECT 1 FROM unnest(a.categories) c WHERE c ILIKE '%histor%')
  AND a.edit_count > 50
  AND a.creation_year >= 2005
  AND wiki_llm_classify(a.content, 'Does this article describe a military conflict or war?')
  AND wiki_llm_classify(a.content, 'Were multiple countries or nations involved?')
  AND wiki_llm_classify(a.content, 'Did the conflict end with a treaty or peace agreement?')
