-- Flight 4: Geography category + high edits + recent + LLM
-- Q4_1: Boundary-changing events with lasting geopolitical impact (2 LLM questions)
SELECT a.article_id
FROM wikipedia_articles a
WHERE EXISTS (SELECT 1 FROM unnest(a.categories) c WHERE c ILIKE '%geograph%')
  AND a.edit_count > 50
  AND a.creation_year >= 2005
  AND wiki_llm_classify(a.content, 'Does this article describe a place or region with significant political or territorial disputes?')
  AND wiki_llm_classify(a.content, 'Are the geopolitical consequences described still relevant in the 21st century?')
