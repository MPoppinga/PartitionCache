-- Flight 4: Multi-constraint + LLM
-- Q4_1: Boundary-changing events with lasting geopolitical impact (2 LLM questions)
SELECT a.article_id
FROM wikipedia_articles a
WHERE EXISTS (SELECT 1 FROM unnest(a.categories) c WHERE c ILIKE '%histor%')
  AND a.creation_year >= 2008
  AND a.content_length BETWEEN 3000 AND 50000
  AND a.edit_count > 30
  AND a.infobox_type IS NOT NULL
  AND wiki_llm_classify(a.content, 'Did this event result in the creation, dissolution, or redrawing of national or territorial boundaries?')
  AND wiki_llm_classify(a.content, 'Are the geopolitical consequences of this event still relevant in the 21st century?')
