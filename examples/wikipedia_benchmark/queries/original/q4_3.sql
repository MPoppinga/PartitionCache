-- Flight 4: Multi-constraint + LLM
-- Q4_3: Deadly events with humanitarian response (2 LLM questions)
SELECT a.article_id
FROM wikipedia_articles a
WHERE EXISTS (SELECT 1 FROM unnest(a.categories) c WHERE c ILIKE '%histor%')
  AND a.creation_year >= 2008
  AND a.content_length BETWEEN 3000 AND 50000
  AND a.edit_count > 30
  AND a.infobox_type IS NOT NULL
  AND wiki_llm_classify(a.content, 'Did this event directly cause significant loss of human life, either through violence, disaster, or disease?')
  AND wiki_llm_classify(a.content, 'Did humanitarian organizations or international aid efforts respond to this event?')
