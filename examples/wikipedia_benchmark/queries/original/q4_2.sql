-- Flight 4: Multi-constraint + LLM
-- Q4_2: Economically-driven historical events (3 LLM questions)
SELECT a.article_id
FROM wikipedia_articles a
WHERE EXISTS (SELECT 1 FROM unnest(a.categories) c WHERE c ILIKE '%histor%')
  AND a.creation_year >= 2008
  AND a.content_length BETWEEN 3000 AND 50000
  AND a.edit_count > 30
  AND a.infobox_type IS NOT NULL
  AND wiki_llm_classify(a.content, 'Was this event primarily triggered by economic factors such as trade disputes, financial crises, or resource scarcity?')
  AND wiki_llm_classify(a.content, 'Did this event cause significant economic disruption such as famine, hyperinflation, or mass unemployment?')
  AND wiki_llm_classify(a.content, 'Is the economic impact of this event quantified with specific numbers or statistics in the article?')
