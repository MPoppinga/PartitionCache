-- Flight 4: Geography category + high edits + recent + LLM
-- Q4_2: Economically significant geographic regions (3 LLM questions)
SELECT a.article_id
FROM wikipedia_articles a
WHERE EXISTS (SELECT 1 FROM unnest(a.categories) c WHERE c ILIKE '%geograph%')
  AND a.edit_count > 50
  AND a.creation_year >= 2005
  AND wiki_llm_classify(a.content, 'Is this article about a country, state, or region with major economic activity?')
  AND wiki_llm_classify(a.content, 'Does the article mention specific industries, trade, or economic output?')
  AND wiki_llm_classify(a.content, 'Has this place experienced economic growth or decline in recent decades?')
