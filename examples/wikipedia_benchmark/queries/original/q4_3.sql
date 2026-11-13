-- Flight 4: Geography category + high edits + recent + LLM
-- Q4_3: Places affected by natural disasters or climate (2 LLM questions)
SELECT a.article_id
FROM wikipedia_articles a
WHERE EXISTS (SELECT 1 FROM unnest(a.categories) c WHERE c ILIKE '%geograph%')
  AND a.edit_count > 50
  AND a.creation_year >= 2005
  AND wiki_llm_classify(a.content, 'Has this place been affected by natural disasters such as earthquakes, floods, hurricanes, or volcanic eruptions?')
  AND wiki_llm_classify(a.content, 'Does the article discuss how the local population or infrastructure was impacted?')
