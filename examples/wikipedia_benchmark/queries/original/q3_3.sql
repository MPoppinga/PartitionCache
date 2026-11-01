-- Flight 3: Science categories + heavy edits + many references + LLM
-- Q3_3: Experimental science with reproducibility concerns (2 LLM questions)
SELECT a.article_id
FROM wikipedia_articles a
WHERE EXISTS (SELECT 1 FROM unnest(a.categories) c
              WHERE c ILIKE '%scien%' OR c ILIKE '%physic%'
                 OR c ILIKE '%chemi%' OR c ILIKE '%biolog%')
  AND a.edit_count > 100
  AND a.num_references > 20
  AND wiki_llm_classify(a.content, 'Does this article discuss experimental methods, laboratory techniques, or empirical observations?')
  AND wiki_llm_classify(a.content, 'Have the findings described been challenged, revised, or debated by other scientists?')
