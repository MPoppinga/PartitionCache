-- Flight 3: Science categories + heavy edits + many references + LLM
-- Q3_1: Fundamental scientific laws with mathematical formulation (2 LLM questions)
SELECT a.article_id
FROM wikipedia_articles a
WHERE EXISTS (SELECT 1 FROM unnest(a.categories) c
              WHERE c ILIKE '%scien%' OR c ILIKE '%physic%'
                 OR c ILIKE '%chemi%' OR c ILIKE '%biolog%')
  AND a.edit_count > 100
  AND a.num_references > 20
  AND wiki_llm_classify(a.content, 'Does this article describe a fundamental law of nature, physical constant, or scientific principle?')
  AND wiki_llm_classify(a.content, 'Does the article include or reference a mathematical equation or formula?')
