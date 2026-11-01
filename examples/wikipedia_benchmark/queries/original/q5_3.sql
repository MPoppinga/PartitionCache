-- Flight 5: Fragment reuse - Flight 3 relational + new LLM questions
-- Q5_3: Medical and health science with public health impact (2 LLM questions)
SELECT a.article_id
FROM wikipedia_articles a
WHERE EXISTS (SELECT 1 FROM unnest(a.categories) c
              WHERE c ILIKE '%scien%' OR c ILIKE '%physic%'
                 OR c ILIKE '%chemi%' OR c ILIKE '%biolog%')
  AND a.edit_count > 100
  AND a.num_references > 20
  AND wiki_llm_classify(a.content, 'Is the primary subject of this article related to medicine, disease, pharmaceuticals, or human health?')
  AND wiki_llm_classify(a.content, 'Has this medical topic affected large populations or been the subject of public health campaigns?')
