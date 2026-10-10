-- Flight 2: Science category + high edits + recent + LLM
-- Q2_1: Scientific breakthroughs with real-world impact (2 LLM questions)
SELECT a.article_id
FROM wikipedia_articles a
WHERE EXISTS (SELECT 1 FROM unnest(a.categories) c WHERE c ILIKE '%scienc%')
  AND a.edit_count > 50
  AND a.creation_year >= 2005
  AND wiki_llm_classify(a.content, 'Is this article about a scientific discovery, invention, or breakthrough?')
  AND wiki_llm_classify(a.content, 'Has the discovery described here been applied in medicine, technology, or industry?')
