-- Flight 5: Fragment reuse - Flight 1 relational + new LLM questions
-- Q5_1: Technological inventions that changed daily life (2 LLM questions)
SELECT a.article_id
FROM wikipedia_articles a
WHERE EXISTS (SELECT 1 FROM unnest(a.categories) c WHERE c ILIKE '%histor%')
  AND a.edit_count > 50
  AND a.creation_year >= 2005
  AND wiki_llm_classify(a.content, 'Is this article about an invention, technological device, or engineering achievement?')
  AND wiki_llm_classify(a.content, 'Did this invention fundamentally change how ordinary people live, work, or communicate?')
