-- Flight 5: Fragment reuse - Flight 2 relational (scienc%) + new LLM questions
-- Q5_2: Cultural traditions and festivals described in science articles (3 LLM questions)
SELECT a.article_id
FROM wikipedia_articles a
WHERE EXISTS (SELECT 1 FROM unnest(a.categories) c WHERE c ILIKE '%scienc%')
  AND a.edit_count > 50
  AND a.creation_year >= 2005
  AND wiki_llm_classify(a.content, 'Does this article describe a cultural practice, tradition, or ritual related to science or nature?')
  AND wiki_llm_classify(a.content, 'Does this practice have historical or religious origins?')
  AND wiki_llm_classify(a.content, 'Is this practice still observed or relevant today?')
