-- Flight 5: Fragment reuse - Flight 2 relational + new LLM questions
-- Q5_2: Cultural traditions and festivals with religious origins (3 LLM questions)
SELECT a.article_id
FROM wikipedia_articles a
WHERE a.creation_year BETWEEN 2010 AND 2015
  AND a.content_length > 5000
  AND wiki_llm_classify(a.content, 'Does this article describe a cultural tradition, festival, ceremony, or celebration?')
  AND wiki_llm_classify(a.content, 'Does this tradition have religious or spiritual origins?')
  AND wiki_llm_classify(a.content, 'Is this tradition still actively practiced today?')
