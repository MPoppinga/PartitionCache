-- TPC-H Q3.2: Revenue by year, FRANCE customers, GERMANY suppliers, 1994-1996
SELECT EXTRACT(YEAR FROM o.o_orderdate) AS l_year,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM lineitem l, orders o, customer c, supplier s,
     nation cn, nation sn
WHERE l.l_orderkey = o.o_orderkey
  AND o.o_custkey = c.c_custkey
  AND l.l_suppkey = s.s_suppkey
  AND c.c_nationkey = cn.n_nationkey
  AND s.s_nationkey = sn.n_nationkey
  AND cn.n_name = 'FRANCE'
  AND sn.n_name = 'GERMANY'
  AND o.o_orderdate >= '1994-01-01' AND o.o_orderdate < '1997-01-01'
GROUP BY EXTRACT(YEAR FROM o.o_orderdate)
ORDER BY l_year
