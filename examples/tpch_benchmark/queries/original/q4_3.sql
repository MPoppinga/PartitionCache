-- TPC-H Q4.3: Revenue by year, AMERICA customers, US suppliers, 1997-1998, Brand#34
SELECT EXTRACT(YEAR FROM o.o_orderdate) AS o_year,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM lineitem l, orders o, customer c, supplier s, part p,
     nation cn, nation sn, region cr
WHERE l.l_orderkey = o.o_orderkey
  AND o.o_custkey = c.c_custkey
  AND l.l_suppkey = s.s_suppkey
  AND l.l_partkey = p.p_partkey
  AND c.c_nationkey = cn.n_nationkey
  AND s.s_nationkey = sn.n_nationkey
  AND cn.n_regionkey = cr.r_regionkey
  AND cr.r_name = 'AMERICA'
  AND sn.n_name = 'UNITED STATES'
  AND s.s_acctbal > 3000
  AND o.o_orderdate >= '1997-01-01' AND o.o_orderdate < '1999-01-01'
  AND p.p_brand = 'Brand#34'
GROUP BY EXTRACT(YEAR FROM o.o_orderdate)
ORDER BY o_year
