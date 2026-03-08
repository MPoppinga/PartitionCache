-- TPC-H Q2.3: Revenue for Brand#34 parts from GERMANY suppliers
SELECT SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM lineitem l, part p, supplier s, nation n
WHERE l.l_partkey = p.p_partkey
  AND l.l_suppkey = s.s_suppkey
  AND s.s_nationkey = n.n_nationkey
  AND n.n_name = 'GERMANY'
  AND p.p_brand = 'Brand#34'
