-- TPC-H Q3.3: Revenue by cust/supp nation, FRANCE/GERMANY both sides, 1995
SELECT cn.n_name AS cust_nation, sn.n_name AS supp_nation,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM lineitem l, orders o, customer c, supplier s,
     nation cn, nation sn
WHERE l.l_orderkey = o.o_orderkey
  AND o.o_custkey = c.c_custkey
  AND l.l_suppkey = s.s_suppkey
  AND c.c_nationkey = cn.n_nationkey
  AND s.s_nationkey = sn.n_nationkey
  AND cn.n_name IN ('FRANCE', 'GERMANY')
  AND sn.n_name IN ('FRANCE', 'GERMANY')
  AND o.o_orderdate >= '1995-01-01' AND o.o_orderdate < '1996-01-01'
GROUP BY cn.n_name, sn.n_name
ORDER BY cust_nation, supp_nation
