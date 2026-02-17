-- TPC-H Q5.1: Revenue by supplier nation, ASIA customers, ASIA suppliers, 1994-1996
SELECT sn.n_name AS supp_nation,
       EXTRACT(YEAR FROM o.o_orderdate) AS l_year,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM lineitem l, orders o, customer c, supplier s,
     nation cn, nation sn, region cr, region sr
WHERE l.l_orderkey = o.o_orderkey
  AND o.o_custkey = c.c_custkey
  AND l.l_suppkey = s.s_suppkey
  AND c.c_nationkey = cn.n_nationkey
  AND s.s_nationkey = sn.n_nationkey
  AND cn.n_regionkey = cr.r_regionkey
  AND sn.n_regionkey = sr.r_regionkey
  AND cr.r_name = 'ASIA'
  AND sr.r_name = 'ASIA'
  AND o.o_orderdate >= '1994-01-01' AND o.o_orderdate < '1997-01-01'
GROUP BY sn.n_name, EXTRACT(YEAR FROM o.o_orderdate)
ORDER BY l_year, revenue DESC
