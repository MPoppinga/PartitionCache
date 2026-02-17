-- TPC-H Q4.2: Profit by year, AMERICA cust+supp, 1997-1998, Manufacturer#1/#2
SELECT EXTRACT(YEAR FROM o.o_orderdate) AS o_year,
       SUM(l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity) AS profit
FROM lineitem l, orders o, customer c, supplier s, part p, partsupp ps,
     nation cn, nation sn, region cr, region sr
WHERE l.l_orderkey = o.o_orderkey
  AND o.o_custkey = c.c_custkey
  AND l.l_suppkey = s.s_suppkey
  AND l.l_partkey = p.p_partkey
  AND l.l_partkey = ps.ps_partkey AND l.l_suppkey = ps.ps_suppkey
  AND c.c_nationkey = cn.n_nationkey
  AND s.s_nationkey = sn.n_nationkey
  AND cn.n_regionkey = cr.r_regionkey
  AND sn.n_regionkey = sr.r_regionkey
  AND cr.r_name = 'AMERICA'
  AND sr.r_name = 'AMERICA'
  AND o.o_orderdate >= '1997-01-01' AND o.o_orderdate < '1999-01-01'
  AND (p.p_mfgr = 'Manufacturer#1' OR p.p_mfgr = 'Manufacturer#2')
GROUP BY EXTRACT(YEAR FROM o.o_orderdate)
ORDER BY o_year
