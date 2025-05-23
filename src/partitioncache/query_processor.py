"""
Methods for splitting and reassembling queries
"""

import hashlib
import itertools
import logging
import re
from collections import defaultdict
from itertools import combinations
from typing import Any, Dict, List, Tuple

import networkx as nx  # type: ignore
import sqlglot
import sqlglot.expressions
import sqlglot.optimizer
import sqlglot.optimizer.canonicalize
import sqlglot.optimizer.normalize
import sqlglot.optimizer.simplify

logger = logging.getLogger("PartitionCache")


def clean_query(query: str) -> str:
    """
    Perform some basic cleaning of the query to ensure a stable output even if the query is formatted differently.
    """

    query = re.sub(r"\s+", " ", query)
    query = re.sub(r"\s*=\s*", "=", query)

    # Normalize the query
    sqlglot_query = sqlglot.parse_one(query)

    # Normalize the query (including building DNF)
    nquery = sqlglot.optimizer.normalize.normalize(sqlglot_query)
    query = nquery.sql()

    # Removing all comments
    query = re.sub(r"--.*", "", query)

    # Removing LIMIT
    query = re.sub(r"LIMIT\s\d+", "", query)

    # Removing tailing semicolon
    query = re.sub(r";\s*$", "", query)

    # Remove all double quotes(postgresql) and backticks(mysql) from the normalized query
    query = query.replace('"', "")  # TODO currently double quotes are not supported in identifiers in PartitionCache
    query = query.replace("`", "")  # TODO currently backticks are not supported in identifiers in PartitionCache
    
    return query


def all_connected_subgraphs(G: nx.Graph, min_comp_size: int, max_comp_size: int):
    """
    Get all connected subgraphs of the given graph.
    """
    con_comp = [c for c in sorted(nx.connected_components(G), key=len, reverse=True)]

    def recursive_local_expand(node_set, possible, excluded, results, max_size):
        """
        Recursive function to add an extra node to the subgraph being formed
        """
        results.append(node_set)
        if len(node_set) == max_size:
            return
        for j in possible - excluded:
            new_node_set = node_set | {j}
            excluded = excluded | {j}
            new_possible = (possible | set(G.neighbors(j))) - excluded
            recursive_local_expand(new_node_set, new_possible, excluded, results, max_size)

    results: list = []
    for cc in con_comp:
        max_size = len(cc)

        excluded = set()
        for i in G:
            excluded.add(i)
            recursive_local_expand({i}, set(G.neighbors(i)) - excluded, excluded, results, max_size)

    result_set: set = {frozenset(r) for r in results}

    ret: dict[int, Any] = {}

    for r in result_set:
        r_len = len(r)
        # print(r, r_len, min_comp_size, max_comp_size)

        if min_comp_size <= r_len <= max_comp_size:
            if r_len in ret:
                ret[len(r)].append(r)
            else:
                ret[len(r)] = [r]

    return ret


def generate_tuples(
    edges: List[Tuple[str, str]],
    table_aliases: List,
    min_component_size: int,
    follow_graph: bool,
) -> Dict[int, List[Tuple[str, str]]]:
    """
    Generates sets of tuples of table aliases. Grouped by the number of tables in the tuple.
    If follow_graph is True, the function will only return partial queries that are connected to each other.
    If follow_graph is False, it will return all possible combinations of tables.
    """
    result: dict = {}
    if follow_graph:
        # Gett all variants with are connected to each other
        g = nx.Graph(edges)
        result = all_connected_subgraphs(g, min_component_size, 15)
    else:
        # Get all Permuations
        for i in range(min_component_size, 15):
            result[i] = list(combinations(table_aliases, i))

    return result


def remove_single_conditions(
    conditions: Dict[str, List[str]],
) -> List[Dict[str, List[str]]]:
    """
    If in one condition more thand one attribute is used, remove one of the attributes (all possible outcomes)
    Returns all new conditions together with the original conditions
    """
    ret = [conditions]
    for key in conditions.keys():
        if len(conditions[key]) > 1:
            for i in range(len(conditions[key])):
                new_conditions = conditions.copy()
                new_conditions[key] = [conditions[key][i]]
                ret.append(new_conditions)
    return ret


def extract_conjunctive_conditions(sql: str) -> list[str]:
    """
    Returns all conjunctive conditions from a query.
    """
    parsed = sqlglot.parse_one(sql)
    where_clause = parsed.find(sqlglot.expressions.Where)
    conditions = []

    def extract_conditions_from_expression(expression):
        if isinstance(expression, sqlglot.expressions.And):
            extract_conditions_from_expression(expression.left)
            extract_conditions_from_expression(expression.right)
        else:
            conditions.append(expression.sql())

    if where_clause:
        extract_conditions_from_expression(where_clause.this)

    return conditions


def extract_and_group_query_conditions(
    query, partition_key
) -> tuple[
    dict[str, list[str]],
    dict[tuple[str, str], list[str]],
    dict[tuple, list[str]],
    list[str],
    dict[tuple, list[str]],
    list[str],
    str,
]:
    """
    Extracts all conditions from a query
    Splits it by distance functions, attributes and subqueries for the partition key
    """
    attribute_conditions: dict[str, list[str]] = dict()  # {table_alias: [conditions]}
    distance_conditions: dict[tuple[str, str], list[str]] = defaultdict(list)  # {(table_alias1, table_alias2): [conditions]}
    other_functions: dict[tuple, list[str]] = defaultdict(list)  # {(table_alias1, ...): [conditions]}
    partition_key_conditions: list[str] = []  #   # List of all subqueries
    or_conditions: dict[tuple, list[str]] = defaultdict(list)  # {(table_alias1, table_alias2, ...): [conditions(w/alias)]}

    # get tables
    tables = re.split("FROM|WHERE", query, flags=re.IGNORECASE)[1]
    tables = tables.split(",")
    tables = [x.strip() for x in tables]

    # warn if more than one table is used
    if len(set([re.split(r'\s+', x)[0] for x in tables])) > 1:
        table_names = set([re.split(r'\s+', x)[0] for x in tables])
        logger.warning(f"More than one table is used in the query ({', '.join(table_names)}). This may cause unexpected behavior.")

    table_aliases = [re.split(r'\s+', x)[-1] for x in tables]
    table = [re.split(r'\s+', x)[0] for x in tables][0]  # TODO Ensure robustness

    # define emtpy list for attribute_conditions for each tablealias
    for ta in table_aliases:
        attribute_conditions[ta] = list()

    # get all conditions from where clause
    condition_list: list[str] = extract_conjunctive_conditions(query)

    # Iterate through all conditions and sort them into table_conditions and distance_conditions
    for condition in condition_list:
        # find/ignore partition_key join_conditions
        if re.match(rf"\w*\.{partition_key}\s=\s\w*\.{partition_key}", condition):
            continue  # Skip as its just the join condition
        elif condition.count(partition_key) >= 1 and sqlglot.parse_one(condition).find(sqlglot.expressions.In):
            # if partition_key is in condition, it is a subquery condition or a list check
            _, *cons = condition.split(".")
            con = ".".join(cons)
            partition_key_conditions.append(con)
            continue

        # count how many times a table alias is in the condition
        nr_alias_in_condition = 0
        for alias in table_aliases:
            nr_alias_in_condition += condition.count(f"{alias}.")  # TODO Needs to be more robust

        # if only one table alias is in the condition, it is an attribute condition
        if nr_alias_in_condition == 1 and "." in condition:
            al, *cons = condition.split(".")
            con = ".".join(cons)
            attribute_conditions[al].append(con)
            continue

        # if two table aliases are in the condition
        else:
            all_alias: set = set(re.findall(r"[a-zA-Z_]\w*(?=\.)", condition))
            if sqlglot.parse_one(condition).find(sqlglot.expressions.Func):
                if len(all_alias) == 2:
                    all_alias_list = sorted(list(all_alias))
                    distance_conditions[(all_alias_list[0], all_alias_list[1])].append(condition)
                    continue
                else:
                    parsed = sqlglot.parse_one(condition)
                    table_identifiers = tuple(sorted(list({col.table for col in parsed.find_all(sqlglot.expressions.Column) if col.table})))
                    other_functions[table_identifiers].append(condition)
                    continue

            elif sqlglot.parse_one(condition).find(sqlglot.expressions.Or):
                parsed = sqlglot.parse_one(condition)
                table_identifiers = tuple(sorted(list({col.table for col in parsed.find_all(sqlglot.expressions.Column) if col.table})))
                or_conditions[table_identifiers].append(condition)
                continue
            else:
                if len(all_alias) == 2:
                    # get all aliases with sqlglot
                    parsed = sqlglot.parse_one(condition)
                    table_identifiers = tuple(sorted(list({col.table for col in parsed.find_all(sqlglot.expressions.Column) if col.table})))
                    distance_conditions[(table_identifiers[0], table_identifiers[1])].append(condition)
                    continue
                else:
                    parsed = sqlglot.parse_one(condition)
                    table_identifiers = tuple(sorted(list({col.table for col in parsed.find_all(sqlglot.expressions.Column) if col.table})))
                    other_functions[table_identifiers].append(condition)
                    continue

    return (
        attribute_conditions,
        distance_conditions,
        other_functions,
        partition_key_conditions,
        or_conditions,
        table_aliases,
        table,
    )


def generate_partial_queries(
    query: str,
    partition_key: str,
    min_component_size,
    follow_graph=True,
    keep_all_attributes=True,
    other_functions_as_distance_conditions=True,
) -> list[str]:
    """
    This function takes a query and returns the list of all possible partial queries.
    query: str: The query to be split
    partition_key: str: The identifier of the search space
    min_component_size: int: The minimum number of tables in the partial queries
    follow_graph: bool: If True, the function will only return partial queries that are connected to each other.    If False, it will return all possible combinations of tables
    keep_all_attributesbool: If True, the function will only return partial queries with the original attributes.    If False, it will return also partial queries with fewer attributes

    return: List[str]: List of all possible partial queries

    """

    # init variables
    ret: list[str] = []  # List of all possible partial queries for return

    (
        attribute_conditions,
        distance_conditions,
        other_functions,
        partition_key_conditions,
        or_conditions,  # TODO Fix # TODO what to fix here?
        table_aliases,
        table,
    ) = extract_and_group_query_conditions(query, partition_key)

    # Get all possible combinations of tables (grouped by number of tables in the tuple)
    query_combinations = generate_tuples(
        list(distance_conditions.keys()),
        table_aliases,
        min_component_size,
        follow_graph,
    )

    all_query_combinations = []
    for v in query_combinations.values():
        all_query_combinations.extend(v)

    # Make sure Table conditions are sorted
    for key in attribute_conditions.keys():
        attribute_conditions[key].sort()

    # Create all possible partial queries based on query_combinations and table_conditions
    # Each loop iteration creates a new partial query
    for combination in all_query_combinations:
        if keep_all_attributes:
            table_conditions_vaiants = [attribute_conditions]
        else:
            table_conditions_vaiants = remove_single_conditions(attribute_conditions)  # create variants with less attributes, TODO make this more efficient

        # If multiple variants of table conditions are available, create a partial query for each variant
        for var_attribute_conditions in table_conditions_vaiants:
            new_table_list = []  # List of all table aliases in this partial query

            # Put table condition keys in the right order based on table conditions item lists
            table_conditions_keys = sorted(
                [y for y in var_attribute_conditions.keys() if y in combination],
                key=lambda x: "".join(var_attribute_conditions[x]),
            )
            if len(table_conditions_keys) == 0:
                continue

            # Filter distance conditions that are relevant for the current combination
            relvant_conditions_for_combination: list[str] = []
            for x in distance_conditions.keys():
                if x[0] in combination and x[1] in combination:
                    dcond: list[str] = distance_conditions[x]
                    relvant_conditions_for_combination.extend(dcond)

            if other_functions_as_distance_conditions:
                for x in other_functions.keys():
                    if all(key in combination for key in x):
                        ocond: list[str] = other_functions[x]
                        relvant_conditions_for_combination.extend(ocond)

            if or_conditions:
                for x in or_conditions.keys():
                    if all(key in combination for key in x):
                        orcond: list[str] = or_conditions[x]
                        relvant_conditions_for_combination.extend(orcond)

            # Build the query
            query_where: list[str] = []
            i = 0  # Index for new table aliases
            for table_condition_key in table_conditions_keys:
                i += 1
                table_alias = f"t{i}"  # Enumerate table aliases
                new_table_list.append(table_alias)
                query_where.extend([f"{table_alias}.{x}" for x in var_attribute_conditions[table_condition_key]])

                # set the correct table alias in the distance conditions
                pattern = r"\b{}\b".format(re.escape(table_condition_key))
                relvant_conditions_for_combination = [re.sub(pattern, table_alias, cc) for cc in relvant_conditions_for_combination]

            # Add distance conditions to the query
            for rdist in relvant_conditions_for_combination:
                query_where.append(rdist)

            # Add join conditions for given partition_key
            for i in range(1, len(new_table_list)):
                for j in range(i + 1, len(new_table_list) + 1):
                    query_where.append(f"{new_table_list[i-1]}.{partition_key} = {new_table_list[j-1]}.{partition_key}")

            new_table_list_with_alias = [f"{table} AS {a}" for a in new_table_list]
            q = f"SELECT DISTINCT {new_table_list[0]}.{partition_key} FROM {', '.join(new_table_list_with_alias)} WHERE {' AND '.join(query_where)}"

            ret.append(q)

            if partition_key_conditions:  # TODO handle CTE expressions
                # Create variants of the query with subqueries
                for i in range(1, len(partition_key_conditions) + 1):
                    for comb in itertools.combinations(partition_key_conditions, i):
                        query_where_comb = query_where.copy()
                        # For each combination of subqueries, create a new partial query
                        for subquery in comb:  # Add all partition_key_conditions of this combination to the query
                            p_subquery = f"{new_table_list[0]}.{subquery}"
                            query_where_comb.append(p_subquery)
                        q = f"SELECT DISTINCT {new_table_list[0]}.{partition_key} FROM {', '.join(new_table_list_with_alias)} WHERE {' AND '.join(query_where_comb)}"
                        ret.append(q)

    # Add also the raw partition_key queries
    for partition_key_part in partition_key_conditions:
        # Get part in brackets
        part = re.search(r"\((.*)\)", partition_key_part)
        if part:
            spart = part.group(1)
        else:
            continue
        sublist = spart.split("INTERSECT")
        for sub in sublist:
            sub = sub.strip()
            ret.append(sub)

    return ret


def is_distance_function(condition: str) -> bool:
    if re.match(
        # r"\w*\([a-zA-Z0-9_\.]*[,\+]\s*[a-zA-Z0-9_\.]*\)", condition
        r"\w*\(.*[,\+].*\)",
        condition,
    ):  # TODO make more robust
        return True
    else:
        return False


def normalize_distance_conditions(original_query: str, bucket_steps: float = 1.0, restrict_to_dist_functions=True) -> str:
    """
    This function takes a query and normalizes the distance conditions.
    It replaces the distance conditions with lower bound bucket bracket  and upper bound bucket bracket
    of the BETWEEN clause.

    Min values gets replaced by the next lower bucket value
    Max values gets replaced by the next higher bucket value
    eg.
    1.6 - 3.6 -> 1 - 4 WITH bucket_steps = 1
    1.6 - 3.6 -> 0 - 4 WITH bucket_steps = 2

    query: str: The query to be normalized
    partition_key: str: The identifier of the search space
    """
    bucket_steps = float(bucket_steps)

    original_query = sqlglot.parse_one(original_query).sql()
    condition_list = extract_conjunctive_conditions(original_query)

    query = original_query
    distance_conditions_between = []
    distance_conditions_smaller = []
    distance_conditions_greater = []

    for condition in condition_list:
        if " BETWEEN " in condition.upper():
            distance_conditions_between.append(condition)
        elif "<" in condition or "<=" in condition:
            distance_conditions_smaller.append(condition)
        elif ">" in condition or ">=" in condition:
            distance_conditions_greater.append(condition)
        else:
            pass

    # TODO Ensure one of the values is a number
    # TODO Ensure number is on second position
    # TODO Ensure number is not negative
    
    # Check if at least one value is a number
    for distance_condition in distance_conditions_between + distance_conditions_smaller + distance_conditions_greater:
        if not any(str(x).replace('.','',1).isdigit() for x in distance_condition.split()):
            logger.warning(f"No numeric value found in distance condition: {distance_condition}")
            
    # Check if number is on right side of comparison operator for distance functions
    for distance_condition in distance_conditions_between + distance_conditions_smaller + distance_conditions_greater:
        if is_distance_function(distance_condition):
            if "<" in distance_condition or ">" in distance_condition:
                parts = re.split(r'(<=|>=|<|>)', distance_condition)
                if len(parts) == 2 and not str(parts[1].strip()).replace('.','',1).isdigit():
                    logger.warning(f"Numeric value not on right side of comparison in distance condition: {distance_condition}")
                
    # Check for negative numbers
    for distance_condition in distance_conditions_between + distance_conditions_smaller + distance_conditions_greater:
        numbers = [float(x) for x in re.findall(r'-?\d*\.?\d+', distance_condition)]
        if any(n < 0 for n in numbers):
            logger.warning(f"Negative value found in distance condition: {distance_condition}")

    for distance_condition in distance_conditions_between:
        if restrict_to_dist_functions and not is_distance_function(distance_condition):
            continue
        lower_bound, upper_bound = distance_condition.split(" BETWEEN ")[1].split(" AND ")

        lower_bound = lower_bound.strip()
        lower_bound = lower_bound.replace(";", "")
        upper_bound = upper_bound.strip()
        upper_bound = upper_bound.replace(";", "")

        # do nothing if either is negative
        if float(lower_bound) < 0 or float(upper_bound) < 0:
            continue

        # Floor lower bound
        lower_boundf = int(float(lower_bound) / bucket_steps) * bucket_steps

        # Ceil upper bound
        if int(float(upper_bound) / bucket_steps) * bucket_steps == float(upper_bound):
            upper_boundf = int(float(upper_bound) / bucket_steps) * bucket_steps
        else:
            upper_boundf = int(float(upper_bound) / bucket_steps) * bucket_steps + bucket_steps

        new_condition = f"{distance_condition.split('BETWEEN')[0]} BETWEEN {lower_boundf:g} AND {upper_boundf:g}"
        query = query.replace(distance_condition, new_condition)

    # TODO ensure robustness with flipped conditions
    for distance_condition in distance_conditions_smaller:
        if restrict_to_dist_functions and not is_distance_function(distance_condition):
            continue
        if "<=" in distance_condition:
            value = distance_condition.split("<=")[1].strip()
            value = value.replace(";", "")
            if float(value) < 0:
                continue
            valuef = (int(float(value) / bucket_steps) * bucket_steps) + bucket_steps
            new_condition = f"{distance_condition.split('<=')[0]} <= {valuef:g}"
        else:
            value = distance_condition.split("<")[1].strip()
            value = value.replace(";", "")
            if float(value) < 0:
                continue
            b = (int(float(value) / bucket_steps) * bucket_steps)
            if b == float(value):
                valuef = b 
            else:
                valuef = b + bucket_steps
            new_condition = f"{distance_condition.split('<')[0]} < {valuef:g}"
        query = query.replace(distance_condition, new_condition)

    for distance_condition in distance_conditions_greater:
        if restrict_to_dist_functions and not is_distance_function(distance_condition):
            continue
        if ">=" in distance_condition:
            value = distance_condition.split(">=")[1].strip()
            value = value.replace(";", "")
            if float(value) < 0:
                continue
            valuef = int(float(value) / bucket_steps) * bucket_steps
            new_condition = f"{distance_condition.split('>=')[0]} >= {valuef:g}"
        else:
            value = distance_condition.split(">")[1].strip()
            value = value.replace(";", "")
            if float(value) < 0:
                continue
            valuef = int(float(value) / bucket_steps) * bucket_steps
            new_condition = f"{distance_condition.split('>')[0]} > {valuef:g}"
        query = query.replace(distance_condition, new_condition)

    return " ".join(query.split())


def generate_all_query_hash_pairs(
    query: str,
    partition_key: str,
    min_component_size,
    follow_graph=True,
    keep_all_attributes=True,
    canonicalize_queries=False,
) -> List[Tuple[str, str]]:
    query_set = set()

    # Clean the query
    query = clean_query(query)

    # Create all possible partial queries
    query_set_diff_combinations = set(
        generate_partial_queries(query, partition_key, min_component_size, follow_graph, keep_all_attributes, other_functions_as_distance_conditions=True)
    )
    query_set.update(query_set_diff_combinations)

    # Create bucket variant with normalized distances (Example: 1.6 - 3.6 -> 1 - 4 WITH bucket_steps = 1)
    nor_dist_query = normalize_distance_conditions(query)

    query_set.update(
        set(
            generate_partial_queries(
                nor_dist_query,
                partition_key,
                min_component_size,
                follow_graph,
                keep_all_attributes,
                other_functions_as_distance_conditions=True,  # TODO evaluate if how performance is affected if is turned off
            )
        )
    )

    if canonicalize_queries:
        # canonicalize each query (to make sure that the hash is unique for the same query)
        # Not needed if the query is already canonicalized (e.g. if the query is always generated by the same functions)
        can_query_set = set()
        for baseq in query_set:  # TODO Performance needs to be improved
            sqlglot_query = sqlglot.parse_one(baseq)
            nquery = sqlglot.optimizer.canonicalize.canonicalize(sqlglot_query)
            q = nquery.sql()
            can_query_set.add(q)
        return [(q, hash_query(q)) for q in can_query_set]
    else:
        return [(q, hash_query(q)) for q in query_set]


def hash_query(query: str) -> str:
    return hashlib.sha1(query.encode()).hexdigest()


def generate_all_hashes(
    query: str,
    partition_key: str,
    min_component_size,
    follow_graph=True,
    fix_attributes=True,
    canonicalize_queries=False,
) -> List[str]:
    """
    Generates all hashes for the given query.
    """
    qh_pairs = generate_all_query_hash_pairs(
        query,
        partition_key,
        min_component_size,
        follow_graph,
        fix_attributes,
        canonicalize_queries,
    )
    return [x[1] for x in qh_pairs]


if __name__ == "__main__":
    print("To use the query processor, please refer to the documentation")
