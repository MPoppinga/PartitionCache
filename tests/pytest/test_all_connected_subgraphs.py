"""
Unit tests for all_connected_subgraphs function.

This function is critical for the follow_graph=True functionality in query processing,
ensuring that spatial queries maintain proper table connectivity.
"""

import networkx as nx
import pytest

from partitioncache.query_processor import all_connected_subgraphs


class TestAllConnectedSubgraphs:
    """Test suite for all_connected_subgraphs function."""

    def test_empty_graph(self):
        """Test with empty graph."""
        G = nx.Graph()
        result = all_connected_subgraphs(G, min_comp_size=1, max_comp_size=5)
        assert result == {}

    def test_single_node(self):
        """Test with single isolated node."""
        G = nx.Graph()
        G.add_node(1)
        result = all_connected_subgraphs(G, min_comp_size=1, max_comp_size=5)
        expected = {1: [frozenset([1])]}
        assert result == expected

    def test_single_edge(self):
        """Test with simple two-node graph."""
        G = nx.Graph()
        G.add_edge(1, 2)
        result = all_connected_subgraphs(G, min_comp_size=1, max_comp_size=5)

        # Should contain: {1}, {2}, {1,2}
        assert 1 in result
        assert 2 in result
        assert len(result[1]) == 2  # {1} and {2}
        assert len(result[2]) == 1  # {1,2}
        assert frozenset([1, 2]) in result[2]

    def test_linear_chain(self):
        """Test with linear chain: 1-2-3."""
        G = nx.Graph()
        G.add_edges_from([(1, 2), (2, 3)])
        result = all_connected_subgraphs(G, min_comp_size=1, max_comp_size=5)

        # Expected: {1}, {2}, {3}, {1,2}, {2,3}, {1,2,3}
        assert 1 in result and len(result[1]) == 3  # All individual nodes
        assert 2 in result and len(result[2]) == 2  # {1,2}, {2,3}
        assert 3 in result and len(result[3]) == 1  # {1,2,3}

        # Verify specific subgraphs
        size_3_subgraphs = [sorted(s) for s in result[3]]
        assert [1, 2, 3] in size_3_subgraphs

    def test_disconnected_components(self):
        """Test with multiple disconnected components."""
        G = nx.Graph()
        G.add_edges_from([(1, 2), (3, 4)])  # Two separate edges
        result = all_connected_subgraphs(G, min_comp_size=1, max_comp_size=5)

        # Should have 4 size-1 and 2 size-2 subgraphs
        assert len(result[1]) == 4  # {1}, {2}, {3}, {4}
        assert len(result[2]) == 2  # {1,2}, {3,4}

        # Verify no size-3 or larger (components are disconnected)
        assert 3 not in result
        assert 4 not in result

    def test_star_topology(self):
        """Test with star topology: center node connected to multiple leaves."""
        G = nx.Graph()
        G.add_edges_from([(1, 2), (1, 3), (1, 4)])  # Node 1 in center
        result = all_connected_subgraphs(G, min_comp_size=1, max_comp_size=5)

        # Should include all combinations of connected nodes
        assert 1 in result and len(result[1]) == 4  # All individual nodes
        assert 2 in result and len(result[2]) == 3  # {1,2}, {1,3}, {1,4}
        assert 3 in result and len(result[3]) == 3  # {1,2,3}, {1,2,4}, {1,3,4}
        assert 4 in result and len(result[4]) == 1  # {1,2,3,4}

    def test_complex_example_from_issue(self):
        """Test with the specific example from the GitHub issue."""
        pairs = {(1, 2), (2, 3), (4, 5), (6, 7), (7, 8), (7, 9)}
        G = nx.Graph()
        G.add_edges_from(pairs)

        result = all_connected_subgraphs(G, min_comp_size=1, max_comp_size=10)

        # Verify all expected subgraphs are present
        [frozenset([i]) for i in range(1, 10)]
        expected_size_2 = [frozenset([1, 2]), frozenset([2, 3]), frozenset([4, 5]), frozenset([6, 7]), frozenset([7, 8]), frozenset([7, 9])]
        expected_size_3 = [frozenset([1, 2, 3]), frozenset([6, 7, 8]), frozenset([6, 7, 9]), frozenset([7, 8, 9])]
        expected_size_4 = [frozenset([6, 7, 8, 9])]

        assert len(result[1]) == 9
        assert len(result[2]) == 6
        assert len(result[3]) == 4
        assert len(result[4]) == 1

        # Verify specific subgraphs
        for expected in expected_size_2:
            assert expected in result[2]
        for expected in expected_size_3:
            assert expected in result[3]
        for expected in expected_size_4:
            assert expected in result[4]

    def test_size_constraints(self):
        """Test min_comp_size and max_comp_size constraints."""
        G = nx.Graph()
        G.add_edges_from([(1, 2), (2, 3), (3, 4)])  # Linear chain of 4 nodes

        # Test min_comp_size=2, max_comp_size=3
        result = all_connected_subgraphs(G, min_comp_size=2, max_comp_size=3)

        # Should exclude size-1 and size-4 subgraphs
        assert 1 not in result
        assert 4 not in result
        assert 2 in result
        assert 3 in result

    def test_cycle_topology(self):
        """Test with cycle topology."""
        G = nx.Graph()
        G.add_edges_from([(1, 2), (2, 3), (3, 4), (4, 1)])  # 4-node cycle
        result = all_connected_subgraphs(G, min_comp_size=1, max_comp_size=5)

        # In a cycle, there should be more connectivity than a linear chain
        assert 1 in result and len(result[1]) == 4
        assert 2 in result and len(result[2]) == 4  # Each node connects to 2 others
        assert 3 in result and len(result[3]) == 4  # Multiple 3-node paths
        assert 4 in result and len(result[4]) == 1  # Full cycle

    def test_performance_larger_graph(self):
        """Test performance with larger graph (10 nodes in a path)."""
        G = nx.Graph()
        edges = [(i, i + 1) for i in range(1, 10)]  # 1-2-3-...-10
        G.add_edges_from(edges)

        # This should complete without timeout
        result = all_connected_subgraphs(G, min_comp_size=1, max_comp_size=5)

        # Verify basic structure
        assert 1 in result and len(result[1]) == 10
        assert 5 in result and len(result[5]) > 0

    def test_complete_subgraph(self):
        """Test with complete graph (every node connected to every other)."""
        G = nx.Graph()
        nodes = [1, 2, 3, 4]
        for i in nodes:
            for j in nodes:
                if i < j:
                    G.add_edge(i, j)

        result = all_connected_subgraphs(G, min_comp_size=1, max_comp_size=4)

        # Complete graph should have maximum connectivity
        assert 4 in result and len(result[4]) == 1  # Only one way to include all nodes
        assert 3 in result and len(result[3]) == 4  # 4 ways to choose 3 nodes
        assert 2 in result and len(result[2]) == 6  # 6 ways to choose 2 nodes
        assert 1 in result and len(result[1]) == 4  # 4 individual nodes

    def test_return_type_is_frozenset(self):
        """Verify that subgraphs are returned as frozensets."""
        G = nx.Graph()
        G.add_edge(1, 2)
        result = all_connected_subgraphs(G, min_comp_size=1, max_comp_size=2)

        # All returned subgraphs should be frozensets
        for size_group in result.values():
            for subgraph in size_group:
                assert isinstance(subgraph, frozenset)

    def test_no_duplicate_subgraphs(self):
        """Verify no duplicate subgraphs in results."""
        G = nx.Graph()
        G.add_edges_from([(1, 2), (2, 3), (1, 3)])  # Triangle
        result = all_connected_subgraphs(G, min_comp_size=1, max_comp_size=3)

        # Check for uniqueness within each size group
        for size_group in result.values():
            unique_subgraphs = set(size_group)
            assert len(unique_subgraphs) == len(size_group), "Found duplicate subgraphs"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

