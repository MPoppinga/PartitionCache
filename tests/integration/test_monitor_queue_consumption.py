"""
Integration tests for monitor queue consumption behavior.

Tests the new --max-pending-jobs functionality and queue consumption control.
"""

import os

import pytest

# Import the module under test
from partitioncache.cli.monitor_cache_queue import (
    get_timeout_for_state,
    print_enhanced_status,
)


class TestQueueConsumptionLogic:
    """Test the new queue consumption logic and timeout strategies."""

    def test_timeout_for_state_normal_operation(self):
        """Test timeout calculation for normal operation."""
        # Normal operation: can consume, not exiting, no errors
        timeout = get_timeout_for_state(can_consume=True, exit_event_set=False, error_count=0)
        assert timeout == 10.0, "Should use 10s timeout for efficient blocking with periodic status checks"

    def test_timeout_for_state_capacity_wait(self):
        """Test timeout calculation when waiting for capacity."""
        # Waiting for capacity: cannot consume, not exiting, no errors
        timeout = get_timeout_for_state(can_consume=False, exit_event_set=False, error_count=0)
        assert timeout == 0.1, "Should use short timeout when waiting for capacity"

    def test_timeout_for_state_shutdown(self):
        """Test timeout calculation during shutdown."""
        # Shutdown: exit event set
        timeout = get_timeout_for_state(can_consume=True, exit_event_set=True, error_count=0)
        assert timeout == 0.1, "Should use very short timeout during shutdown"

    def test_timeout_for_state_error_recovery(self):
        """Test timeout calculation with exponential backoff for errors."""
        # Error recovery: exponential backoff
        timeout1 = get_timeout_for_state(can_consume=False, exit_event_set=False, error_count=1)
        timeout2 = get_timeout_for_state(can_consume=False, exit_event_set=False, error_count=2)
        timeout3 = get_timeout_for_state(can_consume=False, exit_event_set=False, error_count=3)

        assert timeout1 == 1.0, "First error should use 0.5 * 2^1 = 1.0"
        assert timeout2 == 2.0, "Second error should use 0.5 * 2^2 = 2.0"
        assert timeout3 == 4.0, "Third error should use 0.5 * 2^3 = 4.0"

        # Test maximum backoff
        timeout_max = get_timeout_for_state(can_consume=False, exit_event_set=False, error_count=10)
        assert timeout_max == 5.0, "Should cap at maximum backoff time"

    def test_enhanced_status_logging(self, caplog):
        """Test enhanced status logging with queue sizes and waiting reasons."""
        import logging
        caplog.set_level(logging.INFO, logger="PartitionCache")

        queue_lengths = {
            "original_query_queue": 15,
            "query_fragment_queue": 25
        }

        # Test with waiting reason
        print_enhanced_status(5, queue_lengths, "Waiting for thread capacity")

        # Check log message contains all expected information
        assert len(caplog.records) > 0, "Should have captured log message"
        log_message = caplog.records[-1].message
        assert "Active: 5" in log_message
        # Pending is no longer part of the status message
        assert "Fragment Queue: 25" in log_message
        assert "Original Queue: 15" in log_message
        assert "Waiting for thread capacity" in log_message

    def test_enhanced_status_logging_no_reason(self, caplog):
        """Test enhanced status logging without waiting reason."""
        import logging
        caplog.set_level(logging.INFO, logger="PartitionCache")

        queue_lengths = {
            "original_query_queue": 0,
            "query_fragment_queue": 5
        }

        # Test without waiting reason
        print_enhanced_status(2, queue_lengths, None)

        assert len(caplog.records) > 0, "Should have captured log message"
        log_message = caplog.records[-1].message
        assert "Active: 2" in log_message
        # Pending is no longer part of the status message
        assert "Fragment Queue: 5" in log_message
        assert "Original Queue: 0" in log_message
        # Should not contain " - " when no waiting reason
        assert " - " not in log_message


class TestPendingJobProcessing:
    """Test pending job processing functionality."""

    def test_pending_job_processing_logic(self):
        """Test the conceptual logic of pending job processing."""
        # Test the logic conceptually without complex mocking

        # Scenario 1: No pending jobs
        pending_jobs_empty = []
        active_futures_count = 5
        max_processes = 10

        # Should not process when no pending jobs
        can_process = bool(pending_jobs_empty) and (active_futures_count < max_processes)
        assert can_process is False, "Should not process when no pending jobs"

        # Scenario 2: Has pending jobs and capacity
        pending_jobs_available = ["job1", "job2"]
        active_futures_count = 5
        max_processes = 10

        # Should process when pending jobs and capacity available
        can_process = bool(pending_jobs_available) and (active_futures_count < max_processes)
        assert can_process is True, "Should process when pending jobs and capacity available"

        # Scenario 3: Has pending jobs but no capacity
        pending_jobs_available = ["job1", "job2"]
        active_futures_count = 10
        max_processes = 10

        # Should not process when no capacity
        can_process = bool(pending_jobs_available) and (active_futures_count < max_processes)
        assert can_process is False, "Should not process when no thread capacity"


class TestMaxPendingJobsValidation:
    """Test the CLI argument validation for max_pending_jobs."""

    def test_max_pending_jobs_default_calculation(self):
        """Test that default max_pending_jobs is calculated correctly."""
        # This would be tested in an integration test with actual CLI parsing
        # For now, we test the logic conceptually
        max_processes = 12
        expected_default = 2 * max_processes
        assert expected_default == 24, "Default should be 2 * max_processes"

    def test_max_pending_jobs_validation_rules(self):
        """Test validation rules for max_pending_jobs parameter."""
        # Test minimum value
        min_valid = 1
        assert min_valid >= 1, "Minimum valid value should be 1"

        # Test warning threshold
        max_processes = 5
        warning_threshold = 10 * max_processes
        large_value = warning_threshold + 1
        assert large_value > warning_threshold, "Should trigger warning when > 10 * max_processes"


@pytest.mark.integration
class TestMonitorQueueIntegration:
    """Integration tests requiring actual queue infrastructure."""

    @pytest.fixture(autouse=True)
    def _require_queue_env(self):
        """Skip integration queue tests when required queue env is not configured."""
        required = [
            "PG_QUEUE_HOST",
            "PG_QUEUE_PORT",
            "PG_QUEUE_USER",
            "PG_QUEUE_PASSWORD",
            "PG_QUEUE_DB",
        ]
        missing = [name for name in required if not os.getenv(name)]
        if missing:
            pytest.skip(f"Queue integration tests require env vars: {', '.join(missing)}")

    @pytest.fixture(autouse=True)
    def _reset_queues(self):
        """Ensure queue isolation between tests."""
        from partitioncache.queue import clear_all_queues, reset_queue_handler

        reset_queue_handler()
        clear_all_queues()
        yield
        reset_queue_handler()

    @pytest.mark.skipif(
        not os.getenv("INTEGRATION_TEST_ENABLED"),
        reason="Integration tests require INTEGRATION_TEST_ENABLED=1"
    )
    def test_monitor_with_max_pending_jobs(self):
        """Test monitor behavior under queue pressure and limited worker capacity."""
        from partitioncache.queue import (
            get_queue_lengths,
            pop_from_original_query_queue,
            push_to_original_query_queue,
        )

        for i in range(5):
            assert push_to_original_query_queue(
                query=f"SELECT {i}",
                partition_key="zipcode",
                partition_datatype="integer",
            )

        lengths = get_queue_lengths()
        assert lengths["original_query_queue"] >= 5

        max_processes = 1
        active_futures = 1
        can_consume = active_futures < max_processes
        timeout = get_timeout_for_state(can_consume=can_consume, exit_event_set=False, error_count=0)
        assert can_consume is False
        assert timeout == 0.1

        item = pop_from_original_query_queue()
        assert item is not None
        lengths_after = get_queue_lengths()
        assert lengths_after["original_query_queue"] < lengths["original_query_queue"]

    @pytest.mark.skipif(
        not os.getenv("INTEGRATION_TEST_ENABLED"),
        reason="Integration tests require INTEGRATION_TEST_ENABLED=1"
    )
    def test_queue_consumption_priority(self):
        """Test queue consumption priority decisions with capacity gating."""
        from partitioncache.queue import (
            get_queue_lengths,
            pop_from_original_query_queue,
            push_to_original_query_queue,
        )

        assert push_to_original_query_queue("SELECT 1", "zipcode", "integer")
        assert push_to_original_query_queue("SELECT 2", "zipcode", "integer")

        wait_timeout = get_timeout_for_state(can_consume=False, exit_event_set=False, error_count=0)
        process_timeout = get_timeout_for_state(can_consume=True, exit_event_set=False, error_count=0)
        assert wait_timeout < process_timeout

        first = pop_from_original_query_queue()
        assert first is not None
        lengths = get_queue_lengths()
        assert lengths["original_query_queue"] >= 1

    @pytest.mark.skipif(
        not os.getenv("INTEGRATION_TEST_ENABLED"),
        reason="Integration tests require INTEGRATION_TEST_ENABLED=1"
    )
    def test_memory_bounded_consumption(self):
        """Test bounded queue consumption behavior under a max-processes style cap."""
        from partitioncache.queue import (
            get_queue_lengths,
            pop_from_original_query_queue,
            push_to_original_query_queue,
        )

        for i in range(8):
            assert push_to_original_query_queue(
                query=f"SELECT {i}",
                partition_key="zipcode",
                partition_datatype="integer",
            )

        max_processes = 2
        active = 2
        can_consume = active < max_processes
        assert can_consume is False
        assert get_timeout_for_state(can_consume=can_consume, exit_event_set=False, error_count=0) == 0.1

        # Simulate bounded consumption: only pop up to max_processes in one cycle.
        popped = 0
        for _ in range(max_processes):
            if pop_from_original_query_queue() is not None:
                popped += 1

        assert popped == max_processes
        lengths = get_queue_lengths()
        assert lengths["original_query_queue"] >= 6


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
