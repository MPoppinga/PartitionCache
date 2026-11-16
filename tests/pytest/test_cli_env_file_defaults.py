"""Variables in the --env file set the argument defaults of the CLI tools, as the exported variables do.

The defaults of the variant generation and spatial arguments are read from the environment while the parser is built.
The tools load the --env file before that; a flag on the command line still wins over the environment, and an
exported shell variable wins over the file (python-dotenv does not override variables that are already set).
"""

import os
import sys
from unittest.mock import patch

import pytest

from partitioncache.cli import add_to_cache, monitor_cache_queue, postgresql_cache_eviction, postgresql_queue_processor

ENV_FILE_VARS = {
    "PARTITION_CACHE_BUCKET_STEPS": "2.5",
    "PARTITION_CACHE_MIN_COMPONENT_SIZE": "2",
    "PARTITION_CACHE_FOLLOW_GRAPH": "false",
    "PARTITION_CACHE_SKIP_PARTITION_KEY_JOINS": "pk",
    "PARTITION_CACHE_ADD_CONSTRAINTS": '{"t": "x = 1"}',
    "PARTITION_CACHE_REMOVE_CONSTRAINTS_ALL": '["y"]',
    "PARTITION_CACHE_MAX_CONDITIONS_REMOVED": "3",
    "PARTITION_CACHE_GEOMETRY_COLUMN": "shape",
    "PARTITION_CACHE_GRID_CELL_SIZES": "cell=200",
    "PARTITION_CACHE_PARTITION_JOIN_TABLE": "p0",
}


class _Parsed(BaseException):
    """Stops main() right after the arguments are parsed (a BaseException, so no `except Exception` in main() catches it)."""


@pytest.fixture
def env_file(tmp_path):
    path = tmp_path / "run.env"
    path.write_text("".join(f"{name}='{value}'\n" for name, value in ENV_FILE_VARS.items()))
    return path


@pytest.fixture
def clean_env():
    """The variables are unset before the test and the whole environment is restored after it (load_dotenv writes os.environ)."""
    with patch.dict(os.environ):
        for name in ENV_FILE_VARS:
            os.environ.pop(name, None)
        yield


def _parsed_args(module, argv: list[str]):
    """The args namespace main() of ``module`` parses from ``argv``."""
    captured = {}

    def capture(args):
        captured["args"] = args
        raise _Parsed

    with patch.object(sys, "argv", argv), patch.object(module, "parse_variant_generation_json_args", side_effect=capture), pytest.raises(_Parsed):
        module.main()
    return captured["args"]


def _add_argv(*extra: str) -> list[str]:
    return ["pcache-add", "--queue", "--query", "SELECT 1", "--partition-key", "pk", *extra]


def _assert_env_file_values(args) -> None:
    assert args.bucket_steps == 2.5
    assert args.min_component_size == 2
    assert args.follow_graph is False
    assert args.skip_partition_key_joins == ["pk"]
    assert args.add_constraints == '{"t": "x = 1"}'
    assert args.remove_constraints_all == '["y"]'
    assert args.max_conditions_removed == 3
    assert args.geometry_column == "shape"
    assert args.grid_cell_size == {"cell": 200.0}
    assert args.partition_join_table == "p0"


@pytest.mark.parametrize("option", ["--env-file", "--env"])
def test_pcache_add_takes_defaults_from_the_env_file(env_file, clean_env, option):
    _assert_env_file_values(_parsed_args(add_to_cache, _add_argv(option, str(env_file))))


def test_pcache_add_option_equals_form(env_file, clean_env):
    _assert_env_file_values(_parsed_args(add_to_cache, _add_argv(f"--env-file={env_file}")))


def test_pcache_monitor_takes_defaults_from_the_env_file(env_file, clean_env):
    _assert_env_file_values(_parsed_args(monitor_cache_queue, ["pcache-monitor", "--env", str(env_file)]))


def test_flag_wins_over_env_file(env_file, clean_env):
    args = _parsed_args(add_to_cache, _add_argv("--env", str(env_file), "--bucket-steps", "0.5", "--min-component-size", "1"))
    assert args.bucket_steps == 0.5
    assert args.min_component_size == 1
    assert args.follow_graph is False  # still from the file


def test_exported_variable_wins_over_env_file(env_file, clean_env):
    os.environ["PARTITION_CACHE_BUCKET_STEPS"] = "4.0"
    args = _parsed_args(monitor_cache_queue, ["pcache-monitor", "--env", str(env_file)])
    assert args.bucket_steps == 4.0
    assert args.min_component_size == 2


def test_missing_env_file_leaves_the_built_in_defaults(tmp_path, clean_env):
    args = _parsed_args(add_to_cache, _add_argv("--env", str(tmp_path / "absent.env")))
    assert args.bucket_steps == 1.0
    assert args.min_component_size == 1


@pytest.mark.parametrize("module, prog", [(postgresql_queue_processor, "pcache-postgresql-queue-processor"), (postgresql_cache_eviction, "pcache-postgresql-eviction-manager")])
def test_job_owner_default_comes_from_the_env_file(tmp_path, clean_env, module, prog):
    path = tmp_path / "owner.env"
    path.write_text("PARTITIONCACHE_JOB_OWNER=cron_owner\n")
    os.environ.pop("PARTITIONCACHE_JOB_OWNER", None)
    captured = {}

    def capture(args):
        captured["args"] = args
        raise _Parsed

    with patch.object(sys, "argv", [prog, "--env", str(path), "setup"]), patch.object(module, "configure_logging", side_effect=capture), pytest.raises(_Parsed):
        module.main()
    assert captured["args"].job_owner == "cron_owner"
