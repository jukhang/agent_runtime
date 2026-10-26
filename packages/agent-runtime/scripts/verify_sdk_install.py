"""Install a local wheel outside the repository, then import the independent host."""

import argparse
import json
import subprocess
import tempfile
from pathlib import Path

CORE_SMOKE = """
import importlib.util
import sys
from importlib.metadata import version
from agent_runtime import Agent, Runtime, RunHandle
assert Agent(id='portable', name='Portable').id == 'portable'
assert Runtime and RunHandle
assert importlib.util.find_spec('sqlalchemy') is None
assert 'app' not in sys.modules and 'agno' not in sys.modules
print('base-wheel-isolated', version('agent-runtime'))
"""

HOST_SMOKE = """
import sys
from importlib.resources import files
from agent_runtime.storage.run_backend import SqlRunBackend
from agent_runtime.storage.sessions import SqlSessionStore
from agent_runtime.storage.main_factory import MainSetupFactory, MainHostPorts
from agent_runtime.storage.main_worker import SqlMainWorkerHandler
from agent_runtime.storage.planning import SqlPlanningSetupFactory
from agent_runtime.storage.interactions import SqlInteractionService
from centn_example.api import create_app
from centn_example.config import Settings
from centn_example.resource_migrate import config, resource_schema
from centn_example.resources import Resources
from centn_example.approvals import Approvals
from centn_example.delegation import Delegation
from alembic.script import ScriptDirectory
assert SqlRunBackend and SqlSessionStore and MainSetupFactory and create_app and Settings
assert SqlPlanningSetupFactory and SqlInteractionService
assert 'context' in MainHostPorts.__dataclass_fields__
import inspect
assert 'context' in inspect.signature(SqlMainWorkerHandler).parameters
assert Resources and Approvals and Delegation
assert resource_schema('external_runtime') == 'external_runtime_host'
host_migrations = ScriptDirectory.from_config(config('external_runtime_host'))
assert host_migrations.get_heads()
assert files('centn_example').joinpath('resource_migrations','env.py').is_file()
assert files('centn_example').joinpath('resource_migrations','script.py.mako').is_file()
catalogs = files('agent_runtime.storage.migrations.catalogs')
assert catalogs.joinpath('0008_human_input.json').is_file()
assert catalogs.joinpath('0009_approval_events.json').is_file()
from agent_runtime.contracts.payloads import ApprovalUpdated
from agent_runtime.storage.approval_cleanup import SqlApprovalCleanup
assert ApprovalUpdated and SqlApprovalCleanup
assert 'app' not in sys.modules and 'agno' not in sys.modules
assets = files('centn_example').joinpath('ui')
for name in ('index.html','app.mjs','journal.mjs','composer.mjs','style.css'):
    assert assets.joinpath(name).is_file(), name
print('storage-provider-host-isolated')
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", required=True, type=Path)
    options = parser.parse_args()
    wheel = options.wheel.resolve(strict=True)
    if wheel.suffix != ".whl":
        parser.error("--wheel must reference a local wheel")
    example = Path(__file__).resolve().parents[1] / "examples/standalone"
    with tempfile.TemporaryDirectory(prefix="centn-sdk-install-") as folder:
        work = Path(folder)
        environment = work / "venv"
        python = environment / "bin/python"

        def run(*command: str) -> None:
            subprocess.run(command, cwd=work, check=True, timeout=300)

        run("uv", "venv", "--python", "3.12", str(environment))
        run("uv", "pip", "install", "--python", str(python), str(wheel))
        run(str(python), "-I", "-c", CORE_SMOKE)
        run(
            "uv",
            "pip",
            "install",
            "--no-sources",
            "--python",
            str(python),
            f"{wheel}[storage,provider]",
            str(example),
        )
        run(str(python), "-I", "-c", HOST_SMOKE)
    print(json.dumps({"isolatedInstall": "passed", "releaseApproved": False}))


if __name__ == "__main__":
    main()
