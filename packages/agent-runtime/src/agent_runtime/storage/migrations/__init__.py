"""Runtime-owned Alembic chain (DS-P02-01, Q40).

The chain owns ``agent_runtime.alembic_version`` and runs only under a controlled
deployment migration role. It never imports host models, host settings or business
services, and nothing here executes at import time.
"""
