"""Bound connection only; host configuration never leaks into migration files."""

from alembic import context
from sqlalchemy import MetaData

from centn_example.resource_models import ResourceBase

config = context.config
schema = config.attributes["resource_schema"]
connection = config.attributes["connection"]
metadata = MetaData()
for table in ResourceBase.metadata.tables.values():
    table.to_metadata(metadata, schema=schema)
context.configure(
    connection=connection,
    target_metadata=metadata,
    version_table_schema=schema,
    include_schemas=True,
    include_name=lambda name, kind, parent: kind != "schema" or name == schema,
)
with context.begin_transaction():
    context.run_migrations()
