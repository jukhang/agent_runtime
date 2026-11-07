# Shared Schema Ownership

The migration Python entry point accepts optional `host_relations`, an exact set
of disjoint PostgreSQL relation names. Default remains empty and rejects unknown
relations. No wildcard or current/historical Runtime identity may be declared as
host-owned. Names come from trusted deployment configuration, never user inputs.

This allows independently versioned host and Runtime Alembic chains to share a
schema without moving data. Preflight excludes only explicitly declared host
identities. Structural checks continue to compare every Runtime column, index and
constraint. Unexpected undeclared tables still fail. Host tables are neither
upgraded nor dropped by the Runtime chain. A host is responsible for its own
Alembic chain and must share the schema advisory lock to serialize deployment.

For deployments with separate schemas, no host declaration is required. Runtime
CLI defaults remain strict. This change introduces no DDL and no data migration.
