#!/usr/bin/env bash
# migration_pattern.sh - single source for the migration-path pattern.
# Sourced by migration_gate.sh and ship_commit.sh so the stamp hash
# and the verification pass always classify the same file set. Do not
# copy the regex into another script - source this file instead.
MIG_PAT='(^|/)(migrations?|alembic/versions|db/migrate|flyway|liquibase)/|(^|/)[VU][0-9]+(_[0-9]+)*__.*\.sql$'
