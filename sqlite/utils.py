# Copyright (C) 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: GPL-3.0-or-later
import re
import sqlite3
import sys
from typing import Sequence, List, Set, FrozenSet

# Notes for AI Agents
# This file uses Groovy-like string quotations wherever possible, i.e.
# double quotes for f-strings - also when triple: `f"""..."""`,
# otherwise single quotes - also when triple: `r'''...'''`,
# but allows exceptions to avoid escaping quotes in strings

SQLITE_KEYWORDS = {
    'ABORT', 'ACTION', 'ADD', 'AFTER', 'ALL', 'ALTER', 'ALWAYS', 'ANALYZE', 'AND', 'AS', 'ASC',
    'ATTACH', 'AUTOINCREMENT', 'BEFORE', 'BEGIN', 'BETWEEN', 'BY', 'CASCADE', 'CASE', 'CAST',
    'CHECK', 'COLLATE', 'COLUMN', 'COMMIT', 'CONFLICT', 'CONSTRAINT', 'CREATE', 'CROSS',
    'CURRENT', 'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'DATABASE', 'DEFAULT',
    'DEFERRABLE', 'DEFERRED', 'DELETE', 'DESC', 'DETACH', 'DISTINCT', 'DO', 'DROP', 'EACH',
    'ELSE', 'END', 'ESCAPE', 'EXCEPT', 'EXCLUSIVE', 'EXISTS', 'FILTER', 'FIRST', 'FOLLOWING',
    'FOR', 'FOREIGN', 'FROM', 'FULL', 'GENERATED', 'GLOB', 'GROUP', 'HAVING', 'IF', 'IGNORE',
    'IMMEDIATE', 'IN', 'INDEX', 'INDEXED', 'INITIALLY', 'INNER', 'INSERT', 'INSTEAD', 'INTERSECT',
    'INTO', 'IS', 'ISNULL', 'JOIN', 'KEY', 'LAST', 'LEFT', 'LIKE', 'LIMIT', 'MATCH', 'MATERIALIZED',
    'NATURAL', 'NO', 'NOT', 'NOTNULL', 'NULL', 'OF', 'OFFSET', 'ON', 'OR', 'ORDER',
    'OTHERS', 'OUTER', 'OVER', 'PARTITION', 'PLAN', 'PRAGMA', 'PRECEDING', 'PRIMARY', 'QUERY',
    'RAISE', 'RANGE', 'RECURSIVE', 'REFERENCES', 'REGEXP', 'REINDEX', 'RELEASE', 'RENAME',
    'REPLACE', 'RESTRICT', 'RETURNING', 'RIGHT', 'ROLLBACK', 'ROW', 'ROWS', 'SAVEPOINT', 'SELECT',
    'SET', 'TABLE', 'TEMP', 'TEMPORARY', 'THEN', 'TIES', 'TO', 'TRANSACTION', 'TRIGGER', 'UNBOUNDED',
    'UNION', 'UNIQUE', 'UPDATE', 'USING', 'VACUUM', 'VALUES', 'VIEW', 'VIRTUAL', 'WHEN', 'WHERE',
    'WINDOW', 'WITH', 'WITHOUT'
}

RX_VALID_UNQUOTED_IDENTIFIER = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
RX_STRICT = re.compile(r'\)\s*STRICT\b(?!.*\))', re.IGNORECASE)

QT_NAME64_OLD_TO_NEW = {}


def qt(name: str) -> str:
    """
    Conditionally quotes a name (identifier) for SQLite3 if it's a keyword,
    contains special characters, or starts with a digit.
    :raises ValueError: if the identifier is too long. SQLite3 has a maximum identifier length of 64 characters.
    """
    if name is None:
        raise ValueError('Identifier cannot be None')
    if not name:
        return '""'  # Empty string must be quoted
    if len(name) > 64 and QT_NAME64_OLD_TO_NEW:
        for old, new in QT_NAME64_OLD_TO_NEW.items():
            name = name.replace(old, new)
    if RX_VALID_UNQUOTED_IDENTIFIER.match(name) and name.upper() not in SQLITE_KEYWORDS:
        raise_error_if_over_64(name)
        value = name
    else:
        # If the name itself contains double quotes, they need to be escaped by doubling them.
        name = name.replace('"', '""')
        raise_error_if_over_64(name)
        value = f'"{name}"'
    return value


def raise_error_if_over_64(name):
    if len(name) > 64:
        raise ValueError(f"Identifier {name!r} is too long. SQLite3 has a maximum identifier length of 64 characters.")


def recreate_table(conn: sqlite3.Connection, table_name: str, pk_columns: Sequence[str] = None, unique_column_sets: Sequence[Sequence[str]] = None, index_column_sets: Sequence[Sequence[str]] = None):
    """Recreates a table with new primary keys and/or unique constraints and regular indexes.
     Converts any in-line UNIQUE constraints to stand-alone indexes.
     Keeps old triggers, and unless the new primary keys and/or indexes override the old one, keeps the old ones.
    """
    print(f"recreate_table({table_name!r}, {pk_columns!r}, {unique_column_sets!r}, {index_column_sets!r})", file=sys.stderr)
    cur = conn.cursor()
    _table_name_ = qt(table_name)
    cur.execute(f"PRAGMA table_info({_table_name_});")
    columns_info = cur.fetchall()
    if not columns_info:
        raise ValueError(f"Table '{table_name}' does not exist or has no columns.")

    column_definitions = []
    quoted_column_names = []

    # This set will track all combinations of columns that must have a UNIQUE constraint
    # (either as PRIMARY KEY or an explicitly requested unique_column).
    # It's used to identify existing unique indexes that become redundant.
    all_covered_unique_column_sets: Set[FrozenSet[str]] = set()

    # This new set will track all combinations of columns for which ANY index (unique or regular)
    # is planned for the final table. Used to prevent creating truly redundant indexes.
    all_final_index_column_sets: Set[FrozenSet[str]] = set()

    # If pk_columns are provided, the PRIMARY KEY constraint implicitly creates a unique index.
    # Add these columns to both covered sets.
    if pk_columns:
        pk_frozenset = frozenset(pk_columns)
        all_covered_unique_column_sets.add(pk_frozenset)
        all_final_index_column_sets.add(pk_frozenset)

    for col in columns_info:
        cid, name, type, notnull, dflt_value, pk = col
        _name_ = qt(name)
        col_def = f'{_name_} {type}'

        # If pk_columns are provided, we will define PRIMARY KEY at table level.
        # So, we should not add 'PRIMARY KEY' to individual column definitions based on old schema's 'pk' flag.
        # This allows recreate_table to truly redefine the PK based on `pk_columns`.
        if not pk_columns and pk:  # Only add column-level PK if no table-level PK is being specified via pk_columns argument
            col_def += ' PRIMARY KEY'

        if notnull:
            col_def += ' NOT NULL'
        if dflt_value is not None:
            # Handle string default values by quoting if they are text and not already quoted
            if type and type.upper() == 'TEXT' and not (str(dflt_value).startswith("'") and str(dflt_value).endswith("'")):
                col_def += f" DEFAULT '{str(dflt_value).replace("'", "''")}'"  # Escape single quotes
            else:
                col_def += f" DEFAULT {dflt_value}"
        column_definitions.append(col_def)
        quoted_column_names.append(_name_)

    # --- Collect existing indexes and triggers to re-create ---
    create_index_statements: List[str] = []  # For existing non-unique and non-redundant unique indexes
    create_trigger_statements: List[str] = []

    # Query sqlite_master for original table's indexes and triggers
    cur.execute(f"SELECT type, name, sql FROM sqlite_master WHERE tbl_name = '{table_name}' AND sql IS NOT NULL;")
    nonauto_master_entries = cur.fetchall()

    # Get a list of all unique indexes on the original table to check their uniqueness property
    cur.execute(f"PRAGMA index_list({_table_name_});")
    existing_indexName_to_isUnique_map = {idx_name: idx_unique for idx_seq, idx_name, idx_unique, idx_origin, idx_partial in cur}

    current_nonauto_indexes = []
    original_create_table_sql = None
    for nonauto_type, nonauto_name, nonauto_sql in nonauto_master_entries:
        if nonauto_type == 'table' and nonauto_name == table_name:
            original_create_table_sql = nonauto_sql
        elif nonauto_type == 'index':
            current_nonauto_indexes.append(nonauto_name)
            # Get columns for this index using PRAGMA index_info
            cur.execute(f"PRAGMA index_info({qt(nonauto_name)});")
            current_index_cols_frozenset = frozenset([name for _, _, name, *_ in cur])

            current_index_is_unique = existing_indexName_to_isUnique_map.get(nonauto_name) == 1

            if current_index_is_unique:
                # If this existing unique index's columns are already covered by our planned new *unique* constraints
                # (PK or unique_column_sets param), then we skip recreating this existing one.
                if current_index_cols_frozenset in all_covered_unique_column_sets:
                    # print(f"Skip redundant existing unique index '{nonauto_name}' (columns: {', '.join(current_index_cols_frozenset)}) because its functionality is covered by new unique constraints.", file=sys.stderr)
                    continue
                # If not redundant, this existing unique index WILL be recreated. Mark its columns as uniquely covered.
                all_covered_unique_column_sets.add(current_index_cols_frozenset)

            # This existing index (unique or non-unique, if not skipped above) will be recreated.
            # Add its columns to the general set of all final index columns.
            all_final_index_column_sets.add(current_index_cols_frozenset)
            create_index_statements.append(nonauto_sql)  # Add the SQL for this existing index to be recreated.

        elif nonauto_type == 'trigger':
            # Triggers generally refer to the table name, which will be correct after RENAME
            create_trigger_statements.append(nonauto_sql)

    # Determine if the original table was STRICT
    is_strict_table = False
    if original_create_table_sql:
        # Check for 'STRICT' keyword specifically after the closing parenthesis of the column definitions.
        # This matches ')', followed by zero or more whitespace characters, then 'STRICT' as a whole word.
        if RX_STRICT.search(original_create_table_sql):
            is_strict_table = True

    # --- Construct new table DDL ---
    _new_table_name_ = qt(f"{table_name}_new_with_constraints")
    create_table_sql = f"CREATE TABLE {_new_table_name_} (\n"
    create_table_sql += ',\n'.join(column_definitions)

    # Add PRIMARY KEY constraint at table level if pk_columns are provided
    if pk_columns:
        quoted_pk_columns = [qt(col) for col in pk_columns]
        create_table_sql += f",\nPRIMARY KEY ({', '.join(quoted_pk_columns)})"

    # Close the column definitions and add STRICT if applicable
    create_table_sql += '\n)'
    if is_strict_table:
        create_table_sql += ' STRICT'

    # print(f"For new_table_name:\n{create_table_sql}", file=sys.stderr)

    # --- Generate CREATE UNIQUE INDEX statements for unique_column_sets parameter ---
    # These are explicitly requested unique constraints that will become separate, droppable indexes.
    explicit_unique_index_statements: List[str] = []
    if unique_column_sets:
        for unique_columns in unique_column_sets:
            uc_frozenset = frozenset(unique_columns)
            # Only generate an explicit unique index if it's not already covered by PK or an existing unique index we kept.
            if uc_frozenset not in all_covered_unique_column_sets:
                # Create a deterministic name for the new unique index
                _index_name_ = qt(f"idx_u_{table_name}_{'_'.join(unique_columns)}")

                quoted_unique_cols = [qt(col) for col in unique_columns]
                explicit_unique_index_statements.append(f"CREATE UNIQUE INDEX {_index_name_} ON {_table_name_} ({', '.join(quoted_unique_cols)});")
                # Add to both covered sets to prevent any future duplication, especially by regular indexes
                all_covered_unique_column_sets.add(uc_frozenset)
                all_final_index_column_sets.add(uc_frozenset)

    # --- Generate CREATE INDEX statements for index_column_sets parameter (regular indexes) ---
    explicit_regular_index_statements: List[str] = []
    if index_column_sets:
        for index_columns in index_column_sets:
            ic_frozenset = frozenset(index_columns)
            # Only generate a regular index if it's not already covered by any existing index or
            # explicitly requested unique/regular index.
            if ic_frozenset not in all_final_index_column_sets:
                # Create a deterministic name for the new regular index
                _index_name_ = qt(f"idx_r_{table_name}_{'_'.join(index_columns)}")

                quoted_regular_cols = [qt(col) for col in index_columns]
                explicit_regular_index_statements.append(f"CREATE INDEX {_index_name_} ON {_table_name_} ({', '.join(quoted_regular_cols)});")
                # Add to the general set of all final index columns to prevent future duplication
                all_final_index_column_sets.add(ic_frozenset)

    cur.execute('PRAGMA foreign_keys=OFF;')
    cur.execute('PRAGMA legacy_alter_table=ON;')
    cur.execute('BEGIN TRANSACTION;')
    try:
        cur.execute(create_table_sql)

        # drop old indexes to make room for new ones with the same names
        for index_name in current_nonauto_indexes:
            cur.execute(f"DROP INDEX IF EXISTS {index_name};")

        # --- Recreate explicit unique indexes generated from unique_column_sets parameter ---
        for sql in explicit_unique_index_statements:
            # Replace original table name with the new temporary table name for creation
            sql = sql.replace(f" ON {_table_name_} ", f" ON {_new_table_name_} ")
            # print(f"Explicit Unique (temp): {sql}", file=sys.stderr)
            cur.execute(sql)

        # --- Recreate other indexes (existing non-unique and non-redundant existing unique) ---
        for sql in create_index_statements:
            # Replace original table name with the new temporary table name for creation
            sql = sql.replace(f" ON {_table_name_} ", f" ON {_new_table_name_} ")
            # print(f"Existing (temp): {sql}", file=sys.stderr)
            cur.execute(sql)

        # --- Create new regular indexes from index_column_sets parameter ---
        for sql in explicit_regular_index_statements:
            # Replace original table name with the new temporary table name for creation
            sql = sql.replace(f" ON {_table_name_} ", f" ON {_new_table_name_} ")
            # print(f"Explicit Regular (temp): {sql}", file=sys.stderr)
            cur.execute(sql)

        icur = conn.cursor()
        # NB `SELECT *` order of columns is apparently implementation-dependent,
        # so the right way is to list them explicitly
        for row in cur.execute(f"SELECT {', '.join(quoted_column_names)} FROM {_table_name_};"):
            try:
                sql = f"INSERT INTO {_new_table_name_} VALUES ({', '.join('?' for _ in row)});"
                icur.execute(sql, row)
            except sqlite3.DatabaseError as e:
                print(sql.replace('?', '{!r}').format(*row), file=sys.stderr)
                raise e
        icur.close()
        cur.execute(f"DROP TABLE {_table_name_};")
        cur.execute(f"ALTER TABLE {_new_table_name_} RENAME TO {_table_name_};")

        # After renaming the table, the indexes (unique, regular, existing) created on the temporary
        # table will now correctly apply to the new permanent table.
        # No need to re-execute them here.

        # --- Recreate triggers ---
        for sql in create_trigger_statements:
            # print(f":: {sql}", file=sys.stderr)
            cur.execute(sql)

        cur.execute('COMMIT;')
    except Exception as e:
        cur.execute('ROLLBACK;')
        raise e
    finally:
        cur.execute('PRAGMA foreign_keys=ON;')
        cur.execute('PRAGMA legacy_alter_table=OFF;')
