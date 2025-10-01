# Copyright (C) 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: GPL-3.0-or-later
import re
import sqlite3
from typing import Sequence, List, Set, FrozenSet

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
    'NATURAL', 'NO', 'NOT', 'NOTHING', 'NOTNULL', 'NULL', 'OF', 'OFFSET', 'ON', 'OR', 'ORDER',
    'OTHERS', 'OUTER', 'OVER', 'PARTITION', 'PLAN', 'PRAGMA', 'PRECEDING', 'PRIMARY', 'QUERY',
    'RAISE', 'RANGE', 'RECURSIVE', 'REFERENCES', 'REGEXP', 'REINDEX', 'RELEASE', 'RENAME',
    'REPLACE', 'RESTRICT', 'RETURNING', 'RIGHT', 'ROLLBACK', 'ROW', 'ROWS', 'SAVEPOINT', 'SELECT',
    'SET', 'TABLE', 'TEMP', 'TEMPORARY', 'THEN', 'TIES', 'TO', 'TRANSACTION', 'TRIGGER', 'UNBOUNDED',
    'UNION', 'UNIQUE', 'UPDATE', 'USING', 'VACUUM', 'VALUES', 'VIEW', 'VIRTUAL', 'WHEN', 'WHERE',
    'WINDOW', 'WITH', 'WITHOUT'
}

RX_VALID_UNQUOTED_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
RX_STRICT = re.compile(r'\)\s*STRICT\b(?!.*\))', re.IGNORECASE)


def qt(name: str) -> str:
    """
    Conditionally quotes a name (identifier) for SQLite3 if it's a keyword,
    contains special characters, or starts with a digit.
    :raises ValueError: if the identifier is too long. SQLite3 has a maximum identifier length of 64 characters.
    """
    if name is None:
        raise ValueError("Name cannot be None")
    if not name:
        return '""'  # Empty string must be quoted
    if RX_VALID_UNQUOTED_IDENTIFIER.match(name) and name.upper() not in SQLITE_KEYWORDS:
        value = name
    else:
        # If the name itself contains double quotes, they need to be escaped by doubling them.
        value = f'"{name.replace('"', '""')}"'
    if len(value) > 64:
        raise ValueError(f"Identifier {name!r} is too long. SQLite3 has a maximum identifier length of 64 characters.")
    return value


def recreate_table(cur: sqlite3.Cursor, table_name: str, pk_columns: Sequence[str] = None, unique_columns: Sequence[Sequence[str]] = None):
    """Recreates a table with new primary keys and/or unique constraints.
     Converts any in-line UNIQUE constraints to stand-alone indexes.
     Keeps old triggers, and unless the new primary keys and/or unique indexes override the old one, keeps the old ones.
    """
    _table_name_ = qt(table_name)
    cur.execute(f"PRAGMA table_info({_table_name_});")
    columns_info = cur.fetchall()
    if not columns_info:
        raise ValueError(f"Table '{table_name}' does not exist or has no columns.")

    column_definitions = []
    quoted_column_names = []

    # This set will track all combinations of columns that will have a UNIQUE constraint
    # (either as PRIMARY KEY, an explicitly requested unique_column, or an existing unique index we choose to recreate).
    # This helps prevent creating redundant unique indexes.
    all_covered_unique_column_sets: Set[FrozenSet[str]] = set()

    # If pk_columns are provided, the PRIMARY KEY constraint implicitly creates a unique index.
    # Add these columns to our covered set.
    if pk_columns:
        all_covered_unique_column_sets.add(frozenset(pk_columns))

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
    # This list will hold CREATE INDEX statements for non-unique indexes and existing unique indexes
    # that are not redundant with the new PK or explicitly requested unique_columns.
    create_index_statements: List[str] = []
    create_trigger_statements: List[str] = []

    # Query sqlite_master for original table's indexes and triggers
    cur.execute(f"SELECT type, name, sql FROM sqlite_master WHERE tbl_name = '{table_name}' AND sql IS NOT NULL;")
    nonauto_master_entries = cur.fetchall()

    # Get a list of all unique indexes on the original table to check their uniqueness property
    cur.execute(f"PRAGMA index_list({_table_name_});")
    existing_indexName_to_isUnique_map = {idx_name: idx_unique for idx_seq, idx_name, idx_unique, idx_origin, idx_partial in cur}

    original_create_table_sql = None
    for nonauto_type, nonauto_name, nonauto_sql in nonauto_master_entries:
        if nonauto_type == 'table' and nonauto_name == table_name:
            original_create_table_sql = nonauto_sql
        elif nonauto_type == 'index':
            current_index_is_unique = existing_indexName_to_isUnique_map.get(nonauto_name) == 1

            if current_index_is_unique:
                # Get columns for this unique index using PRAGMA index_info
                cur.execute(f"PRAGMA index_info('{nonauto_name}');")
                index_columns_info = cur
                current_index_cols_frozenset = frozenset([name for _, _, name, *_ in index_columns_info])

                # If this unique index's columns are already covered by our planned constraints, skip recreating it.
                if current_index_cols_frozenset in all_covered_unique_column_sets:
                    # print(f"Skip redundant existing unique index '{nonauto_name}' (columns: {', '.join(current_index_cols_frozenset)}) because its functionality is covered.", file=sys.stderr)
                    continue

                # If we decide to recreate this existing unique index, add its columns to our covered set.
                all_covered_unique_column_sets.add(current_index_cols_frozenset)

            # Add the CREATE INDEX statement for non-unique indexes or non-redundant unique indexes.
            # The SQL from sqlite_master correctly references the original table name, which will be the new table name after RENAME.
            create_index_statements.append(nonauto_sql)
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

    # IMPORTANT: We no longer add UNIQUE constraints directly into the CREATE TABLE statement here.
    # Instead, they will be created as separate UNIQUE INDEXes to allow them to be dropped later.
    # Close the column definitions and add STRICT if applicable
    create_table_sql += "\n)"
    if is_strict_table:
        create_table_sql += " STRICT"

    # print(f"For new_table_name:\n{create_table_sql}", file=sys.stderr)

    # --- Generate CREATE UNIQUE INDEX statements for unique_columns parameter ---
    # These are explicitly requested unique constraints that will become separate, droppable indexes.
    explicit_unique_index_statements: List[str] = []
    if unique_columns:
        for unique_constraint in unique_columns:
            uc_frozenset = frozenset(unique_constraint)
            # Only generate an explicit unique index if it's not already covered by PK or an existing unique index we plan to recreate.
            if uc_frozenset not in all_covered_unique_column_sets:
                unique_cols = [col for col in unique_constraint]
                quoted_unique_cols = [qt(col) for col in unique_constraint]
                # Create a deterministic name for the new unique index
                index_name_suffix = '_'.join(col for col in unique_cols)
                _index_name_ = qt(f"idx_u_{table_name}_{index_name_suffix}")

                explicit_unique_index_statements.append(
                    f"CREATE UNIQUE INDEX {_index_name_} ON {_table_name_} ({', '.join(quoted_unique_cols)});"
                )
                # Add to all_covered_unique_column_sets to prevent any future duplication
                all_covered_unique_column_sets.add(uc_frozenset)

    cur.execute("PRAGMA foreign_keys=OFF;")
    cur.execute("PRAGMA legacy_alter_table=ON;")
    cur.execute("BEGIN TRANSACTION;")
    try:
        cur.execute(create_table_sql)
        quoted_columns_list_str = ", ".join(quoted_column_names)
        cur.execute(f"INSERT INTO {_new_table_name_} ({quoted_columns_list_str}) SELECT {quoted_columns_list_str} FROM {_table_name_};")
        cur.execute(f"DROP TABLE {_table_name_};")
        cur.execute(f"ALTER TABLE {_new_table_name_} RENAME TO {_table_name_};")

        # --- Recreate explicit unique indexes generated from unique_columns parameter ---
        for sql in explicit_unique_index_statements:
            # print(f"Explicit: {sql}", file=sys.stderr)
            cur.execute(sql)

        # --- Recreate other indexes (existing non-unique and non-redundant existing unique) ---
        for sql in create_index_statements:
            # print(f"Existing: {sql}", file=sys.stderr)
            cur.execute(sql)

        # --- Recreate triggers ---
        for sql in create_trigger_statements:
            # print(f":: {sql}", file=sys.stderr)
            cur.execute(sql)

        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e
    finally:
        cur.execute("PRAGMA foreign_keys=ON;")
        cur.execute("PRAGMA legacy_alter_table=OFF;")
