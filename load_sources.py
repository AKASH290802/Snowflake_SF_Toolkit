import pandas as pd


def validate_snowflake_column_mapping(column_mapping):
    """Reject mappings that would create duplicate Salesforce column names."""
    if not column_mapping:
        return
    targets = list(column_mapping.values())
    duplicates = sorted({target for target in targets if targets.count(target) > 1})
    if duplicates:
        raise ValueError(
            'Snowflake column mapping has duplicate Salesforce target field(s): '
            + ', '.join(repr(target) for target in duplicates)
            + '. Map each source column to a different field.'
        )


def stream_snowflake_batches(connection, query, chunk_size, column_mapping=None,
                             should_stop=None, on_rows=None):
    """Yield bounded row batches using a dedicated cursor owned by this iterator."""
    if chunk_size < 1:
        raise ValueError('chunk_size must be positive')
    validate_snowflake_column_mapping(column_mapping)
    cursor = connection.cursor()
    fetched = 0
    try:
        if should_stop and should_stop():
            return
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        while not (should_stop and should_stop()):
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            frame = pd.DataFrame.from_records(rows, columns=columns)
            if column_mapping:
                frame = frame.rename(columns=column_mapping)
                frame = frame[[column for column in column_mapping.values() if column in frame.columns]]
            fetched += len(frame)
            if on_rows:
                on_rows(fetched)
            yield frame
    finally:
        cursor.close()