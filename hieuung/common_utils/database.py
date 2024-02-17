import json
import os

from contextlib import contextmanager
from functools import wraps
from airflow.providers.postgres.hooks.postgres import PostgresHook

@contextmanager
def session(conn_id: str,
            database: str):
    """Provide a transactional scope around a series of operations."""
    hook = PostgresHook(postgres_conn_id= conn_id, schema= database)
    _session = hook.get_conn()
    try:
        yield _session
    finally:
        _session.close()


def with_session(func):
    @wraps(func)
    def with_session_func_wrapper(*args, **kwargs):
        if 'session' in kwargs:
            return func(*args, **kwargs)

        with session() as sess:
            return func(*args, session=sess, **kwargs)

    return with_session_func_wrapper


@contextmanager
def transaction(conn_id: str,
            database: str):
    """Provide a transactional scope around a series of operations."""
    hook = PostgresHook(postgres_conn_id= conn_id, schema= database)
    _session = hook.get_conn()
    try:
        yield _session
        _session.commit()
    except:
        _session.rollback()
        raise
    finally:
        _session.close()


def with_transaction(func):
    @wraps(func)
    def transaction_func_wrapper(*args, **kwargs):
        if 'session' in kwargs:
            return func(*args, **kwargs)

        with transaction() as sess:
            return func(*args, session=sess, **kwargs)

    return transaction_func_wrapper