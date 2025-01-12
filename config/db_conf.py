import xml.etree.ElementTree as ET

from sqlalchemy import create_engine, text

from config.logging_conf import logger
from config.sentry_conf import SENTRY_LOGGING


def connect_to_db(db_uri):
    try:
        # Create a SQLAlchemy engine
        engine = create_engine(db_uri)
        return engine
    except Exception as e:
        SENTRY_LOGGING.capture_exception(e)
        logger.error(f"Error connecting to the database: {e}")
        raise e


def load_queries_from_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()

    queries = {}
    for query in root.findall('query'):
        query_id = query.get('id')
        query_text = query.text.strip()
        queries[query_id] = query_text
    return queries


def execute_raw_queries(str_sql, engine, params=None):
    with engine.connect() as conn:
        # Begin a transaction
        trans = conn.begin()
        try:
            logger.info(f"Executing raw queries for: {str_sql}")
            logger.info(f"Params: {params}")
            result = conn.execute(text(str_sql), params or {})
            # Commit the transaction
            trans.commit()
            logger.info(f"Affected rows: {result.rowcount}")
            logger.info("SQL executed successfully and changes committed.")
        except Exception as e:
            # Rollback in case of error
            SENTRY_LOGGING.capture_exception(e)
            trans.rollback()
            logger.error(f"Error executing SQL: {e}")
            raise e


def select_raw_query(str_sql, engine, params=None):
    with engine.connect() as conn:
        try:
            logger.info(f"Select query: {str_sql}")
            logger.info(f"Params: {params}")
            result = conn.execute(text(str_sql), params or {})
            # Fetch all results
            # rows = result.fetchall()
            rows = [dict(row._mapping) for row in result.fetchall()]

            logger.info(f"fetched {len(rows)} rows.")
            return rows
        except Exception as e:
            SENTRY_LOGGING.capture_exception(e)
            logger.error(f"Error executing select query: {e}")
            raise e
