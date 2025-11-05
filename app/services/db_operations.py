import pandas as pd
import traceback
from sqlalchemy import text, Table, MetaData, insert
from sqlalchemy.exc import SQLAlchemyError
from fastapi import HTTPException
import logging
from typing import Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class DBOperationsServices:

    @staticmethod
    def truncate_dataframe_to_table_schema(df: pd.DataFrame, table: Table) -> pd.DataFrame:
        """
        Dynamically truncate dataframe columns to match table schema lengths
        """
        df_clean = df.copy()
        valid_columns = []
        truncation_count = 0
        
        for column_name, column_obj in table.columns.items():
            if column_name in df_clean.columns:
                valid_columns.append(column_name)
                
                # Get max length from column type
                max_length = DBOperationsServices._get_sqlalchemy_length(column_obj.type)
                
                # Apply truncation if we found a length limit
                if max_length is not None and max_length > 0:
                    # Convert to string and truncate
                    original_series = df_clean[column_name].astype(str)
                    original_lengths = original_series.str.len()
                    
                    # Apply truncation
                    truncated_series = original_series.str.slice(0, max_length)
                    df_clean[column_name] = truncated_series
                    
                    # Check if any truncation actually happened
                    truncated_rows = (original_lengths > max_length).sum()
                    
                    if truncated_rows > 0:
                        truncation_count += truncated_rows
                        max_original = original_lengths.max()
                        logger.warning(f"✂️ Truncated {truncated_rows} rows in '{column_name}' from {max_original} to {max_length} chars")
        
        if truncation_count > 0:
            logger.info(f"📝 Total truncations applied: {truncation_count} rows")
        else:
            logger.info("✅ No truncation needed - all data fits within column limits")
        
        return df_clean[valid_columns]

    @staticmethod
    def _get_sqlalchemy_length(col_type) -> int | None:
        """
        Extract length from SQLAlchemy column type
        """
        # Try multiple approaches to get the length
        try:
            # Direct length attribute
            if hasattr(col_type, 'length') and col_type.length is not None:
                return col_type.length
        except:
            pass
        
        try:
            # For types like NVARCHAR, VARCHAR
            if hasattr(col_type, 'type'):
                inner_type = getattr(col_type, 'type')
                if hasattr(inner_type, 'length') and inner_type.length is not None:
                    return inner_type.length
        except:
            pass
        
        # If no length found, check the string representation
        try:
            type_str = str(col_type)
            if 'VARCHAR' in type_str.upper() or 'CHAR' in type_str.upper():
                # Extract length from string like "VARCHAR(50)"
                import re
                match = re.search(r'\((\d+)\)', type_str)
                if match:
                    return int(match.group(1))
        except:
            pass
        
        return None

    @staticmethod
    def delete_and_upload_data(df: pd.DataFrame, table_name: str, db_engine, start_date:str,
        end_date:str) -> Dict[str, Any]:
        """
        Memory-safe batch processing with:
        - Batched deletes to clear table
        - Millisecond-safe datetime conversion
        - Chunked inserts with retry & adaptive chunk sizing
        """
        try:
            # 1) Table name validation
            if not DBOperationsServices._is_valid_table_name(table_name):
                raise HTTPException(status_code=400, detail="Invalid table name")
            logger.info(f"🚀 Starting upload to {table_name} ({len(df):,} rows)")

            with db_engine.begin() as conn:
                # Phase 1: Batched table clearing
                logger.info("🧹 Clearing table with batched deletes...")
                delete_start = datetime.now()
                batch_size = 50_000
                total_deleted = 0

                start_dt = DBOperationsServices._coerce_datetime(start_date)
                end_dt = DBOperationsServices._coerce_datetime(end_date)

                
                delete_stmt = text(
                    f"DELETE TOP ({batch_size}) FROM {table_name} "
                    "WHERE CAST(CreatedDate AS DATE) BETWEEN :start_date AND :end_date"
                )

                

                while True:
                    result = conn.execute(
                        delete_stmt,
                        {"start_date": start_dt, "end_date": end_dt}
                    )
                    print(delete_stmt, {"start_date": start_dt, "end_date": end_dt})
                    deleted = result.rowcount or 0
                    total_deleted += deleted
                    logger.info(f"   Deleted batch of {deleted:,} rows (total: {total_deleted:,})")
                    if deleted == 0:
                        break

                logger.info(f"Table cleared in {(datetime.now() - delete_start).total_seconds():.2f}s")

                # Phase 2: Chunked insert
                metadata = MetaData()
                table = Table(table_name, metadata, autoload_with=db_engine)

                # Warn on column mismatches
                DBOperationsServices.validate_dataframe_against_table(df, table)

                # Single step: filter columns + truncate to schema limits
                df = DBOperationsServices.truncate_dataframe_to_table_schema(df, table)

                insert_chunk_size = 20_000
                total_rows = len(df)
                total_chunks = (total_rows // insert_chunk_size) + (1 if total_rows % insert_chunk_size else 0)
                total_inserted = 0
                chunk_num = 0

                def safe_value(val):
                    if pd.isna(val):
                        return None
                    if isinstance(val, pd.Timestamp):
                        # Convert to datetime and drop to nearest millisecond
                        dt = val.to_pydatetime()
                        ms = int(dt.microsecond / 1000) * 1000
                        return dt.replace(microsecond=ms)
                    return val

                while chunk_num < total_chunks:
                    start = chunk_num * insert_chunk_size
                    end = start + insert_chunk_size
                    chunk = df.iloc[start:end]

                    # build records list without deprecated applymap
                    if not chunk.empty:
                        base = chunk.to_dict(orient="records")
                        records = [
                            {col: safe_value(v) for col, v in row.items()}
                            for row in base
                        ]
                    else:
                        records = []

                    retries = 3
                    while retries > 0:
                        try:
                            insert_start = datetime.now()
                            # insert entire batch
                            conn.execute(insert(table), records)
                            n = len(records)
                            total_inserted += n
                            logger.info(
                                f"📦 Inserted chunk {chunk_num+1}/{total_chunks} "
                                f"({n:,} rows in {(datetime.now()-insert_start).total_seconds():.2f}s) | "
                                f"Total: {total_inserted:,}/{total_rows:,}"
                            )
                            break

                        except Exception as e:
                            retries -= 1
                            logger.warning(
                                f"Insert failed on chunk {chunk_num+1}/{total_chunks} "
                                f"(rows {start+1}-{min(end, total_rows)}): {e}"
                            )
                            logger.debug("Traceback:\n" + traceback.format_exc())
                            if records:
                                logger.debug(f"Sample row: {records[0]}")
                            # shrink chunk and retry
                            insert_chunk_size = max(1, insert_chunk_size // 2)
                            logger.info(f"⚠️ Retrying with smaller chunk size: {insert_chunk_size}")
                            end = start + insert_chunk_size
                            chunk = df.iloc[start:end]
                            base = chunk.to_dict(orient="records")
                            records = [
                                {col: safe_value(v) for col, v in row.items()}
                                for row in base
                            ]
                    else:
                        logger.error(f"❌ Failed to insert chunk {chunk_num+1} after 3 retries.")

                    chunk_num += 1

            return {"status": "success"}

        except SQLAlchemyError as e:
            err = f"Database operation failed: {e}"
            logger.error(f"💥 {err}\n{traceback.format_exc()}")
            raise HTTPException(status_code=500, detail=err)

        except Exception as e:
            err = f"Operation failed: {e}"
            logger.error(f"⛔ {err}\n{traceback.format_exc()}")
            raise HTTPException(status_code=500, detail=err)

    @staticmethod
    def _is_valid_table_name(name: str) -> bool:
        return (bool(name)
                and len(name) <= 128
                and all(c.isalnum() or c in ('_', '$') for c in name))

    @staticmethod
    def _coerce_datetime(value: Any) -> datetime:
        if value is None:
            raise HTTPException(status_code=400, detail="Date value is required")
        if isinstance(value, datetime):
            return value
        try:
            parsed = pd.to_datetime(value)
            return parsed.to_pydatetime() if hasattr(parsed, "to_pydatetime") else parsed
        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid date value: {value}",
            ) from exc

    @staticmethod
    def validate_dataframe_against_table(df: pd.DataFrame, table: Table) -> None:
        cols_table = set(table.columns.keys())
        cols_df = set(df.columns)
        extra = cols_df - cols_table
        missing = cols_table - cols_df
        if extra:
            logger.warning(f"⚠️ DataFrame has extra columns: {extra}")
        if missing:
            logger.warning(f"⚠️ Missing columns in DataFrame: {missing}")