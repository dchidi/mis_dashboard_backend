from tracemalloc import start
from typing import List, Literal
from fastapi import HTTPException
import pandas as pd
from sqlalchemy import TextClause
from app.services.db_operations import DBOperationsServices # noqa;
import logging
import time
import numpy as np
import os
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


class ETL:
    @staticmethod
    async def extraction(
        engine,  # pass SQLAlchemy engine directly
        start_date: str,
        end_date: str,
        country_code: str,
        country_name: str,
        query: TextClause,
        extraction_type: Literal["quote", "sales"] = "quote"
    ) -> pd.DataFrame:
        try:
            start = time.time()
 
            if extraction_type == 'quote':
                # Quote queries already handle end-date as exclusive in SQL using DATEADD(DAY, 1, ?)
                if country_code.lower() in ('nz', 'au'):
                    params = (start_date, end_date)
                else:  # UK, DE, AT have two subqueries, each needs start/end
                    params = (start_date, end_date, start_date, end_date)
            else:
                params = (start_date, end_date)
            

            # Use the engine directly — this engages SQLAlchemy's optimizations
            df = pd.read_sql_query(
                sql=query.text,
                con=engine,
                params= params 
            )

            df["CountryCode"] = country_code
            df["CountryName"] = country_name            
            
            if country_code.lower() in ('at', 'de'):
                df["Brand"] = "Petcover"

            end = time.time()
            logger.info(
                "Extracted %d rows for country_code: %s in %.2f seconds",
                len(df),
                country_code,
                end - start
            )
            return df

        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Extraction failed: {str(e)}"
            )

    @staticmethod
    async def transform(
        data: pd.DataFrame,
        cleanup_date: List[str] = ['CreatedDate', 'ETLDateUploaded']
    ) -> pd.DataFrame:
        try:
            data["ETLDateUploaded"] = pd.Timestamp.today().normalize().strftime("%Y-%m-%d") # Current date ETL was triggered

            # print(data["QuoteCreatedDate"])

            def clean_date(col: str):
                if col in data.columns:
                    parsed = pd.to_datetime(data[col], errors='coerce')
                    parsed = parsed.where(parsed >= pd.Timestamp('1753-01-01'))
                    data[col] = parsed.where(parsed.notna(), np.nan)

            for item in cleanup_date:
                clean_date(item)
            

            # # Save to excel
            # output_dir = "etl_outputs"
            # os.makedirs(output_dir, exist_ok=True)

            # excel_path = os.path.join(output_dir, f"transformed_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
            # data.to_excel(excel_path, index=False, engine="openpyxl")

            # logger.info(f"✅ Transformed data saved to Excel: {excel_path}")            

            return data

        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Transformation failed: {str(e)}"
            )

    @staticmethod
    def load(
        data: pd.DataFrame,
        table_name: str,
        db_engine,
        start_date: str,
        end_date: str,
    ):
        try:
            result = DBOperationsServices.delete_and_upload_data(
                df=data,
                table_name=table_name,
                db_engine=db_engine,
                start_date=start_date,
                end_date=end_date,
            )
            logger.info(result)
            return result
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Loading failed: {str(e)}"
            )
