import asyncio
from fastapi import APIRouter, Depends, Query, HTTPException
import pandas as pd
from datetime import date, timedelta
from sqlalchemy.engine import Engine
from app.db.sqlserver import (
    get_mis_db_engine, get_uk_uts_engine,
    get_nz_uts_engine, get_au_uts_engine,
    get_at_uts_engine, get_de_uts_engine
)
from app.services.etl import ETL
from app.db.sql_server_queries.crm_query import CRM_Mkt_Query

import logging

from app.db.sql_server_queries.au_nz_quote_query import AU_NZ_QUOTE_Query
from app.db.sql_server_queries.uk_de_at_quote_query import UK_DE_AT_QUOTE_Query
from app.db.sql_server_queries.au_nz_sales_query import AU_NZ_SALES_Query
from app.db.sql_server_queries.uk_de_at_sales_query import UK_DE_AT_SALES_Query
from app.db.sql_server_queries.au_nz_free_policy_query import AU_NZ_FREE_POLICY_Query
from app.db.sql_server_queries.uk_de_at_free_policy_query import UK_DE_AT_FREE_POLICY_Query
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/etl_route")
async def etl_route(
    nz_db: Engine = Depends(get_nz_uts_engine),
    au_db: Engine = Depends(get_au_uts_engine),
    mis_db: Engine = Depends(get_mis_db_engine),
    uk_db: Engine = Depends(get_uk_uts_engine),
    at_db: Engine = Depends(get_at_uts_engine),
    de_db: Engine = Depends(get_de_uts_engine),
    start_date: date = Query(default=date.today() - timedelta(days=365)),
    end_date: date = Query(default=date.today())
):
    # Run the three ETL steps concurrently and await their results
    quote_task = await etl_quote(
        nz_db=nz_db, au_db=au_db, mis_db=mis_db, uk_db=uk_db,
        at_db=at_db, de_db=de_db, start_date=start_date, end_date=end_date
    )
    sales_task = await etl_sales(
        nz_db=nz_db, au_db=au_db, mis_db=mis_db, uk_db=uk_db,
        at_db=at_db, de_db=de_db, start_date=start_date, end_date=end_date
    )
    fp_task = await etl_free_policies(
        nz_db=nz_db, au_db=au_db, mis_db=mis_db, uk_db=uk_db,
        at_db=at_db, de_db=de_db, start_date=start_date, end_date=end_date
    )

    # quote_res, sales_res, fp_res = await asyncio.gather(quote_task, sales_task, fp_task)

    return {
        "message": "ETL process completed successfully.",
        "quote_response": quote_task,
        "sales_response": sales_task,
        "free_policy_response": fp_task,
    }


@router.get("/etl_quote")
async def etl_quote(
    nz_db: Engine = Depends(get_nz_uts_engine),
    au_db: Engine = Depends(get_au_uts_engine),
    mis_db: Engine = Depends(get_mis_db_engine),
    uk_db: Engine = Depends(get_uk_uts_engine),
    at_db: Engine = Depends(get_at_uts_engine),
    de_db: Engine = Depends(get_de_uts_engine),
    start_date: date = Query(default=date.today() - timedelta(days=365)),
    end_date: date = Query(default=date.today())
):
    logger.info('quote etl starts')
    table_name = "Quote"

    regions = [
        {"country_code": "NZ", "country_name": "New Zealand", "engine": nz_db, "query":AU_NZ_QUOTE_Query},
        {"country_code": "AU", "country_name": "Australia", "engine": au_db, "query":AU_NZ_QUOTE_Query},
        {"country_code": "UK", "country_name": "United Kingdom", "engine": uk_db, "query":UK_DE_AT_QUOTE_Query},
        {"country_code": "DE", "country_name": "Germany", "engine": de_db, "query":UK_DE_AT_QUOTE_Query},
        {"country_code": "AT", "country_name": "Austria", "engine": at_db, "query":UK_DE_AT_QUOTE_Query},
    ]

    iso_start_date = start_date.isoformat()
    iso_end_date = end_date.isoformat()

    async def extract_and_transform(region):
        extracted_data = await ETL.extraction(
            engine=region["engine"],
            start_date=iso_start_date,
            end_date=iso_end_date,
            country_code=region["country_code"],
            country_name=region["country_name"],
            query=region["query"]
        )
        return await ETL.transform(
            extracted_data,
            [
                'CreatedDate', 'QuoteStartDate', 'QuoteExpiryDate',
                'PolicyStartDate', 'PolicyEndDate', 'PetBirthDate',
                'ETLDateUploaded'
            ]
        )

    tasks = [extract_and_transform(region) for region in regions]
    all_transformed_data = await asyncio.gather(*tasks)

    combined_data = pd.concat(all_transformed_data, ignore_index=True)

    logger.info(
        f"Combined {len(combined_data)} rows from {', '.join([r['country_code'] for r in regions])}"  # noqa
    )

    load_msg = ETL.load(combined_data, table_name, mis_db, start_date=iso_start_date,
            end_date=iso_end_date,)

    return {
        # "message": "ETL process completed successfully.",
        "rows_loaded": len(combined_data),
        "load_status": load_msg
    }



@router.get("/etl_sales")
async def etl_sales(
    nz_db: Engine = Depends(get_nz_uts_engine),
    au_db: Engine = Depends(get_au_uts_engine),
    mis_db: Engine = Depends(get_mis_db_engine),
    uk_db: Engine = Depends(get_uk_uts_engine),
    at_db: Engine = Depends(get_at_uts_engine),
    de_db: Engine = Depends(get_de_uts_engine),
    start_date: date = Query(default=date.today() - timedelta(days=365)),
    end_date: date = Query(default=date.today())
):
    logger.info('Sales etl starts')
    table_name = "Sales"

    regions = [
        {"country_code": "NZ", "country_name": "New Zealand", "engine": nz_db, "query":AU_NZ_SALES_Query},
        {"country_code": "AU", "country_name": "Australia", "engine": au_db, "query":AU_NZ_SALES_Query},
        {"country_code": "UK", "country_name": "United Kingdom", "engine": uk_db, "query":UK_DE_AT_SALES_Query},
        {"country_code": "DE", "country_name": "Germany", "engine": de_db, "query":UK_DE_AT_SALES_Query},
        {"country_code": "AT", "country_name": "Austria", "engine": at_db, "query":UK_DE_AT_SALES_Query},
    ]

    iso_start_date = start_date.isoformat()
    iso_end_date = end_date.isoformat()

    async def extract_and_transform(region):
        extracted_data = await ETL.extraction(
            engine=region["engine"],
            start_date=iso_start_date,
            end_date=iso_end_date,
            country_code=region["country_code"],
            country_name=region["country_name"],
            query=region["query"],
            extraction_type = "sales"
        )
        return await ETL.transform(
            extracted_data,
            ['CreatedDate', 'ActualStartDate', 'ETLDateUploaded']
        )

    tasks = [extract_and_transform(region) for region in regions]
    all_transformed_data = await asyncio.gather(*tasks)

    combined_data = pd.concat(all_transformed_data, ignore_index=True)

    logger.info(
        f"Combined {len(combined_data)} rows from {', '.join([r['country_code'] for r in regions])}"  # noqa
    )

    load_msg = ETL.load(combined_data, table_name, mis_db, start_date=iso_start_date,
            end_date=iso_end_date,)

    return {
        # "message": "ETL process completed successfully.",
        "rows_loaded": len(combined_data),
        "load_status": load_msg
    }


@router.get("/etl_free_policies")
async def etl_free_policies(
    nz_db: Engine = Depends(get_nz_uts_engine),
    au_db: Engine = Depends(get_au_uts_engine),
    mis_db: Engine = Depends(get_mis_db_engine),
    uk_db: Engine = Depends(get_uk_uts_engine),
    at_db: Engine = Depends(get_at_uts_engine),
    de_db: Engine = Depends(get_de_uts_engine),
    start_date: date = Query(default=date.today() - timedelta(days=365)),
    end_date: date = Query(default=date.today())
):
    logger.info('FreePolicy etl starts')
    table_name = "FreePolicySales"

    regions = [
        {"country_code": "NZ", "country_name": "New Zealand", "engine": nz_db, "query":AU_NZ_FREE_POLICY_Query},
        {"country_code": "AU", "country_name": "Australia", "engine": au_db, "query":AU_NZ_FREE_POLICY_Query},
        {"country_code": "UK", "country_name": "United Kingdom", "engine": uk_db, "query":UK_DE_AT_FREE_POLICY_Query},
        {"country_code": "DE", "country_name": "Germany", "engine": de_db, "query":UK_DE_AT_FREE_POLICY_Query},
        {"country_code": "AT", "country_name": "Austria", "engine": at_db, "query":UK_DE_AT_FREE_POLICY_Query},
    ]

    iso_start_date = start_date.isoformat()
    iso_end_date = end_date.isoformat()

    async def extract_and_transform(region):
        extracted_data = await ETL.extraction(
            engine=region["engine"],
            start_date=iso_start_date,
            end_date=iso_end_date,
            country_code=region["country_code"],
            country_name=region["country_name"],
            query=region["query"],
            extraction_type = "sales"
        )
        return await ETL.transform(
            extracted_data,
            ['CreatedDate', 'ETLDateUploaded']
        )

    tasks = [extract_and_transform(region) for region in regions]
    all_transformed_data = await asyncio.gather(*tasks)

    combined_data = pd.concat(all_transformed_data, ignore_index=True)

    logger.info(
        f"Combined {len(combined_data)} rows from {', '.join([r['country_code'] for r in regions])}"  # noqa
    )

    load_msg = ETL.load(combined_data, table_name, mis_db, start_date=iso_start_date,
            end_date=iso_end_date,)

    return {
        # "message": "ETL process completed successfully.",
        "rows_loaded": len(combined_data),
        "load_status": load_msg
    }
