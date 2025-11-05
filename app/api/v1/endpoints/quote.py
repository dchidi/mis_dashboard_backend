from fastapi import APIRouter, Depends, Query
from datetime import date
from sqlalchemy.engine import Engine
from app.db.sqlserver import get_mis_db_engine
from app.services.quote import Quote
from app.services.quote_stream import QuoteStream
from dateutil.relativedelta import relativedelta

from app.core.enums import ReportTypeEnum, QuoteStatusEnum

router = APIRouter()




@router.get("/quote_summary")
async def quoteSummary(
    mis_db: Engine = Depends(get_mis_db_engine),
    start_date: date = Query(default_factory=lambda: date.today().replace(day=1)),
    end_date: date = Query(default_factory=date.today),
    country_codes: str = Query(default="all"),
    brands: str = Query(default="all"),
    pet_types: str = Query(default="all")
):
    result = await Quote.QuoteSummary(
        engine=mis_db,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        country_codes=country_codes,
        brands=brands,
        pet_types=pet_types
    )
    return result


@router.get("/quote_data")
async def quoteData(
    mis_db: Engine = Depends(get_mis_db_engine),
    start_date: date = Query(default_factory=lambda: date.today().replace(day=1)),
    end_date: date = Query(default_factory=date.today),
    country_codes: str = Query(default="all"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=10_000),
    download: bool = Query(False),
    filename: str = Query("quote.csv"),    
    brands: str = Query(default="all"),
    pet_types: str = Query(default="all"),
    reportType: ReportTypeEnum = Query(default=ReportTypeEnum.TOTAL_QUOTES)
):
    if download:
        return await QuoteStream.stream_quote_csv(
            engine=mis_db,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            country_codes=country_codes,
            filename=filename,
            brands=brands,
            pet_types=pet_types
        )

    # normal paginated JSON
    result = await Quote.QuoteData(
        engine=mis_db,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        country_codes=country_codes, 
        skip=skip,
        limit=limit,
        brands=brands,
        pet_types=pet_types,
        report_type=reportType        
    )
    return result


@router.get("/quote_summary_by_pet_type")
async def quoteSummaryByPetType(
    mis_db: Engine = Depends(get_mis_db_engine),
    start_date: date = Query(default_factory=lambda: date.today().replace(day=1)),
    end_date: date = Query(default_factory=date.today),
    country_codes: str = Query(default="all"),
    brands: str = Query(default="all"),
    pet_types: str = Query(default="all")
):
    result = await Quote.QuoteSummaryByPetType(
        engine=mis_db,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        country_codes=country_codes,
        brands=brands,
        pet_types=pet_types
    )
    return result


@router.get("/quote_data_by_pet_type")
async def quoteDataByPetType(
    mis_db: Engine = Depends(get_mis_db_engine),
    start_date: date = Query(default_factory=lambda: date.today().replace(day=1)),
    end_date: date = Query(default_factory=date.today),
    country_codes: str = Query(default="all"),
    # country_codes: Optional[List[str]] = Query(None),   # call as ?country_codes=AT&country_codes=DE
    quoteStatus: QuoteStatusEnum = Query(default=QuoteStatusEnum.ALL),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=10_000),
    download: bool = Query(False),
    filename: str = Query("quote_by_pet_type.csv"),    
    brands: str = Query(default="all"),
    pet_types: str = Query(default="all")
):
    if download:
        return await QuoteStream.stream_quote_by_pet_type_csv(
            engine=mis_db,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            country_codes=country_codes,
            filename=filename,
            quoteStatus=quoteStatus.value,
            brands=brands,
            pet_types=pet_types
        )

    # normal paginated JSON
    result = await Quote.QuoteDataByPetType(
        engine=mis_db,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        country_codes=country_codes,                 # None or List[str] — your service handles both
        quoteStatus=quoteStatus.value,   # 'Live' | 'Lapsed' | 'All'
        skip=skip,
        limit=limit,
        brands=brands,
        pet_types=pet_types
    )
    return result


@router.get("/quote_conversion_summary")
async def quoteConversionSummary(
    mis_db: Engine = Depends(get_mis_db_engine),
    start_date: date = Query(default_factory=lambda: date.today().replace(day=1)),
    end_date: date = Query(default_factory=date.today),
    country_codes: str = Query(default="all"),
    brands: str = Query(default="all"),
    pet_types: str = Query(default="all")
):
    result = await Quote.QuoteConversionSummary(
        engine=mis_db,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        country_codes=country_codes,
        brands=brands,
        pet_types=pet_types
    )
    return result


@router.get("/quote_conversion_data")
async def quoteConversionData(
    mis_db: Engine = Depends(get_mis_db_engine),
    start_date: date = Query(default_factory=lambda: date.today().replace(day=1)),
    end_date: date = Query(default_factory=date.today),
    country_codes: str = Query(default="all"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=10_000),
    download: bool = Query(False),
    filename: str = Query("quote_conversion.csv"),    
    brands: str = Query(default="all"),
    pet_types: str = Query(default="all")
):
    if download:
        return await QuoteStream.stream_quote_conversion_csv(
            engine=mis_db,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            country_codes=country_codes,
            filename=filename,            
            brands=brands,
            pet_types=pet_types
        )

    # normal paginated JSON
    result = await Quote.QuoteConversionReport(
        engine=mis_db,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        country_codes=country_codes,
        skip=skip,
        limit=limit,        
        brands=brands,
        pet_types=pet_types
    )
    return result


@router.get("/quote_rmth_same_period_summary")
async def quoteReceiveMethodSamePeriod(
    mis_db: Engine = Depends(get_mis_db_engine),
    start_date: date = Query(default_factory=lambda: date.today().replace(day=1) - relativedelta(months=6)),
    end_date: date = Query(default_factory=date.today),
    country_codes: str = Query(default="all"),
    historical_months: int = 7,
    
    brands: str = Query(default="all"),
    pet_types: str = Query(default="all")
):
    result = await Quote.QuoteReceiveMethodSamePeriod(
        engine=mis_db,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        country_codes=country_codes,
        months=historical_months,
        brands=brands,
        pet_types=pet_types
    )
    return result


@router.get("/quote_rmth_same_period_report")
async def quoteReceiveMethodSamePeriodData(
    mis_db: Engine = Depends(get_mis_db_engine),
    start_date: date = Query(default_factory=lambda: date.today().replace(day=1) - relativedelta(months=6)),
    end_date: date = Query(default_factory=date.today),
    country_codes: str = Query(default="all"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=10_000),
    download: bool = Query(False),
    filename: str = Query("quote_receive_method.csv"),
    historical_months: int = 7,
    brands: str = Query(default="all"),
    pet_types: str = Query(default="all")
):
    if download:
        return await QuoteStream.stream_quote_receive_method_csv(
            engine=mis_db,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            country_codes=country_codes,
            filename=filename,
            months=historical_months,
            brands=brands,
            pet_types=pet_types
        )

    # normal paginated JSON
    return await Quote.QuoteReceiveMethodSamePeriodReport(
        engine=mis_db,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        country_codes=country_codes,
        skip=skip,
        limit=limit,
        months=historical_months,
        brands=brands,
        pet_types=pet_types
    )
