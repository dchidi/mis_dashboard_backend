import io
import csv
from fastapi.responses import StreamingResponse
from typing import Union, List, Optional
from app.utils.report_helpers import (
    normalize_input, parse_dates, normalize_regions, WhereBuilder,
    format_filename, whereFilters
)
from datetime import datetime, date
import calendar

class QuoteStream:
    @staticmethod
    def _generate_csv_stream(engine, sql: str, params: tuple, filename: str) -> StreamingResponse:
        def generate():
            with engine.connect() as conn:
                result = conn.exec_driver_sql(sql, params, execution_options={"stream_results": True})
                cols = list(result.keys())
                buf = io.StringIO()
                writer = csv.writer(buf, lineterminator="\n")

                writer.writerow(cols)
                yield buf.getvalue(); buf.seek(0); buf.truncate(0)

                while True:
                    rows = result.fetchmany(5000)
                    if not rows:
                        break
                    for r in rows:
                        writer.writerow(list(r))
                    yield buf.getvalue(); buf.seek(0); buf.truncate(0)

        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(generate(), media_type="text/csv", headers=headers)

    @staticmethod
    def _conversion_base_sql(where_sql: str) -> str:
        return f"""
            SELECT
                CountryName, CountryCode, Brand,
                    QuoteNumber, 
                    CASE 
                        WHEN COALESCE(QuoteExpiryDate, QuoteStartDate) IS NULL THEN NULL
                        WHEN CAST(GETDATE() AS DATE) > QuoteExpiryDate THEN 'Lapsed'
                        ELSE 'Live'
                    END AS QuoteStatus,
                    CASE
                        WHEN PolicyNumber IS NULL THEN 'No'
                        WHEN PolicyNumber LIKE '%none%' THEN 'No'
                        ELSE 'Yes'
                    END AS ConvertedQuote,
                    CreatedDate AS QuoteCreatedDate, QuoteStartDate, QuoteExpiryDate, QuoteReceivedMethod,
                    FullName, Email, ContactNo,
                    PetName, PetType, BreedName, PetBirthDate,
                    PolicyNumber, PolicyStartDate, PolicyEndDate
            FROM Quote
            WHERE {where_sql}
            ORDER BY CreatedDate DESC, QuoteNumber
        """

    @staticmethod
    def _quote_base_sql(where_sql: str) -> str:
        return f"""
            SELECT                
                CountryName, CountryCode, Brand,
                    QuoteNumber, 
                    CreatedDate,
                    PolicyNumber,
                    PolicyCreatedDate,
                    CASE 
                        WHEN COALESCE(QuoteExpiryDate, QuoteStartDate) IS NULL THEN NULL
                        WHEN CAST(GETDATE() AS DATE) > QuoteExpiryDate THEN 'Lapsed'
                        ELSE 'Live'
                    END AS QuoteStatus,
                    CASE
                        WHEN PolicyNumber IS NULL THEN 'No'
                        WHEN PolicyNumber LIKE '%none%' THEN 'No'
                        ELSE 'Yes'
                    END AS ConvertedQuote,
                    CASE 
                        WHEN ( NULLIF(LTRIM(RTRIM(FullName)),  '') IS NULL
                            OR NULLIF(LTRIM(RTRIM(Email)),     '') IS NULL
                            OR NULLIF(LTRIM(RTRIM(Address)),   '') IS NULL
                            OR NULLIF(LTRIM(RTRIM(PostCode)),  '') IS NULL
                            OR NULLIF(LTRIM(RTRIM(ContactNo)), '') IS NULL
                            OR NULLIF(LTRIM(RTRIM(PetType)),   '') IS NULL
                            OR NULLIF(LTRIM(RTRIM(PetName)),   '') IS NULL )
                        THEN 'No' 
                        ELSE 'Yes' 
                    END AS QuoteDetailsCompleted,
                    QuoteStartDate, QuoteExpiryDate, QuoteReceivedMethod,
                    FullName, Email, ContactNo,
                    PetName, PetType, BreedName, PetBirthDate    
            FROM Quote
            WHERE {where_sql}
            ORDER BY CreatedDate DESC, QuoteNumber
        """

    @staticmethod
    def quote_by_received_method(where_sql: str) -> str:
        return f"""
            WITH Base AS (
                SELECT
                    CountryName, CountryCode, Brand,
                    QuoteNumber, 
                    CASE 
                        WHEN COALESCE(QuoteExpiryDate, QuoteStartDate) IS NULL THEN NULL
                        WHEN CAST(GETDATE() AS DATE) > QuoteExpiryDate THEN 'Lapsed'
                        ELSE 'Live'
                    END AS QuoteStatus,
                    CASE
                        WHEN PolicyNumber IS NULL THEN 'No'
                        WHEN PolicyNumber LIKE '%none%' THEN 'No'
                        ELSE 'Yes'
                    END AS ConvertedQuote,
                    CreatedDate, QuoteStartDate, QuoteExpiryDate, QuoteReceivedMethod,
                    FullName, Email, ContactNo,
                    PetName, PetType, BreedName, PetBirthDate,
                    PolicyNumber, PolicyStartDate, PolicyEndDate
                FROM Quote
                WHERE {where_sql}
                AND DAY(CreatedDate) >=
                    CASE
                        WHEN ? > DAY(EOMONTH(CreatedDate))
                        THEN DAY(EOMONTH(CreatedDate))
                        ELSE ?
                    END
                AND DAY(CreatedDate) <=
                    CASE
                        WHEN ? > DAY(EOMONTH(CreatedDate))
                        THEN DAY(EOMONTH(CreatedDate))
                        ELSE ?
                    END
            )
            SELECT
                b.*
            FROM Base b
            ORDER BY b.CreatedDate DESC, b.QuoteNumber;
    """

    @staticmethod
    async def stream_quote_conversion_csv(
        engine,
        start_date: str,
        end_date: str,
        country_codes: Union[str, List[str], None] = "all",
        filename: str = "quote_conversion.csv",        
        quoteStatus: str = 'all',
        brands:str = "all",
        pet_types:str = "all"
    ) -> StreamingResponse:
        start_str, end_plus_1, end_str = parse_dates(start_date, end_date)
        country_code_list = normalize_regions(country_codes)

        brand_list = normalize_input(brands)
        pet_list = normalize_input(pet_types)

        wb = (
            WhereBuilder()
            .add("CreatedDate >= ?", start_str)
            .add("CreatedDate < ?", end_plus_1)
        )

        wb = whereFilters(wb=wb,country_codes=country_code_list,brands=brand_list, pets=pet_list)


        sql = QuoteStream._conversion_base_sql(wb.sql())
        params = wb.parameters()

        full_filename = format_filename(filename, start_str, end_str)

        return QuoteStream._generate_csv_stream(engine, sql, params, full_filename)
        # return QuoteStream._generate_csv_stream(engine, sql, params, filename)

    @staticmethod
    async def stream_quote_by_pet_type_csv(
        engine,
        start_date: str,
        end_date: str,
        country_codes: Union[str, List[str], None] = "all",
        filename: str = "quote.csv",
        quoteStatus: str = 'all',
        brands:str = "all",
        pet_types:str = "all"
    ) -> StreamingResponse:
        start_str, end_plus_1, end_str = parse_dates(start_date, end_date)
        country_code_list = normalize_regions(country_codes)

        brand_list = normalize_input(brands)
        pet_list = normalize_input(pet_types)

        wb = (
            WhereBuilder()
            .add("CreatedDate >= ?", start_str)
            .add("CreatedDate < ?", end_plus_1)
        )
        wb = whereFilters(wb=wb,country_codes=country_code_list,brands=brand_list, pets=pet_list)

        sql = QuoteStream._quote_base_sql(wb.sql())
        params = wb.parameters()

        # return QuoteStream._generate_csv_stream(engine, sql, params, filename)
        full_filename = format_filename(filename, start_str, end_str)

        return QuoteStream._generate_csv_stream(engine, sql, params, full_filename)

    
    @staticmethod
    async def stream_quote_csv(
        engine,
        start_date: str,
        end_date: str,
        country_codes: Union[str, List[str], None] = "all",
        filename: str = "quote.csv",
        brands:str = "all",
        pet_types:str = "all"
    ) -> StreamingResponse:
        start_str, end_plus_1, end_str = parse_dates(start_date, end_date)
        country_code_list = normalize_regions(country_codes)

        brand_list = normalize_input(brands)
        pet_list = normalize_input(pet_types)

        wb = (
            WhereBuilder()
            .add("CreatedDate >= ?", start_str)
            .add("CreatedDate < ?", end_plus_1)
        )
        wb = whereFilters(wb=wb,country_codes=country_code_list,brands=brand_list, pets=pet_list)

        sql = QuoteStream._quote_base_sql(wb.sql())
        params = wb.parameters()

        # return QuoteStream._generate_csv_stream(engine, sql, params, filename)
        full_filename = format_filename(filename, start_str, end_str)

        return QuoteStream._generate_csv_stream(engine, sql, params, full_filename)
    

    @staticmethod
    async def stream_quote_receive_method_csv(
        engine,
        start_date: str,
        end_date: str,
        country_codes: Union[str, List[str], None] = "all",
        filename: str = "quote_receive_method.csv",
        months: Optional[int] = None,  
        
        quoteStatus: str = 'all',
        brands:str = "all",
        pet_types:str = "all"
    ) -> StreamingResponse:
        # Derive fixed day window from inputs
        start_dt = datetime.fromisoformat(start_date).date()
        end_dt   = datetime.fromisoformat(end_date).date()
        start_day = start_dt.day
        end_day   = end_dt.day

        brand_list = normalize_input(brands)
        pet_list = normalize_input(pet_types)

        # If months provided, back-compute the effective overall start date
        if months and months > 0:
            total = end_dt.year * 12 + (end_dt.month - 1) - (months - 1)
            y = total // 12
            m = total % 12 + 1
            first_month_last_day = calendar.monthrange(y, m)[1]
            computed_start = date(y, m, min(start_day, first_month_last_day)).isoformat()
            start_str, end_plus_1, end_str = parse_dates(computed_start, end_date)
        else:
            start_str, end_plus_1, end_str = parse_dates(start_date, end_date)

        country_code_list = normalize_regions(country_codes)

        wb = (
            WhereBuilder()
            .add("CreatedDate >= ?", start_str)
            .add("CreatedDate < ?", end_plus_1) 
        )

        
        wb = whereFilters(wb=wb,country_codes=country_code_list,brands=brand_list, pets=pet_list)
        sql = QuoteStream.quote_by_received_method(wb.sql())
        # Params: 4 day-window values to match the two CASE expressions (lower & upper)
        params = (*wb.parameters(), start_day, start_day, end_day, end_day)

        full_filename = format_filename(filename, start_str, end_str)
        return QuoteStream._generate_csv_stream(engine, sql, params, full_filename)
