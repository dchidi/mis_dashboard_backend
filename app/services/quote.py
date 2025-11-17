from fastapi import HTTPException
from typing import List, Union, Dict, Any,Optional
from app.utils.report_helpers import (
    normalize_input, parse_dates, normalize_regions, WhereBuilder, read_df, first_cell_int, whereFilters
)
from datetime import datetime, timezone,date, timedelta  
import pandas as pd
import logging
from calendar import monthrange

from app.utils.date_utils import today
from app.core.enums import ReportTypeEnum

logger = logging.getLogger(__name__)

def add_months(dt: date, months: int) -> date:
    """Add months to a date, clamping the day to the month's last valid day."""
    y = dt.year + (dt.month - 1 + months) // 12
    m = (dt.month - 1 + months) % 12 + 1
    d = min(dt.day, monthrange(y, m)[1])
    return date(y, m, d)



class Quote:
    @staticmethod
    async def QuoteSummary(
        engine, start_date: str, end_date: str,
        country_codes: Union[str, List[str], None] = 'all',
        brands: str = "all",
        pet_types: str = "all",
    ) -> Dict[str, Any]:
        try:
            # --- Parse incoming dates to date objects ---
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date() if isinstance(start_date, str) else start_date
            end_dt   = datetime.strptime(end_date,   "%Y-%m-%d").date() if isinstance(end_date,   str) else end_date
            if end_dt < start_dt:
                raise ValueError("end_date before start_date")

            # Previous period (same month-shift, safe across month lengths)
            prev_start_dt = add_months(start_dt, -1)
            prev_end_dt   = add_months(end_dt,   -1)

            # Strings for current (half-open) using your helper
            start_str, end_plus_1, end_str = parse_dates(start_dt, end_dt)

            # Strings for previous period (half-open)
            prev_start_str = prev_start_dt.strftime("%Y-%m-%d")
            prev_end_plus_1 = (prev_end_dt + timedelta(days=1)).strftime("%Y-%m-%d")

            # Filters
            country_code_list = normalize_regions(country_codes)
            brand_list = normalize_input(brands)
            pet_list = normalize_input(pet_types)

            current_date_str = today()  # if this returns "YYYY-MM-DD" string, it's fine for binding

            wb = WhereBuilder()
            wb = whereFilters(wb=wb, country_codes=country_code_list, brands=brand_list, pets=pet_list)

            # --- Main summary SQL (unchanged semantics, fully parameterized) ---
            sql = f"""
                SELECT 
                    -- current period
                    SUM(CASE WHEN CreatedDate >= ? AND CreatedDate <  ? THEN 1 ELSE 0 END) AS currentPeriodTotalQuotes,

                    -- previous period
                    SUM(CASE WHEN CreatedDate >= ? AND CreatedDate <  ? THEN 1 ELSE 0 END) AS lastPeriodTotalQuotes,

                    -- live/lapsed within current period using app-supplied current_date
                    SUM(CASE WHEN CreatedDate >= ? AND CreatedDate <  ? AND ? <= QuoteExpiryDate 
                        AND (PolicyNumber IS NULL OR PolicyNumber LIKE '%NONE%') THEN 1 ELSE 0 END) AS liveQuotes,
                    SUM(CASE WHEN CreatedDate >= ? AND CreatedDate <  ? AND ?  > QuoteExpiryDate 
                        AND (PolicyNumber IS NULL OR PolicyNumber LIKE '%NONE%') THEN 1 ELSE 0 END) AS lapsedQuotes,

                    -- incomplete details within current period
                    SUM(CASE WHEN CreatedDate >= ? AND CreatedDate <  ? AND
                        ( NULLIF(LTRIM(RTRIM(FullName)),  '') IS NULL
                    OR NULLIF(LTRIM(RTRIM(Email)),     '') IS NULL
                    OR NULLIF(LTRIM(RTRIM(Address)),   '') IS NULL
                    OR NULLIF(LTRIM(RTRIM(PostCode)),  '') IS NULL
                    OR NULLIF(LTRIM(RTRIM(ContactNo)), '') IS NULL
                    OR NULLIF(LTRIM(RTRIM(PetType)),   '') IS NULL
                    OR NULLIF(LTRIM(RTRIM(PetName)),   '') IS NULL )
                    THEN 1 ELSE 0 END) AS incompleteQuoteDetails
                FROM Quote
                WHERE {wb.sql()}
            """

            all_params = [
                # current
                start_str, end_plus_1,
                # previous
                prev_start_str, prev_end_plus_1,
                # live
                start_str, end_plus_1, current_date_str,
                # lapsed
                start_str, end_plus_1, current_date_str,
                # incomplete
                start_str, end_plus_1,
                # filters from wb.sql()
                *wb.parameters(),
            ]

            df: pd.DataFrame = await read_df(engine, sql, all_params)

            # --- Summary shaping ---
            if not df.empty:
                summary_dict = df.iloc[0].to_dict()
                for k, v in summary_dict.items():
                    if hasattr(v, "item"):
                        summary_dict[k] = v.item()
            else:
                summary_dict = {
                    "currentPeriodTotalQuotes": 0,
                    "lastPeriodTotalQuotes": 0,
                    "liveQuotes": 0,
                    "lapsedQuotes": 0,
                    "incompleteQuoteDetails": 0,
                }

            current_period_total = summary_dict["currentPeriodTotalQuotes"]
            incomplete_quotes = summary_dict["incompleteQuoteDetails"]
            # quotes_completeness_percent = round(((current_period_total - incomplete_quotes) / current_period_total) * 100, 2) if current_period_total else 0.0
            quotes_completeness_percent = round(((current_period_total - incomplete_quotes) / current_period_total) * 100, 1) if current_period_total else 0.0

            # Format to remove .0 when it's a whole number
            quotes_completeness_percent = int(quotes_completeness_percent) if quotes_completeness_percent.is_integer() else quotes_completeness_percent

            # --- LTM graph (correct logic) ---
            # Requirement: 13 months ending at end_date's month; for each month, count only days
            # between start_dt.day and end_dt.day (inclusive). Example: 5..10 each month.
            start_day = start_dt.day
            end_day   = end_dt.day

            # Month anchors (1st of month)
            end_month_anchor = date(end_dt.year, end_dt.month, 1)           # current anchor month
            oldest_month_anchor = add_months(end_month_anchor, -12)         # 12 months before = 13 total
            upper_bound_exclusive = add_months(end_month_anchor, 1)         # first day after anchor month

            # If the selected window is within a single month (e.g., MTD),
            # align prior months to the same day-of-month window. Otherwise (e.g., YTD),
            # use full-month aggregation to avoid undercounting earlier months.
            same_calendar_month = (start_dt.year == end_dt.year and start_dt.month == end_dt.month)

            if same_calendar_month:
                ltm_sql = f"""
                    SELECT 
                        YEAR(CreatedDate) AS [year],
                        MONTH(CreatedDate) AS [month],
                        COUNT(QuoteNumber) AS [value]
                    FROM Quote
                    WHERE {wb.sql()}
                    AND CreatedDate >= ? 
                    AND CreatedDate <  ?
                    AND (
                            CASE WHEN ? <= ?
                                THEN CASE WHEN DAY(CreatedDate) BETWEEN ? AND ? THEN 1 ELSE 0 END
                                ELSE CASE WHEN DAY(CreatedDate) >= ? OR DAY(CreatedDate) <= ? THEN 1 ELSE 0 END
                            END
                        ) = 1
                    GROUP BY YEAR(CreatedDate), MONTH(CreatedDate)
                """
                ltm_params = [
                    *wb.parameters(),
                    oldest_month_anchor, upper_bound_exclusive,
                    start_day, end_day,  # for ? <= ?
                    start_day, end_day,  # BETWEEN start..end
                    start_day, end_day   # wrap-around >= start OR <= end
                ]
            else:
                ltm_sql = f"""
                    SELECT 
                        YEAR(CreatedDate) AS [year],
                        MONTH(CreatedDate) AS [month],
                        COUNT(QuoteNumber) AS [value]
                    FROM Quote
                    WHERE {wb.sql()}
                    AND CreatedDate >= ? 
                    AND CreatedDate <  ?
                    GROUP BY YEAR(CreatedDate), MONTH(CreatedDate)
                """
                ltm_params = [
                    *wb.parameters(),
                    oldest_month_anchor, upper_bound_exclusive,
                ]

            ltm_df: pd.DataFrame = await read_df(engine, ltm_sql, ltm_params)

            # Map results to {YYYY-MM: count}
            counts_by_yyyymm: Dict[str, int] = {}
            if not ltm_df.empty:
                for _, row in ltm_df.iterrows():
                    y = int(row["year"]); m = int(row["month"])
                    counts_by_yyyymm[f"{y:04d}-{m:02d}"] = int(row["value"])

            # Emit exactly 13 months from oldest -> newest (anchored to end_date's month)
            month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
            graph_data = []
            for i in range(12, -1, -1):  # 12 months ago ... current month
                anchor = add_months(end_month_anchor, -i)
                key = f"{anchor.year:04d}-{anchor.month:02d}"
                label = f"{month_names[anchor.month-1]} {str(anchor.year)[-2:]}"
                graph_data.append({"month": label, "value": counts_by_yyyymm.get(key, 0)})

            # Meta for the LTM window (optional)
            ltm_start_date = oldest_month_anchor
            ltm_end_date   = add_months(end_month_anchor, 1) - timedelta(days=1)

            return {
                "meta": {
                    "start_date": start_str,
                    "end_date": end_str,
                    "prev_start_date": prev_start_str,
                    "prev_end_date": prev_end_dt.strftime("%Y-%m-%d"),
                    "country_codes": country_code_list or "ALL",
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "ltm_start_date": ltm_start_date.strftime("%Y-%m-%d"),
                    "ltm_end_date": ltm_end_date.strftime("%Y-%m-%d"),
                    "ltm_period_months": 13,
                    "day_window": {"start_day": start_day, "end_day": end_day},
                },
                "currentPeriodTotalQuotes": summary_dict["currentPeriodTotalQuotes"],
                "lastPeriodTotalQuotes": summary_dict["lastPeriodTotalQuotes"],
                "liveQuotes": summary_dict["liveQuotes"],
                "lapsedQuotes": summary_dict["lapsedQuotes"],
                "incompleteQuoteDetails": summary_dict["incompleteQuoteDetails"],
                "quotesCompleteness": f"{quotes_completeness_percent}",
                "graphData": graph_data,
            }

        except ValueError as ve:
            raise HTTPException(status_code=400, detail=f"Invalid dates: {ve}")
        except Exception as e:
            logger.exception("Summary failed: %s", e)
            raise HTTPException(status_code=500, detail="Failed to fetch quote summary")


    @staticmethod
    async def QuoteData(
        engine, start_date: str, end_date: str,
        country_codes: Union[str, List[str], None] = 'all',
        skip: int = 0, limit: int = 100,
        brands:str = "all",
        pet_types:str = "all",
        report_type: ReportTypeEnum = ReportTypeEnum.TOTAL_QUOTES,         
    ) -> Dict[str, Any]:
        try:
            start_str, end_plus_1, _ = parse_dates(start_date, end_date)
            skip = max(0, int(skip))
            limit = min(max(1, int(limit)), 10_000)
            
            country_code_list = normalize_regions(country_codes)
            brand_list = normalize_input(brands)
            pet_list = normalize_input(pet_types)

            wb = (WhereBuilder()
                  .add("CreatedDate >= ?", start_str)
                  .add("CreatedDate < ?", end_plus_1))
            
            
            wb = whereFilters(wb=wb,country_codes=country_code_list,brands=brand_list, pets=pet_list)

            # Define report handlers
            # report_handlers = {
            #     # ReportTypeEnum.TOTAL_QUOTES: Quote._handle_total_quotes,
            #     # ReportTypeEnum.LIVE_QUOTES: Quote._handle_live_quotes,
            #     # ReportTypeEnum.LAPSED_QUOTES: Quote._handle_lapsed_quotes,
            #     # ReportTypeEnum.QUOTE_COMPLETENESS: Quote._handle_quote_completeness,
            #     # ReportTypeEnum.QUOTES_CONVERTED: Quote._handle_quotes_converted,
            #     # ReportTypeEnum.QUOTE_RECEIVED_METHOD: Quote._handle_received_method,
            # }
            
            # # Get the handler and execute (O(1) lookup)
            # handler = report_handlers[report_type]
            # return await handler(engine, start_date, end_date, country_codes, brands, pet_types)

            count_sql = f"SELECT COUNT(QuoteNumber) AS TotalRecords FROM Quote WHERE {wb.sql()}"            
            data_sql = f"""
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
                    CreatedDate, QuoteStartDate, QuoteExpiryDate, QuoteReceivedMethod,
                    FullName, Email, ContactNo,
                    PetName, PetType, BreedName, PetBirthDate                      
                FROM Quote
                WHERE {wb.sql()}
                ORDER BY CreatedDate DESC, QuoteNumber
                OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """

            total_df = await read_df(engine, count_sql, wb.parameters())
            total = first_cell_int(total_df, default=0)

            data_params = (*wb.parameters(), int(skip), int(limit))
            # wb.parameters() + (int(skip), int(limit))
            data_df = await read_df(engine, data_sql, data_params)
            # print(data_sql, data_params)

            return {
                "total": total,
                "skip": skip,
                "limit": limit,
                "data": data_df.to_dict(orient="records") if not data_df.empty else []
            }
        except ValueError as ve:
            raise HTTPException(status_code=400, detail=f"Invalid dates: {ve}")
        except Exception as e:
            logger.exception("QuoteReport failed: %s", e)
            raise HTTPException(status_code=500, detail="Failed to fetch quote report")

    @staticmethod
    async def QuoteSummaryByPetType(
            engine, start_date: str, end_date: str,
            country_codes: Union[str, List[str], None] = 'all',
            brands:str = "all",
            pet_types:str = "all",
            # quoteStatus: str = 'All',        
        ) -> Dict[str, Any]:
            try:
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

                # TODO:: Quote status (Lapsed/Live) will be calculated from QuoteStartDate and QuoteExpiryDate
                # if isinstance(quoteStatus, str) and quoteStatus.strip().lower() != "all":
                #     wb.add("QuoteStatus = ?", quoteStatus.strip())

                sql = f"""
                    SELECT 
                        COUNT(QuoteNumber) AS value,
                        CASE 
                            WHEN LOWER(COALESCE(PetType, '')) LIKE '%cat%'    THEN 'Cat'
                            WHEN LOWER(COALESCE(PetType, '')) LIKE '%dog%'    THEN 'Dog'
                            WHEN LOWER(COALESCE(PetType, '')) LIKE '%horse%'  THEN 'Horse'
                            WHEN LOWER(COALESCE(PetType, '')) LIKE '%exotic%' THEN 'Exotic'
                            WHEN LOWER(COALESCE(PetType, '')) LIKE '%bb_com%'  THEN 'BB'
                            ELSE 'Others'
                        END AS name
                    FROM Quote
                    WHERE {wb.sql()}
                    GROUP BY 
                        CASE 
                            WHEN LOWER(COALESCE(PetType, '')) LIKE '%cat%'    THEN 'Cat'
                            WHEN LOWER(COALESCE(PetType, '')) LIKE '%dog%'    THEN 'Dog'
                            WHEN LOWER(COALESCE(PetType, '')) LIKE '%horse%'  THEN 'Horse'
                            WHEN LOWER(COALESCE(PetType, '')) LIKE '%exotic%' THEN 'Exotic'
                            WHEN LOWER(COALESCE(PetType, '')) LIKE '%bb_com%'  THEN 'BB'
                            ELSE 'Others'
                        END
                    ORDER BY name DESC
                """

                df: pd.DataFrame = await read_df(engine, sql, wb.parameters())
                rows = df.to_dict(orient="records") if not df.empty else []
                totals = {r["name"]: int(r["value"]) for r in rows}

                return {
                    "meta": {
                        "start_date": start_str,
                        "end_date": end_str,
                        "country_codes": country_code_list or "ALL",
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                    },
                    "summary": rows,
                    "totals_by_pet": totals,
                    "total": sum(totals.values()),
                }
            except ValueError as ve:
                raise HTTPException(status_code=400, detail=f"Invalid dates: {ve}")
            except Exception as e:
                logger.exception("Summary failed: %s", e)
                raise HTTPException(status_code=500, detail="Failed to fetch quote summary")

    @staticmethod
    async def QuoteDataByPetType(
        engine, start_date: str, end_date: str,
        country_codes: Union[str, List[str], None] = 'all',
        quoteStatus: str = 'All', 
        skip: int = 0, limit: int = 100,
        brands:str = "all",
        pet_types:str = "all"
    ) -> Dict[str, Any]:
        try:
            start_str, end_plus_1, _ = parse_dates(start_date, end_date)
            skip = max(0, int(skip))
            limit = min(max(1, int(limit)), 10_000)
            
            country_code_list = normalize_regions(country_codes)
            brand_list = normalize_input(brands)
            pet_list = normalize_input(pet_types)

            wb = (WhereBuilder()
                  .add("CreatedDate >= ?", start_str)
                  .add("CreatedDate < ?", end_plus_1))
            
            
            wb = whereFilters(wb=wb,country_codes=country_code_list,brands=brand_list, pets=pet_list)

            
            count_sql = f"SELECT COUNT(QuoteNumber) AS TotalRecords FROM Quote WHERE {wb.sql()}"
            data_sql = f"""
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
                    QuoteStartDate, QuoteExpiryDate, QuoteReceivedMethod,
                    FullName, Email, ContactNo,
                    PetName, PetType, BreedName, PetBirthDate                      
                FROM Quote
                WHERE {wb.sql()}
                ORDER BY CreatedDate DESC, QuoteNumber
                OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """

            total_df = await read_df(engine, count_sql, wb.parameters())
            total = first_cell_int(total_df, default=0)

            data_params = (*wb.parameters(), int(skip), int(limit))
            # wb.parameters() + (int(skip), int(limit))
            data_df = await read_df(engine, data_sql, data_params)
            # print(data_sql, data_params)

            return {
                "total": total,
                "skip": skip,
                "limit": limit,
                "data": data_df.to_dict(orient="records") if not data_df.empty else []
            }
        except ValueError as ve:
            raise HTTPException(status_code=400, detail=f"Invalid dates: {ve}")
        except Exception as e:
            logger.exception("QuoteReport failed: %s", e)
            raise HTTPException(status_code=500, detail="Failed to fetch quote report")

    @staticmethod
    async def QuoteConversionSummary(
        engine, start_date: str, end_date: str,
        country_codes: Union[str, List[str], None] = 'all',
        brands:str = "all", pet_types:str = "all",
        # quoteStatus: str = 'All', 
    ) -> Dict[str, Any]:
        try:
            start_str, end_plus_1, end_str = parse_dates(start_date, end_date)
            country_code_list = normalize_regions(country_codes)

            brand_list = normalize_input(brands)
            pet_list = normalize_input(pet_types)

            # Create empty WhereBuilder
            wb = WhereBuilder()
            wb = whereFilters(wb=wb,country_codes=country_code_list,brands=brand_list, pets=pet_list)
            
            sql = f"""
                SELECT
                    TotalQuotes = COUNT(CASE WHEN CreatedDate >= ? AND CreatedDate < ? THEN QuoteNumber END),
                    TotalSales = SUM(
                        CASE WHEN PolicyNumber IS NOT NULL 
                            AND PolicyNumber NOT LIKE '%NONE%' 
                            AND PolicyCreatedDate >= ? 
                            AND PolicyCreatedDate < ?
                            AND CreatedDate >= ? AND CreatedDate < ?
                        THEN 1 ELSE 0 END
                    ) 
                FROM Quote
                WHERE {wb.sql()}
            """

            
            # df: pd.DataFrame = await read_df(engine, sql, wb.parameters())
            # Combine parameters: PolicyStartDate dates first, then CreatedDate dates, then wb params
            all_params = [start_str, end_plus_1, start_str, end_plus_1, start_str, end_plus_1] + list(wb.parameters())
            # print(sql, all_params)

            df: pd.DataFrame = await read_df(engine, sql, all_params)
            
            if df.empty:
                return {"total_quotes": 0, "converted": 0 , "conversion_percent": 0.0}

            row = df.iloc[0]
            total_quotes = int(row["TotalQuotes"] or 0)
            converted = int(row["TotalSales"] or 0)
            

            if total_quotes == 0:
                conversion_percent = 0.0
            else:
                conversion_percent = round((converted / total_quotes) * 100, 2)

            # Derive the remainder so converted + remainder sum to total for charts
            not_converted = max(total_quotes - converted, 0)
            not_converted_percent = round(100.0 - conversion_percent, 2) if total_quotes > 0 else 0.0

            return {
                "total_quotes": total_quotes,
                "converted": converted,
                "conversion_percent": conversion_percent,  # e.g. 37.52
                # Added for 100% breakdown convenience
                "not_converted": not_converted,
                "not_converted_percent": not_converted_percent,
                "breakdown": [
                    {"name": "Converted", "value": converted, "percent": conversion_percent},
                    {"name": "Not Converted", "value": not_converted, "percent": not_converted_percent},
                ],
                "meta": {"start_date": start_str, "end_date": end_str, "country_codes": country_code_list or "ALL"}
            }
        except ValueError as ve:
            raise HTTPException(status_code=400, detail=f"Invalid dates: {ve}")
        except Exception as e:
            logger.exception("Summary failed: %s", e)
            raise HTTPException(status_code=500, detail="Failed to fetch quote summary")

    @staticmethod
    async def QuoteConversionReport(
        engine,
        start_date: str,
        end_date: str,
        country_codes: Union[str, List[str], None] = 'all',
        skip: int = 0,
        limit: int = 100,
        brands:str = "all", 
        pet_types:str = "all",
        quoteStatus: str = 'All', 
    ) -> Dict[str, Any]:
        try:
            start_str, end_plus_1, _ = parse_dates(start_date, end_date)
            skip = max(0, int(skip))
            limit = min(max(1, int(limit)), 10_000)
            country_code_list = normalize_regions(country_codes)

            brand_list = normalize_input(brands)
            pet_list = normalize_input(pet_types)

            wb = (WhereBuilder()
                  .add("CreatedDate >= ?", start_str)
                  .add("CreatedDate < ?", end_plus_1)
                )
            
            wb = whereFilters(wb=wb,country_codes=country_code_list,brands=brand_list, pets=pet_list)

            count_sql = f"SELECT COUNT(QuoteNumber) AS TotalRecords FROM Quote WHERE {wb.sql()}"

            data_sql = f"""
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
                WHERE {wb.sql()}
                ORDER BY CreatedDate DESC, QuoteNumber
                OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """

            total_df = await read_df(engine, count_sql, wb.parameters())
            total = first_cell_int(total_df, default=0)

            data_params = wb.parameters() + (int(skip), int(limit))
            data_df = await read_df(engine, data_sql, data_params)
            # data_df["Converted"] = data_df["Converted"].astype(bool)

            return {
                "total": total,
                "skip": skip,
                "limit": limit,
                "data": data_df.to_dict(orient="records") if not data_df.empty else []
            }
        except ValueError as ve:
            raise HTTPException(status_code=400, detail=f"Invalid dates: {ve}")
        except Exception as e:
            logger.exception("QuoteConversionReport failed: %s", e)
            raise HTTPException(status_code=500, detail="Failed to fetch quote report")

    @staticmethod
    async def QuoteReceiveMethodSamePeriod(
        engine,
        start_date: str,
        end_date: str,
        country_codes: Union[str, List[str], None] = 'all',
        months: Optional[int] = 7,  
        brands:str = "all", 
        pet_types:str = "all",
        # quoteStatus: str = 'All', 
    ) -> Dict[str, Any]:
        try:
            # Fixed day window from the inputs
            start_dt = datetime.fromisoformat(start_date).date()
            end_dt   = datetime.fromisoformat(end_date).date()
            start_day = start_dt.day
            end_day   = end_dt.day

            # If months provided, recompute an earlier start using end_date's month to build trend.
            # This restores historical multi-month trend behavior.
            if months and months > 0:
                total = end_dt.year * 12 + (end_dt.month - 1) - (months - 1)
                y = total // 12
                m = total % 12 + 1
                first_month_last_day = monthrange(y, m)[1]
                used_start_dt = date(y, m, min(start_day, first_month_last_day))
                start_str, end_plus_1, end_str = parse_dates(used_start_dt.isoformat(), end_date)
            else:
                used_start_dt = start_dt
                start_str, end_plus_1, end_str = parse_dates(start_date, end_date)

            country_code_list = normalize_regions(country_codes)
            
            brand_list = normalize_input(brands)
            pet_list = normalize_input(pet_types)

            wb = (WhereBuilder()
                  .add("CreatedDate >= ?", start_str)
                  .add("CreatedDate < ?", end_plus_1)
                )
            wb = whereFilters(wb=wb,country_codes=country_code_list,brands=brand_list, pets=pet_list)
            
            # Use the user's selected window to decide MTD-style alignment vs full-months
            same_calendar_month = (start_dt.year == end_dt.year and start_dt.month == end_dt.month)

            if same_calendar_month:
                sql = f"""
                    SELECT
                        COUNT(QuoteNumber) AS value,
                        QuoteReceivedMethod,
                        DATEFROMPARTS(YEAR(CreatedDate), MONTH(CreatedDate), 1) AS QuoteReportingPeriod
                    FROM Quote
                    WHERE {wb.sql()}
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
                    GROUP BY
                        QuoteReceivedMethod,
                        DATEFROMPARTS(YEAR(CreatedDate), MONTH(CreatedDate), 1)
                    ORDER BY QuoteReportingPeriod ASC
                """
                # Params: lower bound (start_day) twice, upper bound (end_day) twice
                df: pd.DataFrame = await read_df(engine, sql, (*wb.parameters(), start_day, start_day, end_day, end_day))
            else:
                sql = f"""
                    SELECT
                        COUNT(QuoteNumber) AS value,
                        QuoteReceivedMethod,
                        DATEFROMPARTS(YEAR(CreatedDate), MONTH(CreatedDate), 1) AS QuoteReportingPeriod
                    FROM Quote
                    WHERE {wb.sql()}
                    GROUP BY
                        QuoteReceivedMethod,
                        DATEFROMPARTS(YEAR(CreatedDate), MONTH(CreatedDate), 1)
                    ORDER BY QuoteReportingPeriod ASC
                """
                df: pd.DataFrame = await read_df(engine, sql, (*wb.parameters(),))

            if df.empty:
                chart, totals = [], {}
                period_totals = {"web": 0, "phone": 0}
            else:
                # Normalize types for reliable pivot/reindex
                df["QuoteReportingPeriod"] = pd.to_datetime(df["QuoteReportingPeriod"])  # keep datetime for index
                df["QuoteReceivedMethod"] = (
                    df["QuoteReceivedMethod"].astype(str).str.strip().str.lower()
                    .replace({"contact center": "phone", "contact_center": "phone"})
                )

                wide = (df.pivot_table(
                            index="QuoteReportingPeriod",
                            columns="QuoteReceivedMethod",
                            values="value",
                            aggfunc="sum",
                            fill_value=0
                        )
                        .rename(columns=lambda c: str(c).strip().lower()))

                # Ensure datetime index for alignment with full_index
                if not isinstance(wide.index, pd.DatetimeIndex):
                    wide.index = pd.to_datetime(wide.index)

                for col in ["web", "phone"]:
                    if col not in wide.columns:
                        wide[col] = 0

                # Build a continuous monthly index from the effective start month to the end month
                start_anchor = pd.Timestamp(year=used_start_dt.year, month=used_start_dt.month, day=1)
                end_anchor = pd.Timestamp(year=end_dt.year, month=end_dt.month, day=1)
                full_index = pd.date_range(start=start_anchor, end=end_anchor, freq="MS")
                wide = wide.reindex(full_index, fill_value=0).sort_index()

                chart = [
                    {"date": idx.strftime("%b %y"),
                    "web": int(wide.at[idx, "web"]),
                    "phone": int(wide.at[idx, "phone"])}
                    for idx in wide.index
                ]
                # Totals keyed by normalized receive method names
                totals = (
                    df.groupby("QuoteReceivedMethod")["value"].sum().astype(int).to_dict()
                )

                # Period-only totals for current window (web/phone only)
                # Use the original URL start/end dates, not the historical used_start_dt.
                _period_start_str, _period_end_plus_1, _ = parse_dates(start_date, end_date)
                wb_period = (
                    WhereBuilder()
                    .add("CreatedDate >= ?", _period_start_str)
                    .add("CreatedDate < ?", _period_end_plus_1)
                )
                wb_period = whereFilters(wb=wb_period, country_codes=country_code_list, brands=brand_list, pets=pet_list)
                period_sql = f"""
                    SELECT QuoteReceivedMethod, COUNT(QuoteNumber) AS value
                    FROM Quote
                    WHERE {wb_period.sql()}
                    GROUP BY QuoteReceivedMethod
                """
                period_df = await read_df(engine, period_sql, wb_period.parameters())
                period_totals = {"web": 0, "phone": 0}
                if not period_df.empty:
                    for _, r in period_df.iterrows():
                        m = str(r.get("QuoteReceivedMethod", "")).strip().lower()
                        v = int(r.get("value", 0))
                        if m == "web":
                            period_totals["web"] += v
                        elif m == "phone" or m == "contact center" or m == "contact_center":
                            period_totals["phone"] += v

            return {
                "meta": {
                    "start_date": start_str,
                    "end_date": end_str,
                    "country_codes": country_code_list or "ALL",
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "months": months,
                    "window_start_day": start_day,
                    "window_end_day": end_day,
                },
                "chart": chart,
                "totals_by_receive_method": totals,
                "current_period_total": period_totals,
                "total_quotes": sum(totals.values()) if totals else 0,
            }

        except ValueError as ve:
            raise HTTPException(status_code=400, detail=f"Invalid dates: {ve}")
        except Exception as e:
            logger.exception("Summary failed: %s", e)
            raise HTTPException(status_code=500, detail="Failed to fetch quote summary")

    @staticmethod
    async def QuoteReceiveMethodSamePeriodReport(
        engine,
        start_date: str,
        end_date: str,
        country_codes: Union[str, List[str], None] = 'all',
        skip: int = 0,
        limit: int = 100,
        months: Optional[int] = 7,        
        brands:str = "all", 
        pet_types:str = "all",
        quoteStatus: str = 'All', 
    ) -> Dict[str, Any]:
        try:
            skip = max(0, int(skip))
            limit = min(max(1, int(limit)), 10_000)

            # Fixed day window from the inputs
            start_dt = datetime.fromisoformat(start_date).date()
            end_dt   = datetime.fromisoformat(end_date).date()
            start_day = start_dt.day
            end_day   = end_dt.day

            # If months provided, compute an earlier start for multi-month trend/reporting
            if months and months > 0:
                total = end_dt.year * 12 + (end_dt.month - 1) - (months - 1)
                y = total // 12
                m = total % 12 + 1
                first_month_last_day = monthrange(y, m)[1]
                computed_start = date(y, m, min(start_day, first_month_last_day)).isoformat()
                start_str, end_plus_1, end_str = parse_dates(computed_start, end_date)
            else:
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
            

            # Single query: return data page + windowed total count
            data_sql = f"""
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
                    WHERE {wb.sql()}
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
                    b.*,
                    COUNT(QuoteNumber) OVER() AS TotalRecords
                FROM Base b
                ORDER BY b.CreatedDate DESC, b.QuoteNumber
                OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;
            """

            params = (*wb.parameters(), start_day, start_day, end_day, end_day, int(skip), int(limit))
            data_df = await read_df(engine, data_sql, params)

            if data_df.empty:
                total = 0
                records = []
            else:
                total = int(data_df.iloc[0].get("TotalRecords", 0)) if "TotalRecords" in data_df.columns else 0
                if "TotalRecords" in data_df.columns:
                    data_df = data_df.drop(columns=["TotalRecords"])
                records = data_df.to_dict(orient="records")

            return {
                "total": total,
                "skip": skip,
                "limit": limit,
                "start_date": start_str,
                "end_date": end_str,
                "country_codes": country_code_list,
                "data": records,
            }

        except ValueError as ve:
            raise HTTPException(status_code=400, detail=f"Invalid dates: {ve}")
        except Exception as e:
            logger.exception("QuoteReceiveMethodSamePeriodReport failed: %s", e)
            raise HTTPException(status_code=500, detail="Failed to fetch quote report")

