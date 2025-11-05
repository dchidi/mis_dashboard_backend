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



class Sales:
    @staticmethod
    async def SalesSummary(
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

            # --- LTM graph (correct logic) ---
            # Requirement: 13 months ending at end_date's month; for each month, count only days
            # between start_dt.day and end_dt.day (inclusive). Example: 5..10 each month.
            start_day = start_dt.day
            end_day   = end_dt.day

            # Month anchors (1st of month)
            end_month_anchor = date(end_dt.year, end_dt.month, 1)           # current anchor month
            oldest_month_anchor = add_months(end_month_anchor, -12)         # 12 months before = 13 total
            upper_bound_exclusive = add_months(end_month_anchor, 1)         # first day after anchor month

            # For MTD windows, align prior months to the same day-of-month window.
            # For multi-month windows (e.g., YTD), use full-month aggregation.
            same_calendar_month = (start_dt.year == end_dt.year and start_dt.month == end_dt.month)

            if same_calendar_month:
                ltm_sql = f"""
                    SELECT 
                        YEAR(CreatedDate) AS [year],
                        MONTH(CreatedDate) AS [month],
                        COUNT(PolicyNumber) AS [value]
                    FROM Sales
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
                        COUNT(PolicyNumber) AS [value]
                    FROM Sales
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
                "graphData": graph_data,
            }

        except ValueError as ve:
            raise HTTPException(status_code=400, detail=f"Invalid dates: {ve}")
        except Exception as e:
            logger.exception("Summary failed: %s", e)
            raise HTTPException(status_code=500, detail="Failed to fetch sales summary")

    @staticmethod
    async def SalesByPetType(
            engine, start_date: str, end_date: str,
            country_codes: Union[str, List[str], None] = 'all',
            brands:str = "all",
            pet_types:str = "all",      
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


                sql = f"""
                    SELECT 
                        COUNT(PolicyNumber) AS value,
                        CASE 
                            WHEN LOWER(COALESCE(PetType, '')) LIKE '%cat%'    THEN 'Cat'
                            WHEN LOWER(COALESCE(PetType, '')) LIKE '%dog%'    THEN 'Dog'
                            WHEN LOWER(COALESCE(PetType, '')) LIKE '%horse%'  THEN 'Horse'
                            WHEN LOWER(COALESCE(PetType, '')) LIKE '%exotic%' THEN 'Exotic'
                            WHEN LOWER(COALESCE(PetType, '')) LIKE '%bb_com%'  THEN 'BB'
                            ELSE 'Others'
                        END AS name
                    FROM Sales
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
    async def FreePolicySales(
        engine,
        start_date: str,
        end_date: str,
        country_codes: Union[str, List[str], None] = "all",
        brands: str = "all",
        pet_types: str = "all",
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
            wb = whereFilters(wb=wb, country_codes=country_code_list, brands=brand_list, pets=pet_list)

            sql = f"""
                WITH base AS (
                    SELECT PolicyStatusName, PetType, SaleMethod
                    FROM FreePolicySales
                    WHERE {wb.sql()}
                ),
                status_agg AS (
                    SELECT PolicyStatusName AS name, COUNT(*) AS value,
                           CAST(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0) AS DECIMAL(5,1)) AS PctOfTotal
                    FROM base
                    GROUP BY PolicyStatusName
                ),
                pet_agg AS (
                    SELECT PetType AS name, COUNT(*) AS value,
                           CAST(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0) AS DECIMAL(5,1)) AS PctOfTotal
                    FROM base
                    GROUP BY PetType
                ),
                channel_agg AS (
                    SELECT SaleMethod AS name, COUNT(*) AS value,
                           CAST(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0) AS DECIMAL(5,1)) AS PctOfTotal
                    FROM base
                    GROUP BY SaleMethod
                )
                SELECT 'status' AS level, name, value, PctOfTotal FROM status_agg
                UNION ALL
                SELECT 'pet_type' AS level, name, value, PctOfTotal FROM pet_agg
                UNION ALL
                SELECT 'channel' AS level, name, value, PctOfTotal FROM channel_agg
                ORDER BY level, value DESC;
            """

            df: pd.DataFrame = await read_df(engine, sql, wb.parameters())

            if df.empty:
                by_status = []
                by_pet = []
                by_channel = []
                grand_total = 0
            else:
                # normalize numeric types
                df["value"] = df["value"].astype(int)
                df["PctOfTotal"] = df["PctOfTotal"].astype(float)

                status_df = df[df["level"] == "status"][ ["name", "value", "PctOfTotal"] ]
                pet_df    = df[df["level"] == "pet_type"][ ["name", "value", "PctOfTotal"] ]
                channel_df    = df[df["level"] == "channel"][ ["name", "value", "PctOfTotal"] ]
                by_status = status_df.to_dict(orient="records")
                by_pet    = pet_df.to_dict(orient="records")
                by_channel    = channel_df.to_dict(orient="records")
                grand_total = int(status_df["value"].sum()) if not status_df.empty else 0

            return {
                "meta": {
                    "start_date": start_str,
                    "end_date": end_str,
                    "country_codes": country_code_list or "ALL",
                    "brands": brand_list or "ALL",
                    "pet_types": pet_list or "ALL",
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                },
                # Back-compat keys for UI reuse
                # "by_status": by_status,
                # "by_pet": by_pet,
                # Explicit groups
                "by_status": by_status,
                "by_pet_type": by_pet,
                "by_channel": by_channel,
                "total": grand_total,
            }
        except ValueError as ve:
            raise HTTPException(status_code=400, detail=f"Invalid dates: {ve}")
        except Exception as e:
            logger.exception("FreePolicySalesSimple failed: %s", e)
            raise HTTPException(status_code=500, detail="Failed to fetch free policy summary")

    @staticmethod
    async def FreePolicyData(
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

            count_sql = f"SELECT COUNT(PolicyNumber) AS TotalRecords FROM FreePolicySales WHERE {wb.sql()}"            
            data_sql = f"""
                SELECT 
                    CountryCode, CountryName, CreatedDate, QuoteNumber, PolicyNumber,
                    SubAgentName, AgentCategoryId,
                    CASE
                        WHEN AgentCategoryId = 8 THEN 'Breeder'
                        WHEN AgentCategoryId = 7 THEN 'Pet Business'
                        WHEN AgentCategoryId = 6 THEN 'Charities'
                        WHEN AgentCategoryId = 5 THEN 'Vet'
                    END AS AgentCategory,
                    PetType, ProductName, StateName,
                    SaleMethod, PolicyStatusName, Brand
                FROM FreePolicySales
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
    async def SalesData(
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

            count_sql = f"SELECT COUNT(PolicyNumber) AS TotalRecords FROM Sales WHERE {wb.sql()}"            
            data_sql = f"""
                SELECT
                    CountryName, CountryCode, Brand,
                    QuoteNumber, 
                    CreatedDate, ActualStartDate,
                    ProductName, PetType, ClientName,
                    PetName, PetType, SaleMethod, PolicyNumber                      
                FROM Sales
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
    async def SalesReceiveMethodSamePeriod(
        engine,
        start_date: str,
        end_date: str,
        country_codes: Union[str, List[str], None] = 'all',
        months: Optional[int] = 7,  
        brands:str = "all", 
        pet_types:str = "all",
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
            
            same_calendar_month = (start_dt.year == end_dt.year and start_dt.month == end_dt.month)

            if same_calendar_month:
                sql = f"""
                    SELECT
                        COUNT(PolicyNumber) AS value,
                        SaleMethod,
                        DATEFROMPARTS(YEAR(CreatedDate), MONTH(CreatedDate), 1) AS SalesReportingPeriod
                    FROM Sales
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
                        SaleMethod,
                        DATEFROMPARTS(YEAR(CreatedDate), MONTH(CreatedDate), 1)
                    ORDER BY SalesReportingPeriod ASC
                """
                # Params: lower bound (start_day) twice, upper bound (end_day) twice
                df: pd.DataFrame = await read_df(engine, sql, (*wb.parameters(), start_day, start_day, end_day, end_day))
            else:
                sql = f"""
                    SELECT
                        COUNT(PolicyNumber) AS value,
                        SaleMethod,
                        DATEFROMPARTS(YEAR(CreatedDate), MONTH(CreatedDate), 1) AS SalesReportingPeriod
                    FROM Sales
                    WHERE {wb.sql()}
                    GROUP BY
                        SaleMethod,
                        DATEFROMPARTS(YEAR(CreatedDate), MONTH(CreatedDate), 1)
                    ORDER BY SalesReportingPeriod ASC
                """
                df: pd.DataFrame = await read_df(engine, sql, (*wb.parameters(),))

            if df.empty:
                chart, totals = [], {}
                period_totals = {"web": 0, "phone": 0}
            else:
                # Keep SalesReportingPeriod as datetime for proper reindexing later
                df["SalesReportingPeriod"] = pd.to_datetime(df["SalesReportingPeriod"])

                # Normalize SaleMethod into canonical buckets used by the UI
                df["SaleMethod"] = (
                    df["SaleMethod"].astype(str).str.strip().str.lower()
                    .replace({"contact center": "phone", "contact_center": "phone"})
                )

                wide = (df.pivot_table(
                            index="SalesReportingPeriod",
                            columns="SaleMethod",
                            values="value",
                            aggfunc="sum",
                            fill_value=0
                        ))

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
                totals = {str(k): int(v) for k, v in df.groupby("SaleMethod")["value"].sum().items()}

                # Period-only totals for current window (web/phone only),
                # using the original URL start/end dates rather than the historical anchor.
                _period_start_str, _period_end_plus_1, _ = parse_dates(start_date, end_date)
                wb_period = (
                    WhereBuilder()
                    .add("CreatedDate >= ?", _period_start_str)
                    .add("CreatedDate < ?", _period_end_plus_1)
                )
                wb_period = whereFilters(wb=wb_period, country_codes=country_code_list, brands=brand_list, pets=pet_list)
                period_sql = f"""
                    SELECT
                        CASE
                            WHEN LOWER(LTRIM(RTRIM(SaleMethod))) IN ('contact center','contact_center','phone') THEN 'phone'
                            WHEN LOWER(LTRIM(RTRIM(SaleMethod))) = 'web' THEN 'web'
                            ELSE LOWER(LTRIM(RTRIM(SaleMethod)))
                        END AS SaleMethod,
                        COUNT(PolicyNumber) AS value
                    FROM Sales
                    WHERE {wb_period.sql()}
                    GROUP BY CASE
                        WHEN LOWER(LTRIM(RTRIM(SaleMethod))) IN ('contact center','contact_center','phone') THEN 'phone'
                        WHEN LOWER(LTRIM(RTRIM(SaleMethod))) = 'web' THEN 'web'
                        ELSE LOWER(LTRIM(RTRIM(SaleMethod)))
                    END
                """
                period_df = await read_df(engine, period_sql, wb_period.parameters())
                period_totals = {"web": 0, "phone": 0}
                if not period_df.empty:
                    for _, r in period_df.iterrows():
                        m = str(r.get("SaleMethod", "")).strip().lower()
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
                "total_sales": sum(totals.values()) if totals else 0,
            }

        except ValueError as ve:
            raise HTTPException(status_code=400, detail=f"Invalid dates: {ve}")
        except Exception as e:
            logger.exception("Summary failed: %s", e)
            raise HTTPException(status_code=500, detail="Failed to fetch sales summary")

    

