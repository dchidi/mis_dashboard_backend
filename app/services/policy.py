from fastapi import HTTPException
from typing import List, Optional, Union, Dict, Any
from app.utils.report_helpers import normalize_input, parse_dates, normalize_regions, WhereBuilder, read_df
from datetime import datetime, timezone, date
import pandas as pd
import logging
import calendar


logger = logging.getLogger(__name__)


class Policy:
    
    @staticmethod
    async def PolicyMonthlyStatusSummary(
        engine,
        start_date: str,
        end_date: str,
        regions: Union[str, List[str], None] = "all",
        policy_status: Union[str, List[str]] = "all",
        policy_type: str = "all",
        date_basis: str = "QuoteCreatedDate",
        months: Optional[int] = 6,              
        brands:str = "all", 
        pet_types:str = "all",
    ) -> Dict[str, Any]:
        try:
            # Derive the day window from the user's original dates
            start_dt = datetime.fromisoformat(start_date).date()
            end_dt   = datetime.fromisoformat(end_date).date()
            start_day = start_dt.day
            end_day   = end_dt.day

            # If months is provided, override the effective start of the range
            if months and months > 0:
                # Jump to the first month in range (months-1 months before end month)
                total = end_dt.year * 12 + (end_dt.month - 1) - (months - 1)
                y = total // 12
                m = total % 12 + 1
                # Clamp the start_day to that month's length
                first_month_last_day = calendar.monthrange(y, m)[1]
                computed_start = date(y, m, min(start_day, first_month_last_day)).isoformat()
                start_str, end_plus_1, end_str = parse_dates(computed_start, end_date)
            else:
                # Fallback: use the provided window as-is
                start_str, end_plus_1, end_str = parse_dates(start_date, end_date)

            region_list = normalize_regions(regions)

            brand_list = normalize_input(brands)
            pet_list = normalize_input(pet_types)


            # Validate date_basis column
            allowed_date_fields = {
                "QuoteCreatedDate", "OriginalPolicyStartDate", "PolicyEndDate",
                "QuoteStartDate", "QuoteEndDate"
            }
            if date_basis not in allowed_date_fields:
                date_basis = "QuoteCreatedDate"

            # Normalize policy_status
            status_filter: Union[None, List[str]] = None
            if isinstance(policy_status, list):
                status_filter = [s.strip() for s in policy_status if s and s.strip()] or None
            elif isinstance(policy_status, str) and policy_status.strip().lower() != "all":
                status_filter = [policy_status.strip()]

            # Normalize policy_type (maps to FreePolicy)
            free_policy_filter: Union[None, List[str]] = None
            pt = (policy_type or "all").strip()
            if pt.lower() in ("yes", "no"):
                free_policy_filter = [pt.capitalize()]  # 'Yes' or 'No'

            # WHERE clause (no per-day cutoff here; handled by SQL CASE clamps)
            wb = (
                WhereBuilder()
                .add(f"{date_basis} >= ?", start_str)
                .add(f"{date_basis} < ?", end_plus_1)
            )

            if region_list:
                wb.add_in("CountryCode", region_list)
            if brand_list:
                wb.add_in("Brand", brand_list)

            pet_patterns = {
                "cat":   "%cat%",
                "dog":   "%dog%",
                "horse": "%horse%",
                "exotic":"%exotic%",
                "bbc":    "%bbcom%",
                "bbcom": "%bbcom%",
            }
            pet_tokens = [p.lower() for p in pet_list]

            if pet_tokens:
                likes, params = [], []
                for p in pet_tokens:
                    patt = pet_patterns.get(p)
                    if patt:
                        likes.append("LOWER(COALESCE(PetType, '')) LIKE ?")
                        params.append(patt)
                if likes:
                    wb.add("(" + " OR ".join(likes) + ")", *params)

            if status_filter:
                wb.add_in("CustomerStatus", status_filter)
            if free_policy_filter:
                wb.add_in("FreePolicy", free_policy_filter)

            # SQL: filter each month to the SAME day window [start_day .. end_day], clamped by month length
            sql = f"""
                SELECT
                    COUNT(*) AS value,
                    DATEFROMPARTS(YEAR({date_basis}), MONTH({date_basis}), 1) AS PolicyReportingPeriod
                FROM CRM
                WHERE {wb.sql()}
                AND DAY({date_basis}) >=
                    CASE
                        WHEN ? > DAY(EOMONTH({date_basis}))
                        THEN DAY(EOMONTH({date_basis}))
                        ELSE ?
                    END
                AND DAY({date_basis}) <=
                    CASE
                        WHEN ? > DAY(EOMONTH({date_basis}))
                        THEN DAY(EOMONTH({date_basis}))
                        ELSE ?
                    END
                GROUP BY DATEFROMPARTS(YEAR({date_basis}), MONTH({date_basis}), 1)
                ORDER BY PolicyReportingPeriod ASC
            """

            # Params: tuple-pack to satisfy pylance/types
            params = (*wb.parameters(), start_day, start_day, end_day, end_day)

            df: pd.DataFrame = await read_df(engine, sql, params)

            if df.empty:
                chart, totals = [], {}
            else:
                df["PolicyReportingPeriod"] = pd.to_datetime(df["PolicyReportingPeriod"])

                # Create a DataFrame with policy counts
                wide = df.set_index("PolicyReportingPeriod")[["value"]].rename(columns={"value": "policyCount"})

                # Ensure all months are included (from start to end of range)
                full_index = pd.date_range(
                    start=df["PolicyReportingPeriod"].min(),
                    end=df["PolicyReportingPeriod"].max(),
                    freq="MS"
                )
                wide = wide.reindex(full_index, fill_value=0).sort_index()

                # Format to match chart structure
                chart = [
                    {"date": idx.strftime("%b %y"), "count": int(wide.at[idx, "policyCount"])}
                    for idx in wide.index
                ]
                totals = {"total_policies": int(df["value"].sum())}

            return {
                "meta": {
                    "start_date": start_str,   # effective start (may be overridden by `months`)
                    "end_date": end_str,
                    "months": months,
                    "date_basis": date_basis,
                    "regions": region_list or "ALL",
                    "policy_status": status_filter or "ALL",
                    "policy_type": free_policy_filter or "ALL",
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "window_start_day": start_day,
                    "window_end_day": end_day,
                },
                "chart": chart,
                "total_policies": totals["total_policies"] if totals else 0,
            }

        except ValueError as ve:
            raise HTTPException(status_code=400, detail=f"Invalid dates: {ve}")
        except Exception as e:
            logger.exception("PolicyMonthlyStatusSummary failed: %s", e)
            raise HTTPException(status_code=500, detail="Failed to fetch policy summary")

    @staticmethod
    async def PolicyStatusRaw(
        engine,
        start_date: str,
        end_date: str,
        regions: Union[str, List[str], None] = "all",
        policy_status: Union[str, List[str]] = "all",
        policy_type: str = "all",
        date_basis: str = "QuoteCreatedDate",
        skip: int = 0,
        limit: int = 100,
        order: str = "DESC",
        months: Optional[int] = None,
              
        brands:str = "all", 
        pet_types:str = "all",
    ) -> Dict[str, Any]:
        try:
            # --- pagination guards ---
            skip = max(0, int(skip))
            limit = min(max(1, int(limit)), 10_000)

            # --- derive day-window from the raw inputs ---
            start_dt = datetime.fromisoformat(start_date).date()
            end_dt   = datetime.fromisoformat(end_date).date()
            start_day = start_dt.day
            end_day   = end_dt.day

            # --- if months is provided, override effective start of overall range ---
            if months and months > 0:
                # go back (months-1) months from end_dt's month
                total = end_dt.year * 12 + (end_dt.month - 1) - (months - 1)
                y = total // 12
                m = total % 12 + 1
                # clamp start_day to that month's last day
                first_month_last_day = calendar.monthrange(y, m)[1]
                computed_start = date(y, m, min(start_day, first_month_last_day)).isoformat()
                start_str, end_plus_1, end_str = parse_dates(computed_start, end_date)
            else:
                # default: use provided start/end as the overall range
                start_str, end_plus_1, end_str = parse_dates(start_date, end_date)

            # this is kept for backward-compat meta
            target_day = end_day

            # --- regions ---
            region_list = normalize_regions(regions)

            brand_list = normalize_input(brands)
            pet_list = normalize_input(pet_types)

            # --- whitelist date_basis ---
            allowed_date_fields = {
                "QuoteCreatedDate", "OriginalPolicyStartDate", "PolicyEndDate",
                "QuoteStartDate", "QuoteEndDate"
            }
            if date_basis not in allowed_date_fields:
                date_basis = "QuoteCreatedDate"

            order = "DESC" if str(order).upper() != "ASC" else "ASC"

            # --- normalize filters ---
            status_filter: Union[None, List[str]] = None
            if isinstance(policy_status, list):
                status_filter = [s.strip() for s in policy_status if s and s.strip()] or None
            elif isinstance(policy_status, str) and policy_status.strip().lower() != "all":
                status_filter = [policy_status.strip()]

            free_policy_filter: Union[None, List[str]] = None
            pt = (policy_type or "all").strip().lower()
            if pt in ("yes", "no"):
                free_policy_filter = [pt.capitalize()]  # 'Yes' or 'No'

            # --- WHERE builder (no day filter here; we clamp per-month inside SQL) ---
            wb = (
                WhereBuilder()
                .add(f"{date_basis} >= ?", start_str)
                .add(f"{date_basis} < ?", end_plus_1)  # exclusive upper bound
                .add_in("CountryCode", region_list)
            )
            if status_filter:
                wb.add_in("CustomerStatus", status_filter)
            if free_policy_filter:
                wb.add_in("FreePolicy", free_policy_filter)

            # --- query: CTE + windowed total + page, with SAME day-window per month (start_day..end_day) ---
            sql = f"""
                WITH Base AS (
                    SELECT
                        Brand,
                        COALESCE(CAST(NULLIF(Country, '') AS NVARCHAR(100)), CountryCode) AS Country,
                        BusinessName, BusinessType, CustomerStatus, FreePolicy, QuoteStatus,
                        QuoteNumber, PolicyNumber, QuoteReceivedMethod,
                        QuoteCreatedDate, QuoteStartDate, QuoteEndDate,
                        OriginalPolicyStartDate, PolicyEndDate,
                        FirstName, LastName, Email, ContactNo, EmailConcent,
                        PetName, PetType, PetBirthDate, PetBreedId, BreedName,
                        CountryCode,
                        CAST(CASE WHEN PolicyNumber IS NULL THEN 0 ELSE 1 END AS BIT) AS Converted
                    FROM CRM
                    WHERE {wb.sql()}
                    AND DAY({date_basis}) >=
                        CASE
                            WHEN ? > DAY(EOMONTH({date_basis}))
                            THEN DAY(EOMONTH({date_basis}))
                            ELSE ?
                        END
                    AND DAY({date_basis}) <=
                        CASE
                            WHEN ? > DAY(EOMONTH({date_basis}))
                            THEN DAY(EOMONTH({date_basis}))
                            ELSE ?
                        END
                )
                SELECT
                    GETUTCDATE() AS DateExtracted,
                    b.*,
                    COUNT(*) OVER() AS TotalRecords
                FROM Base b
                ORDER BY b.{date_basis} {order}, b.PolicyNumber
                OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;
            """

            # params: tuple-pack so pylance is happy
            params = (*wb.parameters(), start_day, start_day, end_day, end_day, int(skip), int(limit))
            df: pd.DataFrame = await read_df(engine, sql, params)

            if df.empty:
                total = 0
                records = []
            else:
                total = int(df.iloc[0].get("TotalRecords", 0)) if "TotalRecords" in df.columns else 0
                if "TotalRecords" in df.columns:
                    df = df.drop(columns=["TotalRecords"])
                records = df.to_dict(orient="records")

            return {
                "meta": {
                    "start_date": start_str,
                    "end_date": end_str,
                    "months": months,
                    "window_start_day": start_day,
                    "window_end_day": end_day,
                    "day_cutoff": target_day,  # kept for backward compatibility
                    "date_basis": date_basis,
                    "regions": region_list or "ALL",
                    "policy_status": status_filter or "ALL",
                    "policy_type": free_policy_filter or "ALL",
                    "order": order,
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                },
                "total": total,
                "skip": skip,
                "limit": limit,
                "data": records,
            }

        except ValueError as ve:
            raise HTTPException(status_code=400, detail=f"Invalid dates: {ve}")
        except Exception as e:
            logger.exception("PolicyStatusRaw failed: %s", e)
            raise HTTPException(status_code=500, detail="Failed to fetch raw policy data")
