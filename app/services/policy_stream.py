from fastapi.responses import StreamingResponse
from typing import Union, List, Optional
from app.utils.report_helpers import (
    normalize_input, parse_dates, normalize_regions, WhereBuilder,
    format_filename, generate_csv_stream, whereFilters
)
from datetime import datetime, date
import calendar
import io
import csv

class PolicyStream:
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
    def _policy_raw_base_sql(where_sql: str, date_basis: str, order: str) -> str:
        # Raw rows, no grouping. `date_basis` and `order` are pre-validated.
        # Use CASE to clamp start/end day to the month's last day.
        return f"""
            SELECT
                GETUTCDATE() AS DateExtracted,
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
            WHERE {where_sql}
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
            ORDER BY {date_basis} {order}, PolicyNumber
        """
    
    @staticmethod
    def _sales_raw_base_sql(where_sql: str) -> str:
        return f"""
            SELECT                
                CountryName, CountryCode, Brand,
                QuoteNumber, 
                CreatedDate, ActualStartDate,
                ProductName, PetType, ClientName,
                PetName, PetType, PolicyNumber  
            FROM Sales
            WHERE {where_sql}
            ORDER BY CreatedDate DESC
        """

    @staticmethod
    def _free_policy_raw_base_sql(where_sql: str) -> str:
        return f"""
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
            WHERE {where_sql}
            ORDER BY CreatedDate DESC
        """

    @staticmethod
    async def stream_policy_status_raw_csv(
        engine,
        start_date: str,
        end_date: str,
        regions: Union[str, List[str], None] = "all",
        policy_status: Union[str, List[str]] = "all",
        policy_type: str = "all",
        date_basis: str = "QuoteCreatedDate",
        order: str = "DESC",
        filename: str = "policy_status_raw.csv",
        months: Optional[int] = None,
        brands:str = "all", 
        pet_types:str = "all",
    ) -> StreamingResponse:
        # Derive day window from inputs
        start_day = datetime.fromisoformat(start_date).day
        end_dt = datetime.fromisoformat(end_date).date()
        end_day = end_dt.day

        # If months provided, recompute effective overall start using end_date’s month
        if months and months > 0:
            total = end_dt.year * 12 + (end_dt.month - 1) - (months - 1)
            y = total // 12
            m = total % 12 + 1
            first_month_last_day = calendar.monthrange(y, m)[1]
            computed_start = date(y, m, min(start_day, first_month_last_day)).isoformat()
            start_str, end_plus_1, end_str = parse_dates(computed_start, end_date)
        else:
            # Parse date window (inclusive start, exclusive end_plus_1 for WHERE)
            start_str, end_plus_1, end_str = parse_dates(start_date, end_date)

        # Normalize regions
        region_list = normalize_regions(regions)

        brand_list = normalize_input(brands)
        pet_list = normalize_input(pet_types)

        # Whitelist date basis to avoid injection
        allowed_date_fields = {
            "QuoteCreatedDate", "OriginalPolicyStartDate", "PolicyEndDate",
            "QuoteStartDate", "QuoteEndDate"
        }
        if date_basis not in allowed_date_fields:
            date_basis = "QuoteCreatedDate"

        # Normalize order
        order = "ASC" if str(order).upper() == "ASC" else "DESC"

        # Normalize policy_status filter
        status_filter: Union[None, List[str]] = None
        if isinstance(policy_status, list):
            status_filter = [s.strip() for s in policy_status if s and s.strip()] or None
        elif isinstance(policy_status, str) and policy_status.strip().lower() != "all":
            status_filter = [policy_status.strip()]

        # Normalize policy_type -> FreePolicy filter
        free_policy_filter: Union[None, List[str]] = None
        pt = (policy_type or "all").strip()
        if pt.lower() in ("yes", "no"):
            free_policy_filter = [pt.capitalize()]  # 'Yes' or 'No'

        # Build WHERE (no per-day cutoff here; handled in base SQL)
        wb = (
            WhereBuilder()
            .add(f"{date_basis} >= ?", start_str)
            .add(f"{date_basis} < ?", end_plus_1)
            .add_in("CountryCode", region_list)
        )
        if status_filter:
            wb.add_in("CustomerStatus", status_filter)
        if free_policy_filter:
            wb.add_in("FreePolicy", free_policy_filter)

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
            if likes:  # only add if we recognized at least one category
                wb.add("(" + " OR ".join(likes) + ")", *params)

        sql = PolicyStream._policy_raw_base_sql(wb.sql(), date_basis=date_basis, order=order)

        # Params: tuple-pack (start_day twice, end_day twice for the CASEs)
        params = (*wb.parameters(), start_day, start_day, end_day, end_day)

        # Filename: include the window for clarity
        base_filename = filename.replace(".csv", "")
        full_filename = f"{base_filename}_d{start_day}-{end_day}_{start_str}_to_{end_str}.csv"

        return generate_csv_stream(engine, sql, params, full_filename)
    
    @staticmethod
    async def stream_sales_raw_csv(
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

        sql = PolicyStream._sales_raw_base_sql(wb.sql())
        params = wb.parameters()

        # return QuoteStream._generate_csv_stream(engine, sql, params, filename)
        full_filename = format_filename(filename, start_str, end_str)

        return PolicyStream._generate_csv_stream(engine, sql, params, full_filename)
    
    @staticmethod
    async def stream_free_policy_raw_csv(
        engine,
        start_date: str,
        end_date: str,
        country_codes: Union[str, List[str], None] = "all",
        filename: str = "free_policy.csv",
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

        sql = PolicyStream._free_policy_raw_base_sql(wb.sql())
        params = wb.parameters()

        # return QuoteStream._generate_csv_stream(engine, sql, params, filename)
        full_filename = format_filename(filename, start_str, end_str)

        return PolicyStream._generate_csv_stream(engine, sql, params, full_filename)

    