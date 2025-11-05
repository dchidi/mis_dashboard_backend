from __future__ import annotations
from typing import List, Any, Tuple, Union, Sequence
from datetime import datetime, timedelta, date
import pandas as pd
import anyio
import io
import csv
from fastapi.responses import StreamingResponse

# -------- dates --------
def parse_dates(start_date: Union[str, date], end_date: Union[str, date]) -> Tuple[str, str, str]:
    """Returns (start_str, end_plus_1_str, end_str) in 'YYYY-MM-DD'."""
    if isinstance(start_date, date):
        sd = start_date
    else:
        sd = datetime.strptime(start_date, "%Y-%m-%d").date()
    if isinstance(end_date, date):
        ed = end_date
    else:
        ed = datetime.strptime(end_date, "%Y-%m-%d").date()
    if ed < sd:
        raise ValueError("end_date before start_date")
    return sd.strftime("%Y-%m-%d"), (ed + timedelta(days=1)).strftime("%Y-%m-%d"), ed.strftime("%Y-%m-%d")

# -------- format filename -----------
def format_filename(base: str, start_date: str, end_date: str) -> str:
    return f"{base.rstrip('.csv')}_{start_date}_to_{end_date}.csv"

# -------- regions --------
def normalize_regions(regions: Union[str, List[str], None]) -> List[str]:
    """Accepts 'AT,DE', ['AT','DE'], None -> returns cleaned list; empty list => no region filter."""
    if regions is None:
        return []
    if isinstance(regions, str):
        s = regions.strip()
        if not s or s.lower() == "all":
            return []
        return [r.strip() for r in s.split(",") if r.strip()]
    # list/tuple
    return [str(r).strip() for r in regions if str(r).strip()]

# -------- normalize str/array input. Replace normalize_regions with this --------
def normalize_input(args: Union[str, List[str], None]) -> List[str]:
    """
    Accepts 'A,B', ['A','B'], None. Returns cleaned list.
    If 'all' is present anywhere (case-insensitive), returns [] => no filter.
    """
    if args is None:
        return []
    if isinstance(args, str):
        tokens = [t.strip() for t in args.split(",") if t.strip()]
    else:
        tokens = [str(t).strip() for t in args if str(t).strip()]

    # If any token is 'all', treat as no filter
    if any(t.lower() == "all" for t in tokens):
        return []

    # de-dupe while preserving order
    seen = set()
    out: List[str] = []
    for t in tokens:
        tl = t.lower()
        if tl not in seen:
            seen.add(tl)
            out.append(t)
    return out


# -------- WHERE builder for pyodbc '?' style --------
class WhereBuilder:
    def __init__(self) -> None:
        self.parts: List[str] = []
        self.params: List[Any] = []

    def add(self, clause: str, *params: Any) -> "WhereBuilder":
        self.parts.append(clause)
        if params:
            self.params.extend(params)
        return self

    def add_in(self, column: str, values: Sequence[Any]) -> "WhereBuilder":
        vals = list(values)
        if not vals:
            return self
        if len(vals) == 1:
            return self.add(f"{column} = ?", vals[0])
        q = ", ".join("?" for _ in vals)
        return self.add(f"{column} IN ({q})", *vals)

    def sql(self) -> str:
        return " AND ".join(self.parts) if self.parts else "1=1"

    def parameters(self) -> Tuple[Any, ...]:
        return tuple(self.params)

# -------- pandas runner (offloads to a worker thread) --------
async def read_df(engine, sql: str, params: Sequence[Any] = ()) -> pd.DataFrame:
    return await anyio.to_thread.run_sync(
        lambda: pd.read_sql_query(sql=sql, con=engine, params=tuple(params))
    )

def first_cell_int(df: pd.DataFrame, default: int = 0) -> int:
    if df.empty:
        return default
    v = df.iat[0, 0]
    return int(v.item() if hasattr(v, "item") else v)


# _________ Filters _____________________
def whereFilters(country_codes:list, wb:WhereBuilder, brands:list, pets:list) -> WhereBuilder:
    pet_patterns = {
        "cat":   "%cat%",
        "dog":   "%dog%",
        "horse": "%horse%",
        "exotic":"%exotic%",
        "bbc":    "%bb_com%",
        "bbcom": "%bb_com%",
    }

    # Normalize case for exact-match filters to avoid collation/case issues
    if country_codes:
        codes_upper = [str(c).strip().upper() for c in country_codes if str(c).strip()]
        if codes_upper:
            wb.add_in("UPPER(CountryCode)", codes_upper)
    if brands:
        brands_upper = [str(b).strip().upper() for b in brands if str(b).strip()]
        if brands_upper:
            wb.add_in("UPPER(Brand)", brands_upper)
    
    pet_tokens = [p.lower() for p in pets]

    if pet_tokens:
        likes, params = [], []
        for p in pet_tokens:
            patt = pet_patterns.get(p)
            if patt:
                likes.append("LOWER(COALESCE(PetType, '')) LIKE ?")
                params.append(patt)
        if likes:  # only add if we recognized at least one category
            wb.add("(" + " OR ".join(likes) + ")", *params)
    return wb

# ---------- Generate CSV Stream -----------------
def generate_csv_stream(engine, sql: str, params: tuple, filename: str) -> StreamingResponse:
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
