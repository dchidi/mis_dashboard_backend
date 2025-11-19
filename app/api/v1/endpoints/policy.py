from fastapi import APIRouter, Depends, Query
from datetime import date
from sqlalchemy.engine import Engine
from app.db.sqlserver import get_mis_db_engine
from app.services.policy import Policy
from app.services.policy_stream import PolicyStream
from enum import Enum

from dateutil.relativedelta import relativedelta
from app.core.dependencies import require_authentication


router = APIRouter(dependencies=[Depends(require_authentication)])


class PolicyStatus(str, Enum):
    CANCELLED = 'Cancel'
    EXPIRED = 'Expired'
    ACTIVE = 'Active'
    ALL = 'All'
    PENDING = 'Pending'
    REFER = 'Refer'
    RENEWAL_POLICY = 'Renewal Policy'
    SUSPENDED = 'Suspended'


class FreePolicy(str, Enum):
    YES = 'Yes'
    NO = 'No'
    ALL = 'All'


@router.get("/policy_summary")
async def PolicySummary(
    mis_db: Engine = Depends(get_mis_db_engine),
    start_date: date = Query(default_factory=lambda: date.today().replace(day=1) - relativedelta(months=12)),
    end_date: date = Query(default_factory=date.today),
    regions: str = Query(default="all"),
    policy_status: PolicyStatus = Query(default=PolicyStatus.ALL),
    free_policy: FreePolicy = Query(default=FreePolicy.ALL),
    historical_months: int = 7,
    brands: str = Query(default="all"),
    pet_types: str = Query(default="all")
):
    result = await Policy.PolicyMonthlyStatusSummary(
        engine=mis_db,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        regions=regions,
        policy_status=policy_status.value,
        policy_type=free_policy.value,
        date_basis="QuoteCreatedDate",
        months=historical_months,
        brands=brands,
        pet_types=pet_types
    )
    return result

@router.get("/policy_data")
async def PolicyData(
    mis_db: Engine = Depends(get_mis_db_engine),
    start_date: date = Query(default_factory=lambda: date.today().replace(day=1) - relativedelta(months=12)),
    end_date: date = Query(default_factory=date.today),
    regions: str = Query(default="all"),
    policy_status: PolicyStatus = Query(default=PolicyStatus.ALL),
    free_policy: FreePolicy = Query(default=FreePolicy.ALL),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=10_000),
    download: bool = Query(False),
    filename: str = Query("Policy.csv"),
    historical_months: int = 7,    
    brands: str = Query(default="all"),
    pet_types: str = Query(default="all")
):
    if download:
        return await PolicyStream.stream_policy_status_raw_csv(
            engine=mis_db,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            regions=regions,
            filename=filename,
            policy_status=policy_status.value,
            policy_type=free_policy.value,
            date_basis="QuoteCreatedDate",
            order="DESC",
            months=historical_months,            
            brands=brands,
            pet_types=pet_types
        )

    # normal paginated JSON
    result = await Policy.PolicyStatusRaw(
        engine=mis_db,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        regions=regions,
        policy_status=policy_status.value,
        policy_type=free_policy.value,
        date_basis="QuoteCreatedDate",
        skip=skip,
        limit=limit,
        order="DESC",
        months=historical_months,        
        brands=brands,
        pet_types=pet_types

    )
    return result
