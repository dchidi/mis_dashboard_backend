from sqlalchemy import text


CRM_Mkt_Query = text("""
--------------------------------------------------------------------------------
-- 0) Input Parameters (passed from Python, not declared here)
-- :start_date — inclusive lower bound
-- :end_date   — exclusive upper bound
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- 1) Pull & rank all quotes within date range by (First, Last, PetType, PetName)
--------------------------------------------------------------------------------
WITH QuoteData AS (
  SELECT
    q.Id AS QuoteId,
    q.QuoteNumber,
    q.CreatedDate AS QuoteCreatedDate,
    q.QuoteDate AS QuoteStartDate,
    q.ExpireDate AS QuoteEndDate,
    q.FirstName,
    q.LastName,
    q.Email,
    COALESCE(q.Mobile, q.PrimaryContactNumber, c.PrimaryContactNumber, c.AlternativeContactNumber) AS ContactNo,
    CASE
      WHEN c.SendDocumentOnEmail = 1 THEN 'Yes'
      ELSE 'No'
    END AS EmailConcent,  -- ensure customer consent
    q.PetBirthDate,
    q.PetTypeId,
    q.PetName AS PetName,
    pd.ProductCode,
    CASE 
      WHEN pd.ProductCode LIKE '%dog%'    THEN 'Dog'
      WHEN pd.ProductCode LIKE '%cat%'    THEN 'Cat'
      WHEN pd.ProductCode LIKE '%exotic%' THEN 'Exotic'
      WHEN pd.ProductCode LIKE '%horse%'  THEN 'Horse'
      ELSE pd.ProductCode
    END AS PetType,
    pd.IsFreeProduct AS IsFreeProduct,
    CASE 
      WHEN Q.QuoteSaveFrom = 2 THEN 'Web'
      ELSE 'Phone'  -- Default to Phone if not Web
    END AS QuoteReceivedMethod,
    CASE 
      WHEN COALESCE(q.ExpireDate, q.QuoteDate) IS NULL THEN NULL
      WHEN CAST(GETDATE() AS DATE) > q.ExpireDate THEN 'Lapsed'
      ELSE 'Live'
    END AS QuoteStatus,
    ROW_NUMBER() OVER (
      PARTITION BY q.FirstName, q.LastName, q.PetName
      ORDER BY
        -- 1) Converted quotes first (i.e., any policy ever)
        CASE WHEN EXISTS (SELECT 1 FROM PolicyActivity pa WHERE pa.QuoteId = q.Id) THEN 0 ELSE 1 END,
        -- 2) Newest quote
        q.QuoteDate DESC,
        -- 3) Quotes with breed info preferred
        CASE WHEN COALESCE(NULLIF(q.PetBreedId,0), NULLIF(q.PetSeconderyBreedId,0), NULLIF(q.PetBreedId3,0)) IS NOT NULL THEN 0 ELSE 1 END,
        -- 4) Tie-breaker
        q.Id
    ) AS rn
  FROM Quote q
  LEFT JOIN Product pd ON pd.Id = q.ProductId
  LEFT JOIN Client c ON c.Id = q.ClientId
  WHERE
    q.QuoteDate >= ?
    AND q.QuoteDate <  ?
    AND q.FirstName NOT LIKE '%Test%'
    AND q.LastName  NOT LIKE '%Test%'
    AND q.Email     NOT LIKE '%Test%'
    AND q.PetName   NOT LIKE '%Test%'
    AND q.Email     NOT LIKE '%prowerse%'
),

--------------------------------------------------------------------------------
-- 2) Pick the top-ranked quote per pet group
--------------------------------------------------------------------------------
PickedQuote AS (
  SELECT * FROM QuoteData WHERE rn = 1
),

--------------------------------------------------------------------------------
-- 3) Get the latest policy per picked quote (if any exists)
--------------------------------------------------------------------------------
LatestPolicy AS (
  SELECT
    pa.QuoteId,
    pa.PolicyNumber,
    p.ActualStartDate AS OriginalPolicyStartDate,
    p.ActualEndDate   AS PolicyEndDate,
    p.PolicyStatusId,
    ROW_NUMBER() OVER (
      PARTITION BY pa.QuoteId
      ORDER BY pa.CreatedDate DESC
    ) AS rn
  FROM PolicyActivity pa
  JOIN Policy p ON p.PolicyNumber = pa.PolicyNumber
  WHERE pa.QuoteId IN (SELECT QuoteId FROM PickedQuote)
),

PolicyInfo AS (
  SELECT
    lp.QuoteId,
    lp.PolicyNumber,
    lp.OriginalPolicyStartDate,
    lp.PolicyEndDate,
    mps.PolicyStatusName
  FROM LatestPolicy lp
  LEFT JOIN Master.PolicyStatus mps ON mps.Id = lp.PolicyStatusId
  WHERE lp.rn = 1
),

--------------------------------------------------------------------------------
-- 4) Assemble final dataset- attach policy info and resolve pet breed
--------------------------------------------------------------------------------
FinalResult AS (
  SELECT
    'Petcover' AS Brand,
    '' AS Country,
    '' AS BusinessName,
    '' AS BusinessType,
    CASE 
      WHEN pi.PolicyNumber IS NOT NULL THEN
        CASE 
          WHEN GETDATE() < pi.PolicyEndDate THEN
            CASE 
              WHEN pi.PolicyStatusName IN ('Active', 'Converted') THEN 'Active'
              ELSE pi.PolicyStatusName 
            END
          ELSE 'Expired'
        END
      ELSE 'Quote'
    END AS CustomerStatus,
    CASE WHEN pq.IsFreeProduct = 1 THEN 'Yes' ELSE 'No' END AS FreePolicy,
    pq.QuoteStatus,
    pq.QuoteNumber,
    pi.PolicyNumber,
    pq.QuoteCreatedDate,
    pq.QuoteStartDate,
    pq.QuoteEndDate,
    pi.OriginalPolicyStartDate,
    pi.PolicyEndDate,
    pq.QuoteReceivedMethod,
    pq.FirstName,
    pq.LastName,
    pq.Email,
    pq.ContactNo,
    pq.EmailConcent,
    pq.PetName,
    pq.PetType,
    pq.PetBirthDate,
    COALESCE(
      NULLIF(rq.PetBreedId, 0),
      NULLIF(rq.PetSeconderyBreedId, 0),
      NULLIF(rq.PetBreedId3, 0)
    ) AS PetBreedId,
    mb.BreedName
  FROM PickedQuote pq
  LEFT JOIN PolicyInfo pi ON pi.QuoteId = pq.QuoteId
  LEFT JOIN Quote rq ON rq.Id = pq.QuoteId
  LEFT JOIN Master.Breed mb ON mb.Id = COALESCE(
    NULLIF(rq.PetBreedId, 0),
    NULLIF(rq.PetSeconderyBreedId, 0),
    NULLIF(rq.PetBreedId3, 0)
  )
),

--------------------------------------------------------------------------------
-- 5) Deduplicate by customer-policy if exists, else pet-quote
--------------------------------------------------------------------------------
Deduped AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY 
        CASE 
          WHEN PolicyNumber IS NOT NULL THEN PolicyNumber
          ELSE CONCAT(FirstName, '|', LastName, '|', PetName)
        END
      ORDER BY
        CASE WHEN PolicyNumber IS NOT NULL THEN OriginalPolicyStartDate END DESC,
        QuoteCreatedDate DESC
    ) AS final_rn
  FROM FinalResult
)

--------------------------------------------------------------------------------
-- 6) Return only the top result per group
--------------------------------------------------------------------------------
SELECT
  Brand, Country, BusinessName, BusinessType,
  CustomerStatus, FreePolicy, QuoteStatus,
  QuoteNumber, PolicyNumber, QuoteReceivedMethod,
  QuoteCreatedDate, QuoteStartDate, QuoteEndDate,
  OriginalPolicyStartDate, PolicyEndDate,
  FirstName, LastName, Email, ContactNo, EmailConcent,
  PetName, PetType, PetBirthDate,
  PetBreedId, BreedName
FROM Deduped
WHERE final_rn = 1
ORDER BY QuoteCreatedDate DESC, QuoteNumber;
""")  # noqa