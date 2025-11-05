DECLARE @StartDate date = '2025-10-01';
DECLARE @EndDate   date = '2025-10-05';

;WITH
-- Build BB_Activity once per QuoteId from QAD.QuestionAnswer (your column)
bb_activity AS (
    SELECT
        QA.QuoteId,
        STRING_AGG(QD.QuestionDetailName, ', ') AS BB_Activity
    FROM QuoteQuestionAnswer QA WITH (NOLOCK)
    JOIN QuoteQuestionAnswerDetail QAD WITH (NOLOCK)
      ON QAD.QuoteQuestionAnswerId = QA.Id
    CROSS APPLY STRING_SPLIT(CAST(QAD.QuestionAnswer AS nvarchar(max)), ',') s
    JOIN QuestionDetail QD WITH (NOLOCK)
      ON QD.Id = TRY_CAST(s.value AS int)
    WHERE QA.QuestionId = 33024
    GROUP BY QA.QuoteId
),
quoteData AS (
    /* ---- Branch 1: core products incl. BB (2053) ---- */
    SELECT 
        Q.Id,
        ISNULL(Q.Title, C.Title) AS Title,
        ISNULL(Q.FirstName, C.FirstName) AS FirstName,
        ISNULL(Q.LastName, C.LastName) AS LastName,
        (ISNULL(Q.FirstName, C.FirstName) + ' ' + ISNULL(Q.LastName, C.LastName)) AS FullName,
        Q.Email,
        Q.ContactNo,
        ISNULL(Q.Address1, C.Address1) AS Address1,
        ISNULL(Q.Address2, C.Address2) AS Address2,
        ISNULL(Q.Suburb, C.Suburb) AS Suburb,
        ISNULL(Q.PostCode, C.Postcode) AS PostCode,
        Q.PetName,
        CASE WHEN Q.ProductId = 2053 THEN 'BB_Commercial' ELSE PTY.PetTypeName END AS PetType,
        Q.QuoteNumber,
        Q.CreatedDate,
        Q.QuoteDate,
        Q.ExpireDate,
        Q.Petbirthdate,
        ISNULL(Q.IsPetIdProduct, 0) AS IsPetId,
        BA.BB_Activity,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(Q.CreatedDate AS date),
                         Q.Email,
                         CASE WHEN Q.ProductId = 2053 THEN COALESCE(BA.BB_Activity, 'BB_Commercial') ELSE PTY.PetTypeName END,
                         Q.PetName
            ORDER BY Q.Id DESC
        ) AS rowno
    FROM Quote Q WITH (NOLOCK)
    LEFT JOIN Client C WITH (NOLOCK) ON Q.ClientId = C.Id
    LEFT JOIN HearAboutUs H WITH (NOLOCK) ON Q.HearAboutUs = H.Id
    LEFT JOIN VuePetType PTY WITH (NOLOCK)
      ON (CASE 
            WHEN Q.ProductId IN (2049,2050,2051,2052) THEN 3 -- Horse
            WHEN Q.ProductId IN (2,3,4) THEN 4               -- Exotic
            WHEN Q.ProductId IN (19,20,21,22,23,25,26,28,29) THEN 6 -- Dog
            WHEN Q.ProductId IN (6,7,8,9,10,11,12,13,14) THEN 7      -- Cat
          END) = PTY.PetType_ID
    LEFT JOIN bb_activity BA ON BA.QuoteId = Q.Id
    WHERE 
        Q.CreatedDate >= @StartDate
        AND Q.CreatedDate < DATEADD(DAY, 1, @EndDate)         -- SARGable bound
        AND Q.CreatedBy IS NOT NULL 
        AND Q.QuoteSaveFrom IS NOT NULL
        AND Q.QuoteParentId IS NULL
        AND Q.FirstName NOT LIKE '%test%'
        AND Q.LastName  NOT LIKE '%test%'
        AND Q.Email     NOT LIKE '%petcovergroup%'
        AND Q.Email     NOT LIKE '%prowerse%'
        AND Q.PetName   NOT LIKE '%test%'
        AND Q.ProductId IN (
            2049,2050,2051,2052,              -- Horse
            2,3,4,                            -- Exotic
            19,20,21,22,23,25,26,28,29,       -- Dog
            6,7,8,9,10,11,12,13,14,           -- Cat
            2053                               -- BB
        )
        AND (
            Q.ProductId <> 2053 OR ISNULL(H.Name,'') NOT IN (
                'Omnis 7 Commercial / Phoenix Migration Renewal',
                'Omnis 7 Commercial / Phoenix Migration Mid-Term'
            )
        )

    UNION ALL

    /* ---- Branch 2: New Horse products (ExecutiveId required) ---- */
    SELECT 
        Q.Id,
        ISNULL(Q.Title, C.Title) AS Title,
        ISNULL(Q.FirstName, C.FirstName) AS FirstName,
        ISNULL(Q.LastName, C.LastName) AS LastName,
        (ISNULL(Q.FirstName, C.FirstName) + ' ' + ISNULL(Q.LastName, C.LastName)) AS FullName,
        Q.Email,
        Q.ContactNo,
        ISNULL(Q.Address1, C.Address1) AS Address1,
        ISNULL(Q.Address2, C.Address2) AS Address2,
        ISNULL(Q.Suburb, C.Suburb) AS Suburb,
        ISNULL(Q.PostCode, C.Postcode) AS PostCode,
        Q.PetName,
        PTY.PetTypeName AS PetType,
        Q.QuoteNumber,
        Q.CreatedDate,
        Q.QuoteDate,
        Q.ExpireDate,
        Q.Petbirthdate,
        ISNULL(Q.IsPetIdProduct, 0) AS IsPetId,
        NULL AS BB_Activity,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(Q.CreatedDate AS date),
                         Q.Email,
                         PTY.PetTypeName,
                         Q.PetName
            ORDER BY Q.Id DESC
        ) AS rowno
    FROM Quote Q WITH (NOLOCK)
    LEFT JOIN Client C WITH (NOLOCK) ON Q.ClientId = C.Id
    LEFT JOIN VuePetType PTY WITH (NOLOCK)
      ON (CASE WHEN Q.ProductId IN (
                2031,2032,2033,2034,2035,2036,
                2037,2038,2039,2040,2041,2042,
                2043,2044,2045,2046,2047
          ) THEN 3 END) = PTY.PetType_ID
    WHERE 
        Q.CreatedDate >= @StartDate
        AND Q.CreatedDate < DATEADD(DAY, 1, @EndDate)
        AND Q.QuoteParentId IS NULL
        AND Q.FirstName NOT LIKE '%test%'
        AND Q.LastName  NOT LIKE '%test%'
        AND Q.Email     NOT LIKE '%petcovergroup%'
        AND Q.Email     NOT LIKE '%prowerse%'
        AND Q.PetName   NOT LIKE '%test%'
        AND Q.ProductId IN (
            2031,2032,2033,2034,2035,2036,
            2037,2038,2039,2040,2041,2042,
            2043,2044,2045,2046,2047
        )
        AND Q.ExecutiveId IS NOT NULL
)
SELECT 
    *
FROM quoteData qd where rowno = 1

OPTION (RECOMPILE);
