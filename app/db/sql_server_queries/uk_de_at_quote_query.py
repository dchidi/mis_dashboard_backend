from sqlalchemy import text


UK_DE_AT_QUOTE_Query = text("""
WITH quoteData AS (
    SELECT 
        Q.Id AS Id,
        ISNULL(Q.Title, C.Title) AS Title,
        ISNULL(Q.FirstName, C.FirstName) AS FirstName,
        ISNULL(Q.LastName, C.LastName) AS LastName,
        (ISNULL(Q.FirstName, C.FirstName) + ' ' + ISNULL(Q.LastName, C.LastName)) AS FullName,
        Q.Email AS Email,
        ISNULL(Q.Address1, C.Address1) AS Address1,
        ISNULL(Q.Address2, C.Address2) AS Address2,
        ISNULL(Q.Suburb, C.Suburb) AS Suburb,
        ISNULL(Q.PostCode, C.Postcode) AS PostCode,
        Q.PetName AS PetName,
        CASE WHEN Q.ProductId = 2053 THEN 'BB_Commercial' ELSE PTY.PetTypeName END AS PetType,
        Q.QuoteNumber AS QuoteNumber,
        Q.CreatedDate,
        Q.QuoteDate,
        Q.ExpireDate,
        Q.Petbirthdate,
        ISNULL(Q.IsPetIdProduct, 0) AS IsPetId,
        (SELECT STRING_AGG(QuestionDetailName, ', ') FROM QuestionDetail WHERE Id IN (SELECT * FROM StringSplit((SELECT QuestionAnswer FROM QuoteQuestionAnswerDetail QAD
        JOIN QuoteQuestionAnswer QA ON QA.Id = QAD.QuoteQuestionAnswerId
        WHERE QuoteId = Q.Id AND QuestionId = 33024 AND Q.ProductId = 2053), ','))) AS BB_Activity,
        PP.ActualStartDate PolicyStartDate,
        PP.ActualEndDate PolicyEndDate,                            
        PP.CreatedDate AS PolicyCreatedDate,
        MB.BreedName,
        COALESCE(Q.Mobile, Q.PrimaryContactNumber, C.PrimaryContactNumber, C.AlternativeContactNumber) AS ContactNo,
        CASE WHEN Q.QuoteSaveFrom = 1 THEN 'NB' WHEN Q.QuoteSaveFrom = 2 THEN 'NB' WHEN Q.QuoteSaveFrom = 0 THEN 'Endorsement, Amendment' ELSE 'Renew' END AS QuoteTransactionType,
        CASE WHEN Q.QuoteSaveFrom = 2 THEN 'Web' ELSE 'Phone' END AS QuoteReceivedMethod,
        PA.PolicyNumber,
        ROW_NUMBER() OVER (PARTITION BY CAST(Q.CreatedDate AS DATE), Q.Email,
            CASE WHEN Q.ProductId = 2053 THEN (SELECT STRING_AGG(QuestionDetailName, ', ') FROM QuestionDetail WHERE Id IN (SELECT * FROM StringSplit((SELECT QuestionAnswer FROM QuoteQuestionAnswerDetail QAD
            JOIN QuoteQuestionAnswer QA ON QA.Id = QAD.QuoteQuestionAnswerId
            WHERE QuoteId = Q.Id AND QuestionId = 33024 AND Q.ProductId = 2053), ',')))
            ELSE PTY.PetTypeName END, Q.PetName
            ORDER BY Q.Id DESC) AS rowno
    FROM Quote Q WITH(NOLOCK)
    LEFT JOIN Client C WITH(NOLOCK) ON Q.ClientId = C.Id
    LEFT JOIN HearAboutUs H ON Q.HearAboutUs = H.Id
    LEFT JOIN VuePetType PTY WITH(NOLOCK) ON (CASE WHEN Q.ProductId IN (2049,2050,2051,2052) THEN 3
                                                   WHEN Q.ProductId IN (2,3,4) THEN 4
                                                   WHEN Q.ProductId IN (19,20,21,22,23,25,26,28,29) THEN 6
                                                   WHEN Q.ProductId IN (6,7,8,9,10,11,12,13,14) THEN 7 END) = PTY.PetType_ID
    LEFT JOIN PolicyActivity PA ON PA.QuoteId = Q.Id
    LEFT JOIN Policy PP ON PP.Id = PA.PolicyId
    LEFT JOIN [Master].[Breed] MB ON MB.Id = COALESCE(NULLIF(Q.PetBreedId, 0), NULLIF(Q.PetSeconderyBreedId, 0), NULLIF(Q.PetBreedId3, 0))
    WHERE 
        CAST(Q.CreatedDate as date) >= ? 
        AND CAST(Q.CreatedDate as date) < DATEADD(DAY, 1, ?)
        AND Q.CreatedBy IS NOT NULL 
        AND Q.QuoteSaveFrom IS NOT NULL
        AND Q.QuoteParentId IS NULL
        AND Q.FirstName NOT LIKE '%test%'
        AND Q.LastName NOT LIKE '%test%'
        AND Q.Email NOT LIKE '%petcovergroup%'
        AND Q.Email NOT LIKE '%prowerse%'
        AND Q.PetName NOT LIKE '%test%'
        AND Q.ProductId IN (2049,2050,2051,2052,2,3,4,19,20,21,22,23,25,26,28,29,6,7,8,9,10,11,12,13,14,2053)
        AND ((Q.ProductId != 2053) OR (Q.ProductId = 2053 AND H.Name NOT IN ('Omnis 7 Commercial / Phoenix Migration Renewal', 'Omnis 7 Commercial / Phoenix Migration Mid-Term')))
    
    UNION
    
    SELECT 
        Q.Id AS Id,
        ISNULL(Q.Title, C.Title) AS Title,
        ISNULL(Q.FirstName, C.FirstName) AS FirstName,
        ISNULL(Q.LastName, C.LastName) AS LastName,
        (ISNULL(Q.FirstName, C.FirstName) + ' ' + ISNULL(Q.LastName, C.LastName)) AS FullName,
        Q.Email AS Email,
        ISNULL(Q.Address1, C.Address1) AS Address1,
        ISNULL(Q.Address2, C.Address2) AS Address2,
        ISNULL(Q.Suburb, C.Suburb) AS Suburb,
        ISNULL(Q.PostCode, C.Postcode) AS PostCode,
        Q.PetName AS PetName,
        PTY.PetTypeName AS PetType,
        Q.QuoteNumber AS QuoteNumber,
        Q.CreatedDate,
        Q.QuoteDate,
        Q.ExpireDate,
        Q.Petbirthdate,
        ISNULL(Q.IsPetIdProduct, 0) AS IsPetId,
        '' AS BB_Activity,
        PP.ActualStartDate PolicyStartDate,
        PP.ActualEndDate PolicyEndDate,                            
        PP.CreatedDate AS PolicyCreatedDate,
        MB.BreedName,
        COALESCE(Q.Mobile, Q.PrimaryContactNumber, C.PrimaryContactNumber, C.AlternativeContactNumber) AS ContactNo,
        CASE WHEN Q.QuoteSaveFrom = 1 THEN 'NB' WHEN Q.QuoteSaveFrom = 2 THEN 'NB' WHEN Q.QuoteSaveFrom = 0 THEN 'Endorsement, Amendment' ELSE 'Renew' END AS QuoteTransactionType,
        CASE WHEN Q.QuoteSaveFrom = 2 THEN 'Web' ELSE 'Phone' END AS QuoteReceivedMethod,
        PA.PolicyNumber,
        ROW_NUMBER() OVER (PARTITION BY CAST(Q.CreatedDate AS DATE), Q.Email, CASE WHEN Q.ProductId = 2053 THEN '' ELSE PTY.PetTypeName END, Q.PetName ORDER BY Q.Id DESC) AS rowno
    FROM Quote Q WITH(NOLOCK)
    LEFT JOIN Client C WITH(NOLOCK) ON Q.ClientId = C.Id
    LEFT JOIN VuePetType PTY WITH(NOLOCK) ON (CASE WHEN Q.ProductId IN (2031,2032,2033,2034,2035,2036,2037,2038,2039,2040,2041,2042,2043,2044,2045,2046,2047) THEN 3 END) = PTY.PetType_ID
    LEFT JOIN PolicyActivity PA ON PA.QuoteId = Q.Id
    LEFT JOIN Policy PP ON PP.Id = PA.PolicyId
    LEFT JOIN [Master].[Breed] MB ON MB.Id = COALESCE(NULLIF(Q.PetBreedId, 0), NULLIF(Q.PetSeconderyBreedId, 0), NULLIF(Q.PetBreedId3, 0))
    WHERE
        CAST(Q.CreatedDate as date) >= ? 
	    AND CAST(Q.CreatedDate as date) < DATEADD(DAY, 1, ?)
        AND Q.QuoteParentId IS NULL
        AND Q.FirstName NOT LIKE '%test%'
        AND Q.LastName NOT LIKE '%test%'
        AND Q.Email NOT LIKE '%petcovergroup%'
        AND Q.Email NOT LIKE '%prowerse%'
        AND Q.PetName NOT LIKE '%test%'
        AND Q.ProductId IN (2031,2032,2033,2034,2035,2036,2037,2038,2039,2040,2041,2042,2043,2044,2045,2046,2047)
        AND Q.ExecutiveId IS NOT NULL
)
SELECT 
    CASE 
        WHEN qd.PetType = 'BB_Commercial' THEN 'BB'
        WHEN qd.IsPetId = 0 THEN 'BPIS'
        WHEN qd.IsPetId = 1 THEN 'PetId'
        ELSE 'Unknown'
    END AS Brand,
    PolicyCreatedDate,
    QuoteNumber, CreatedDate, QuoteDate AS QuoteStartDate, ExpireDate AS QuoteExpiryDate, QuoteReceivedMethod, PolicyNumber, PolicyStartDate, PolicyEndDate,
    FullName, Email, CAST(CONCAT(Address1, ', ', Address2) AS NVARCHAR(MAX)) AS Address, Suburb, PostCode, ContactNo,
    PetName, PetType, PetBirthDate, BreedName
FROM quoteData qd 
WHERE rowno = 1

""")  # noqa