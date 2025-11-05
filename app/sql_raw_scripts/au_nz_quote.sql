
DECLARE @StartDate date = '2025-10-01';
DECLARE @EndDate   date = '2025-10-05';

--select top 10 * from quote;

WITH quoteData AS (
    SELECT
        Q.Id,
        COALESCE(Q.Title, C.Title)              AS Title,
        COALESCE(Q.FirstName, C.FirstName)      AS FirstName,
        COALESCE(Q.LastName,  C.LastName)       AS LastName,
        COALESCE(Q.FirstName, C.FirstName) + ' ' + COALESCE(Q.LastName, C.LastName) AS FullName,
        Q.Email,        
        COALESCE(Q.Address1, C.Address1)        AS Address1,
        COALESCE(Q.Address2, C.Address2)        AS Address2,
        COALESCE(Q.Suburb,  C.Suburb)           AS Suburb,
        COALESCE(Q.PostCode, C.Postcode)        AS PostCode,
        Q.PetName,
        PTY.PetTypeName                         AS PetType,
        Q.QuoteNumber,
        Q.CreatedDate,
        Q.QuoteDate,
        Q.ExpireDate,
        Q.PetBirthdate,
		
		PP.ActualStartDate PolicyStartDate,
		PP.ActualEndDate PolicyEndDate,
		MB.BreedName,
		COALESCE(Q.Mobile, Q.PrimaryContactNumber, C.PrimaryContactNumber, C.AlternativeContactNumber) AS ContactNo,
		CASE WHEN Q.QuoteSaveFrom = 1 THEN 'NB' WHEN Q.QuoteSaveFrom = 2 THEN 'NB' WHEN Q.QuoteSaveFrom = 0 THEN 'Endorsement, Amendment' ELSE 'Renew' END as QuoteTransactionType,
		CASE WHEN Q.QuoteSaveFrom = 2 THEN 'Web' ELSE 'Phone' END as QuoteReceivedMethod,
		PA.PolicyNumber,

        ROW_NUMBER() OVER (
            PARTITION BY
                CAST(Q.CreatedDate AS date),
                LOWER(LTRIM(RTRIM(Q.Email))),
                PTY.PetTypeName,
                LOWER(LTRIM(RTRIM(Q.PetName)))
            ORDER BY Q.Id DESC
        ) AS rowno
    FROM Quote Q WITH (NOLOCK)
    /* tie the Product to the Quote, and filter products here */
    JOIN Product P WITH (NOLOCK)
        ON P.Id = Q.ProductId
       AND P.ParentProductId IS NOT NULL
       AND P.ProductName NOT LIKE '%INTRODUCTORY%'
    LEFT JOIN VuePetType PTY WITH (NOLOCK)
        ON PTY.PetType_ID = P.PetTypeId
    LEFT JOIN Client C WITH (NOLOCK)
        ON C.Id = Q.ClientId


	LEFT JOIN PolicyActivity PA
		ON PA.QuoteId = Q.Id
	LEFT JOIN Policy PP 
		ON PP.Id = PA.PolicyId
	LEFT JOIN [Master].[Breed] MB
    ON MB.Id = COALESCE(
                 NULLIF(Q.PetBreedId,          0),
                 NULLIF(Q.PetSeconderyBreedId, 0),
                 NULLIF(Q.PetBreedId3,         0)
               )


    WHERE
        Q.CreatedDate >= @StartDate
        AND Q.CreatedDate < DATEADD(DAY, 1, @EndDate)
        AND Q.CreatedBy IS NOT NULL
        AND Q.QuoteSaveFrom IS NOT NULL
        AND Q.QuoteParentId IS NULL
        AND ISNULL(Q.IsPetIdProduct, 0) <> 1
        AND Q.MigratedQuoteId IS NULL
        AND ISNULL(Q.FirstName,'') NOT LIKE '%test%'
        AND ISNULL(Q.LastName,'')  NOT LIKE '%test%'
        AND ISNULL(Q.Email,'')     NOT LIKE '%petcovergroup%'
        AND ISNULL(Q.Email,'')     NOT LIKE '%prowerse%'
        AND ISNULL(Q.PetName,'')   NOT LIKE '%test%'
)
SELECT 
	'Petcover' Brand, '' CountryCode, '' CountryName,
	QuoteNumber, CreatedDate, QuoteDate, ExpireDate, QuoteReceivedMethod, PolicyNumber, PolicyStartDate, PolicyEndDate,
	FullName, Email, CONCAT(Address1, ', ', Address2) Address, Suburb, PostCode, ContactNo,
	PetName, PetType, PetBirthDate, BreedName
FROM quoteData
WHERE PetType IS NOT NULL
  AND rowno = 1
ORDER BY CAST(CreatedDate AS date);
