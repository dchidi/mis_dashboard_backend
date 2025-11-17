from sqlalchemy import text


AU_NZ_SALES_Query = text("""
WITH policyData as (
SELECT 
	P.CreatedDate as CreatedDate,
	P.PolicyNumber,
    P.ActualStartDate,
    PO.ProductName,
	CASE 
		WHEN PO.ProductCode LIKE '%CAT%' THEN 'Cat'
        WHEN PO.ProductCode LIKE '%DOG%' THEN 'Dog'
        WHEN PO.ProductCode LIKE '%EQUINE%' THEN 'Horse'
        WHEN PO.ProductCode LIKE '%HORSE%' THEN 'Horse'
        WHEN PO.ProductCode LIKE '%EXOTIC%' THEN 'Exotic'
        WHEN PO.ProductCode LIKE '%PROF%' THEN 'Professional'
        ELSE 'OTHER'
    END AS PetType,        
    C.FirstName AS ClientName,
    P.PetName,
    CASE 
		WHEN U.FirstName IN ('FIT', 'Web') THEN 'Web' 
        ELSE 'Phone' 
    END AS SaleMethod,
    Q.QuoteNumber,
    Q.CreatedDate AS QuoteCreatedDate

  FROM Policy P
	LEFT JOIN PolicyCancellation PC
		ON PC.PolicyId = P.Id
	LEFT JOIN Client C 
		ON P.ClientId = C.Id
	LEFT JOIN PolicyActivity PA 
		ON PA.PolicyId = P.Id and PA.TransactionTypeId = 1
	INNER JOIN [dbo].[Product] PO 
        ON PO.Id = PA.ProductId
	LEFT JOIN Quote Q 
        ON Q.Id = PA.QuoteId
    LEFT JOIN [dbo].[User] U 
        ON P.ExecutiveId = U.Id

WHERE
	CAST(P.CreatedDate as date) >= ? 
	AND CAST(P.CreatedDate as date) < DATEADD(DAY, 1, ?)
	AND ISNULL(P.IsFreeProduct,0) = 0
	AND P.InsuredName NOT LIKE '%test%'
	AND P.PetName NOT LIKE '%test%'
	AND C.FirstName NOT LIKE '%test%'
	AND C.LastName NOT LIKE '%test%'
	AND C.Email NOT LIKE '%petcovergroup%'
	AND C.Email NOT LIKE '%prowerse%'
	AND P.ExecutiveId IS NOT NULL
	AND ISNULL(P.IsMigrated,0) = 0  -- Remove Migrated Policy
	AND (PC.Id IS NULL OR PC.CreatedDate >= DateAdd(Day, 1, P.CreatedDate))
)

SELECT 
	'Petcover' Brand,    
	PolicyNumber,
	CreatedDate,
    ActualStartDate,
    ProductName,
    PetType,
    ClientName,
    PetName,
    SaleMethod,
    QuoteNumber,
    QuoteCreatedDate
                         
FROM policyData pd
""")  # noqa