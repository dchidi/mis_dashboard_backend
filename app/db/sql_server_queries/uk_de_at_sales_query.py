from sqlalchemy import text


UK_DE_AT_SALES_Query = text("""
WITH policyData as (
SELECT 
	CASE 
        WHEN Q.ProductId = 2053 THEN 'BB_Commercial' 
        ELSE PTY.PetTypeName
    END AS PetType,
	P.CreatedDate as CreatedDate,
	P.PolicyNumber,
    P.ActualStartDate,
    PO.ProductName,
	C.FirstName AS ClientName,
    P.PetName,
    CASE 
		WHEN U.FirstName IN ('FIT', 'Web') THEN 'Web' 
        ELSE 'Phone' 
    END AS SaleMethod,
    Q.QuoteNumber,
    Q.CreatedDate AS QuoteCreatedDate,
	ISNULL(P.IsPetIdProduct, 0) as IsPetId

FROM Policy P
LEFT JOIN PolicyCancellation PC 
    ON PC.PolicyId = P.Id
LEFT JOIN Client C 
    ON P.ClientId = C.Id
LEFT JOIN PolicyActivity PA 
    ON PA.PolicyId = P.Id and PA.TransactionTypeId = 1
LEFT JOIN [dbo].[Product] PO 
    ON PO.Id = PA.ProductId
LEFT JOIN Quote Q 
    ON Q.Id = PA.QuoteId
LEFT JOIN [dbo].[User] U 
    ON P.ExecutiveId = U.Id
LEFT JOIN HearAboutUs H 
    ON Q.HearAboutUs = H.Id
LEFT JOIN VuePetType PTY WITH(NOLOCK) 
    ON (
            CASE 
                WHEN PA.ProductId in (2049,2050,2051,2052,  -- Old Horse
										2031,2032,2033,2034,2035,2036,   -- New Horse
										2037,2038,2039,2040,2041,2042,
										2043,2044,2045,2046,2047) THEN 3 -- Horse
				WHEN PA.ProductId in (2,3,4) THEN 4 -- Exotic
				WHEN PA.ProductId in (19,20,21,22,23,25,26,28,29) THEN 6 -- Dog
				WHEN PA.ProductId in (6,7,8,9,10,11,12,13,14) THEN 7 -- Cat
			END
        ) = PTY.PetType_ID

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
    AND PA.ProductId IN (2049,2050,2051,2052,           -- Old Horse
						2031,2032,2033,2034,2035,2036, -- New Horse
						2037,2038,2039,2040,2041,2042,
						2043,2044,2045,2046,2047,
						2,3,4,						   -- Exotic
						19,20,21,22,23,25,26,28,29,    -- Dog
						6,7,8,9,10,11,12,13,14, 	   -- Cat
						2053)					 	   -- BB
   and P.ExecutiveId IS NOT NULL
   and (PC.Id IS NULL or PC.CreatedDate >= DateAdd(Day, 1, P.CreatedDate))
   and ((PA.ProductId != 2053) or (PA.ProductId = 2053 and H.Name NOT IN ('Omnis 7 Commercial / Phoenix Migration Renewal', 'Omnis 7 Commercial / Phoenix Migration Mid-Term')))
)

SELECT 
	CASE 
		WHEN pd.IspetId = 1 THEN 'PetId'
		WHEN PD.PetType LIKE '%BB_COM%' THEN 'BB'
		ELSE 'BPIS' 
	END AS Brand,    
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
FROM 
policyData pd 
""")  # noqa