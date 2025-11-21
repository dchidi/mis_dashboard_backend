from sqlalchemy import text


AU_NZ_FREE_POLICY_Query = text("""
    SELECT
        'Petcover' Brand,
		Q.QuoteNumber,
        --P.Id AS FreePolicyId,
        P.PolicyNumber,
        P.CreatedDate,
        --P.IsFreeProduct,
        --SA.Id                AS SubAgentId,
        SA.Name              AS SubAgentName,
        SA.AgentCategoryId,
        CASE 
                            WHEN LOWER(COALESCE(PO.ProductCode, '')) LIKE '%cat%'    THEN 'Cat'
                            WHEN LOWER(COALESCE(PO.ProductCode, '')) LIKE '%dog%'    THEN 'Dog'
                            WHEN LOWER(COALESCE(PO.ProductCode, '')) LIKE '%horse%'  THEN 'Horse'
                            WHEN LOWER(COALESCE(PO.ProductCode, '')) LIKE '%exotic%' THEN 'Exotic'
                            WHEN LOWER(COALESCE(PO.ProductCode, '')) LIKE '%bb_com%'  THEN 'BB'
                            ELSE 'Others'
                        END AS PetType,
        PO.ProductName,
        ST.StateName,
        --P.PolicyStatusId,
		CASE 
			WHEN U.FirstName IN ('FIT', 'Web') THEN 'Web' 
			ELSE 'Phone' 
		END AS SaleMethod,
        PS.PolicyStatusName
    FROM Policy P
    INNER JOIN PolicyActivity PA ON PA.PolicyId = P.Id
    LEFT  JOIN [Master].[PolicyStatus] PS ON PS.Id = P.PolicyStatusId
    INNER  JOIN [dbo].[Product] PO        ON PO.Id = PA.ProductId
    LEFT  JOIN Quote Q                   ON Q.Id = PA.QuoteId
    INNER  JOIN SubAgent SA               ON SA.Id = Q.SubAgentId
    LEFT  JOIN [Master].[State] ST       ON ST.Id = SA.StateId	
    LEFT JOIN [dbo].[User] U 
        ON P.ExecutiveId = U.Id
    WHERE
        P.IsFreeProduct = 1
        AND PA.TransactionTypeId = 1                 
        AND P.PolicyNumber not like '%TEST%'
        AND SA.Email NOT LIKE '%TEST%' AND SA.Email NOT LIKE '%PROWSE%' 
        AND SA.Email NOT LIKE '%prowerse%' AND SA.Email NOT LIKE '%prower%'
        AND COALESCE(P.PetName, PA.PetName)  NOT LIKE '%TEST%'   
        AND CAST(P.CreatedDate as date) >= ? 
	    AND CAST(P.CreatedDate as date) < DATEADD(DAY, 1, ?)
""")  # noqa