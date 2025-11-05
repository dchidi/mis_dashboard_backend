from sqlalchemy import text


UK_DE_AT_FREE_POLICY_Query = text("""
With fp as(
SELECT
        ISNULL(P.IsPetIdProduct, 0) AS PetId,
        Q.QuoteNumber,
        --P.Id AS FreePolicyId,
        P.PolicyNumber,
        P.CreatedDate,
        --P.IsFreeProduct,
        --SA.Id                AS SubAgentId,
        SA.Name              AS SubAgentName,
        SA.AgentCategoryId,
        CASE                
            WHEN Q.ProductId = 2053 THEN 'BB_Commercial' 
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
    LEFT  JOIN [dbo].[Product] PO        ON PO.Id = PA.ProductId
    LEFT  JOIN Quote Q                   ON Q.Id = PA.QuoteId
    LEFT  JOIN SubAgent SA               ON SA.Id = Q.SubAgentId
    LEFT  JOIN [Master].[State] ST       ON ST.Id = SA.StateId	
    LEFT JOIN [dbo].[User] U 
        ON P.ExecutiveId = U.Id
    WHERE
        P.IsFreeProduct = 1
        AND PA.TransactionTypeId = 1                 -- new/issue (adjust if needed)
        AND P.PolicyNumber NOT LIKE '%TEST%'
        AND ISNULL(SA.Email,'') NOT LIKE '%TEST%'
        AND ISNULL(SA.Email,'') NOT LIKE '%PROW%'    -- keep your prior exclusions as needed
        AND CAST(P.CreatedDate as date) >= ? 
	    AND CAST(P.CreatedDate as date) < DATEADD(DAY, 1, ?)
)
SELECT QuoteNumber, PolicyNumber, CreatedDate, SubAgentName, AgentCategoryId, PetType, ProductName, StateName, SaleMethod, PolicyStatusName,                    
CASE 
    WHEN fp.PetId = 1 THEN 'PetId'
    WHEN fp.PetType LIKE '%BB_COM%' THEN 'BB'
    ELSE 'BPIS' 
END AS Brand 
From fp 

""")  # noqa