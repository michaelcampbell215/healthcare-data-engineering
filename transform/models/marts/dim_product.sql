WITH source AS (
    SELECT * FROM {{ ref('stg_general_payments') }}
),
unpivoted_products AS (
    -- Get Product 1
    SELECT 
        product_name_1 AS product_name, 
        product_category_1 AS product_category, 
        product_type_1 AS product_type, 
        product_ndc_1 AS ndc 
    FROM source WHERE product_name_1 IS NOT NULL

    UNION ALL
    -- Get Product 2
    SELECT 
        product_name_2 AS product_name, 
        product_category_2 AS product_category, 
        product_type_2 AS product_type, 
        product_ndc_2 AS ndc 
    FROM source WHERE product_name_2 IS NOT NULL

    UNION ALL
    -- Get Product 3
    SELECT 
        product_name_3 AS product_name, 
        product_category_3 AS product_category, 
        product_type_3 AS product_type, 
        product_ndc_3 AS ndc 
    FROM source WHERE product_name_3 IS NOT NULL

    UNION ALL
    -- Get Product 4
    SELECT 
        product_name_4 AS product_name, 
        product_category_4 AS product_category, 
        product_type_4 AS product_type, 
        product_ndc_4 AS ndc 
    FROM source WHERE product_name_4 IS NOT NULL
    
    UNION ALL
    -- Get Product 5
    SELECT 
        product_name_5 AS product_name, 
        product_category_5 AS product_category, 
        product_type_5 AS product_type, 
        product_ndc_5 AS ndc 
    FROM source WHERE product_name_5 IS NOT NULL
)

SELECT DISTINCT * FROM unpivoted_products
