SELECT distinct
    s.recipient_city,
    s.recipient_state,
    CASE   
        WHEN s.recipient_city = 'San Juan' THEN 'PR'  
        WHEN s.recipient_city = 'CHICAGO' THEN 'IL'
        WHEN s.recipient_city = 'BLACKFOOT' THEN 'ID'
        WHEN s.recipient_city = 'MEMPHIS' THEN 'TN'
        WHEN s.recipient_city = 'DEDEDO' THEN 'GU'
        WHEN s.recipient_city = 'BROOKLYN' THEN 'NY'
        WHEN s.recipient_city IN ('ENCINITAS', 'SAN MATEO') THEN 'CA'
        WHEN s.recipient_city IN ('WEST PALM BEACH', 'JACKSONVILLE', 'FORT LAUDERDALE') THEN 'FL'
        WHEN s.recipient_city IN ('SUGARLAND', 'SPRING', 'TE', 'FORT WORTH', 'MCALLEN') THEN 'TX'
        WHEN s.recipient_province IN ('CA','DE','FL','GA','KY','LA','MA','MD','NY','OK','PA','PR','TN','TX','WA') THEN s.recipient_province
        ELSE NULL
    END AS clean_state,
    s.recipient_zip_code,
    CASE 
        WHEN s.recipient_province IN ('CA','DE','FL','GA','KY','LA','MA','MD','NY','OK','PA','PR', 'TE', 'TN','TX','WA') THEN s.recipient_postal_code
        WHEN s.recipient_province IN ('GUAM', 'PUERTO RICO', 'FORT BEND', 'TEXAS', 'FLORIDA', 'TENNESSEE', 'ILLINOIS', 'SAN MATEO', 'USA', 'FLORIDA FL', 'SAN DIEGO', 'HIDALGO COUNTY', 'IDAHO') THEN s.recipient_postal_code
        ELSE NULL 
    END AS clean_zip,
     s.recipient_province,
    CASE 
        WHEN s.recipient_province IN ('CA','DE','FL','GA','KY','LA','MA','MD','NY','OK','PA','PR', 'TE', 'TN','TX','WA') THEN NULL
        WHEN s.recipient_province IN ('GUAM', 'PUERTO RICO', 'FORT BEND', 'TEXAS', 'FLORIDA', 'TENNESSEE', 'ILLINOIS', 'SAN MATEO', 'USA', 'FLORIDA FL', 'SAN DIEGO', 'HIDALGO COUNTY', 'IDAHO') THEN NULL
        ELSE null
	END AS clean_province
FROM general_payments g
INNER JOIN stg_general_payments s ON g.payment_id = s.staging_id
WHERE s.recipient_country IN ('United States', 'United States Minor Outlying Islands') 
  AND s.recipient_province != '';




SELECT distinct
            recipient_city,
            CASE 	
                WHEN recipient_city = 'Camp Foster' AND recipient_country = 'Japan' AND recipient_zip_code = '96362' THEN 'Fpo'
                WHEN recipient_city = 'Yokota Air Force Base' AND recipient_country = 'Japan' AND recipient_zip_code = '96326' THEN 'Apo'
                WHEN recipient_city = 'Kadena Ab' AND recipient_country = 'Japan' AND recipient_zip_code = '96367' THEN 'Apo'
                WHEN recipient_city = 'Seoul' AND recipient_country = 'Korea (republic Of)' AND recipient_zip_code = '962055652' THEN 'Apo'
                ELSE NULL
            END as ccity,	

            recipient_state,
            CASE
                WHEN recipient_city = 'APO' AND recipient_country = 'United Arab Emirates' AND recipient_zip_code = '09603' THEN 'AP'  
                WHEN recipient_city = 'Apo' AND recipient_country = 'Germany' AND recipient_zip_code IN ('09094','09165','09096','09180','09244','09126') THEN 'AE'        
                WHEN recipient_province = 'AE' AND recipient_country = 'Italy' AND recipient_zip_code = '09636' THEN 'AE'   
                WHEN recipient_city in ('FPO', 'Apo') AND recipient_country = 'Japan' AND recipient_zip_code IN ('96328','96362','96306') THEN 'AP'    
                WHEN recipient_city = 'APO' AND recipient_country = 'Korea (democratic People\'s Republic Of)' AND recipient_zip_code = '962782060' THEN 'AP' 
                WHEN recipient_province = 'RHODE ISLAND' and recipient_country = 'Afghanistan' then 'RI'
                WHEN recipient_province = 'Washington' and recipient_country = 'Aland Islands' then 'WA'
                WHEN recipient_province = 'Washington' and recipient_country = 'Austria' then 'WA'
                WHEN recipient_province = 'TEXAS' and recipient_country = 'Cook Islands' then 'TX'
                WHEN recipient_city = 'Camp Foster' AND recipient_country = 'Japan' AND recipient_zip_code = '96362' THEN 'AP'
                WHEN recipient_city = 'Yokota Air Force Base' AND recipient_country = 'Japan' AND recipient_zip_code = '96326' THEN 'AP'
                WHEN recipient_city = 'Kadena Ab' AND recipient_country = 'Japan' AND recipient_zip_code = '96367' THEN 'AP'
                WHEN recipient_city = 'Seoul' AND recipient_country = 'Korea (republic Of)' AND recipient_zip_code = '962055652' THEN 'AP'
                ELSE NULL
            END as cstate,
			recipient_postal_code,
            CASE
                WHEN recipient_city IN ('APO', 'YOKOTA AIR FORCE BASE', 'FPO') THEN recipient_postal_code
                WHEN recipient_city = 'SEOUL' AND recipient_province = 'APO AP' THEN recipient_postal_code
                WHEN recipient_city = 'CAMP FOSTER' AND recipient_province = 'OK' THEN recipient_postal_code
                WHEN recipient_city = 'KADENA AB' AND recipient_province = 'OKINAWA' THEN recipient_postal_code
                WHEN recipient_city = 'PROVIDENCE' and recipient_province = 'RHODE ISLAND' THEN recipient_postal_code
                WHEN recipient_city = 'CONROE' and recipient_province = 'TEXAS' THEN recipient_postal_code
                WHEN recipient_city = 'PA' AND recipient_state = '15905-4305' AND recipient_zip_code = 'United States' THEN '15905'                
                WHEN recipient_city in ('VANCOUVER', 'Seattle') and recipient_province = 'Washington' THEN recipient_postal_code
                ELSE NULL
            END as czip,           
            recipient_country,
            CASE 
                WHEN recipient_province = 'RHODE ISLAND' AND recipient_country = 'Afghanistan' THEN 'United States'
                WHEN recipient_province = 'Washington' AND recipient_country = 'Aland Islands' THEN 'United States'
                WHEN recipient_province = 'Washington' AND recipient_country = 'Austria' THEN 'United States'
                WHEN recipient_province = 'TEXAS' AND recipient_country = 'Cook Islands' THEN 'United States'
                WHEN recipient_city IN ('Fpo', 'Apo', 'Kadena Ab', 'Yokota Air Force Base', 'Camp Foster', 'Kadena Ab', 'Seoul') 
                    AND recipient_country IN ('Germany', 'Korea (republic Of)','Korea (democratic People\'s Republic Of)', 'Italy', 'Japan','United Arab Emirates') 
                    THEN 'United States'
                ELSE NULL
            END as ccounty,
            recipient_province
FROM stg_general_payments 
where recipient_country NOT IN ('United States', 'United States Minor Outlying Islands');

