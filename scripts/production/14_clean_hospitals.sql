-- =========================================================================
-- Batch Clean Hospitals (Fixed: Removed Destructive Update)
-- =========================================================================

DELIMITER $$

DROP PROCEDURE IF EXISTS BatchCleanHospitals $$
CREATE PROCEDURE BatchCleanHospitals()
BEGIN
    DECLARE min_id BIGINT;
    DECLARE max_id BIGINT;
    DECLARE batch_size INT DEFAULT 10000; 
    DECLARE current_start BIGINT;
    DECLARE current_end BIGINT;
    DECLARE total_rows_updated INT DEFAULT 0;
    DECLARE rows_in_this_batch INT DEFAULT 0;
    DECLARE last_processed_id BIGINT;
    
    SELECT MIN(payment_id), MAX(payment_id) INTO min_id, max_id FROM general_payments;
    
    -- Resume Logic
    SELECT MAX(batch_end_id) INTO last_processed_id 
    FROM migration_log 
    WHERE process_name = 'BatchCleanHospitals' AND log_type = 'BATCH';

    IF last_processed_id IS NULL THEN
        SET current_start = min_id;
    ELSE
        SET current_start = last_processed_id + 1;
    END IF;
    
    SELECT CONCAT('Starting Batch: Hospitals from ID: ', current_start) as Status;
    
    WHILE current_start <= max_id DO
        SET current_end = current_start + batch_size;        
        START TRANSACTION;

        -- Standardize Hospital Names & Map to Systems
        UPDATE general_payments
        SET teaching_hospital_name = CASE 
            -- A
            WHEN teaching_hospital_name LIKE 'Adventhealth%' OR teaching_hospital_name LIKE 'Advent Health %' OR teaching_hospital_name LIKE 'Adventist Hinsdale%' OR teaching_hospital_name LIKE 'Adventist Lagrange%' OR teaching_hospital_name LIKE 'Adventist Bolingbrook%' OR teaching_hospital_name LIKE 'Adventist GlenOaks%' THEN 'AdventHealth'
            WHEN teaching_hospital_name LIKE 'Adventist Health %' OR teaching_hospital_name LIKE 'Adventist Health Glendale%' OR teaching_hospital_name LIKE 'Adventist Health Hanford%' OR teaching_hospital_name LIKE 'Adventist Health Ukiah%' OR teaching_hospital_name LIKE 'Adventist Health White%' THEN 'Adventist Health'
            WHEN teaching_hospital_name LIKE 'Advocate %' OR teaching_hospital_name LIKE 'Advocate Christ%' OR teaching_hospital_name LIKE 'Advocate Lutheran%' OR teaching_hospital_name LIKE 'Advocate Northside%' THEN 'Advocate Health Care'
            WHEN teaching_hospital_name LIKE 'Ahn %' OR teaching_hospital_name LIKE 'Allegheny Health%' OR teaching_hospital_name LIKE 'Ahn The Medical Center%' THEN 'Allegheny Health Network'
            WHEN teaching_hospital_name LIKE 'Saint Vincent Hospital%' AND recipient_city = 'Erie' THEN 'Allegheny Health Network'
            WHEN teaching_hospital_name LIKE 'Mercy Hospital%' AND recipient_state = 'MN' THEN 'Allina Health'
            WHEN teaching_hospital_name LIKE 'Whitesburg Arh%' THEN 'Appalachian Regional Healthcare'
            WHEN teaching_hospital_name LIKE 'Ascension %' OR teaching_hospital_name LIKE 'Saint Thomas %' OR teaching_hospital_name LIKE 'Saint Vincents Birmingham%' OR teaching_hospital_name LIKE 'Saint Vincents East%' THEN 'Ascension Health'
            WHEN teaching_hospital_name LIKE 'Saint Thomas%' AND recipient_state = 'TN' THEN 'Ascension Health'
            WHEN teaching_hospital_name LIKE 'Saint Vincents%' AND recipient_state = 'AL' THEN 'Ascension Health'
            WHEN (teaching_hospital_name LIKE 'Saint Alexius Medical Center%' AND recipient_city = 'Hoffman Estates') THEN 'Ascension Health'
            WHEN (teaching_hospital_name LIKE 'Saint Agnes Hospital%' AND recipient_state = 'MD') THEN 'Ascension Health'
            WHEN teaching_hospital_name LIKE 'Saint John Medical Center%' AND recipient_city = 'Tulsa' THEN 'Ascension Health'
            WHEN teaching_hospital_name LIKE 'Atrium Health%' THEN 'Atrium Health'
            WHEN teaching_hospital_name LIKE 'Aurora Health%' OR teaching_hospital_name LIKE 'Aurora Lakeland%' OR teaching_hospital_name LIKE 'Aurora Medical%' THEN 'Aurora Health Care'

            -- B
            WHEN teaching_hospital_name LIKE 'Banner %' THEN 'Banner Health'
            WHEN teaching_hospital_name LIKE 'Banner University Medical Center Phx' THEN 'Banner University Medical Center Phoenix'
            WHEN teaching_hospital_name LIKE 'Baptist Health %' OR (teaching_hospital_name LIKE 'Baptist %' AND teaching_hospital_name NOT LIKE 'Baptist Memorial %') THEN 'Baptist Health'
            WHEN teaching_hospital_name LIKE 'Doctors Hospital%' AND recipient_city = 'Coral Gables' AND recipient_state = 'FL' THEN 'Baptist Health South Florida'
            WHEN teaching_hospital_name LIKE 'Baylor%' THEN 'Baylor Scott & White Health'
            WHEN teaching_hospital_name LIKE 'Baystate Medical Center%' THEN 'Baystate Health'
            WHEN teaching_hospital_name LIKE 'Memorial Hospital Of South Bend%' THEN 'Beacon Health System'
            WHEN teaching_hospital_name LIKE 'Beaumont Health%' OR teaching_hospital_name LIKE 'Beaumont Hospital%' THEN 'Beaumont Health'
            WHEN teaching_hospital_name LIKE 'Barnes Jewish%' THEN 'BJC HealthCare'
            WHEN teaching_hospital_name LIKE 'Saint Louis Childrens%' THEN 'BJC HealthCare'
            WHEN (teaching_hospital_name LIKE 'Mercy Health%' OR teaching_hospital_name LIKE 'Mercy Saint Anne%' OR teaching_hospital_name LIKE 'Mercy Hospital Anderson%') AND recipient_state IN ('OH', 'KY') THEN 'Bon Secours Mercy Health'
            WHEN (teaching_hospital_name LIKE 'Saint Elizabeth%' OR teaching_hospital_name LIKE 'Saint Charles Hospital%' OR teaching_hospital_name LIKE 'Saint Lukes Hospital%' AND recipient_city = 'Maumee' OR teaching_hospital_name LIKE 'Saint Ritas%' OR teaching_hospital_name LIKE 'Saint Vincent Medical Center%') AND recipient_state = 'OH' THEN 'Bon Secours Mercy Health'
            WHEN (teaching_hospital_name LIKE 'Saint Marys Hospital%' OR teaching_hospital_name LIKE 'Saint Francis Medical Center%') AND recipient_state = 'VA' THEN 'Bon Secours Mercy Health'
            WHEN teaching_hospital_name LIKE 'Broward Health%' THEN 'Broward Health'

            -- C
            WHEN teaching_hospital_name LIKE 'Capital Health Medical Center%' THEN 'Capital Health'
            WHEN teaching_hospital_name LIKE 'Bayonne Medical Center%' THEN 'CarePoint Health'
            WHEN teaching_hospital_name LIKE 'Carilion%' THEN 'Carilion Clinic'
            WHEN teaching_hospital_name LIKE 'Good Samaritan Hospital%' AND recipient_city = 'West Islip' AND recipient_state = 'NY' THEN 'Catholic Health (Long Island)'
            WHEN (teaching_hospital_name LIKE 'Saint Charles Hospital%' OR teaching_hospital_name LIKE 'Saint Francis Hospital%' AND recipient_city = 'Roslyn' OR teaching_hospital_name LIKE 'Saint Joseph Hospital%' AND recipient_city = 'Bethpage') AND recipient_state = 'NY' THEN 'Catholic Health (Long Island)'
            WHEN (teaching_hospital_name LIKE 'Mercy Hospital Of Buffalo%' OR (teaching_hospital_name LIKE 'Mercy Medical Center%' AND recipient_city = 'Rockville Centre')) THEN 'Catholic Health (NY)'
            WHEN teaching_hospital_name LIKE 'Childrens Hospital & Res Center Oakland%' THEN 'Children\'s Hospital & Research Center Oakland'
            WHEN teaching_hospital_name LIKE 'Childrens Hospital Of Orange Count%' THEN 'Children\'s Hospital of Orange County'
            WHEN teaching_hospital_name LIKE 'Childrens Hospital Of The Kings Da%' THEN 'Children\'s Hospital of The King\'s Daughters'
            WHEN teaching_hospital_name LIKE 'Childrens Hospital Of San Antonio%' THEN 'Children\'s Hospital of San Antonio'
            WHEN teaching_hospital_name LIKE 'Cleveland Clinic%' OR teaching_hospital_name LIKE 'Ccf %' OR teaching_hospital_name = 'Hillcrest Hospital' OR teaching_hospital_name = 'Huron Hospital' OR teaching_hospital_name LIKE 'South Pointe Hospital%' THEN 'Cleveland Clinic'
            WHEN teaching_hospital_name LIKE 'Memorial Health Care System%' AND recipient_state = 'TN' THEN 'CommonSpirit Health'
            WHEN (teaching_hospital_name LIKE 'Mercy Medical Center Merced%' OR teaching_hospital_name LIKE 'Mercy San Juan%') THEN 'CommonSpirit Health'
            WHEN teaching_hospital_name LIKE 'Mercy Medical Center%' AND recipient_city = 'Roseburg' THEN 'CommonSpirit Health'
            WHEN teaching_hospital_name LIKE 'Saint Josephs Hospital%' AND recipient_state = 'AZ' THEN 'CommonSpirit Health'
            WHEN (teaching_hospital_name LIKE 'Saint Josephs Behavioral%' OR teaching_hospital_name LIKE 'Saint Josephs Medical Center%' OR teaching_hospital_name LIKE 'Saint Mary Medical Center%' AND recipient_city = 'Long Beach' OR teaching_hospital_name LIKE 'Saint Marys Medical Center%' AND recipient_city = 'San Francisco') AND recipient_state = 'CA' THEN 'CommonSpirit Health'
            WHEN teaching_hospital_name LIKE 'Saint Joseph Hospital%' AND recipient_state = 'KY' THEN 'CommonSpirit Health'
            WHEN teaching_hospital_name LIKE 'Saint Vincent%' AND recipient_state = 'AR' THEN 'CommonSpirit Health'
            WHEN (teaching_hospital_name LIKE 'Saint Francis Hospital%' OR teaching_hospital_name LIKE 'Saint Joseph Medical Center%') AND recipient_state = 'WA' THEN 'CommonSpirit Health'
            WHEN teaching_hospital_name LIKE 'Saint Alexius Medical Center%' AND recipient_state = 'ND' THEN 'CommonSpirit Health'
            WHEN teaching_hospital_name LIKE 'Saint Rose Dominican%' THEN 'CommonSpirit Health'
            WHEN (teaching_hospital_name LIKE 'Community Health Network%' OR teaching_hospital_name LIKE 'Community Hospital South%') AND recipient_state = 'IN' THEN 'Community Health Network'
            WHEN teaching_hospital_name LIKE 'Community Regional Medical Center%' AND recipient_state = 'CA' THEN 'Community Medical Centers (Fresno)'
            WHEN teaching_hospital_name LIKE 'Cmh Of San Buenaventura%' THEN 'Community Memorial Healthcare'

            -- D
            WHEN teaching_hospital_name LIKE 'Deaconess%' AND recipient_state IN ('IN', 'KY') THEN 'Deaconess Health System'
            WHEN teaching_hospital_name LIKE 'Dell Seton Medical %' THEN 'Dell Seton Medical Center'
            WHEN teaching_hospital_name LIKE 'Doctors Hospital At Renaissance%' THEN 'DHR Health'
            WHEN teaching_hospital_name LIKE 'Memorial Medical Center%' AND recipient_state = 'PA' THEN 'Duke LifePoint Healthcare'

            -- E
            WHEN teaching_hospital_name LIKE 'Saint Josephs Of Atlanta%' THEN 'Emory Healthcare'

            -- F
            WHEN teaching_hospital_name LIKE 'Community Memorial Hospital%' AND recipient_state = 'WI' THEN 'Froedtert Health'

            -- G
            WHEN teaching_hospital_name LIKE 'Good Samaritan Hospital%' AND recipient_city = 'Vincennes' AND recipient_state = 'IN' THEN 'Good Samaritan (Vincennes)'

            -- H
            WHEN teaching_hospital_name LIKE 'Saint Vincents Medical Center%' AND recipient_state = 'CT' THEN 'Hartford HealthCare'
            WHEN teaching_hospital_name LIKE 'Amend%Centerpoint%' THEN 'HCA Healthcare'
            WHEN teaching_hospital_name LIKE 'Amend%Research Medical%' THEN 'HCA Healthcare'
            WHEN teaching_hospital_name LIKE 'HCA %' OR teaching_hospital_name LIKE 'HCA Florida%' OR teaching_hospital_name LIKE 'Cjw Medical Center%' OR teaching_hospital_name LIKE 'Riverside Community Hospital%' OR teaching_hospital_name LIKE 'Lewisgale%' OR teaching_hospital_name LIKE 'Medical City %' OR teaching_hospital_name LIKE 'Tristar %' OR teaching_hospital_name = 'Swedish Medical Center' OR teaching_hospital_name = 'West Florida Hospital' THEN 'HCA Healthcare'
            WHEN teaching_hospital_name LIKE 'Houston Northwest Medical Center%' THEN 'HCA Healthcare'
            WHEN teaching_hospital_name LIKE 'Houston Healthcare Medical Center%' AND recipient_city = 'Houston' AND recipient_state = 'TX' THEN 'HCA Healthcare'
            WHEN (teaching_hospital_name LIKE 'Memorial Health University%' OR teaching_hospital_name LIKE 'Memorial Hospital Of Jacksonville%' OR teaching_hospital_name LIKE 'Memorial Satilla%') THEN 'HCA Healthcare'
            WHEN teaching_hospital_name LIKE 'Mercy Hospital%' AND recipient_city = 'Miami' THEN 'HCA Healthcare'
            WHEN teaching_hospital_name LIKE 'Methodist Hospital%' AND recipient_city = 'San Antonio' THEN 'HCA Healthcare'
            WHEN teaching_hospital_name LIKE 'Presbyterian Saint Lukes%' THEN 'HCA Healthcare'
            WHEN teaching_hospital_name LIKE 'Saint Marks Hospital%' AND recipient_state = 'UT' THEN 'HCA Healthcare'
            WHEN teaching_hospital_name LIKE 'Saint Petersburg General%' THEN 'HCA Healthcare'
            WHEN teaching_hospital_name LIKE 'W a Foote Memorial%' THEN 'Henry Ford Health'
            WHEN (teaching_hospital_name LIKE 'Houston Medical Center%' OR teaching_hospital_name LIKE 'Houston Healthcare%') AND recipient_state = 'GA' THEN 'Houston Healthcare'
            WHEN (teaching_hospital_name LIKE 'Saint Elizabeth Hospital%' OR teaching_hospital_name LIKE 'Saint Johns Hospital%' OR teaching_hospital_name LIKE 'Saint Marys Hospital%') AND recipient_state = 'IL' THEN 'HSHS (Hospital Sisters)'

            -- I
            WHEN teaching_hospital_name LIKE 'Bass Baptist%' THEN 'INTEGRIS Health'
            WHEN (teaching_hospital_name LIKE 'Saint Joseph Hospital%' OR teaching_hospital_name LIKE 'Saint Marys Hospital & Medical Center%' OR teaching_hospital_name LIKE 'Saint Vincent Healthcare%') AND recipient_state IN ('CO', 'MT') THEN 'Intermountain Health'

            -- J
            WHEN teaching_hospital_name LIKE 'John H Stroger%' OR teaching_hospital_name LIKE 'John H. Stroger%' THEN 'John H Stroger Jr Hospital Of Cook County'
            WHEN teaching_hospital_name LIKE 'Johns Hopkins%' OR teaching_hospital_name LIKE 'The Johns Hopkins%' THEN 'Johns Hopkins Hospital'
            WHEN teaching_hospital_name LIKE 'Tchd D B A Jps%' THEN 'JPS Health Network'

            -- K
            WHEN teaching_hospital_name LIKE 'KFH %' OR teaching_hospital_name LIKE 'Kfh -%' OR teaching_hospital_name LIKE 'KFH-%' OR teaching_hospital_name LIKE 'Kaiser Foundation%' THEN 'Kaiser Permanente'

            -- L
            WHEN teaching_hospital_name LIKE 'Memorial Medical Center%' AND recipient_state = 'NM' THEN 'LifePoint Health'

            -- M
            WHEN teaching_hospital_name LIKE 'Bryn Mawr%' THEN 'Main Line Health'
            WHEN teaching_hospital_name LIKE 'Mclean Hospital%' THEN 'Mass General Brigham'
            WHEN teaching_hospital_name LIKE 'Mayo Clinic%' OR teaching_hospital_name LIKE 'Mayo Foundation%' OR teaching_hospital_name LIKE 'Mchs-%' THEN 'Mayo Clinic'
            WHEN teaching_hospital_name LIKE 'Bay Regional Medical Center%' THEN 'McLaren Health Care'
            WHEN teaching_hospital_name LIKE 'Mclaren%' THEN 'McLaren Health Care'
            WHEN teaching_hospital_name LIKE 'Ut Md Anderson%' THEN 'MD Anderson Cancer Center'
            WHEN teaching_hospital_name LIKE 'Good Samaritan Hospital%' AND recipient_city = 'Baltimore' AND recipient_state = 'MD' THEN 'MedStar Health'
            WHEN teaching_hospital_name LIKE 'Medstar%' THEN 'MedStar Health'
            WHEN teaching_hospital_name LIKE 'Memorial Hospital At Gulfport%' THEN 'Memorial Health System (MS)'
            WHEN (teaching_hospital_name LIKE 'Memorial Regional Hospital%' OR teaching_hospital_name LIKE 'Memorial Hospital West%') AND recipient_state = 'FL' THEN 'Memorial Healthcare System'
            WHEN teaching_hospital_name LIKE 'Memorial Hermann%' OR teaching_hospital_name LIKE 'Memorial Hermann Tirr%' THEN 'Memorial Hermann Health System'
            WHEN teaching_hospital_name LIKE 'Memorial Hospital For Cancer And All%' THEN 'Memorial Sloan Kettering Cancer Center'
            WHEN teaching_hospital_name LIKE 'Memorialcare%' THEN 'MemorialCare Health System'
            WHEN (teaching_hospital_name LIKE 'Mercy Hospital Saint Louis%' OR teaching_hospital_name LIKE 'Mercy Hospital Fort Smith%' OR teaching_hospital_name LIKE 'Mercy Medical Center%' AND recipient_city = 'Rogers') THEN 'Mercy'
            WHEN teaching_hospital_name LIKE 'Mercy Medical Center%' AND recipient_state = 'MD' THEN 'Mercy Medical Center (Baltimore)'
            WHEN teaching_hospital_name LIKE 'Mercy Health System%' AND recipient_state = 'WI' THEN 'MercyHealth (WI/IL)'
            WHEN teaching_hospital_name LIKE 'Mercyone%' OR (teaching_hospital_name LIKE 'Mercy Medical Center%' AND recipient_city = 'Des Moines') THEN 'MercyOne'
            WHEN teaching_hospital_name LIKE 'Methodist%Dallas%' OR teaching_hospital_name LIKE 'Methodist Charlton%' THEN 'Methodist Health System (Dallas)'
            WHEN teaching_hospital_name LIKE 'Methodist Hospitals%' AND recipient_state = 'IN' THEN 'Methodist Hospitals (Indiana)'
            WHEN teaching_hospital_name LIKE 'Methodist H C Memphis%' THEN 'Methodist Le Bonheur Healthcare'
            WHEN teaching_hospital_name LIKE 'Midmichigan Medical%' THEN 'Mid Michigan Medical Center'
            WHEN teaching_hospital_name LIKE 'Montefiore %' OR teaching_hospital_name LIKE 'Saint Lukes Cornwall%' THEN 'Montefiore Health System'
            WHEN (teaching_hospital_name LIKE 'Mount Sinai%' OR teaching_hospital_name LIKE 'New York Eye And Ear%') AND recipient_state = 'NY' THEN 'Mount Sinai Health System'
            WHEN teaching_hospital_name LIKE 'Mount Sinai%' AND recipient_state = 'FL' THEN 'Mount Sinai Medical Center (FL)'
            WHEN teaching_hospital_name LIKE 'Capital Region Medical Center%' THEN 'MU Health Care'
            WHEN teaching_hospital_name LIKE 'Deaconess%' AND recipient_state = 'WA' THEN 'MultiCare Health System'

            -- N
            WHEN teaching_hospital_name LIKE 'New York Presbyterian%' OR teaching_hospital_name LIKE 'Newyork Presbyterian%' THEN 'New York-Presbyterian'
            WHEN teaching_hospital_name LIKE 'New York-Presbyterian%' OR teaching_hospital_name LIKE 'New York P.i%' THEN 'New York-Presbyterian Hospital'
            WHEN teaching_hospital_name LIKE 'Northwell Health%' OR teaching_hospital_name LIKE 'Long Island Jewish%' OR teaching_hospital_name LIKE 'South Shore University%' THEN 'Northwell Health'
            WHEN teaching_hospital_name LIKE 'Presbyterian Hospital%' AND recipient_state = 'NC' THEN 'Novant Health'
            WHEN teaching_hospital_name LIKE 'Nyc Health%' OR teaching_hospital_name LIKE 'Elmhurst Hospital%' THEN 'NYC Health + Hospitals'
            WHEN teaching_hospital_name LIKE 'Nyu %' OR teaching_hospital_name = 'Lutheran Medical Center' THEN 'NYU Langone Health'

            -- O
            WHEN teaching_hospital_name LIKE 'Ochsner%' THEN 'Ochsner Health'
            WHEN teaching_hospital_name LIKE 'Doctors Hospital%' AND recipient_city = 'Columbus' AND recipient_state = 'OH' THEN 'OhioHealth'
            WHEN teaching_hospital_name LIKE 'Bayfront Health Saint Petersburg%' THEN 'Orlando Health'
            WHEN (teaching_hospital_name LIKE 'Saint Anthony Medical Center%' OR teaching_hospital_name LIKE 'Saint Francis Medical Center%') AND recipient_state = 'IL' THEN 'OSF HealthCare'

            -- P
            WHEN teaching_hospital_name LIKE 'Presbyterian Medical Center%' AND recipient_state = 'PA' THEN 'Penn Medicine'
            WHEN teaching_hospital_name LIKE 'Good Samaritan Hospital%' AND recipient_city = 'Dayton' AND recipient_state = 'OH' THEN 'Premier Health'
            WHEN teaching_hospital_name LIKE 'Presbyterian Hospital%' AND recipient_state = 'NM' THEN 'Presbyterian Healthcare Services'
            WHEN (teaching_hospital_name LIKE 'Saint Clares Hospital%' OR teaching_hospital_name LIKE 'Saint Marys Hospital Passaic%' OR teaching_hospital_name LIKE 'Saint Michaels Medical Center%') AND recipient_state = 'NJ' THEN 'Prime Healthcare'
            WHEN teaching_hospital_name LIKE 'Ph Baptist%' OR teaching_hospital_name LIKE 'Ph Greer%' OR teaching_hospital_name LIKE 'Ph Hillcrest%' OR teaching_hospital_name LIKE 'Ph Patewood%' THEN 'Prisma Health'
            WHEN teaching_hospital_name LIKE 'Providence %' OR teaching_hospital_name LIKE 'Prov Regl%' OR teaching_hospital_name LIKE 'Prov Sacred%' OR (teaching_hospital_name LIKE '%Eureka%' AND teaching_hospital_name LIKE '%Joseph%') OR teaching_hospital_name LIKE 'Uw Medicine/northwest%' THEN 'Providence'
            WHEN teaching_hospital_name LIKE 'Saint Joseph Hospital Eureka%' THEN 'Providence St. Joseph Health'
            WHEN teaching_hospital_name LIKE 'Saint Patrick Hospital%' AND recipient_state = 'MT' THEN 'Providence St. Joseph Health'

            -- R
            WHEN teaching_hospital_name LIKE 'Robert Wood Johnson%' THEN 'Robert Wood Johnson University Hospital'

            -- S
            WHEN teaching_hospital_name LIKE 'Saint Lukes Regional%' AND recipient_state = 'ID' THEN 'St. Luke\'s Health System (ID)'
            WHEN teaching_hospital_name LIKE 'Saint Lukes Hospital Of Kansas City%' THEN 'Saint Luke\'s Health System (KC)'
            WHEN teaching_hospital_name LIKE 'Saint Lukes%' AND recipient_state = 'MO' THEN 'St. Luke\'s Hospital (St. Louis)'
            WHEN teaching_hospital_name LIKE 'Saint Luke%' AND recipient_state = 'PA' THEN 'St. Luke\'s University Health Network'
            WHEN teaching_hospital_name LIKE 'Sanford%' THEN 'Sanford Health'
            WHEN teaching_hospital_name LIKE 'Sbh Health System%' THEN 'SBH Health System'
            WHEN teaching_hospital_name LIKE 'Scripps%' THEN 'Scripps Health'
            WHEN teaching_hospital_name LIKE 'Sentara%' THEN 'Sentara Healthcare'
            WHEN teaching_hospital_name LIKE 'Memorial Hospital Of Carbondale%' THEN 'Southern Illinois Healthcare'
            WHEN (teaching_hospital_name LIKE 'Saint Clare Hospital%' AND recipient_state = 'WI') OR (teaching_hospital_name LIKE 'Saint Marys Medical Center%' AND recipient_city = 'Blue Springs') THEN 'SSM Health'
            WHEN teaching_hospital_name LIKE 'Good Samaritan Medical Center%' AND recipient_state = 'MA' THEN 'Steward Health Care'
            WHEN teaching_hospital_name LIKE 'Saint Joseph Medical Center%' AND recipient_city = 'Houston' THEN 'Steward Health Care'

            -- T
            WHEN teaching_hospital_name LIKE 'Doctors Medical Center Of Modesto%' THEN 'Tenet Healthcare'
            WHEN teaching_hospital_name LIKE 'Saint Francis Hospital%' AND recipient_state = 'TN' THEN 'Tenet Healthcare'
            WHEN teaching_hospital_name LIKE 'Saint Vincent Hospital%' AND recipient_state = 'MA' THEN 'Tenet Healthcare'
            WHEN teaching_hospital_name LIKE 'Saint Marys Medical Center%' AND recipient_city = 'West Palm Beach' THEN 'Tenet Healthcare'
            WHEN teaching_hospital_name LIKE 'Tx Health Harris Methodist%' THEN 'Texas Health Resources'
            WHEN teaching_hospital_name LIKE 'The Childrens Hospital Of Phila%' THEN 'The Children\'s Hospital of Philadelphia'
            WHEN teaching_hospital_name LIKE 'Thedacare Regiona%' THEN 'Thedacare Regional Medical Center'
            WHEN teaching_hospital_name LIKE 'Good Samaritan Hospital%' AND recipient_city = 'Cincinnati' AND recipient_state = 'OH' THEN 'TriHealth'
            WHEN teaching_hospital_name LIKE 'Mercy Health%' AND recipient_state = 'MI' THEN 'Trinity Health'
            WHEN teaching_hospital_name LIKE 'Saint Joseph Mercy%' OR teaching_hospital_name LIKE 'Saint Mary Mercy%' OR teaching_hospital_name LIKE 'Saint Marys Of Michigan%' OR (teaching_hospital_name LIKE 'Saint Marys Health Care%' AND recipient_state = 'MI') THEN 'Trinity Health'
            WHEN teaching_hospital_name LIKE 'Saint Alphonsus%' THEN 'Trinity Health'
            WHEN teaching_hospital_name LIKE 'Saint Anns Hospital%' AND recipient_state = 'OH' THEN 'Trinity Health'
            WHEN (teaching_hospital_name LIKE 'Saint Francis Hospital%' OR teaching_hospital_name LIKE 'Saint Marys Hospital%') AND recipient_state = 'CT' THEN 'Trinity Health'
            WHEN (teaching_hospital_name LIKE 'Saint Peters Hospital%' AND recipient_city = 'Albany') THEN 'Trinity Health'
            WHEN (teaching_hospital_name LIKE 'Saint Josephs Hospital Health Center%' AND recipient_city = 'Syracuse') THEN 'Trinity Health'
            WHEN teaching_hospital_name LIKE 'Saint Mary Medical Center%' AND recipient_city = 'Langhorne' THEN 'Trinity Health'
            WHEN teaching_hospital_name LIKE 'Saint Josephs Reg Medical Center%' AND recipient_state = 'IN' THEN 'Trinity Health'
            WHEN teaching_hospital_name LIKE 'Saint Marys Health Care System%' AND recipient_city = 'Athens' THEN 'Trinity Health'
            WHEN teaching_hospital_name LIKE 'Saint Francis Hospital Wilmington%' THEN 'Trinity Health'
            WHEN teaching_hospital_name LIKE 'Saint Agnes Medical Center%' AND recipient_city = 'Fresno' THEN 'Trinity Health'

            -- U
            WHEN teaching_hospital_name LIKE 'Uams Medical Center%' THEN 'UAMS Health'
            WHEN teaching_hospital_name LIKE 'Memorial Health System%' AND recipient_state = 'CO' THEN 'UCHealth'
            WHEN teaching_hospital_name LIKE 'Uf Health%' THEN 'UF Health'
            WHEN teaching_hospital_name LIKE 'Umass Memorial%' THEN 'UMass Memorial Health'
            WHEN teaching_hospital_name LIKE 'Uhs Hospitals%' THEN 'United Health Services (NY)'
            WHEN teaching_hospital_name LIKE 'Methodist Medical Center Of Illinois%' THEN 'UnityPoint Health'
            WHEN teaching_hospital_name LIKE 'Saint Lukes%' AND recipient_state = 'IA' THEN 'UnityPoint Health'
            WHEN teaching_hospital_name LIKE 'Uh %' OR teaching_hospital_name LIKE 'Uh Ahuja%' OR teaching_hospital_name LIKE 'Uh Cleveland%' THEN 'University Hospitals (Cleveland)'
            WHEN teaching_hospital_name LIKE 'Uc Davis%' OR teaching_hospital_name LIKE 'Uci Medical%' OR teaching_hospital_name LIKE 'Ucsd Medical%' OR teaching_hospital_name LIKE 'Ucsf Medical%' THEN 'University of California Health'
            WHEN teaching_hospital_name LIKE 'U Of U Hospitals%' OR teaching_hospital_name LIKE 'U Of Utah%' THEN 'University of Utah Health'
            WHEN teaching_hospital_name LIKE 'Unm Sandoval%' THEN 'UNM Health'
            WHEN teaching_hospital_name LIKE 'Uofl Health%' THEN 'UofL Health'
            WHEN teaching_hospital_name LIKE 'Upmc%' OR teaching_hospital_name LIKE 'University Of Pittsburgh Med%' THEN 'UPMC'
            WHEN teaching_hospital_name LIKE 'Memorial Hospital%' AND recipient_city = 'York' THEN 'UPMC'
            WHEN teaching_hospital_name LIKE 'Upmc Childrens Hospital Of Pgh' THEN 'UPMC Children\'s Hospital of Pittsburgh'
            WHEN teaching_hospital_name LIKE 'Ut Health%Tyler%' THEN 'UT Health East Texas'
            WHEN teaching_hospital_name LIKE 'Ut Southwestern%' THEN 'UT Southwestern Medical Center'
            WHEN teaching_hospital_name LIKE 'The University Of Texas Medical Br%' THEN 'UTMB Health'
            WHEN teaching_hospital_name LIKE 'University Of Washington%' OR teaching_hospital_name LIKE 'Uw Medicine%' OR teaching_hospital_name = 'Valley Medical Center' THEN 'UW Medicine'

            -- General Standardization
            WHEN teaching_hospital_name LIKE 'Childrens Hospital%' AND teaching_hospital_name NOT LIKE 'Children\'s%' THEN REPLACE(teaching_hospital_name, 'Childrens Hospital', 'Children\'s Hospital')
            WHEN teaching_hospital_name LIKE 'St. %' THEN REPLACE(teaching_hospital_name, 'St. ', 'Saint ')
            WHEN teaching_hospital_name LIKE 'St %' THEN REPLACE(teaching_hospital_name, 'St ', 'Saint ')
            WHEN teaching_hospital_name LIKE 'Univ Of %' THEN REPLACE(teaching_hospital_name, 'Univ Of', 'University of')
            WHEN teaching_hospital_name LIKE '%Of Pgh' THEN REPLACE(teaching_hospital_name, 'Of Pgh', 'Of Pittsburgh')

            ELSE  TitleCase(TRIM(teaching_hospital_name))
        END
        
        WHERE payment_id >= current_start 
          AND payment_id < current_end
          AND recipient_type LIKE '%Hospital%';     
        
        -- REMOVED DESTRUCTIVE UPDATE HERE
        
        SET rows_in_this_batch = ROW_COUNT();
        SET total_rows_updated = total_rows_updated + rows_in_this_batch;
        
        -- Batch Logging
        INSERT INTO migration_log (process_name, log_type, batch_end_id, rows_processed) 
        VALUES ('BatchCleanHospitals', 'BATCH', current_end - 1, rows_in_this_batch);

        COMMIT;        
        
        SET current_start = current_end;
    END WHILE;

    -- Summary Logging
    INSERT INTO migration_log (process_name, log_type, batch_end_id, rows_processed, notes) 
    VALUES ('BatchCleanHospitals', 'SUMMARY', max_id, total_rows_updated, 'Finished entire range successfully');

    SELECT CONCAT('Batch Complete: Hospitals. Rows Processed: ', total_rows_updated) as Status;
END $$

DELIMITER ;

CALL BatchCleanHospitals();
DROP PROCEDURE IF EXISTS BatchCleanHospitals;