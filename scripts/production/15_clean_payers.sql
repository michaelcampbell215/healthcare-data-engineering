-- =========================================================================
-- Batch Clean Payers (Optimized + Standardized Logging)
-- =========================================================================

DELIMITER $$

DROP PROCEDURE IF EXISTS BatchCleanPayers $$
CREATE PROCEDURE BatchCleanPayers()
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
    WHERE process_name = 'BatchCleanPayers' AND log_type = 'BATCH';

    IF last_processed_id IS NULL THEN
        SET current_start = min_id;
    ELSE
        SET current_start = last_processed_id + 1;
    END IF;

    SELECT CONCAT('Starting Batch: Payers (Optimized) from ID: ', current_start) as Status;

    WHILE current_start <= max_id DO
        SET current_end = current_start + batch_size;        
        START TRANSACTION;

        UPDATE general_payments
        SET 
            -- 1. Consolidated Payer Name Logic
            payer_name = CASE
                -- =========================================================
                -- TIER 1: MAJOR CONSOLIDATED MANUFACTURERS (Complex Groups)
                -- =========================================================
                
                -- Johnson & Johnson
                WHEN payer_name LIKE 'Janssen%'
                    OR payer_name LIKE 'Johnson &%' 
                    OR payer_name LIKE 'Ethicon%' 
                    OR payer_name LIKE 'DePuy%' 
                    OR payer_name LIKE 'Biosense Webster%' 
                    OR payer_name LIKE 'Mentor Worldwide%' THEN 'Johnson & Johnson'

                -- Medtronic
                WHEN payer_name LIKE 'Medtronic%' 
                    OR payer_name LIKE 'Covidien%' 
                    OR payer_name LIKE 'MiniMed%' THEN 'Medtronic'

                -- Stryker
                WHEN payer_name LIKE '%Stryker%' 
                    OR payer_name LIKE '%Wright Medical%' 
                    OR payer_name LIKE '%K2M%' 
                    OR payer_name LIKE '%Physio-Control%' THEN 'Stryker'

                -- Bausch Health
                WHEN payer_name LIKE 'Bausch & Lomb%' 
                    OR payer_name LIKE 'Bausch + Lomb%' 
                    OR payer_name LIKE 'Bausch Health%' 
                    OR payer_name LIKE 'Ortho Dermatologics%' 
                    OR payer_name LIKE 'Solta Medical%' 
                    OR payer_name LIKE 'OraPharma%' 
                    OR payer_name LIKE 'Salix%' 
                    OR payer_name = 'Salix Pharmaceuticals, A Division Of Bausch Health Us' THEN 'Bausch Health'

                -- BD (Becton, Dickinson)
                WHEN payer_name LIKE '%Becton, Dickinson%' 
                    OR payer_name LIKE '%Bard%' 
                    OR payer_name = 'Bd' THEN 'BD'

                -- Boston Scientific
                WHEN payer_name LIKE '%Boston Scientific%' 
                    OR payer_name LIKE '%Guidant%' 
                    OR payer_name LIKE '%American Medical Systems%' 
                    OR payer_name LIKE '%BTG International%' THEN 'Boston Scientific'

                -- Novartis
                WHEN payer_name LIKE 'Novartis%' 
                    OR payer_name LIKE '%Sandoz%' THEN 'Novartis'

                -- Roche / Genentech
                WHEN payer_name LIKE 'Roche%' 
                    OR payer_name LIKE 'Genentech%' 
                    OR payer_name = 'F. Hoffmann-La Roche Ag' THEN 'Roche'

                -- Sanofi
                WHEN payer_name LIKE 'Sanofi%' 
                    OR payer_name LIKE 'Genzyme%' THEN 'Sanofi'
                
                -- Thermo Fisher
                WHEN payer_name LIKE 'Phadia%' 
                    OR payer_name LIKE 'Fisher Scientific%' THEN 'Thermo Fisher Scientific'

                -- Henry Schein
                WHEN payer_name LIKE 'Henry%' 
                    OR payer_name LIKE 'Ortho Organizers%' 
                    OR payer_name LIKE 'Ortho Technology%' THEN 'Henry Schein'

                -- =========================================================
                -- TIER 2: STANDARD NORMALIZATION (A-Z)
                -- =========================================================
                WHEN payer_name LIKE 'Astellas%' THEN 'Astellas Pharma'
                WHEN payer_name LIKE 'Asahi Intecc%' THEN 'Asahi Intecc'
                WHEN payer_name LIKE 'Agiliti %' THEN 'Agiliti Health'
                WHEN payer_name LIKE 'Alcon %' THEN 'Alcon'
                WHEN payer_name LIKE 'Angelus Industria%' OR payer_name LIKE 'Angelus USA%' THEN 'Angelus Dental'
                WHEN payer_name LIKE 'Southern Anesthesia%' THEN 'Ace Southern'
                WHEN payer_name LIKE 'Applied Medical Re%' THEN 'Applied Medical'
                WHEN payer_name LIKE 'AstraZeneca%' THEN 'AstraZeneca'
                WHEN payer_name LIKE 'B Braun%' OR payer_name LIKE 'Aesculap%' THEN 'B. Braun'
                WHEN payer_name LIKE 'BioProtect%' THEN 'BioProtect'
                WHEN payer_name LIKE 'BIOTRONIK%' THEN 'Biotronik'
                WHEN payer_name LIKE 'Boehringer Ingelheim%' THEN 'Boehringer Ingelheim'
                WHEN payer_name LIKE 'Biocryst%' THEN 'BioCryst Pharmaceuticals'
                WHEN payer_name LIKE 'Canon Medical%' OR payer_name LIKE 'Canon Healthcare%' THEN 'Canon Medical Systems'
                WHEN payer_name LIKE 'Cardinal Health%' THEN 'Cardinal Health'
                WHEN payer_name LIKE 'Celltrion%' THEN 'Celltrion'
                WHEN payer_name LIKE 'Corza%' OR payer_name LIKE 'Surgical Specialties%' THEN 'Corza Medical'
                WHEN payer_name LIKE 'Colgate Oral%' THEN 'Colgate-Palmolive'
                WHEN payer_name LIKE 'Carl Zeiss%' THEN 'Carl Zeiss Meditec'
                WHEN payer_name LIKE 'CSL Vifor%' THEN 'CSL Vifor'
                WHEN payer_name LIKE 'Csl B%' THEN 'CSL Behring'
                WHEN payer_name LIKE 'Daiichi Sankyo%' THEN 'Daiichi Sankyo'
                WHEN payer_name LIKE 'Dentsply%' OR payer_name LIKE 'Sirona Dental%' THEN 'Dentsply Sirona'
                WHEN payer_name LIKE 'DentalEZ%' THEN 'DentalEZ'
                WHEN payer_name LIKE 'Dentium%' THEN 'Dentium'
                WHEN payer_name LIKE 'Dompe%' THEN 'Dompe'
                WHEN payer_name LIKE 'Drreddy%' THEN 'Dr Reddy\'s Laboratories'
                WHEN payer_name LIKE 'Dutch Ophthalmic%' THEN 'Dutch Ophthalmic Research Center'
                WHEN payer_name LIKE 'Elekta%' OR payer_name LIKE 'Nucletron%' THEN 'Elekta'
                WHEN payer_name LIKE 'Endo %' THEN 'Endo Pharmaceuticals'
                WHEN payer_name LIKE 'Ferring%' THEN 'Ferring Pharmaceuticals'
                WHEN payer_name LIKE 'Fresenius%' THEN 'Fresenius'
                WHEN payer_name LIKE 'Fujifilm%' THEN 'Fujifilm'
                WHEN payer_name LIKE 'Fisher & Paykel Healthcare%' THEN 'Fisher & Paykel Healthcare'
                WHEN payer_name LIKE 'Fusion Orthopedics%' THEN 'Fusion Orthopedics'
                WHEN payer_name like 'Galderma Laboratories%' THEN 'Galderma Laboratories'
                WHEN payer_name like 'Geistlich Pharma%' THEN 'Geistlich Pharma'
                WHEN payer_name like 'Genbiopro' THEN 'GenBioPro'
                WHEN payer_name like 'Grifols%' THEN 'Grifols'
                WHEN payer_name like 'Gc Ameri%' THEN 'GC America'
                WHEN payer_name LIKE 'Helsinn%' THEN 'Helsinn Group'
                WHEN payer_name LIKE 'Hoya%' THEN 'Hoya Surgical Optics'
                WHEN payer_name LIKE 'Iti%' OR payer_name LIKE 'Intra-Cellular%' THEN 'Intra-Cellular Therapies'
                WHEN payer_name LIKE 'Ignite%' THEN 'Ignite Orthopedics'
                WHEN payer_name LIKE 'Insightec%' THEN 'Insightec'
                WHEN payer_name LIKE 'Ipsen%' THEN 'Ipsen'
                WHEN payer_name LIKE 'IBSA%' THEN 'IBSA Group'
                WHEN payer_name LIKE 'Impulse Dynamics%' THEN 'Impulse Dynamics'
                WHEN payer_name LIKE 'Jubilant%' THEN 'Jubilant Pharma'
                WHEN payer_name LIKE 'Karl%' THEN 'Karl Storz'
                WHEN payer_name LIKE 'Kowa%' THEN 'Kowa'
                WHEN payer_name LIKE 'Kuros%' THEN 'Kuros Biosciences'
                WHEN payer_name LIKE 'Kiniksa Pharmaceuticals%' then 'Kiniksa Pharmaceuticals International PLC'
                WHEN payer_name LIKE 'Leica%' THEN 'Leica Microsystems'
                WHEN payer_name LIKE 'Lumenis%' THEN 'Lumenis'
                WHEN payer_name LIKE 'Lkc Tech%' THEN 'LKC Technologies'
                WHEN payer_name LIKE 'Merck%' THEN 'Merck & Co.'
                WHEN payer_name LIKE 'Mallinckrodt%' THEN 'Mallinckrodt'
                WHEN payer_name LIKE 'Maquet%' THEN 'Maquet'
                WHEN payer_name LIKE 'Materialise%' THEN 'Materialise'
                WHEN payer_name LIKE 'Maxx%' THEN 'Maxx Health'
                WHEN payer_name LIKE 'McKesson%' THEN 'McKesson Corporation'
                WHEN payer_name LIKE 'Merz%' THEN 'Merz'
                WHEN payer_name LIKE 'MicroPort%' THEN 'MicroPort'
                WHEN payer_name LIKE 'Mylan%' THEN 'Mylan'
                WHEN payer_name LIKE 'Molli S%' THEN 'Molli Surgical'
                WHEN payer_name = 'Mml Us' then 'MML US'
                WHEN payer_name = 'Mevion_medical_systems_' THEN 'Mevion Medical Systems'
                WHEN payer_name LIKE 'Mitsubishi Tanabe%' THEN 'Mitsubishi Tanabe Pharma'
                WHEN payer_name LIKE 'Novo%' THEN 'Novo Nordisk'
                WHEN payer_name LIKE 'Novocure%' THEN 'Novocure'
                WHEN payer_name LIKE 'Noven%' THEN 'Noven Pharmaceuticals'
                WHEN payer_name LIKE 'Olympus%' THEN 'Olympus Corporation'
                WHEN payer_name LIKE 'Orpyx%' THEN 'Orpyx Medical Technologies'
                WHEN payer_name LIKE 'OrthoPediatrics%' THEN 'OrthoPediatrics'
                WHEN payer_name LIKE 'Otsuka%' THEN 'Otsuka Pharmaceutical'
                WHEN payer_name LIKE 'Oculus Surgical%' OR payer_name = 'Oculus' THEN 'OCULUS Optikgeräte'
                WHEN payer_name LIKE 'Omnia Medical%' THEN 'Omnia Medical'
                WHEN payer_name LIKE 'Omnia Srl%' THEN 'Omnia SRL'
                WHEN payer_name LIKE 'Optos%' THEN 'Optos'
                WHEN payer_name LIKE 'Ortho-Clinical%' THEN 'QuidelOrtho'
                WHEN payer_name LIKE 'Orthofix%' THEN 'Orthofix Medical'
                WHEN payer_name LIKE 'Ortho Development%' THEN 'Ortho Development'
                WHEN payer_name LIKE 'Ortho2%' THEN 'Ortho2'
                WHEN payer_name LIKE 'OrthoXel%' THEN 'OrthoXel'
                WHEN payer_name LIKE 'Perfuze%' THEN 'Perfuze'
                WHEN payer_name LIKE 'Quest%' THEN 'Quest Medical'
                WHEN payer_name LIKE 'Regeneron%' THEN 'Regeneron'
                WHEN payer_name = 'Recordati_rare_diseases_' then 'Recordati Rare Diseases'
                WHEN payer_name LIKE 'Renalytix%' then 'Renalytix AI'
                WHEN payer_name LIKE 'Spineart%' THEN 'Spineart'
                WHEN payer_name = 'Scpharmaceuticals' then 'scPharmaceuticals'
                WHEN payer_name LIKE 'Smith+Neph%' THEN 'Smith + Nephew'
                WHEN payer_name LIKE '%Samsung%' OR payer_name LIKE 'NeuroLogica%' THEN 'Samsung'
                WHEN payer_name LIKE 'Sumitomo%' THEN 'Sumitomo Pharma'
                WHEN payer_name LIKE 'SMAIO%' THEN 'SMAIO'
                WHEN payer_name LIKE 'SI-BONE%' THEN 'SI-BONE'
                WHEN payer_name LIKE 'Sysmex%' THEN 'Sysmex'
                WHEN payer_name LIKE '%Teleflex%' THEN 'Teleflex Incorporated'
                WHEN payer_name LIKE 'Terumo%' THEN 'Terumo Medical Corporation'
                WHEN payer_name LIKE 'TG T%' THEN 'TG Therapeutics'
                WHEN payer_name = 'Tempus Ai' then 'Tempus AI'
                WHEN payer_name = 'United Medical Systems (DE)' then 'United Medical Systems'
                WHEN payer_name = 'United Imaging Healthcare North America' then 'United Imaging Healthcare'
                WHEN payer_name LIKE 'UCB%' THEN 'UCB'
                WHEN payer_name LIKE 'Voco %' then 'VOCO America'
                WHEN payer_name LIKE 'Xvivo%' then 'XVIVO Perfusion'
                WHEN payer_name LIKE 'Vision%' then 'VisionRT'
                WHEN payer_name LIKE 'Vivaquant%' then 'VivaQuant'
                WHEN payer_name LIKE 'Vifo%' then 'Vifor Pharma'
                WHEN payer_name LIKE 'Xiros%' THEN 'Xiros'
                WHEN payer_name LIKE 'Zimmer Biomet%' OR payer_name LIKE 'Zimmer Holdings%' THEN 'Zimmer Biomet'
                ELSE CleanManufactureName(TitleCase(payer_name))
            END,
            
            -- 2. Consolidated Subsidiary Name Logic
            subsidiary_name = CASE
                WHEN subsidiary_name LIKE '%, A Division Of%' THEN TRIM(SUBSTRING_INDEX(subsidiary_name, ' A', 1))
                WHEN subsidiary_name LIKE '%(D/b/a%' THEN TRIM(REPLACE(SUBSTRING_INDEX(subsidiary_name, '(D/b/a', -1), ')', ''))
                WHEN subsidiary_name LIKE '%(A/k/a%' THEN TRIM(SUBSTRING_INDEX(subsidiary_name, '(', 1))
                WHEN subsidiary_name LIKE '%, A Subsidiary Of%' THEN TRIM(SUBSTRING_INDEX(subsidiary_name, ' A', 1))
                ELSE CleanManufactureName(TitleCase(subsidiary_name))
            END,

            -- 3. Consolidated Third Party Logic
            name_of_third_party_entity = IF(name_of_third_party_entity IS NOT NULL, CleanManufactureName(TitleCase(name_of_third_party_entity)), name_of_third_party_entity)

        WHERE payment_id >= current_start AND payment_id < current_end;

        SET rows_in_this_batch = ROW_COUNT();
        SET total_rows_updated = total_rows_updated + rows_in_this_batch;
        
        -- Batch Logging
        INSERT INTO migration_log (process_name, log_type, batch_end_id, rows_processed) 
        VALUES ('BatchCleanPayers', 'BATCH', current_end - 1, rows_in_this_batch);

        COMMIT;        
        SET current_start = current_end;
    END WHILE;

    -- Summary Logging
    INSERT INTO migration_log (process_name, log_type, batch_end_id, rows_processed, notes) 
    VALUES ('BatchCleanPayers', 'SUMMARY', max_id, total_rows_updated, 'Finished entire range successfully');

    SELECT CONCAT('Batch Complete: Payers. Rows Processed: ', total_rows_updated) as Status;
END $$

DELIMITER ;

CALL BatchCleanPayers();
DROP PROCEDURE IF EXISTS BatchCleanPayers;
