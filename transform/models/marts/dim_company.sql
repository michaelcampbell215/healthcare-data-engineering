WITH source AS (
    SELECT DISTINCT
        subsidiary_id,
        subsidiary_name,
        payer_name,
        subsidiary_state,
        subsidiary_country
    FROM {{ ref('stg_general_payments') }}
    WHERE subsidiary_id IS NOT NULL
),
cleaned AS (
    SELECT
        subsidiary_id,

        -- Strip legal suffix patterns from subsidiary names.
        -- BigQuery REGEXP_EXTRACT replaces Phase 1's SUBSTRING_INDEX.
        CASE
            WHEN REGEXP_CONTAINS(subsidiary_name, r'(?i),\s*A Division Of')
                THEN TRIM(REGEXP_EXTRACT(subsidiary_name, r'(?i)^(.*?),\s*A Division Of'))
            WHEN REGEXP_CONTAINS(subsidiary_name, r'(?i)\(D/b/a')
                THEN TRIM(REPLACE(REGEXP_EXTRACT(subsidiary_name, r'(?i)\(D/b/a\s*(.+)$'), ')', ''))
            WHEN REGEXP_CONTAINS(subsidiary_name, r'(?i)\(A/k/a')
                THEN TRIM(REGEXP_EXTRACT(subsidiary_name, r'^(.*?)\s*\('))
            WHEN REGEXP_CONTAINS(subsidiary_name, r'(?i),\s*A Subsidiary Of')
                THEN TRIM(REGEXP_EXTRACT(subsidiary_name, r'(?i)^(.*?),\s*A Subsidiary Of'))
            ELSE INITCAP(TRIM(REGEXP_REPLACE(subsidiary_name, r'\s+', ' ')))
        END AS subsidiary_name,

        -- Consolidate brand/subsidiary variants to parent company.
        -- Ported from Phase 1 BatchCleanPayers. LIKE is valid BigQuery syntax.
        CASE
            -- ================================================================
            -- TIER 1: MAJOR CONSOLIDATED MANUFACTURERS
            -- ================================================================
            WHEN payer_name LIKE 'Janssen%'
              OR payer_name LIKE 'Johnson &%'
              OR payer_name LIKE 'Ethicon%'
              OR payer_name LIKE 'DePuy%'
              OR payer_name LIKE 'Biosense Webster%'
              OR payer_name LIKE 'Mentor Worldwide%'              THEN 'Johnson & Johnson'

            WHEN payer_name LIKE 'Medtronic%'
              OR payer_name LIKE 'Covidien%'
              OR payer_name LIKE 'MiniMed%'                       THEN 'Medtronic'

            WHEN payer_name LIKE '%Stryker%'
              OR payer_name LIKE '%Wright Medical%'
              OR payer_name LIKE '%K2M%'
              OR payer_name LIKE '%Physio-Control%'               THEN 'Stryker'

            WHEN payer_name LIKE 'Bausch & Lomb%'
              OR payer_name LIKE 'Bausch + Lomb%'
              OR payer_name LIKE 'Bausch Health%'
              OR payer_name LIKE 'Ortho Dermatologics%'
              OR payer_name LIKE 'Solta Medical%'
              OR payer_name LIKE 'OraPharma%'
              OR payer_name LIKE 'Salix%'                         THEN 'Bausch Health'

            WHEN payer_name LIKE '%Becton, Dickinson%'
              OR payer_name LIKE '%Bard%'
              OR payer_name = 'Bd'                                THEN 'BD'

            WHEN payer_name LIKE '%Boston Scientific%'
              OR payer_name LIKE '%Guidant%'
              OR payer_name LIKE '%American Medical Systems%'
              OR payer_name LIKE '%BTG International%'            THEN 'Boston Scientific'

            WHEN payer_name LIKE 'Novartis%'
              OR payer_name LIKE '%Sandoz%'                       THEN 'Novartis'

            WHEN payer_name LIKE 'Roche%'
              OR payer_name LIKE 'Genentech%'
              OR payer_name = 'F. Hoffmann-La Roche Ag'           THEN 'Roche'

            WHEN payer_name LIKE 'Sanofi%'
              OR payer_name LIKE 'Genzyme%'                       THEN 'Sanofi'

            WHEN payer_name LIKE 'Phadia%'
              OR payer_name LIKE 'Fisher Scientific%'             THEN 'Thermo Fisher Scientific'

            WHEN payer_name LIKE 'Henry%'
              OR payer_name LIKE 'Ortho Organizers%'
              OR payer_name LIKE 'Ortho Technology%'              THEN 'Henry Schein'

            -- ================================================================
            -- TIER 2: STANDARD NORMALIZATION (A–Z)
            -- ================================================================
            WHEN payer_name LIKE 'Agiliti %'                      THEN 'Agiliti Health'
            WHEN payer_name LIKE 'Alcon %'                        THEN 'Alcon'
            WHEN payer_name LIKE 'Angelus Industria%'
              OR payer_name LIKE 'Angelus USA%'                   THEN 'Angelus Dental'
            WHEN payer_name LIKE 'Southern Anesthesia%'           THEN 'Ace Southern'
            WHEN payer_name LIKE 'Applied Medical Re%'            THEN 'Applied Medical'
            WHEN payer_name LIKE 'Asahi Intecc%'                  THEN 'Asahi Intecc'
            WHEN payer_name LIKE 'Astellas%'                      THEN 'Astellas Pharma'
            WHEN payer_name LIKE 'AstraZeneca%'                   THEN 'AstraZeneca'
            WHEN payer_name LIKE 'B Braun%'
              OR payer_name LIKE 'Aesculap%'                      THEN 'B. Braun'
            WHEN payer_name LIKE 'Biocryst%'                      THEN 'BioCryst Pharmaceuticals'
            WHEN payer_name LIKE 'BioProtect%'                    THEN 'BioProtect'
            WHEN payer_name LIKE 'BIOTRONIK%'                     THEN 'Biotronik'
            WHEN payer_name LIKE 'Boehringer Ingelheim%'          THEN 'Boehringer Ingelheim'
            WHEN payer_name LIKE 'Canon Medical%'
              OR payer_name LIKE 'Canon Healthcare%'              THEN 'Canon Medical Systems'
            WHEN payer_name LIKE 'Cardinal Health%'               THEN 'Cardinal Health'
            WHEN payer_name LIKE 'Carl Zeiss%'                    THEN 'Carl Zeiss Meditec'
            WHEN payer_name LIKE 'Celltrion%'                     THEN 'Celltrion'
            WHEN payer_name LIKE 'Colgate Oral%'                  THEN 'Colgate-Palmolive'
            WHEN payer_name LIKE 'Corza%'
              OR payer_name LIKE 'Surgical Specialties%'          THEN 'Corza Medical'
            WHEN payer_name LIKE 'CSL Vifor%'                     THEN 'CSL Vifor'
            WHEN payer_name LIKE 'Csl B%'                         THEN 'CSL Behring'
            WHEN payer_name LIKE 'Daiichi Sankyo%'                THEN 'Daiichi Sankyo'
            WHEN payer_name LIKE 'DentalEZ%'                      THEN 'DentalEZ'
            WHEN payer_name LIKE 'Dentsply%'
              OR payer_name LIKE 'Sirona Dental%'                 THEN 'Dentsply Sirona'
            WHEN payer_name LIKE 'Dentium%'                       THEN 'Dentium'
            WHEN payer_name LIKE 'Dompe%'                         THEN 'Dompe'
            WHEN payer_name LIKE 'Drreddy%'                       THEN 'Dr. Reddy\'s Laboratories'
            WHEN payer_name LIKE 'Dutch Ophthalmic%'              THEN 'Dutch Ophthalmic Research Center'
            WHEN payer_name LIKE 'Elekta%'
              OR payer_name LIKE 'Nucletron%'                     THEN 'Elekta'
            WHEN payer_name LIKE 'Endo %'                         THEN 'Endo Pharmaceuticals'
            WHEN payer_name LIKE 'Ferring%'                       THEN 'Ferring Pharmaceuticals'
            WHEN payer_name LIKE 'Fisher & Paykel Healthcare%'    THEN 'Fisher & Paykel Healthcare'
            WHEN payer_name LIKE 'Fresenius%'                     THEN 'Fresenius'
            WHEN payer_name LIKE 'Fujifilm%'                      THEN 'Fujifilm'
            WHEN payer_name LIKE 'Fusion Orthopedics%'            THEN 'Fusion Orthopedics'
            WHEN payer_name LIKE 'Galderma Laboratories%'         THEN 'Galderma Laboratories'
            WHEN payer_name LIKE 'Gc Ameri%'                      THEN 'GC America'
            WHEN payer_name LIKE 'Geistlich Pharma%'              THEN 'Geistlich Pharma'
            WHEN payer_name LIKE 'Genbiopro%'                     THEN 'GenBioPro'
            WHEN payer_name LIKE 'Grifols%'                       THEN 'Grifols'
            WHEN payer_name LIKE 'Helsinn%'                       THEN 'Helsinn Group'
            WHEN payer_name LIKE 'Hoya%'                          THEN 'Hoya Surgical Optics'
            WHEN payer_name LIKE 'IBSA%'                          THEN 'IBSA Group'
            WHEN payer_name LIKE 'Ignite%'                        THEN 'Ignite Orthopedics'
            WHEN payer_name LIKE 'Impulse Dynamics%'              THEN 'Impulse Dynamics'
            WHEN payer_name LIKE 'Insightec%'                     THEN 'Insightec'
            WHEN payer_name LIKE 'Intra-Cellular%'
              OR payer_name LIKE 'Iti%'                           THEN 'Intra-Cellular Therapies'
            WHEN payer_name LIKE 'Ipsen%'                         THEN 'Ipsen'
            WHEN payer_name LIKE 'Jubilant%'                      THEN 'Jubilant Pharma'
            WHEN payer_name LIKE 'Karl%'                          THEN 'Karl Storz'
            WHEN payer_name LIKE 'Kiniksa Pharmaceuticals%'       THEN 'Kiniksa Pharmaceuticals International PLC'
            WHEN payer_name LIKE 'Kowa%'                          THEN 'Kowa'
            WHEN payer_name LIKE 'Kuros%'                         THEN 'Kuros Biosciences'
            WHEN payer_name LIKE 'Leica%'                         THEN 'Leica Microsystems'
            WHEN payer_name LIKE 'Lkc Tech%'                      THEN 'LKC Technologies'
            WHEN payer_name LIKE 'Lumenis%'                       THEN 'Lumenis'
            WHEN payer_name LIKE 'Mallinckrodt%'                  THEN 'Mallinckrodt'
            WHEN payer_name LIKE 'Maquet%'                        THEN 'Maquet'
            WHEN payer_name LIKE 'Materialise%'                   THEN 'Materialise'
            WHEN payer_name LIKE 'Maxx%'                          THEN 'Maxx Health'
            WHEN payer_name LIKE 'McKesson%'                      THEN 'McKesson Corporation'
            WHEN payer_name LIKE 'Merck%'                         THEN 'Merck & Co.'
            WHEN payer_name LIKE 'Merz%'                          THEN 'Merz'
            WHEN payer_name = 'Mevion_medical_systems_'           THEN 'Mevion Medical Systems'
            WHEN payer_name LIKE 'MicroPort%'                     THEN 'MicroPort'
            WHEN payer_name LIKE 'Mitsubishi Tanabe%'             THEN 'Mitsubishi Tanabe Pharma'
            WHEN payer_name LIKE 'Molli S%'                       THEN 'Molli Surgical'
            WHEN payer_name = 'Mml Us'                            THEN 'MML US'
            WHEN payer_name LIKE 'Mylan%'                         THEN 'Mylan'
            WHEN payer_name LIKE 'Novo%'                          THEN 'Novo Nordisk'
            WHEN payer_name LIKE 'Noven%'                         THEN 'Noven Pharmaceuticals'
            WHEN payer_name LIKE 'Novocure%'                      THEN 'Novocure'
            WHEN payer_name LIKE 'Oculus Surgical%'
              OR payer_name = 'Oculus'                            THEN 'OCULUS Optikgeräte'
            WHEN payer_name LIKE 'Olympus%'                       THEN 'Olympus Corporation'
            WHEN payer_name LIKE 'Omnia Medical%'                 THEN 'Omnia Medical'
            WHEN payer_name LIKE 'Omnia Srl%'                     THEN 'Omnia SRL'
            WHEN payer_name LIKE 'Optos%'                         THEN 'Optos'
            WHEN payer_name LIKE 'Ortho Development%'             THEN 'Ortho Development'
            WHEN payer_name LIKE 'Ortho-Clinical%'                THEN 'QuidelOrtho'
            WHEN payer_name LIKE 'Ortho2%'                        THEN 'Ortho2'
            WHEN payer_name LIKE 'Orthofix%'                      THEN 'Orthofix Medical'
            WHEN payer_name LIKE 'OrthoPediatrics%'               THEN 'OrthoPediatrics'
            WHEN payer_name LIKE 'OrthoXel%'                      THEN 'OrthoXel'
            WHEN payer_name LIKE 'Otsuka%'                        THEN 'Otsuka Pharmaceutical'
            WHEN payer_name LIKE 'Perfuze%'                       THEN 'Perfuze'
            WHEN payer_name LIKE 'Quest%'                         THEN 'Quest Medical'
            WHEN payer_name = 'Recordati_rare_diseases_'          THEN 'Recordati Rare Diseases'
            WHEN payer_name LIKE 'Regeneron%'                     THEN 'Regeneron'
            WHEN payer_name LIKE 'Renalytix%'                     THEN 'Renalytix AI'
            WHEN payer_name LIKE '%Samsung%'
              OR payer_name LIKE 'NeuroLogica%'                   THEN 'Samsung'
            WHEN payer_name = 'Scpharmaceuticals'                 THEN 'scPharmaceuticals'
            WHEN payer_name LIKE 'SI-BONE%'                       THEN 'SI-BONE'
            WHEN payer_name LIKE 'SMAIO%'                         THEN 'SMAIO'
            WHEN payer_name LIKE 'Smith+Neph%'                    THEN 'Smith + Nephew'
            WHEN payer_name LIKE 'Spineart%'                      THEN 'Spineart'
            WHEN payer_name LIKE 'Sumitomo%'                      THEN 'Sumitomo Pharma'
            WHEN payer_name LIKE 'Sysmex%'                        THEN 'Sysmex'
            WHEN payer_name = 'Tempus Ai'                         THEN 'Tempus AI'
            WHEN payer_name LIKE '%Teleflex%'                     THEN 'Teleflex Incorporated'
            WHEN payer_name LIKE 'Terumo%'                        THEN 'Terumo Medical Corporation'
            WHEN payer_name LIKE 'TG T%'                          THEN 'TG Therapeutics'
            WHEN payer_name LIKE 'UCB%'                           THEN 'UCB'
            WHEN payer_name = 'United Medical Systems (DE)'       THEN 'United Medical Systems'
            WHEN payer_name = 'United Imaging Healthcare North America' THEN 'United Imaging Healthcare'
            WHEN payer_name LIKE 'Vifo%'                          THEN 'Vifor Pharma'
            WHEN payer_name LIKE 'Vision%'                        THEN 'VisionRT'
            WHEN payer_name LIKE 'Vivaquant%'                     THEN 'VivaQuant'
            WHEN payer_name LIKE 'Voco %'                         THEN 'VOCO America'
            WHEN payer_name LIKE 'Xiros%'                         THEN 'Xiros'
            WHEN payer_name LIKE 'Xvivo%'                         THEN 'XVIVO Perfusion'
            WHEN payer_name LIKE 'Zimmer Biomet%'
              OR payer_name LIKE 'Zimmer Holdings%'               THEN 'Zimmer Biomet'

            ELSE INITCAP(TRIM(REGEXP_REPLACE(payer_name, r'\s+', ' ')))
        END AS payer_name,

        subsidiary_state,
        subsidiary_country

    FROM source
)

SELECT * FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY subsidiary_id ORDER BY payer_name) = 1
