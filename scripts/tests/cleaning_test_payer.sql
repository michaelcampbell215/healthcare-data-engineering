select distinct
submitting_applicable_mfr_or_gpo_name,
CASE
        -- A
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Astellas%' THEN 'Astellas Pharma'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Asahi Intecc%' THEN 'Asahi Intecc'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Agiliti %' THEN 'Agiliti Health'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Alcon %' THEN 'Alcon'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Angelus Industria%' OR submitting_applicable_mfr_or_gpo_name LIKE 'Angelus USA%' THEN 'Angelus Dental'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Southern Anesthesia%' THEN 'Ace Southern'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Applied Medical Re%' THEN 'Applied Medical'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'AstraZeneca%' THEN 'AstraZeneca'

        -- B
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'B Braun%' THEN 'B. Braun'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Aesculap%' THEN 'B. Braun'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Bausch & Lomb%' OR submitting_applicable_mfr_or_gpo_name LIKE 'Bausch + Lomb%' THEN 'Bausch Health'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Bausch Health%' THEN 'Bausch Health'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE '%Becton, Dickinson%' OR submitting_applicable_mfr_or_gpo_name LIKE '%Bard%' OR submitting_applicable_mfr_or_gpo_name = 'Bd' THEN 'BD'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'BioProtect%' THEN 'BioProtect'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'BIOTRONIK%' THEN 'Biotronik'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Boehringer Ingelheim%' THEN 'Boehringer Ingelheim'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE '%Boston Scientific%' 
             OR submitting_applicable_mfr_or_gpo_name LIKE '%Guidant%' 
             OR submitting_applicable_mfr_or_gpo_name LIKE '%American Medical Systems%' 
             OR submitting_applicable_mfr_or_gpo_name LIKE '%BTG International%' THEN 'Boston Scientific'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Ortho Dermatologics%' THEN 'Bausch Health'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Solta Medical%' OR submitting_applicable_mfr_or_gpo_name LIKE 'OraPharma%' THEN 'Bausch Health'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Salix%' THEN 'Bausch Health'
        WHEN submitting_applicable_mfr_or_gpo_name = 'Salix Pharmaceuticals, A Division Of Bausch Health Us' THEN 'Bausch Health'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Biocryst%' THEN 'BioCryst Pharmaceuticals'
        
        -- C
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Canon Medical%' OR submitting_applicable_mfr_or_gpo_name LIKE 'Canon Healthcare%' THEN 'Canon Medical Systems'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Cardinal Health%' THEN 'Cardinal Health'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Celltrion%' THEN 'Celltrion'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Corza%' OR submitting_applicable_mfr_or_gpo_name LIKE 'Surgical Specialties%' THEN 'Corza Medical'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Colgate Oral%' THEN 'Colgate-Palmolive'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Carl Zeiss%' THEN 'Carl Zeiss Meditec'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'CSL Vifor%' THEN 'CSL Vifor'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Csl B%' THEN 'CSL Behring'
        -- D
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Daiichi Sankyo%' THEN 'Daiichi Sankyo'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Dentsply%' OR submitting_applicable_mfr_or_gpo_name LIKE 'Sirona Dental%' THEN 'Dentsply Sirona'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'DentalEZ%' THEN 'DentalEZ'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Dentium%' THEN 'Dentium'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Dompe%' THEN 'Dompe'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Drreddy%' THEN 'Dr Reddy\'s Laboratories'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Dutch Ophthalmic%' THEN 'Dutch Ophthalmic Research Center'
            
        -- E
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Elekta%' THEN 'Elekta'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Nucletron%' THEN 'Elekta'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Endo %' THEN 'Endo Pharmaceuticals'

        -- F
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Ferring%' THEN 'Ferring Pharmaceuticals'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Fresenius%' THEN 'Fresenius'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Fujifilm%' THEN 'Fujifilm'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Fisher & Paykel Healthcare%' THEN 'Fisher & Paykel Healthcare'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Fusion Orthopedics%' THEN 'Fusion Orthopedics'

        -- G
        WHEN submitting_applicable_mfr_or_gpo_name like 'Galderma Laboratories%' THEN 'Galderma Laboratories'
        WHEN submitting_applicable_mfr_or_gpo_name like 'Geistlich Pharma%' THEN 'Geistlich Pharma'
        WHEN submitting_applicable_mfr_or_gpo_name like 'Genbiopro' THEN 'GenBioPro'
        WHEN submitting_applicable_mfr_or_gpo_name like 'Genentech%' or submitting_applicable_mfr_or_gpo_name = 'F. Hoffmann-La Roche Ag' THEN 'Genentech'
        WHEN submitting_applicable_mfr_or_gpo_name like 'Grifols%' THEN 'Grifols'
        WHEN submitting_applicable_mfr_or_gpo_name like 'Gc Ameri%' THEN 'GC America'

        -- H
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Helsinn%' THEN 'Helsinn Group'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Henry%' THEN 'Henry Schein'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Hoya%' THEN 'Hoya Surgical Optics'

        -- I        
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Iti%' OR submitting_applicable_mfr_or_gpo_name LIKE 'Intra-Cellular%' THEN 'Intra-Cellular Therapies'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Ignite%' THEN 'Ignite Orthopedics'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Insightec%' THEN 'Insightec'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Ipsen%' THEN 'Ipsen'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'IBSA%' THEN 'IBSA Group'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Impulse Dynamics%' THEN 'Impulse Dynamics'
        
        -- J
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Jubilant%' THEN 'Jubilant Pharma'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Janssen%' 
            OR submitting_applicable_mfr_or_gpo_name LIKE 'Johnson &%' 
            OR submitting_applicable_mfr_or_gpo_name LIKE 'Ethicon%' 
            OR submitting_applicable_mfr_or_gpo_name LIKE 'DePuy%' 
            OR submitting_applicable_mfr_or_gpo_name LIKE 'Biosense Webster%' 
            OR submitting_applicable_mfr_or_gpo_name LIKE 'Mentor Worldwide%' THEN 'Johnson & Johnson'
                
        -- K
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Karl%' THEN 'Karl Storz'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Kowa%' THEN 'Kowa'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Kuros%' THEN 'Kuros Biosciences'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Kiniksa Pharmaceuticals%' then 'Kiniksa Pharmaceuticals International PLC'

        -- L
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Leica%' THEN 'Leica Microsystems'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Lumenis%' THEN 'Lumenis'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Lkc Tech%' THEN 'LKC Technologies'

        -- M
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Merck%' THEN 'Merck & Co.'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Medtronic%' THEN 'Medtronic'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Covidien%' THEN 'Medtronic'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'MiniMed%' THEN 'Medtronic'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Mallinckrodt%' THEN 'Mallinckrodt'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Maquet%' THEN 'Maquet'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Materialise%' THEN 'Materialise'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Maxx%' THEN 'Maxx Health'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'McKesson%' THEN 'McKesson Corporation'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Merz%' THEN 'Merz'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'MicroPort%' THEN 'MicroPort'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Mylan%' THEN 'Mylan'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Molli S%' THEN 'Molli Surgical'
        WHEN submitting_applicable_mfr_or_gpo_name = 'Mml Us' then 'MML US'
        WHEN submitting_applicable_mfr_or_gpo_name = 'Mevion_medical_systems_' THEN 'Mevion Medical Systems'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Mitsubishi Tanabe%' THEN 'Mitsubishi Tanabe Pharma'
        
        -- N
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Novartis%' THEN 'Novartis'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Novo%' THEN 'Novo Nordisk'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Novocure%' THEN 'Novocure'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE '%Sandoz%' THEN 'Novartis'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Noven%' THEN 'Noven Pharmaceuticals'

        -- O
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Olympus%' THEN 'Olympus Corporation'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Orpyx%' THEN 'Orpyx Medical Technologies'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'OrthoPediatrics%' THEN 'OrthoPediatrics'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Otsuka%' THEN 'Otsuka Pharmaceutical'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Oculus Surgical%' 
            OR submitting_applicable_mfr_or_gpo_name = 'Oculus' THEN 'OCULUS Optikgeräte'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Omnia Medical%' THEN 'Omnia Medical'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Omnia Srl%' THEN 'Omnia SRL'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Optos%' THEN 'Optos'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Ortho Organizers%' 
            OR submitting_applicable_mfr_or_gpo_name LIKE 'Ortho Technology%' THEN 'Henry Schein'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Ortho-Clinical%' THEN 'QuidelOrtho'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Orthofix%' THEN 'Orthofix Medical'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Ortho Development%' THEN 'Ortho Development'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Ortho2%' THEN 'Ortho2'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'OrthoXel%' THEN 'OrthoXel'
            
          
        -- P
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Perfuze%' THEN 'Perfuze'

        -- Q
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Quest%' THEN 'Quest Medical'

        -- R
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Regeneron%' THEN 'Regeneron'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Roche%' THEN 'Roche'
        WHEN submitting_applicable_mfr_or_gpo_name = 'Recordati_rare_diseases_' then 'Recordati Rare Diseases'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Renalytix%' then 'Renalytix AI'

        -- S
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Spineart%' THEN 'Spineart'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Sanofi%' THEN 'Sanofi'
        WHEN submitting_applicable_mfr_or_gpo_name = 'Scpharmaceuticals' then 'scPharmaceuticals'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Smith+Neph%' THEN 'Smith + Nephew'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE '%Samsung%' THEN 'Samsung'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Sumitomo%' THEN 'Sumitomo Pharma'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE '%Stryker%' THEN 'Stryker'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE '%Wright Medical%' THEN 'Stryker'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE '%K2M%' THEN 'Stryker'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE '%Physio-Control%' THEN 'Stryker'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'SMAIO%' THEN 'SMAIO'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Sanofi%' OR submitting_applicable_mfr_or_gpo_name LIKE 'Genzyme%' THEN 'Sanofi'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'NeuroLogica%' THEN 'Samsung'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'SI-BONE%' THEN 'SI-BONE'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Sysmex%' THEN 'Sysmex'

        -- T
        WHEN submitting_applicable_mfr_or_gpo_name LIKE '%Teleflex%' THEN 'Teleflex Incorporated'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Terumo%' THEN 'Terumo Medical Corporation'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Phadia%' THEN 'Thermo Fisher Scientific'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Fisher Scientific%' THEN 'Thermo Fisher Scientific'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'TG T%' THEN 'TG Therapeutics'
        WHEN submitting_applicable_mfr_or_gpo_name = 'Tempus Ai' then 'Tempus AI'

        -- U
        WHEN submitting_applicable_mfr_or_gpo_name = 'United Medical Systems (DE)' then 'United Medical Systems'
        WHEN submitting_applicable_mfr_or_gpo_name = 'United Imaging Healthcare North America' then 'United Imaging Healthcare'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'UCB%' THEN 'UCB'

        -- V
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Voco %' then 'VOCO America'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Xvivo%' then 'XVIVO Perfusion'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Vision%' then 'VisionRT'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Vivaquant%' then 'VivaQuant'
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Vifo%' then 'Vifor Pharma'

        -- X
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Xiros%' THEN 'Xiros'
        
        -- Z
        WHEN submitting_applicable_mfr_or_gpo_name LIKE 'Zimmer Biomet%' 
            OR submitting_applicable_mfr_or_gpo_name LIKE 'Zimmer Holdings%' THEN 'Zimmer Biomet'
            
        ELSE submitting_applicable_mfr_or_gpo_name
    END
    from stg_general_payments;