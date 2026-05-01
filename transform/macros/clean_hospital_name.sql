{% macro clean_hospital_name(hospital_name, city, state) %}
CASE
    -- A
    WHEN {{ hospital_name }} LIKE 'Adventhealth%' OR {{ hospital_name }} LIKE 'Advent Health %' OR {{ hospital_name }} LIKE 'Adventist Hinsdale%' OR {{ hospital_name }} LIKE 'Adventist Lagrange%' OR {{ hospital_name }} LIKE 'Adventist Bolingbrook%' OR {{ hospital_name }} LIKE 'Adventist GlenOaks%' THEN 'AdventHealth'
    WHEN {{ hospital_name }} LIKE 'Adventist Health %' OR {{ hospital_name }} LIKE 'Adventist Health Glendale%' OR {{ hospital_name }} LIKE 'Adventist Health Hanford%' OR {{ hospital_name }} LIKE 'Adventist Health Ukiah%' OR {{ hospital_name }} LIKE 'Adventist Health White%' THEN 'Adventist Health'
    WHEN {{ hospital_name }} LIKE 'Advocate %' OR {{ hospital_name }} LIKE 'Advocate Christ%' OR {{ hospital_name }} LIKE 'Advocate Lutheran%' OR {{ hospital_name }} LIKE 'Advocate Northside%' THEN 'Advocate Health Care'
    WHEN {{ hospital_name }} LIKE 'Ahn %' OR {{ hospital_name }} LIKE 'Allegheny Health%' OR {{ hospital_name }} LIKE 'Ahn The Medical Center%' THEN 'Allegheny Health Network'
    WHEN {{ hospital_name }} LIKE 'Saint Vincent Hospital%' AND {{ city }} = 'Erie' THEN 'Allegheny Health Network'
    WHEN {{ hospital_name }} LIKE 'Mercy Hospital%' AND {{ state }} = 'MN' THEN 'Allina Health'
    WHEN {{ hospital_name }} LIKE 'Whitesburg Arh%' THEN 'Appalachian Regional Healthcare'
    WHEN {{ hospital_name }} LIKE 'Ascension %' OR {{ hospital_name }} LIKE 'Saint Thomas %' OR {{ hospital_name }} LIKE 'Saint Vincents Birmingham%' OR {{ hospital_name }} LIKE 'Saint Vincents East%' THEN 'Ascension Health'
    WHEN {{ hospital_name }} LIKE 'Saint Thomas%' AND {{ state }} = 'TN' THEN 'Ascension Health'
    WHEN {{ hospital_name }} LIKE 'Saint Vincents%' AND {{ state }} = 'AL' THEN 'Ascension Health'
    WHEN {{ hospital_name }} LIKE 'Saint Alexius Medical Center%' AND {{ city }} = 'Hoffman Estates' THEN 'Ascension Health'
    WHEN {{ hospital_name }} LIKE 'Saint Agnes Hospital%' AND {{ state }} = 'MD' THEN 'Ascension Health'
    WHEN {{ hospital_name }} LIKE 'Saint John Medical Center%' AND {{ city }} = 'Tulsa' THEN 'Ascension Health'
    WHEN {{ hospital_name }} LIKE 'Atrium Health%' THEN 'Atrium Health'
    WHEN {{ hospital_name }} LIKE 'Aurora Health%' OR {{ hospital_name }} LIKE 'Aurora Lakeland%' OR {{ hospital_name }} LIKE 'Aurora Medical%' THEN 'Aurora Health Care'

    -- B
    WHEN {{ hospital_name }} LIKE 'Banner %' THEN 'Banner Health'
    WHEN {{ hospital_name }} LIKE 'Banner University Medical Center Phx' THEN 'Banner University Medical Center Phoenix'
    WHEN {{ hospital_name }} LIKE 'Baptist Health %' OR ({{ hospital_name }} LIKE 'Baptist %' AND {{ hospital_name }} NOT LIKE 'Baptist Memorial %') THEN 'Baptist Health'
    WHEN {{ hospital_name }} LIKE 'Doctors Hospital%' AND {{ city }} = 'Coral Gables' AND {{ state }} = 'FL' THEN 'Baptist Health South Florida'
    WHEN {{ hospital_name }} LIKE 'Baylor%' THEN 'Baylor Scott & White Health'
    WHEN {{ hospital_name }} LIKE 'Baystate Medical Center%' THEN 'Baystate Health'
    WHEN {{ hospital_name }} LIKE 'Memorial Hospital Of South Bend%' THEN 'Beacon Health System'
    WHEN {{ hospital_name }} LIKE 'Beaumont Health%' OR {{ hospital_name }} LIKE 'Beaumont Hospital%' THEN 'Beaumont Health'
    WHEN {{ hospital_name }} LIKE 'Barnes Jewish%' THEN 'BJC HealthCare'
    WHEN {{ hospital_name }} LIKE 'Saint Louis Childrens%' THEN 'BJC HealthCare'
    WHEN ({{ hospital_name }} LIKE 'Mercy Health%' OR {{ hospital_name }} LIKE 'Mercy Saint Anne%' OR {{ hospital_name }} LIKE 'Mercy Hospital Anderson%') AND {{ state }} IN ('OH', 'KY') THEN 'Bon Secours Mercy Health'
    WHEN ({{ hospital_name }} LIKE 'Saint Elizabeth%' OR {{ hospital_name }} LIKE 'Saint Charles Hospital%' OR ({{ hospital_name }} LIKE 'Saint Lukes Hospital%' AND {{ city }} = 'Maumee') OR {{ hospital_name }} LIKE 'Saint Ritas%' OR {{ hospital_name }} LIKE 'Saint Vincent Medical Center%') AND {{ state }} = 'OH' THEN 'Bon Secours Mercy Health'
    WHEN ({{ hospital_name }} LIKE 'Saint Marys Hospital%' OR {{ hospital_name }} LIKE 'Saint Francis Medical Center%') AND {{ state }} = 'VA' THEN 'Bon Secours Mercy Health'
    WHEN {{ hospital_name }} LIKE 'Broward Health%' THEN 'Broward Health'

    -- C
    WHEN {{ hospital_name }} LIKE 'Capital Health Medical Center%' THEN 'Capital Health'
    WHEN {{ hospital_name }} LIKE 'Bayonne Medical Center%' THEN 'CarePoint Health'
    WHEN {{ hospital_name }} LIKE 'Carilion%' THEN 'Carilion Clinic'
    WHEN {{ hospital_name }} LIKE 'Good Samaritan Hospital%' AND {{ city }} = 'West Islip' AND {{ state }} = 'NY' THEN 'Catholic Health (Long Island)'
    WHEN ({{ hospital_name }} LIKE 'Saint Charles Hospital%' OR ({{ hospital_name }} LIKE 'Saint Francis Hospital%' AND {{ city }} = 'Roslyn') OR ({{ hospital_name }} LIKE 'Saint Joseph Hospital%' AND {{ city }} = 'Bethpage')) AND {{ state }} = 'NY' THEN 'Catholic Health (Long Island)'
    WHEN ({{ hospital_name }} LIKE 'Mercy Hospital Of Buffalo%' OR ({{ hospital_name }} LIKE 'Mercy Medical Center%' AND {{ city }} = 'Rockville Centre')) THEN 'Catholic Health (NY)'
    WHEN {{ hospital_name }} LIKE 'Childrens Hospital & Res Center Oakland%' THEN 'Children\'s Hospital & Research Center Oakland'
    WHEN {{ hospital_name }} LIKE 'Childrens Hospital Of Orange Count%' THEN 'Children\'s Hospital of Orange County'
    WHEN {{ hospital_name }} LIKE 'Childrens Hospital Of The Kings Da%' THEN 'Children\'s Hospital of The King\'s Daughters'
    WHEN {{ hospital_name }} LIKE 'Childrens Hospital Of San Antonio%' THEN 'Children\'s Hospital of San Antonio'
    WHEN {{ hospital_name }} LIKE 'Cleveland Clinic%' OR {{ hospital_name }} LIKE 'Ccf %' OR {{ hospital_name }} = 'Hillcrest Hospital' OR {{ hospital_name }} = 'Huron Hospital' OR {{ hospital_name }} LIKE 'South Pointe Hospital%' THEN 'Cleveland Clinic'
    WHEN {{ hospital_name }} LIKE 'Memorial Health Care System%' AND {{ state }} = 'TN' THEN 'CommonSpirit Health'
    WHEN {{ hospital_name }} LIKE 'Mercy Medical Center Merced%' OR {{ hospital_name }} LIKE 'Mercy San Juan%' THEN 'CommonSpirit Health'
    WHEN {{ hospital_name }} LIKE 'Mercy Medical Center%' AND {{ city }} = 'Roseburg' THEN 'CommonSpirit Health'
    WHEN {{ hospital_name }} LIKE 'Saint Josephs Hospital%' AND {{ state }} = 'AZ' THEN 'CommonSpirit Health'
    WHEN ({{ hospital_name }} LIKE 'Saint Josephs Behavioral%' OR {{ hospital_name }} LIKE 'Saint Josephs Medical Center%' OR ({{ hospital_name }} LIKE 'Saint Mary Medical Center%' AND {{ city }} = 'Long Beach') OR ({{ hospital_name }} LIKE 'Saint Marys Medical Center%' AND {{ city }} = 'San Francisco')) AND {{ state }} = 'CA' THEN 'CommonSpirit Health'
    WHEN {{ hospital_name }} LIKE 'Saint Joseph Hospital%' AND {{ state }} = 'KY' THEN 'CommonSpirit Health'
    WHEN {{ hospital_name }} LIKE 'Saint Vincent%' AND {{ state }} = 'AR' THEN 'CommonSpirit Health'
    WHEN ({{ hospital_name }} LIKE 'Saint Francis Hospital%' OR {{ hospital_name }} LIKE 'Saint Joseph Medical Center%') AND {{ state }} = 'WA' THEN 'CommonSpirit Health'
    WHEN {{ hospital_name }} LIKE 'Saint Alexius Medical Center%' AND {{ state }} = 'ND' THEN 'CommonSpirit Health'
    WHEN {{ hospital_name }} LIKE 'Saint Rose Dominican%' THEN 'CommonSpirit Health'
    WHEN ({{ hospital_name }} LIKE 'Community Health Network%' OR {{ hospital_name }} LIKE 'Community Hospital South%') AND {{ state }} = 'IN' THEN 'Community Health Network'
    WHEN {{ hospital_name }} LIKE 'Community Regional Medical Center%' AND {{ state }} = 'CA' THEN 'Community Medical Centers (Fresno)'
    WHEN {{ hospital_name }} LIKE 'Cmh Of San Buenaventura%' THEN 'Community Memorial Healthcare'

    -- D
    WHEN {{ hospital_name }} LIKE 'Deaconess%' AND {{ state }} IN ('IN', 'KY') THEN 'Deaconess Health System'
    WHEN {{ hospital_name }} LIKE 'Dell Seton Medical %' THEN 'Dell Seton Medical Center'
    WHEN {{ hospital_name }} LIKE 'Doctors Hospital At Renaissance%' THEN 'DHR Health'
    WHEN {{ hospital_name }} LIKE 'Memorial Medical Center%' AND {{ state }} = 'PA' THEN 'Duke LifePoint Healthcare'

    -- E
    WHEN {{ hospital_name }} LIKE 'Saint Josephs Of Atlanta%' THEN 'Emory Healthcare'

    -- F
    WHEN {{ hospital_name }} LIKE 'Community Memorial Hospital%' AND {{ state }} = 'WI' THEN 'Froedtert Health'

    -- G
    WHEN {{ hospital_name }} LIKE 'Good Samaritan Hospital%' AND {{ city }} = 'Vincennes' AND {{ state }} = 'IN' THEN 'Good Samaritan (Vincennes)'

    -- H
    WHEN {{ hospital_name }} LIKE 'Saint Vincents Medical Center%' AND {{ state }} = 'CT' THEN 'Hartford HealthCare'
    WHEN {{ hospital_name }} LIKE 'HCA %' OR {{ hospital_name }} LIKE 'HCA Florida%' OR {{ hospital_name }} LIKE 'Cjw Medical Center%' OR {{ hospital_name }} LIKE 'Riverside Community Hospital%' OR {{ hospital_name }} LIKE 'Lewisgale%' OR {{ hospital_name }} LIKE 'Medical City %' OR {{ hospital_name }} LIKE 'Tristar %' OR {{ hospital_name }} = 'Swedish Medical Center' OR {{ hospital_name }} = 'West Florida Hospital' THEN 'HCA Healthcare'
    WHEN {{ hospital_name }} LIKE 'Houston Northwest Medical Center%' THEN 'HCA Healthcare'
    WHEN {{ hospital_name }} LIKE 'Houston Healthcare Medical Center%' AND {{ city }} = 'Houston' AND {{ state }} = 'TX' THEN 'HCA Healthcare'
    WHEN {{ hospital_name }} LIKE 'Memorial Health University%' OR {{ hospital_name }} LIKE 'Memorial Hospital Of Jacksonville%' OR {{ hospital_name }} LIKE 'Memorial Satilla%' THEN 'HCA Healthcare'
    WHEN {{ hospital_name }} LIKE 'Mercy Hospital%' AND {{ city }} = 'Miami' THEN 'HCA Healthcare'
    WHEN {{ hospital_name }} LIKE 'Methodist Hospital%' AND {{ city }} = 'San Antonio' THEN 'HCA Healthcare'
    WHEN {{ hospital_name }} LIKE 'Presbyterian Saint Lukes%' THEN 'HCA Healthcare'
    WHEN {{ hospital_name }} LIKE 'Saint Marks Hospital%' AND {{ state }} = 'UT' THEN 'HCA Healthcare'
    WHEN {{ hospital_name }} LIKE 'Saint Petersburg General%' THEN 'HCA Healthcare'
    WHEN {{ hospital_name }} LIKE 'W a Foote Memorial%' THEN 'Henry Ford Health'
    WHEN ({{ hospital_name }} LIKE 'Houston Medical Center%' OR {{ hospital_name }} LIKE 'Houston Healthcare%') AND {{ state }} = 'GA' THEN 'Houston Healthcare'
    WHEN ({{ hospital_name }} LIKE 'Saint Elizabeth Hospital%' OR {{ hospital_name }} LIKE 'Saint Johns Hospital%' OR {{ hospital_name }} LIKE 'Saint Marys Hospital%') AND {{ state }} = 'IL' THEN 'HSHS (Hospital Sisters)'

    -- I
    WHEN {{ hospital_name }} LIKE 'Bass Baptist%' THEN 'INTEGRIS Health'
    WHEN ({{ hospital_name }} LIKE 'Saint Joseph Hospital%' OR {{ hospital_name }} LIKE 'Saint Marys Hospital & Medical Center%' OR {{ hospital_name }} LIKE 'Saint Vincent Healthcare%') AND {{ state }} IN ('CO', 'MT') THEN 'Intermountain Health'

    -- J
    WHEN {{ hospital_name }} LIKE 'John H Stroger%' OR {{ hospital_name }} LIKE 'John H. Stroger%' THEN 'John H Stroger Jr Hospital Of Cook County'
    WHEN {{ hospital_name }} LIKE 'Johns Hopkins%' OR {{ hospital_name }} LIKE 'The Johns Hopkins%' THEN 'Johns Hopkins Hospital'
    WHEN {{ hospital_name }} LIKE 'Tchd D B A Jps%' THEN 'JPS Health Network'

    -- K
    WHEN {{ hospital_name }} LIKE 'KFH %' OR {{ hospital_name }} LIKE 'Kfh -%' OR {{ hospital_name }} LIKE 'KFH-%' OR {{ hospital_name }} LIKE 'Kaiser Foundation%' THEN 'Kaiser Permanente'

    -- L
    WHEN {{ hospital_name }} LIKE 'Memorial Medical Center%' AND {{ state }} = 'NM' THEN 'LifePoint Health'

    -- M
    WHEN {{ hospital_name }} LIKE 'Bryn Mawr%' THEN 'Main Line Health'
    WHEN {{ hospital_name }} LIKE 'Mclean Hospital%' THEN 'Mass General Brigham'
    WHEN {{ hospital_name }} LIKE 'Mayo Clinic%' OR {{ hospital_name }} LIKE 'Mayo Foundation%' OR {{ hospital_name }} LIKE 'Mchs-%' THEN 'Mayo Clinic'
    WHEN {{ hospital_name }} LIKE 'Bay Regional Medical Center%' THEN 'McLaren Health Care'
    WHEN {{ hospital_name }} LIKE 'Mclaren%' THEN 'McLaren Health Care'
    WHEN {{ hospital_name }} LIKE 'Ut Md Anderson%' THEN 'MD Anderson Cancer Center'
    WHEN {{ hospital_name }} LIKE 'Good Samaritan Hospital%' AND {{ city }} = 'Baltimore' AND {{ state }} = 'MD' THEN 'MedStar Health'
    WHEN {{ hospital_name }} LIKE 'Medstar%' THEN 'MedStar Health'
    WHEN {{ hospital_name }} LIKE 'Memorial Hospital At Gulfport%' THEN 'Memorial Health System (MS)'
    WHEN ({{ hospital_name }} LIKE 'Memorial Regional Hospital%' OR {{ hospital_name }} LIKE 'Memorial Hospital West%') AND {{ state }} = 'FL' THEN 'Memorial Healthcare System'
    WHEN {{ hospital_name }} LIKE 'Memorial Hermann%' OR {{ hospital_name }} LIKE 'Memorial Hermann Tirr%' THEN 'Memorial Hermann Health System'
    WHEN {{ hospital_name }} LIKE 'Memorial Hospital For Cancer And All%' THEN 'Memorial Sloan Kettering Cancer Center'
    WHEN {{ hospital_name }} LIKE 'Memorialcare%' THEN 'MemorialCare Health System'
    WHEN ({{ hospital_name }} LIKE 'Mercy Hospital Saint Louis%' OR {{ hospital_name }} LIKE 'Mercy Hospital Fort Smith%' OR ({{ hospital_name }} LIKE 'Mercy Medical Center%' AND {{ city }} = 'Rogers')) THEN 'Mercy'
    WHEN {{ hospital_name }} LIKE 'Mercy Medical Center%' AND {{ state }} = 'MD' THEN 'Mercy Medical Center (Baltimore)'
    WHEN {{ hospital_name }} LIKE 'Mercy Health System%' AND {{ state }} = 'WI' THEN 'MercyHealth (WI/IL)'
    WHEN {{ hospital_name }} LIKE 'Mercyone%' OR ({{ hospital_name }} LIKE 'Mercy Medical Center%' AND {{ city }} = 'Des Moines') THEN 'MercyOne'
    WHEN {{ hospital_name }} LIKE 'Methodist%Dallas%' OR {{ hospital_name }} LIKE 'Methodist Charlton%' THEN 'Methodist Health System (Dallas)'
    WHEN {{ hospital_name }} LIKE 'Methodist Hospitals%' AND {{ state }} = 'IN' THEN 'Methodist Hospitals (Indiana)'
    WHEN {{ hospital_name }} LIKE 'Methodist H C Memphis%' THEN 'Methodist Le Bonheur Healthcare'
    WHEN {{ hospital_name }} LIKE 'Midmichigan Medical%' THEN 'Mid Michigan Medical Center'
    WHEN {{ hospital_name }} LIKE 'Montefiore %' OR {{ hospital_name }} LIKE 'Saint Lukes Cornwall%' THEN 'Montefiore Health System'
    WHEN ({{ hospital_name }} LIKE 'Mount Sinai%' OR {{ hospital_name }} LIKE 'New York Eye And Ear%') AND {{ state }} = 'NY' THEN 'Mount Sinai Health System'
    WHEN {{ hospital_name }} LIKE 'Mount Sinai%' AND {{ state }} = 'FL' THEN 'Mount Sinai Medical Center (FL)'
    WHEN {{ hospital_name }} LIKE 'Capital Region Medical Center%' THEN 'MU Health Care'
    WHEN {{ hospital_name }} LIKE 'Deaconess%' AND {{ state }} = 'WA' THEN 'MultiCare Health System'

    -- N
    WHEN {{ hospital_name }} LIKE 'New York Presbyterian%' OR {{ hospital_name }} LIKE 'Newyork Presbyterian%' THEN 'New York-Presbyterian'
    WHEN {{ hospital_name }} LIKE 'New York-Presbyterian%' OR {{ hospital_name }} LIKE 'New York P.i%' THEN 'New York-Presbyterian Hospital'
    WHEN {{ hospital_name }} LIKE 'Northwell Health%' OR {{ hospital_name }} LIKE 'Long Island Jewish%' OR {{ hospital_name }} LIKE 'South Shore University%' THEN 'Northwell Health'
    WHEN {{ hospital_name }} LIKE 'Presbyterian Hospital%' AND {{ state }} = 'NC' THEN 'Novant Health'
    WHEN {{ hospital_name }} LIKE 'Nyc Health%' OR {{ hospital_name }} LIKE 'Elmhurst Hospital%' THEN 'NYC Health + Hospitals'
    WHEN {{ hospital_name }} LIKE 'Nyu %' OR {{ hospital_name }} = 'Lutheran Medical Center' THEN 'NYU Langone Health'

    -- O
    WHEN {{ hospital_name }} LIKE 'Ochsner%' THEN 'Ochsner Health'
    WHEN {{ hospital_name }} LIKE 'Doctors Hospital%' AND {{ city }} = 'Columbus' AND {{ state }} = 'OH' THEN 'OhioHealth'
    WHEN {{ hospital_name }} LIKE 'Bayfront Health Saint Petersburg%' THEN 'Orlando Health'
    WHEN ({{ hospital_name }} LIKE 'Saint Anthony Medical Center%' OR {{ hospital_name }} LIKE 'Saint Francis Medical Center%') AND {{ state }} = 'IL' THEN 'OSF HealthCare'

    -- P
    WHEN {{ hospital_name }} LIKE 'Presbyterian Medical Center%' AND {{ state }} = 'PA' THEN 'Penn Medicine'
    WHEN {{ hospital_name }} LIKE 'Good Samaritan Hospital%' AND {{ city }} = 'Dayton' AND {{ state }} = 'OH' THEN 'Premier Health'
    WHEN {{ hospital_name }} LIKE 'Presbyterian Hospital%' AND {{ state }} = 'NM' THEN 'Presbyterian Healthcare Services'
    WHEN ({{ hospital_name }} LIKE 'Saint Clares Hospital%' OR {{ hospital_name }} LIKE 'Saint Marys Hospital Passaic%' OR {{ hospital_name }} LIKE 'Saint Michaels Medical Center%') AND {{ state }} = 'NJ' THEN 'Prime Healthcare'
    WHEN {{ hospital_name }} LIKE 'Ph Baptist%' OR {{ hospital_name }} LIKE 'Ph Greer%' OR {{ hospital_name }} LIKE 'Ph Hillcrest%' OR {{ hospital_name }} LIKE 'Ph Patewood%' THEN 'Prisma Health'
    WHEN {{ hospital_name }} LIKE 'Providence %' OR {{ hospital_name }} LIKE 'Prov Regl%' OR {{ hospital_name }} LIKE 'Prov Sacred%' OR {{ hospital_name }} LIKE 'Uw Medicine/northwest%' THEN 'Providence'
    WHEN {{ hospital_name }} LIKE 'Saint Joseph Hospital Eureka%' THEN 'Providence St. Joseph Health'
    WHEN {{ hospital_name }} LIKE 'Saint Patrick Hospital%' AND {{ state }} = 'MT' THEN 'Providence St. Joseph Health'

    -- R
    WHEN {{ hospital_name }} LIKE 'Robert Wood Johnson%' THEN 'Robert Wood Johnson University Hospital'

    -- S
    WHEN {{ hospital_name }} LIKE 'Saint Lukes Regional%' AND {{ state }} = 'ID' THEN 'St. Luke\'s Health System (ID)'
    WHEN {{ hospital_name }} LIKE 'Saint Lukes Hospital Of Kansas City%' THEN 'Saint Luke\'s Health System (KC)'
    WHEN {{ hospital_name }} LIKE 'Saint Lukes%' AND {{ state }} = 'MO' THEN 'St. Luke\'s Hospital (St. Louis)'
    WHEN {{ hospital_name }} LIKE 'Saint Luke%' AND {{ state }} = 'PA' THEN 'St. Luke\'s University Health Network'
    WHEN {{ hospital_name }} LIKE 'Sanford%' THEN 'Sanford Health'
    WHEN {{ hospital_name }} LIKE 'Sbh Health System%' THEN 'SBH Health System'
    WHEN {{ hospital_name }} LIKE 'Scripps%' THEN 'Scripps Health'
    WHEN {{ hospital_name }} LIKE 'Sentara%' THEN 'Sentara Healthcare'
    WHEN {{ hospital_name }} LIKE 'Memorial Hospital Of Carbondale%' THEN 'Southern Illinois Healthcare'
    WHEN ({{ hospital_name }} LIKE 'Saint Clare Hospital%' AND {{ state }} = 'WI') OR ({{ hospital_name }} LIKE 'Saint Marys Medical Center%' AND {{ city }} = 'Blue Springs') THEN 'SSM Health'
    WHEN {{ hospital_name }} LIKE 'Good Samaritan Medical Center%' AND {{ state }} = 'MA' THEN 'Steward Health Care'
    WHEN {{ hospital_name }} LIKE 'Saint Joseph Medical Center%' AND {{ city }} = 'Houston' THEN 'Steward Health Care'

    -- T
    WHEN {{ hospital_name }} LIKE 'Doctors Medical Center Of Modesto%' THEN 'Tenet Healthcare'
    WHEN {{ hospital_name }} LIKE 'Saint Francis Hospital%' AND {{ state }} = 'TN' THEN 'Tenet Healthcare'
    WHEN {{ hospital_name }} LIKE 'Saint Vincent Hospital%' AND {{ state }} = 'MA' THEN 'Tenet Healthcare'
    WHEN {{ hospital_name }} LIKE 'Saint Marys Medical Center%' AND {{ city }} = 'West Palm Beach' THEN 'Tenet Healthcare'
    WHEN {{ hospital_name }} LIKE 'Tx Health Harris Methodist%' THEN 'Texas Health Resources'
    WHEN {{ hospital_name }} LIKE 'The Childrens Hospital Of Phila%' THEN 'The Children\'s Hospital of Philadelphia'
    WHEN {{ hospital_name }} LIKE 'Thedacare Regiona%' THEN 'Thedacare Regional Medical Center'
    WHEN {{ hospital_name }} LIKE 'Good Samaritan Hospital%' AND {{ city }} = 'Cincinnati' AND {{ state }} = 'OH' THEN 'TriHealth'
    WHEN {{ hospital_name }} LIKE 'Mercy Health%' AND {{ state }} = 'MI' THEN 'Trinity Health'
    WHEN {{ hospital_name }} LIKE 'Saint Joseph Mercy%' OR {{ hospital_name }} LIKE 'Saint Mary Mercy%' OR {{ hospital_name }} LIKE 'Saint Marys Of Michigan%' OR ({{ hospital_name }} LIKE 'Saint Marys Health Care%' AND {{ state }} = 'MI') THEN 'Trinity Health'
    WHEN {{ hospital_name }} LIKE 'Saint Alphonsus%' THEN 'Trinity Health'
    WHEN {{ hospital_name }} LIKE 'Saint Anns Hospital%' AND {{ state }} = 'OH' THEN 'Trinity Health'
    WHEN ({{ hospital_name }} LIKE 'Saint Francis Hospital%' OR {{ hospital_name }} LIKE 'Saint Marys Hospital%') AND {{ state }} = 'CT' THEN 'Trinity Health'
    WHEN {{ hospital_name }} LIKE 'Saint Peters Hospital%' AND {{ city }} = 'Albany' THEN 'Trinity Health'
    WHEN {{ hospital_name }} LIKE 'Saint Josephs Hospital Health Center%' AND {{ city }} = 'Syracuse' THEN 'Trinity Health'
    WHEN {{ hospital_name }} LIKE 'Saint Mary Medical Center%' AND {{ city }} = 'Langhorne' THEN 'Trinity Health'
    WHEN {{ hospital_name }} LIKE 'Saint Josephs Reg Medical Center%' AND {{ state }} = 'IN' THEN 'Trinity Health'
    WHEN {{ hospital_name }} LIKE 'Saint Marys Health Care System%' AND {{ city }} = 'Athens' THEN 'Trinity Health'
    WHEN {{ hospital_name }} LIKE 'Saint Francis Hospital Wilmington%' THEN 'Trinity Health'
    WHEN {{ hospital_name }} LIKE 'Saint Agnes Medical Center%' AND {{ city }} = 'Fresno' THEN 'Trinity Health'

    -- U
    WHEN {{ hospital_name }} LIKE 'Uams Medical Center%' THEN 'UAMS Health'
    WHEN {{ hospital_name }} LIKE 'Memorial Health System%' AND {{ state }} = 'CO' THEN 'UCHealth'
    WHEN {{ hospital_name }} LIKE 'Uf Health%' THEN 'UF Health'
    WHEN {{ hospital_name }} LIKE 'Umass Memorial%' THEN 'UMass Memorial Health'
    WHEN {{ hospital_name }} LIKE 'Uhs Hospitals%' THEN 'United Health Services (NY)'
    WHEN {{ hospital_name }} LIKE 'Methodist Medical Center Of Illinois%' THEN 'UnityPoint Health'
    WHEN {{ hospital_name }} LIKE 'Saint Lukes%' AND {{ state }} = 'IA' THEN 'UnityPoint Health'
    WHEN {{ hospital_name }} LIKE 'Uh %' OR {{ hospital_name }} LIKE 'Uh Ahuja%' OR {{ hospital_name }} LIKE 'Uh Cleveland%' THEN 'University Hospitals (Cleveland)'
    WHEN {{ hospital_name }} LIKE 'Uc Davis%' OR {{ hospital_name }} LIKE 'Uci Medical%' OR {{ hospital_name }} LIKE 'Ucsd Medical%' OR {{ hospital_name }} LIKE 'Ucsf Medical%' THEN 'University of California Health'
    WHEN {{ hospital_name }} LIKE 'U Of U Hospitals%' OR {{ hospital_name }} LIKE 'U Of Utah%' THEN 'University of Utah Health'
    WHEN {{ hospital_name }} LIKE 'Unm Sandoval%' THEN 'UNM Health'
    WHEN {{ hospital_name }} LIKE 'Uofl Health%' THEN 'UofL Health'
    WHEN {{ hospital_name }} LIKE 'Upmc%' OR {{ hospital_name }} LIKE 'University Of Pittsburgh Med%' THEN 'UPMC'
    WHEN {{ hospital_name }} LIKE 'Memorial Hospital%' AND {{ city }} = 'York' THEN 'UPMC'
    WHEN {{ hospital_name }} LIKE 'Upmc Childrens Hospital Of Pgh' THEN 'UPMC Children\'s Hospital of Pittsburgh'
    WHEN {{ hospital_name }} LIKE 'Ut Health%Tyler%' THEN 'UT Health East Texas'
    WHEN {{ hospital_name }} LIKE 'Ut Southwestern%' THEN 'UT Southwestern Medical Center'
    WHEN {{ hospital_name }} LIKE 'The University Of Texas Medical Br%' THEN 'UTMB Health'
    WHEN {{ hospital_name }} LIKE 'University Of Washington%' OR {{ hospital_name }} LIKE 'Uw Medicine%' OR {{ hospital_name }} = 'Valley Medical Center' THEN 'UW Medicine'

    -- General standardization (applied before ELSE fallback)
    WHEN {{ hospital_name }} LIKE 'Childrens Hospital%' AND {{ hospital_name }} NOT LIKE 'Children\'s%'
        THEN REPLACE({{ hospital_name }}, 'Childrens Hospital', 'Children\'s Hospital')
    WHEN {{ hospital_name }} LIKE 'St. %'
        THEN REPLACE({{ hospital_name }}, 'St. ', 'Saint ')
    WHEN {{ hospital_name }} LIKE 'St %'
        THEN REPLACE({{ hospital_name }}, 'St ', 'Saint ')
    WHEN {{ hospital_name }} LIKE 'Univ Of %'
        THEN REPLACE({{ hospital_name }}, 'Univ Of', 'University of')
    WHEN {{ hospital_name }} LIKE '%Of Pgh'
        THEN REPLACE({{ hospital_name }}, 'Of Pgh', 'Of Pittsburgh')

    ELSE INITCAP(TRIM({{ hospital_name }}))
END
{% endmacro %}
