-- =========================================================================
-- Setup Custom Cleaning Functions & Procedures
-- =========================================================================

-- DROP Obsolete or Existing Functions
DROP FUNCTION IF EXISTS `surname_ProperCase`;
DROP FUNCTION IF EXISTS `TitleCase`;
DROP FUNCTION IF EXISTS `CleanAddress`;
DROP FUNCTION IF EXISTS `StandardizeDelimiter`;
DROP FUNCTION IF EXISTS `CleanName`;
DROP FUNCTION IF EXISTS `CleanManufactureName`;
DROP FUNCTION IF EXISTS `CleanHospitalName`;
DROP FUNCTION IF EXISTS `CleanSpecialty`;
DROP FUNCTION IF EXISTS `CleanProductName`;
DROP FUNCTION IF EXISTS `CleanCity`;
DROP PROCEDURE IF EXISTS `BatchCleanHospital`;

DELIMITER $$

-- =========================================================================
-- FUNCTION: TitleCase
-- =========================================================================
CREATE FUNCTION `TitleCase`(str VARCHAR(255)) RETURNS varchar(255) CHARSET utf8mb4
    DETERMINISTIC
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE len INT;
    DECLARE result VARCHAR(255);
    DECLARE c CHAR(1);
    IF str IS NULL THEN RETURN NULL; END IF;
    SET result = LOWER(TRIM(str));
    SET result = REPLACE(result, ' - ', '-'); 
    SET result = REPLACE(result, ' -', '-');  
    SET result = REPLACE(result, '- ', '-'); 
    WHILE INSTR(result, '  ') > 0 DO SET result = REPLACE(result, '  ', ' '); END WHILE;
    SET len = LENGTH(result);
    WHILE (i <= len) DO
        IF i = 1 OR SUBSTRING(result, i - 1, 1) IN (' ', '-', '(', '/') THEN            
            SET c = UPPER(SUBSTRING(result, i, 1));
            SET result = CONCAT(LEFT(result, i - 1), c, SUBSTRING(result, i + 1));
        END IF;        
        SET i = i + 1;
    END WHILE;
    RETURN result;
END$$

-- =========================================================================
-- FUNCTION: CleanAddress 
-- =========================================================================
CREATE FUNCTION `CleanAddress`(address VARCHAR(255)) RETURNS varchar(255) CHARSET utf8mb4
    DETERMINISTIC
BEGIN
    IF address IS NULL THEN RETURN NULL; END IF;

    SET address = UPPER(address);

    -- Prefix 'ST' check (Saint vs Street)
    SET address = REGEXP_REPLACE(address, '^ST\\.?\\b', 'SAINT');

    -- Street Suffixes
    SET address = REGEXP_REPLACE(address, '\\bAVE\\.?\\b', 'AVENUE');
    SET address = REGEXP_REPLACE(address, '\\bBLVD\\.?\\b', 'BOULEVARD');
    SET address = REGEXP_REPLACE(address, '\\bCTR\\.?\\b', 'CENTER');
    SET address = REGEXP_REPLACE(address, '\\bCIR\\.?\\b', 'CIRCLE');
    SET address = REGEXP_REPLACE(address, '\\bCT\\.?\\b', 'COURT');
    SET address = REGEXP_REPLACE(address, '\\bDR\\.?\\b', 'DRIVE');
    SET address = REGEXP_REPLACE(address, '\\bHWY\\.?\\b', 'HIGHWAY');
    SET address = REGEXP_REPLACE(address, '\\bLN\\.?\\b', 'LANE');
    SET address = REGEXP_REPLACE(address, '\\bPKWY\\.?\\b', 'PARKWAY');
    SET address = REGEXP_REPLACE(address, '\\bPL\\.?\\b', 'PLACE');
    SET address = REGEXP_REPLACE(address, '\\bPLZ\\.?\\b', 'PLAZA'); -- Added PLZ
    SET address = REGEXP_REPLACE(address, '\\bRD\\.?\\b', 'ROAD');
    SET address = REGEXP_REPLACE(address, '\\bSQ\\.?\\b', 'SQUARE');
    SET address = REGEXP_REPLACE(address, '\\bST\\.?\\b', 'STREET'); 
    SET address = REGEXP_REPLACE(address, '\\bTER\\.?\\b', 'TERRACE');
    SET address = REGEXP_REPLACE(address, '\\bVLY\\.?\\b', 'VALLEY');
    SET address = REGEXP_REPLACE(address, '\\bTRL\\.?\\b', 'TRAIL');
    SET address = REGEXP_REPLACE(address, '\\bJCT\\.?\\b', 'JUNCTION');
    SET address = REGEXP_REPLACE(address, '\\bMTN\\.?\\b', 'MOUNTAIN');

    -- Highway & High-Speed Road Additions
    SET address = REGEXP_REPLACE(address, '\\bPKE\\.?\\b', 'PIKE');
    SET address = REGEXP_REPLACE(address, '\\bEXPY\\.?\\b', 'EXPRESSWAY');
    SET address = REGEXP_REPLACE(address, '\\bFWY\\.?\\b', 'FREEWAY');
    SET address = REGEXP_REPLACE(address, '\\bTPKE\\.?\\b', 'TURNPIKE');

    -- Unit Designators
    SET address = REGEXP_REPLACE(address, '\\bSTE\\.?\\b', 'SUITE');
    SET address = REGEXP_REPLACE(address, '\\bAPT\\.?\\b', 'APARTMENT');
    SET address = REGEXP_REPLACE(address, '\\bBLDG\\.?\\b', 'BUILDING');
    SET address = REGEXP_REPLACE(address, '\\bUNIT\\b', 'UNIT');
    SET address = REGEXP_REPLACE(address, '\\bFL\\.?\\b', 'FLOOR');
    SET address = REGEXP_REPLACE(address, '\\bBOX\\b', 'PO BOX');

    -- Directionals (PROTECTED from possessives like Women's)
    -- We ensure N/S/E/W are surrounded by whitespace or start/end of string
    -- and explicitly NOT preceded by an apostrophe.
    
    SET address = REGEXP_REPLACE(address, '(^|\\s)N\\.?($|\\s)', ' NORTH ');
    SET address = REGEXP_REPLACE(address, '(^|\\s)S\\.?($|\\s)', ' SOUTH ');
    SET address = REGEXP_REPLACE(address, '(^|\\s)E\\.?($|\\s)', ' EAST ');
    SET address = REGEXP_REPLACE(address, '(^|\\s)W\\.?($|\\s)', ' WEST ');
    
    SET address = REGEXP_REPLACE(address, '\\bNE\\.?\\b', 'NORTHEAST');
    SET address = REGEXP_REPLACE(address, '\\bNW\\.?\\b', 'NORTHWEST');
    SET address = REGEXP_REPLACE(address, '\\bSE\\.?\\b', 'SOUTHEAST');
    SET address = REGEXP_REPLACE(address, '\\bSW\\.?\\b', 'SOUTHWEST');

    -- Clean up extra spaces and trim ends
    SET address = REGEXP_REPLACE(address, ' +', ' ');

    RETURN TitleCase(TRIM(address)); 
END$$

-- =========================================================================
-- FUNCTION: CleanManufactureName
-- =========================================================================
CREATE FUNCTION CleanManufactureName(raw_name VARCHAR(255)) RETURNS VARCHAR(255) DETERMINISTIC
BEGIN
    DECLARE clean_name VARCHAR(255);
    IF raw_name IS NULL THEN RETURN NULL; END IF;
    SET clean_name = raw_name;
    
    -- 1. Standard Suffixes
    SET clean_name = REGEXP_REPLACE(clean_name, '(?i)\\b(Inc|Incorporated|Llc|L\\.L\\.C|Ltd|Limited|Corp|Corporation|Lp|L\\.P|Co|Company|Plc|Gmbh|A\\.G|S\\.A|S\\.R\\.L|P\\.C)\\b\\.?', '');
    
    -- 2. International / Medical Specific variations
    SET clean_name = REGEXP_REPLACE(clean_name, '(?i)\\bUsa\\b', 'USA');
    SET clean_name = REGEXP_REPLACE(clean_name, '\\bUs$', 'US');
    
    -- 3. Cleanup punctuation and whitespace
    SET clean_name = TRIM(clean_name);
    SET clean_name = REGEXP_REPLACE(clean_name, '[,. ]+$', ''); -- Trailing commas/dots/spaces
    SET clean_name = REGEXP_REPLACE(clean_name, ' +', ' '); -- Collapse multiple spaces
    
    return TRIM(clean_name);
END $$

-- =========================================================================
-- FUNCTION: CleanName
-- =========================================================================
CREATE FUNCTION CleanName(raw_name VARCHAR(255)) RETURNS VARCHAR(255) DETERMINISTIC
BEGIN
    DECLARE clean_name VARCHAR(255);
    IF raw_name IS NULL THEN RETURN NULL; END IF;
    
    SET clean_name = TRIM(raw_name);
    
    -- Remove wrapping quotes and parentheses
    SET clean_name = TRIM(BOTH '\'' FROM clean_name);
    SET clean_name = TRIM(BOTH '"' FROM clean_name);
    SET clean_name = REPLACE(REPLACE(clean_name, '(', ' '), ')', ' ');
    
    -- Standardize Titles
    SET clean_name = REGEXP_REPLACE(clean_name, '(?i)\\bDOCTOR\\b|\\bDR\\.?\\b', 'Dr');
    SET clean_name = REGEXP_REPLACE(clean_name, '(?i)\\bPROFESSOR\\b|\\bPROF\\.?\\b', 'Prof');
    
    -- Strip all NON-LETTERS from start and end
    SET clean_name = REGEXP_REPLACE(clean_name, '^[^a-zA-Z]+', '');
    SET clean_name = REGEXP_REPLACE(clean_name, '[^a-zA-Z]+$', '');
    
    -- Collapse spaces
    SET clean_name = REGEXP_REPLACE(clean_name, '[[:space:]]+', ' ');
    
    -- Handle Explicit Garbage Tokens
    IF UPPER(TRIM(clean_name)) IN ('-', '---', '0', '1', 'NONE', 'N/A', '') THEN 
        RETURN NULL; 
    END IF; 
    
    IF LENGTH(clean_name) < 1 THEN RETURN NULL; END IF;

    RETURN TitleCase(TRIM(clean_name));
END $$

-- =========================================================================
-- FUNCTION: CleanSpecialty
-- =========================================================================
CREATE FUNCTION CleanSpecialty(input_spec VARCHAR(500)) RETURNS VARCHAR(500) CHARSET utf8mb4
    DETERMINISTIC
BEGIN
    DECLARE result VARCHAR(500);
    IF input_spec IS NULL THEN RETURN NULL; END IF;
    SET result = input_spec;
    SET result = REGEXP_REPLACE(result, '^(Allopathic|Physician Assistants|Podiatric|Dental|Eye|Chiropractic|Nursing)[^|]*\\|', '');
    RETURN TRIM(result);
END $$

-- =========================================================================
-- FUNCTION: CleanProductName
-- =========================================================================
CREATE FUNCTION CleanProductName(raw_name VARCHAR(255)) RETURNS VARCHAR(255) CHARSET utf8mb4
    DETERMINISTIC
BEGIN
    DECLARE result VARCHAR(255);
    IF raw_name IS NULL THEN RETURN NULL; END IF;
    
    -- Base Clean (TitleCase)
    SET result = TitleCase(TRIM(raw_name));
    
    -- Strip Trademarks 
    SET result = REPLACE(result, '(R)', '');
    SET result = REPLACE(result, '(TM)', '');
    
    -- Strip Generic Device Suffixes
    SET result = REGEXP_REPLACE(result, ' Collection Kit$', '');
    SET result = REGEXP_REPLACE(result, ' Transmitter$', '');
    
    -- Strip Trailing Comma + Single Char (", S")
    SET result = REGEXP_REPLACE(result, ', [a-zA-Z0-9]$', '');
    
    RETURN TRIM(result);
END $$


-- =========================================================================
-- FUNCTION: CleanCity
-- =========================================================================
CREATE FUNCTION CleanCity(recipient_city VARCHAR(255)) RETURNS VARCHAR(255)
DETERMINISTIC
BEGIN
    DECLARE result VARCHAR(255);
    SET result = recipient_city;

    -- Using \b ensures we match the word, not just a string of letters
    SET result = REGEXP_REPLACE(result, '\\bHts\\.?$', 'Heights');
    SET result = REGEXP_REPLACE(result, '\\bHgts$', 'Heights');
    SET result = REGEXP_REPLACE(result, '\\bHls$', 'Hills');
    SET result = REGEXP_REPLACE(result, '\\bSp(r)?gs$', 'Springs');
    SET result = REGEXP_REPLACE(result, '\\bTwp$', 'Township');
    SET result = REGEXP_REPLACE(result, '\\bFt$', 'Fort');
    SET result = REGEXP_REPLACE(result, '\\bMt$', 'Mount');
    SET result = REGEXP_REPLACE(result, '\\bSt\\.$', 'Saint');
    -- Only match 'St' if it is a whole word to avoid 'West' -> 'WeSaint'
    SET result = REGEXP_REPLACE(result, '\\bSt$', 'Saint'); 
    SET result = REGEXP_REPLACE(result, '\\bSte$', 'Sainte');

    RETURN result;
END $$


-- =========================================================================
-- FUNCTION: CleanHospitalName
-- =========================================================================
CREATE FUNCTION CleanHospitalName(input_name VARCHAR(255)) 
RETURNS VARCHAR(255)
DETERMINISTIC
BEGIN
    DECLARE result VARCHAR(255);
    SET result = input_name;

    SET result = REGEXP_REPLACE(result, '(?i)\\bMem\\b\\.?', 'Memorial');
    SET result = REGEXP_REPLACE(result, '(?i)\\bHospt\\b\\.?', 'Hospital');
    SET result = REGEXP_REPLACE(result, '(?i)\\bDist\\b\\.?', 'District');
    SET result = REGEXP_REPLACE(result, '(?i)\\bCo\\b\\.?(?=\\s+(Hosp|Med|Dist))', 'County');
    SET result = REGEXP_REPLACE(result, '(?i)\\bFlorid\\b', 'Florida');
    SET result = REGEXP_REPLACE(result, '(?i)Medical\\s+(Ct|Ctn|Ce|Cente)\\b\\.?', 'Medical Center');
    SET result = REGEXP_REPLACE(result, '(?i)\\bSt\\b\\.?', 'Saint');
    SET result = REGEXP_REPLACE(result, '(?i)\\bInstit\\b\\.?', 'Institute');
    SET result = REGEXP_REPLACE(result, '(?i)\\bHlth\\b', 'Health');
    SET result = REGEXP_REPLACE(result, '(?i)\\b(Hosp|Hos|Hospitalital)\\b\\.?', 'Hospital');
    SET result = REGEXP_REPLACE(result, '(?i)\\bCtrs\\b\\.?', 'Centers');
    SET result = REGEXP_REPLACE(result, '(?i)\\b(Ctr|Cntr|Cent)\\b\\.?', 'Center');
    SET result = REGEXP_REPLACE(result, '(?i)\\bChildr\\b', 'Children');
    SET result = REGEXP_REPLACE(result, '(?i)\\bMed\\b\\.?', 'Medical');
    SET result = REGEXP_REPLACE(result, '(?i)\\bUniv\\b\\.?', 'University');
    SET result = REGEXP_REPLACE(result, '(?i)\\bKfh\\b\\.?', 'KFH');
    SET result = REGEXP_REPLACE(result, '(?i)\\bSys\\b\\.?', 'System');
    SET result = REGEXP_REPLACE(result, '(?i)\\bInc\\.?\\b|\\bIncorporated\\b', '');
    SET result = REGEXP_REPLACE(result, '(?i)\\bL\\.?L\\.?C\\.?\\b', '');
    SET result = REGEXP_REPLACE(result, '(?i)\\bLtd\\.?\\b|\\bLimited\\b', '');
    SET result = REGEXP_REPLACE(result, '(?i)\\bCorp\\.?\\b|\\bCorporation\\b', '');
    SET result = REGEXP_REPLACE(result, '(?i)\\bP\\.?C\\.?\\b', '');
    SET result = REGEXP_REPLACE(result, '[,.]+$', '');

    RETURN TRIM(REGEXP_REPLACE(result, ' +', ' '));
END$$

DELIMITER ;