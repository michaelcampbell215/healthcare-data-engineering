-- =========================================================================
-- Build Reference Table: Product Categories
-- Purpose: Map messy raw categories (typos, variations) to standardized buckets
-- =========================================================================

CREATE TABLE IF NOT EXISTS ref_product_categories (
    original_category VARCHAR(255) PRIMARY KEY,
    standardized_category VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =========================================================================
-- SEED DATA: High-Volume Mappings
-- =========================================================================
INSERT INTO ref_product_categories (original_category, standardized_category) VALUES
    -- Cardiovascular Grouping
    ('Cardiology', 'Cardiovascular Disease'),
    ('Cardiovascular', 'Cardiovascular Disease'),
    ('Cardiovascular And Metabolism', 'Cardiovascular Disease'),
    ('Cardiovascular & Metabolism', 'Cardiovascular Disease'), -- New
    ('Cardiovascular;metabolic Disease', 'Cardiovascular Disease'), -- New
    ('Cardiology/Vascular Diseases', 'Cardiovascular Disease'),
    ('Cardio-Renal', 'Cardiovascular Disease'), -- New
    ('Heart Failure', 'Cardiovascular Disease'),
    ('Heart Failure And Hypertension', 'Cardiovascular Disease'),
    ('Hypertension', 'Cardiovascular Disease'),
    ('Thrombosis', 'Cardiovascular Disease'),
    ('Atrial Fibrillation', 'Cardiovascular Disease'),
    ('Electrophysiology', 'Cardiovascular Disease'),

    -- Orthopedics
    ('Orthopedics', 'Orthopedics'),
    ('Orthopaedics', 'Orthopedics'),
    ('Orhtopedics', 'Orthopedics'),
    ('Orthpedics', 'Orthopedics'),
    ('Orhopedic', 'Orthopedics'),
    ('Orthpdec', 'Orthopedics'),
    ('Orthopedic Surgery', 'Orthopedics'),
    ('Spine', 'Orthopedics'),
    ('Spine Surgery', 'Orthopedics'),
    ('Joint Reconstruction', 'Orthopedics'),
    ('Trauma', 'Orthopedics'),
    ('Sports Medicine', 'Orthopedics'),
    ('Bone Health', 'Orthopedics'), -- New

    -- Ophthalmology
    ('Ophthalmology', 'Ophthalmology'),
    ('Opthalamics', 'Ophthalmology'),
    ('Opthlamics', 'Ophthalmology'),
    ('Ophthamology', 'Ophthalmology'),
    ('Optometry', 'Ophthalmology'),
    ('Eye Care', 'Ophthalmology'),
    ('Glaucoma', 'Ophthalmology'),
    ('Retina', 'Ophthalmology'),

    -- Neuroscience & Neurology
    ('Neuroscience', 'Neurology'),
    ('Neurology', 'Neurology'),
    ('Cns', 'Neurology'),
    ('Central Nervous System', 'Neurology'),
    ('Multiple Sclerosis', 'Neurology'),
    ('Epilepsy', 'Neurology'),
    ('Migraine', 'Neurology'),
    ('Parkinson\'s Disease', 'Neurology'),

    -- Psychiatry & Mental Health
    ('Psychiatry', 'Psychiatry & Mental Health'),
    ('Mental Health', 'Psychiatry & Mental Health'),
    ('Schizophrenia', 'Psychiatry & Mental Health'),
    ('Depression', 'Psychiatry & Mental Health'),
    ('Psychology', 'Psychiatry & Mental Health'),
    ('Adhd', 'Psychiatry & Mental Health'),
    ('Psychology/Psychiatric', 'Psychiatry & Mental Health'), -- New
    ('Neuropsychiatry', 'Psychiatry & Mental Health'), -- New
    ('Cns Stimulant For Adhd', 'Psychiatry & Mental Health'), -- New
    ('Viloxazine Hydrochloride', 'Psychiatry & Mental Health'), -- New (ADHD Drug)

    -- Endocrinology & Diabetes
    ('Endocrinology', 'Endocrinology'),
    ('Diabetes', 'Diabetes'),
    ('Diabetes Care', 'Diabetes'), -- New
    ('Obesity', 'Obesity'),
    ('Metabolism', 'Endocrinology'),
    ('Thyroid', 'Endocrinology'),

    -- Immunology & Inflammation
    ('Immunology', 'Immunology & Inflammation'),
    ('Inflammation', 'Immunology & Inflammation'),
    ('Rheumatology', 'Immunology & Inflammation'),
    ('Autoimmune', 'Immunology & Inflammation'),
    ('Allergy', 'Immunology & Inflammation'),
    ('Inflammation And Immunology', 'Immunology & Inflammation'), -- New (Fixes 131k rows)
    ('Inflammation/Rare Disease', 'Immunology & Inflammation'), -- New

    -- Pain Management
    ('Pain', 'Pain Management'), -- New (Standardizing on "Management")
    ('Pain Management', 'Pain Management'), -- New
    ('Acute Pain', 'Pain Management'),

    -- Oncology
    ('Oncology', 'Oncology'),
    ('Cancer', 'Oncology'),
    ('Hematology', 'Oncology'), 
    ('Hematology/Oncology', 'Oncology'),
    
    -- Gastro
    ('Gastroenterology', 'Gastroenterology'),
    ('Gi', 'Gastroenterology'),
    ('Digestive Health', 'Gastroenterology'),

    -- Respiratory
    ('Respiratory', 'Respiratory'),
    ('Pulmonology', 'Respiratory'),
    ('Asthma', 'Respiratory'),
    ('Copd', 'Respiratory'),
    ('Pulmonary', 'Respiratory'),

    -- Urology (Validating)
    ('Urology', 'Urology'), -- New

    -- Internal Medicine
    ('Internal Medicine', 'Internal Medicine'), -- New

    -- Surgery (General)
    ('Surgery', 'Surgery'),
    ('General Surgery', 'Surgery'),
    ('Robotic Surgery', 'Surgery'),
    ('Devices', 'Medical Devices'), -- New (Standardizing generic term)
    ('Electromedical And Electrotherapeutic Apparatus', 'Medical Devices') -- New

ON DUPLICATE KEY UPDATE standardized_category = VALUES(standardized_category);

SELECT CONCAT('Reference Table Updated. Total Mappings: ', COUNT(*)) as status FROM ref_product_categories;