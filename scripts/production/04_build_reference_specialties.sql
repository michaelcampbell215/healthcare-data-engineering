-- =========================================================================
-- Build Reference Table: Recipient Specialties
-- Purpose: Map synonyms (Family -> Family Medicine) and fix spellings
-- =========================================================================

CREATE TABLE IF NOT EXISTS ref_specialties (
    original_specialty VARCHAR(255) PRIMARY KEY,
    standardized_specialty VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =========================================================================
-- SEED DATA: Top 20 & Common Variations
-- =========================================================================
INSERT INTO ref_specialties (original_specialty, standardized_specialty) VALUES
    -- Family Medicine
    ('Family', 'Family Medicine'),
    ('Family Health', 'Family Medicine'),
    ('General Practice', 'Family Medicine'), -- Often grouped, or keep separate? Standardizing for now.

    -- Internal Medicine
    ('Internist', 'Internal Medicine'),
    ('Adult Medicine', 'Internal Medicine'),

    -- Psychiatry
    ('Psychiatric/Mental Health', 'Psychiatry & Mental Health'),
    ('Psychiatry', 'Psychiatry & Mental Health'),
    ('Psychiatric/Mental Health, Adult', 'Psychiatry & Mental Health'),
    ('Psychiatric/Mental Health, Child & Adolescent', 'Psychiatry & Mental Health'),
    ('Behavioral Neurology & Neuropsychiatry', 'Psychiatry & Mental Health'),

    -- Orthopedics (Spelling & Abbreviations)
    ('Orthopaedic Surgery', 'Orthopedic Surgery'),
    ('Orthopaedic', 'Orthopedic Surgery'),
    ('Orthopedic', 'Orthopedic Surgery'),
    ('Orthopaedic Trauma', 'Orthopedic Surgery'),
    ('Adult Reconstructive Orthopaedic Surgery', 'Orthopedic Surgery'),
    ('Foot & Ankle Surgery', 'Orthopedic Surgery'), -- Often a sub-specialty, but usually rolled up
    ('Hand Surgery', 'Orthopedic Surgery'),

    -- Eye Care
    ('Ophthalmology', 'Ophthalmology'),
    ('Ophthalmic', 'Ophthalmology'),
    ('Optometrist', 'Optometry'),
    ('Optician', 'Optometry'),

    -- Nursing / PA
    ('Nurse Practitioner', 'Advanced Practice Nursing'),
    ('Clinical Nurse Specialist', 'Advanced Practice Nursing'),
    ('Registered Nurse', 'Nursing'),
    ('Physician Assistant', 'Physician Assistant'),
    ('PA', 'Physician Assistant'),

    -- Oncology
    ('Hematology & Oncology', 'Oncology'),
    ('Hematology', 'Oncology'), -- Often grouped in commercial analysis
    ('Medical Oncology', 'Oncology'),
    ('Gynecologic Oncology', 'Oncology'),

    -- Cardiology
    ('Cardiovascular Disease', 'Cardiology'),
    ('Interventional Cardiology', 'Cardiology'),
    ('Nuclear Cardiology', 'Cardiology'),
    ('Advanced Heart Failure and Transplant Cardiology', 'Cardiology'),

    -- Women's Health
    ('Obstetrics & Gynecology', 'Ob/Gyn'),
    ('Obstetrics', 'Ob/Gyn'),
    ('Gynecology', 'Ob/Gyn'),
    ('Women\'s Health', 'Ob/Gyn'),

    -- Standardization
    ('Foot and Ankle Surgery', 'Podiatry'), -- Or Ortho? CMS distinguishes Podiatrists.
    ('Podiatrist', 'Podiatry'),
    ('Oral & Maxillofacial Surgery', 'Oral & Maxillofacial Surgery'),
    ('Oral and Maxillofacial Surgery', 'Oral & Maxillofacial Surgery')

ON DUPLICATE KEY UPDATE standardized_specialty = VALUES(standardized_specialty);

SELECT CONCAT('Specialty Ref Table Built. Mappings: ', COUNT(*)) as status FROM ref_specialties;
