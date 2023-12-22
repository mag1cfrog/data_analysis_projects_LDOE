-- sql
DROP TABLE IF EXISTS eden_178_188_student_roster;

-- sql
CREATE TABLE eden_178_188_student_roster AS
  (
  WITH
  constants AS (
      SELECT '10' AS highschool_grade_value
  ), 
  FilteredData  AS (
      -- Filter 1: Rollup Flag and Include Flags
      SELECT * 
      FROM accountability
      WHERE (
              (rollup_flag = 'Y') 
              AND (
                  include_flag_ela IN ('Y', 'P') 
                  OR include_flag_english_i IN ('Y', 'P')
                  )
          OR test_type = 5
          )
  ), 

  SexRacialFiltered AS (
      -- Filter 2: Permitted Sex or Racial
      SELECT *
      FROM FilteredData
      WHERE gender IN ('M', 'F')
          AND CAST(ethnicityrace AS VARCHAR) IN ('1', '2', '3', '4', '5', '6', '7')
  ), 

  ScholarFiltered AS (
      -- Filter 3: remove scholar students
      SELECT *
      FROM SexRacialFiltered
      WHERE SUBSTRING(accountability_site_code, 1, 3) NOT IN ('000', '700', '888', '999', '997')
  ), 

  TestTypeFiltered AS (
      -- Filter 4: delete type=9 and totunits=0 and grade in 05-08
      SELECT *
      FROM ScholarFiltered
      WHERE NOT (test_type = 9 AND total_units = '0' AND document_grade IN ('05', '06', '07', '08'))
  ), 

  GroupFiltered AS (
      -- Filter 5: delete special groups
      SELECT *
      FROM TestTypeFiltered
      WHERE assess_group_code NOT IN ('NPS', 'NPB', 'RES', 'TRI')
  ), 

  GradeTransformed AS (
      -- transform HS grade into appropriate value
      SELECT *,
          CASE document_grade
              WHEN '09' then (SELECT highschool_grade_value FROM constants)
              WHEN '10' then (SELECT highschool_grade_value FROM constants)
              WHEN '11' then (SELECT highschool_grade_value FROM constants)
              WHEN '12' then (SELECT highschool_grade_value FROM constants)
              WHEN '16' then (SELECT highschool_grade_value FROM constants)
              WHEN 'T9' then (SELECT highschool_grade_value FROM constants)
              ELSE document_grade
          END AS transformed_grade
      FROM GroupFiltered
  ), 

  SiteConversion AS (
      SELECT SITECD_SIS, SITECD_EDEN
      FROM READ_CSV('C:\\Hanbo Wang\\commonly_used_data\\site_conversion.csv', HEADER=TRUE, AUTO_DETECT=TRUE)
  ), 

  UpdatedEDENSites AS (
      SELECT 
          g.*,
          COALESCE(s.SITECD_EDEN, g.accountability_site_code) AS EDEN_sitecode
      FROM GradeTransformed g
      LEFT JOIN SiteConversion s ON g.accountability_site_code = s.SITECD_SIS
  ), 

  EdenSchool AS (
      SELECT *
      FROM READ_PARQUET('C:\\Hanbo Wang\\EDEN\\EDEN_School_List.parquet')
      WHERE Final_Status_Code NOT IN (2, 6, 7)
  ),  

  FilteredEdenSchool AS (
      SELECT u.*
      FROM UpdatedEDENSites u
      JOIN EdenSchool e ON u.EDEN_sitecode = e.site   
  ), 

  EdenSchool_2 AS(
    SELECT EDEN_SITECD
    FROM READ_PARQUET('C:\\Hanbo Wang\\commonly_used_data\\EDEN_School_List.parquet')
  ), 

  FilteredEdenSchool_2 AS (
    SELECT *, SUBSTRING(EDEN_sitecode, 1, 3) AS EDEN_leacode
    FROM FilteredEdenSchool
    WHERE EDEN_sitecode IN (SELECT EDEN_SITECD FROM EdenSchool_2)
  ),

  Accomodation AS (
    SELECT *,
    CASE
      -- LEAP Connect
      WHEN test_type = 1 THEN True

      -- LEAP 3-8
      WHEN test_type = 3 THEN
        CASE
          
          WHEN education_classification = 1 AND
                (braille = '1' 
                OR large_print = '1' 
                OR answers_recorded = '1' 
                OR extended_time = '1' 
                OR transferred_answers = '1' 
                OR individualsmall_group_administration = '1' 
                OR read_aloud_kurzweil = '1' 
                OR read_aloud_recorded_voice = '1' 
                OR read_aloud_human_reader = '1') THEN True

          WHEN el_flag = 'Y' AND
                (el_test_accomodations_ela_extended_time = '1' 
                OR el_test_accomodations_ela_individualsmall_group_administration = '1' 
                OR provision_of_englishnative_language_wordtoword_dictionary_no_definitions = '1' 
                OR test_administered_by_esl_teacher_or_individual_providing_language_services = '1' 
                OR directions_in_native_language = '1') THEN True

          WHEN (education_classification IN (0, 2) AND section_504_flag = 'Y') AND
                ("504_test_accomodations_ela_large_print" = '1' 
                OR "504_test_accomodations_ela_answers_recorded" = '1' 
                OR "504_test_accomodations_ela_extended_time" = '1' 
                OR "504_test_accomodations_ela_transferred_answers" = '1' 
                OR "504_test_accomodations_ela_individualsmall_group_administration" = '1' 
                OR "504_test_accomodations_ela_read_aloud_kurzweil" = '1' 
                OR "504_test_accomodations_ela_read_aloud_recorded_voice" = '1' 
                OR "504_test_accomodations_ela_read_aloud_human_reader" = '1') THEN True

          WHEN texttospeech_leap = '1' 
            OR human_read_aloud_leap = '1' 
            OR native_language_wordtoword_dictionary_leap = '1' 
            OR directions_in_native_language_leap = '1' 
            OR transferred_answers_leap = '1' 
            OR answers_recorded_leap = '1' 
            OR extended_time_leap = '1' 
            OR individual_or_small_group_administration_leap = '1' 
            OR online_test_accomodations_ela_accommodated_paper = '1' 
            OR online_test_accomodations_ela_braille = '1' 
            OR communication_assistance_scripts_leap = '1' 
            OR assistive_technology_leap_connect = '1' THEN True

          WHEN personal_needs_profile_ela_individualsmall_group_administration = '1' THEN True
          ELSE False
        END

      -- IAP
      WHEN test_type = 6 THEN
        CASE
          WHEN 
            COALESCE(CAST(iap_information_texttospeech AS VARCHAR), '') = 'Y' OR 
            COALESCE(CAST(iap_information_accommodated_paper AS VARCHAR), '') = 'Y' OR 
            COALESCE(CAST(iap_information_braille AS VARCHAR), '') = 'Y' OR 
            COALESCE(CAST(humantest_read_aloud AS VARCHAR), '') = 'Y' OR 
            COALESCE(CAST(iap_information_native_language_wordtoword_dictionary AS VARCHAR), '') = 'Y' OR 
            COALESCE(CAST(iap_information_directions_in_native_language AS VARCHAR), '') = 'Y' OR 
            COALESCE(CAST(iap_information_transferred_answers AS VARCHAR), '') = 'Y' OR 
            COALESCE(CAST(iap_information_answers_recorded AS VARCHAR), '') = 'Y' OR 
            COALESCE(CAST(iap_information_extended_time AS VARCHAR), '') = 'Y' OR 
            COALESCE(CAST(iap_information_individual_or_small_group_administration AS VARCHAR), '') = 'Y' OR 
            COALESCE(CAST(communication_assistance_script AS VARCHAR), '') = 'Y' OR 
            COALESCE(CAST(recorded_voice_file AS VARCHAR), '') = 'Y' OR 
            COALESCE(CAST(kurzweil AS VARCHAR), '') = 'Y'
          THEN True

          ELSE False
        END
        -- LEAP High School
      WHEN test_type = 9 THEN
        CASE
          WHEN COALESCE(CAST(english_i_accommodations_texttospeech AS VARCHAR), '') = 'Y'  
            OR COALESCE(CAST(spanish_test_filler AS VARCHAR), '') = 'Y' 
            OR COALESCE(CAST(english_i_accommodations_filler AS VARCHAR), '') = 'Y' 
            OR COALESCE(CAST(english_i_accommodations_braille AS VARCHAR), '') = 'Y'
            OR COALESCE(CAST(english_i_accommodations_accommodated_paper AS VARCHAR), '') = 'Y'
            OR COALESCE(CAST(english_i_accommodations_human_read_aloud AS VARCHAR), '') = 'Y'
            OR COALESCE(CAST(english_i_accommodations_native_language_wordtoword_dictionary AS VARCHAR), '') = 'Y'
            OR COALESCE(CAST(english_i_accommodations_directions_in_native_language AS VARCHAR), '') = 'Y'
            OR COALESCE(CAST(english_i_accommodations_communication_assistance AS VARCHAR), '') = 'Y'
            OR COALESCE(CAST(english_i_accommodations_transferred_answers AS VARCHAR), '') = 'Y'
            OR COALESCE(CAST(english_i_accommodations_answers_recorded AS VARCHAR), '') = 'Y'
            OR COALESCE(CAST(calculator_filler AS VARCHAR), '') = 'Y'
            OR COALESCE(CAST(english_i_accommodations_extended_time AS VARCHAR), '') = 'Y'
            OR COALESCE(CAST(english_i_accommodations_individual_or_small_group_administration AS VARCHAR), '') = 'Y'
          THEN True
          ELSE False
        END



      ELSE False
    END AS accomodation_flag
    FROM FilteredEdenSchool_2
  ),

  data_group AS (
    SELECT *,
    CASE
      WHEN transformed_grade IN ('03', '04', '05', '06', '07', '08') THEN 876
      WHEN transformed_grade NOT IN ('03', '04', '05', '06', '07', '08') THEN 877
    END AS data_group
    FROM Accomodation
  ),

  assessment_administered_flags AS (
    SELECT *,
    CASE
      WHEN initial_tester_flag_ela IN ('I', 'R') AND COALESCE(CAST(test_taken_flag_ela AS VARCHAR), '') = 'Y' AND updated_accountability_achievement_level_ela IS NOT NULL THEN True
      WHEN initial_tester_flag_ela IN ('I', 'R') AND COALESCE(CAST(test_taken_flag_ela AS VARCHAR), '') = 'N' THEN False
      ELSE False
    END AS take_rla_flag,
    CASE
      WHEN initial_tester_flag_english_i IN ('I', 'R') AND COALESCE(CAST(test_taken_flag_english_i AS VARCHAR), '') = 'Y' AND updated_accountability_achievement_level_english_i IS NOT NULL THEN True
      WHEN initial_tester_flag_english_i IN ('I', 'R') AND COALESCE(CAST(test_taken_flag_english_i AS VARCHAR), '') = 'N' THEN False
      ELSE False
    END AS take_hs_flag,
    CASE
      WHEN initial_tester_flag_ela IN ('I', 'R') AND COALESCE(CAST(test_taken_flag_ela AS VARCHAR), '') = 'Y' AND updated_accountability_achievement_level_ela IS NOT NULL THEN True
      WHEN initial_tester_flag_ela IN ('I', 'R') AND COALESCE(CAST(test_taken_flag_ela AS VARCHAR), '') = 'N' THEN False
      ELSE False
    END AS take_connect_flag,
    CASE
      WHEN COALESCE(CAST(excused_flag_ela AS VARCHAR), '') = 'Y' AND updated_accountability_code_ela = '80' THEN True
      ELSE False
    END AS medical_excuse_flag
    FROM data_group
  ),

  assessment_administered AS (
    SELECT *,
    CASE 
      -- Assessment Administered RLA (lower grades)
      WHEN data_group = 876 AND take_rla_flag AND test_type IN (3, 9) AND NOT accomodation_flag THEN 'REGASSWOACC'
      WHEN data_group = 876 AND take_rla_flag AND test_type IN (3, 9) AND accomodation_flag THEN 'REGASSWACC'
      WHEN data_group = 876 AND take_rla_flag AND test_type = 6 AND NOT accomodation_flag THEN 'IADAPLASMTWOACC'
      WHEN data_group = 876 AND take_rla_flag AND test_type = 6 AND accomodation_flag THEN 'IADAPLASMTWACC'
      -- Assessment Administered RLA (HS)
      WHEN data_group = 877 AND take_hs_flag AND test_type IN (3, 9) AND NOT accomodation_flag THEN 'HSREGASMTIWOACC'
      WHEN data_group = 877 AND take_hs_flag AND test_type IN (3, 9) AND accomodation_flag THEN 'HSREGASMTIWACC'
      WHEN data_group = 877 AND take_hs_flag AND test_type = 6 AND NOT accomodation_flag THEN 'IADAPLASMTWOACC'
      WHEN data_group = 877 AND take_hs_flag AND test_type = 6 AND accomodation_flag THEN 'IADAPLASMTWACC'
      -- Assessment Administered LEAP connect
      WHEN take_connect_flag AND test_type = 1 THEN 'ALTASSALTACH'
      END AS assessment_administered
    FROM assessment_administered_flags  
  ),

  participation_status AS (
    SELECT *,
    CASE 
      -- Assessment Administered RLA (lower grades)
      WHEN data_group = 876 AND take_rla_flag AND test_type IN (3, 9) AND NOT accomodation_flag THEN 'REGPARTWOACC'
      WHEN data_group = 876 AND take_rla_flag AND test_type IN (3, 9) AND accomodation_flag THEN 'REGPARTWACC'
      WHEN data_group = 876 AND NOT take_rla_flag THEN 'NPART'
      
      WHEN data_group = 876 AND take_rla_flag AND test_type = 6 AND NOT accomodation_flag THEN 'PIADAPLASMWOACC'
      WHEN data_group = 876 AND take_rla_flag AND test_type = 6 AND accomodation_flag THEN 'PIADAPLASMWACC'
    
      -- Assessment Administered RLA (HS)
      WHEN data_group = 877 AND take_hs_flag AND test_type IN (3, 9) AND NOT accomodation_flag THEN 'PHSRGASMIWOACC'
      WHEN data_group = 877 AND take_hs_flag AND test_type IN (3, 9) AND accomodation_flag THEN 'PHSRGASMIWACC'
      WHEN data_group = 877 AND NOT take_hs_flag THEN 'NPART'

      WHEN data_group = 877 AND take_hs_flag AND test_type = 6 AND NOT accomodation_flag THEN 'PIADAPLASMWOACC'
      WHEN data_group = 877 AND take_hs_flag AND test_type = 6 AND accomodation_flag THEN 'PIADAPLASMWACC'

      -- Assessment Administered LEAP connect
      WHEN take_connect_flag AND test_type = 1 THEN 'ALTASSALTACH'

      -- ELPT
      WHEN test_type = 5 THEN 'PARTELP'
      END AS participation_status,
    CASE 
      WHEN medical_excuse_flag  THEN 'MEDEXEMPT'
      ELSE participation_status
    END AS participation_status_2

    FROM assessment_administered  
  ), 

  other_flags AS(
    SELECT *,

    -- proficency status
    CASE
      WHEN updated_accountability_achievement_level_ela IN ('ADV', 'MAS') OR updated_accountability_achievement_level_english_i IN ('ADV', 'MAS') THEN 'PROFICIENT'
      WHEN updated_accountability_achievement_level_ela IN ('BAS', 'APP', 'UNS') OR updated_accountability_achievement_level_english_i IN ('BAS', 'APP', 'UNS') THEN 'NOTPROFICIENT'
    END AS proficiency_status,

    -- ethnicity
    CASE
      WHEN CAST(ethnicityrace AS VARCHAR) = '1' THEN 'MHL'
      WHEN CAST(ethnicityrace AS VARCHAR) = '2' THEN 'MAN'
      WHEN CAST(ethnicityrace AS VARCHAR) = '3' THEN 'MA'
      WHEN CAST(ethnicityrace AS VARCHAR) = '4' THEN 'MB'
      WHEN CAST(ethnicityrace AS VARCHAR) = '5' THEN 'MNP'
      WHEN CAST(ethnicityrace AS VARCHAR) = '6' THEN 'MW'
      WHEN CAST(ethnicityrace AS VARCHAR) = '7' THEN 'MM'
    END AS racial_ethnic,

    -- disability status
    CASE
      WHEN education_classification = 1 THEN 'WDIS'
      WHEN test_type = 1 THEN 'WDIS'
      ELSE NULL
    END AS disability_status,  

    -- English Learner Status
    CASE
      WHEN el_flag = 'Y' THEN 'LEP'
      ELSE NULL
    END AS english_learner_status,

    -- Economically Disadvantage Status
    CASE
      WHEN economically_disadvantaged = 1 THEN 'ECODIS'
      ELSE NULL
    END AS economically_disadvantaged_status,

    -- Migratory Status
    CASE
      WHEN migrant_flag = 'Y' THEN 'MS'
      ELSE NULL
    END AS migratory_status,

    -- Homeless Enrolled Status
    CASE
      WHEN summarized_mckinneyvento_act_homeless = 'Y' THEN 'HOMELSENRL'
      ELSE NULL
    END AS homeless_enrolled_status,

    -- Foster Care Status
    CASE
      WHEN foster_care = 'Y' THEN 'FCS'
      ELSE NULL
    END AS foster_care_status,

    -- Military Connected Student Status
    CASE
      WHEN military_affiliated = 'Y' THEN 'MILCNCTD'
      ELSE NULL
    END AS military_connected_student_status

    FROM participation_status
  )

  SELECT CAST(new_lasid AS int64) AS lasid,
          assessment_administered,
          transformed_grade,
          proficiency_status,
          racial_ethnic,
          gender,
          disability_status,
          english_learner_status,
          economically_disadvantaged_status,
          migratory_status,
          homeless_enrolled_status,
          foster_care_status,
          military_connected_student_status,
          participation_status,
          EDEN_sitecode,
          EDEN_leacode,
          test_type,
          rollup_flag,
          el_flag,
          proficiency_updated
          
  FROM other_flags
  );


-- sql
CREATE OR REPLACE VIEW student_roster_188 AS
SELECT *
FROM eden_178_188_student_roster
WHERE NOT (test_type = 5
    AND (
      transformed_grade IN ('KG', '01', '02')
      OR rollup_flag != 'Y'
      OR el_flag != 'Y'
      OR lasid IS NULL
      OR proficiency_updated IS NULL
  ));

  
-- sql
CREATE OR REPLACE VIEW student_roster_178 AS
SELECT *
FROM eden_178_188_student_roster
WHERE test_type != 5;
