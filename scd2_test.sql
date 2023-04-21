/* DATE MATCHING TEST (SCD) - сounterparty 
 * Проверка гладкости версионности
 * effective_to_date должно быть равно предыдущему
 * effective_from_date
 */
WITH test_scd AS 
(
    SELECT  
    PK_field_1, 
    PK_field_2,
    effective_from_date,
    effective_to_date,
    lead(effective_from_date, 1, '2999-12-31') OVER (
        PARTITION BY PK_field_1 ORDER BY effective_from_date
        ) next_eff_from,
    lag(effective_to_date, 1, '1900-01-01') OVER (
        PARTITION BY PK_field_1 ORDER BY effective_to_date 
        ) prev_eff_to    
    FROM dictionary
)
SELECT *
FROM test_scd 
WHERE (NOT (effective_from_date = '1900-01-01' AND effective_to_date = '2999-12-31')) 
       AND (effective_to_date <> next_eff_from OR effective_from_date <> prev_eff_to);