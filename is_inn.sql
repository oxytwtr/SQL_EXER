/* INN TEST
 * Проверка корректности 10 или 12 значного ИНН
 * Author: Rinat Mukhtarov (github.com/rin-nas)
 */
CREATE OR REPLACE FUNCTION is_inn(inn TEXT) 
    RETURNS boolean
    IMMUTABLE
    STRICT
    PARALLEL safe -- Postgres 10 or later
    LANGUAGE plpgsql
AS
$$
DECLARE
    controlSum integer := 0;
    controlSum2 integer := 0;
    digits integer[];
    inn_length integer;
BEGIN

    IF octet_length(inn) NOT IN (10, 12) OR inn ~ '\D' THEN
        RETURN FALSE;
    END IF;
   
    IF inn IS NULL THEN
        RETURN FALSE;
    END IF;

    digits = string_to_array(inn, null)::integer[];
    
    IF octet_length(inn) = 10 THEN
        -- Проверка контрольных цифр для 10-значного ИНН
        controlSum :=
               2 * digits[1]
            +  4 * digits[2]
            + 10 * digits[3]
            +  3 * digits[4]
            +  5 * digits[5]
            +  9 * digits[6]
            +  4 * digits[7]
            +  6 * digits[8]
            +  8 * digits[9];
        RETURN  (controlSum % 11) % 10 = digits[10];
    END IF;
        -- Проверка контрольных цифр для 12-значного ИНН
    controlSum :=
           7 * digits[1]
        +  2 * digits[2]
        +  4 * digits[3]
        + 10 * digits[4]
        +  3 * digits[5]
        +  5 * digits[6]
        +  9 * digits[7]
        +  4 * digits[8]
        +  6 * digits[9]
        +  8 * digits[10];

    IF (controlSum % 11) % 10 != digits[11] THEN
        return FALSE;
    END IF;

    controlSum2 :=
           3 * digits[1]
        +  7 * digits[2]
        +  2 * digits[3]
        +  4 * digits[4]
        + 10 * digits[5]
        +  3 * digits[6]
        +  5 * digits[7]
        +  9 * digits[8]
        +  4 * digits[9]
        +  6 * digits[10]
        +  8 * digits[11];

    RETURN (controlSum2 % 11) % 10 = digits[12];
END;
$$;