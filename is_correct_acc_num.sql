/* ACCOUNT NUM CONTROL SUM TEST
 * Проверка корректности номера счета
 * Для коррсчетов использовать флаг is_corr=TRUE
 */
CREATE OR REPLACE FUNCTION is_correct_acc_num
    (acc_num TEXT, bic TEXT, is_corr boolean DEFAULT FALSE) 
    RETURNS boolean
    IMMUTABLE
    STRICT
    PARALLEL safe -- Postgres 10 or later
    LANGUAGE plpgsql
AS
$$
DECLARE
    mask integer[];
    digits_bic integer[];
    digits_acc integer[];
BEGIN

    IF octet_length(acc_num) != 20 OR acc_num ~ '\D' THEN
        RETURN FALSE;
    END IF;
    IF octet_length(bic) != 9 OR 
       bic ~ '\D' OR 
       left(bic, 2) != '04' THEN
        RETURN FALSE;
    END IF;   
    
    IF acc_num IS NULL OR bic IS NULL THEN
        RETURN FALSE;
    END IF;
    
    IF is_corr = FALSE THEN
        digits_bic = string_to_array(right(bic, 3), NULL)::integer[];
    ELSE digits_bic = '{0}'::integer[] || string_to_array(substr(bic, 5, 2), NULL)::integer[];
    END IF;
   
    digits_acc = digits_bic || string_to_array(acc_num, NULL)::integer[];
    mask = '{7, 1, 3, 7, 1, 3, 7, 1, 3, 7, 1, 3, 7, 1, 3, 7, 1, 3, 7, 1, 3, 7, 1}'::integer[];

    RETURN (SELECT sum(s) FROM UNNEST( 
                 ARRAY( SELECT a*b
                     FROM UNNEST(digits_acc, mask) AS t(a,b)
                      )
                                      ) AS s) % 10 = 0; 
END;
$$; 

/* ACCOUNT NUM - BALANCE ACC TEST
 * Проверка соответствия номера балансового счета номеру счета 
 */
CREATE OR REPLACE FUNCTION is_correct_ba_in_acc_num
    (acc_num TEXT, bal_acc_num TEXT) 
    RETURNS boolean
    IMMUTABLE
    STRICT
    PARALLEL safe -- Postgres 10 or later
    LANGUAGE plpgsql
AS
$$
DECLARE
    mask integer[];
    digits_bic integer[];
    digits_acc integer[];
BEGIN
    IF octet_length(acc_num) != 20 OR acc_num ~ '\D' THEN
        RETURN FALSE;
    END IF;
   
    IF octet_length(bal_acc_num) != 5 OR bal_acc_num ~ '\D' THEN
        RETURN FALSE;
    END IF;
    
    IF left(acc_num, 5) = bal_acc_num THEN
        RETURN TRUE;
    ELSE
        RETURN FALSE;
    END IF;
END;
$$;

SELECT * 
FROM account a 
WHERE is_correct_ba_in_acc_num(account_num, balance_account_cd) = False;

/* ACCOUNT NUM - CURRENCY TEST
 * Проверка соответствия валюты номеру счета 
 */
CREATE OR REPLACE FUNCTION is_correct_cur_in_acc_num
    (acc_num TEXT, cur_cd TEXT) 
    RETURNS boolean
    IMMUTABLE
    STRICT
    PARALLEL safe -- Postgres 10 or later
    LANGUAGE plpgsql
AS
$$
DECLARE
    mask integer[];
    digits_bic integer[];
    digits_acc integer[];
BEGIN
    IF octet_length(acc_num) != 20 OR acc_num ~ '\D' THEN
        RETURN FALSE;
    END IF;

    IF octet_length(cur_cd) != 3 OR cur_cd ~ '\D' THEN
        RETURN FALSE;
    END IF;

    IF substr(acc_num, 6, 3) = cur_cd THEN
        RETURN TRUE;
    ELSE
        RETURN FALSE;
    END IF;
END;
$$;