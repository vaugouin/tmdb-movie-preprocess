-- Verification lecture seule des trois tables de RECONCILIATION-001.
-- Voie 1 (SELECT / information_schema seulement).

-- 1. Les trois tables existent, avec leur nombre de colonnes.
SELECT TABLE_NAME, TABLE_ROWS, ENGINE, TABLE_COLLATION,
       (SELECT COUNT(*) FROM information_schema.COLUMNS c
         WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME) AS N_COLS
  FROM information_schema.TABLES t
 WHERE t.TABLE_SCHEMA = DATABASE()
   AND t.TABLE_NAME IN ('T_WC_T2S_SOURCE_CLAIM','T_WC_T2S_CLAIM_ATTRIBUTE','T_WC_T2S_VOCAB_MAP')
 ORDER BY t.TABLE_NAME;

-- 2. Amorce des attributs (attendu : 5 lignes).
SELECT ATTRIBUTE, VALUE_KIND, COMPARE_MODE, DEFAULT_UNIT, TOLERANCE_PCT,
       IS_SCOPED_PLACE, IS_SCOPED_TIME
  FROM T_WC_T2S_CLAIM_ATTRIBUTE
 ORDER BY ATTRIBUTE;

-- 3. Amorce du vocabulaire (attendu : 3 lignes, unites de duree).
SELECT DIMENSION, ID_WIKIDATA, CANONICAL_CODE, FACTOR_TO_CANONICAL, LABEL
  FROM T_WC_T2S_VOCAB_MAP
 ORDER BY DIMENSION, ID_WIKIDATA;

-- 4. La table de faits est vide (attendu : 0).
SELECT COUNT(*) AS N_CLAIMS FROM T_WC_T2S_SOURCE_CLAIM;
