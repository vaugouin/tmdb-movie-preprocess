-- TMDB-MOVIE-PREPROCESS-045 : ID_CRITERION et ID_CRITERION_SPINE sur T_WC_T2S_SERIE
--
-- POURQUOI. Le numero de spine Criterion s'arretait aux films. La collection Criterion
-- compte pourtant ONZE series (Dekalog, Berlin Alexanderplatz, Scenes from a Marriage,
-- World on a Wire, The Underground Railroad, Fishing with John, Eight Hours Don't Make a
-- Day, Tanner 88, Phantom India, Agnes Varda: From Here to There, Cartesius), et le
-- spine 42 EST une serie, Fishing with John (ID_SERIE = 61820). Une recherche par numero
-- de spine ne pouvait donc pas les atteindre, ce qui condamne les evaluations 2459/2460
-- de fastapi-text2sql.
--
-- LE PERIMETRE EST PLUS PETIT QU'IL N'Y PARAIT, verifie dans le schema le 2026-09-01 :
--   T_WC_T2S_MOVIE ............ les deux colonnes, presentes
--   T_WC_T2S_SERIE ............ ABSENTES, c'est tout le manque
--   T_WC_WIKIDATA_MOVIE_V1 .... presentes
--   T_WC_WIKIDATA_SERIE_V1 .... PRESENTES DEJA, la symetrie amont existe
--   T_WC_TMDB_MOVIE / _SERIE .. aucune, la donnee n'a jamais transite par la couche TMDb
-- Rien a faire cote TMDb, donc, contrairement a ce qu'on pouvait supposer : la donnee
-- vient de Wikidata. Une table a completer, deux lignes a ajouter au processus 5.
--
-- TYPE ET NOM copies a l'identique de T_WC_T2S_MOVIE, int(11) NULL avec un index chacun.
-- Une meme donnee doit porter le meme nom et le meme type partout dans le modele, comme
-- le rappelle alter-t2s-imdb-votes.sql.
--
-- ORDRE DES OPERATIONS, il compte, et c'est la meme lecon que -194.
--   1. Jouer ce script.
--   2. Lancer le preprocess (processus 5, T2S_SERIE : l'UPDATE d'enrichissement Wikidata
--      pose desormais les deux colonnes dans la meme passe, sans balayage supplementaire).
--   3. Verifier la couverture avec les sections B a D ci-dessous.
--   4. SEULEMENT ENSUITE livrer FASTAPI-TEXT2SQL-237, qui declare les colonnes dans
--      text_to_sql.md et reecrit la regle "Criterion spine number". Ce prompt affirme
--      aujourd'hui noir sur blanc que T_WC_T2S_SERIE n'a PAS cette colonne : phrase vraie
--      le 2026-08-31, fausse des que ce script est joue. Declarer trop tot pointe le
--      modele vers une colonne vide, declarer trop tard le laisse avec une affirmation
--      perimee, ce qui est pire qu'une lacune puisqu'il ne cherchera pas une colonne dont
--      on lui a dit qu'elle n'existe pas.
--
-- COMMENT PRODUIRE LA SORTIE :
--   mysql --force -t vaugouindb < doc/sql/alter-t2s-serie-criterion.sql > doc/sql/alter-t2s-serie-criterion-AAAAMMJJ.txt 2>&1

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ============================================================================
-- A. LA MIGRATION
-- ============================================================================

ALTER TABLE T_WC_T2S_SERIE
    ADD COLUMN ID_CRITERION       INT(11) DEFAULT NULL AFTER PLEX_MEDIA_KEY,
    ADD COLUMN ID_CRITERION_SPINE INT(11) DEFAULT NULL AFTER ID_CRITERION,
    ADD KEY ID_CRITERION       (ID_CRITERION),
    ADD KEY ID_CRITERION_SPINE (ID_CRITERION_SPINE);

SELECT 'A1. Les deux colonnes existent et sont indexees' AS SECTION;

SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'T_WC_T2S_SERIE'
  AND COLUMN_NAME IN ('ID_CRITERION', 'ID_CRITERION_SPINE')
ORDER BY ORDINAL_POSITION;

-- ============================================================================
-- B. LA COUVERTURE, A LIRE APRES LE PREPROCESS
-- ============================================================================
--
-- LE PIEGE, herite de -043 et paye une fois deja. Ces colonnes sont des ENTIERS et la
-- garde numerique de f_wikidatabestvaluesql ecarte la valeur '0', sentinelle "absent" de
-- l'epoque V1. Tout controle s'ecrit donc `col IS NOT NULL AND col <> 0`, JAMAIS
-- `IS NOT NULL` seul : c'est l'artefact qui avait fait annoncer "0 Criterion retrouve sur
-- 19924" alors que le vrai chiffre etait 1673 sur 1673.
-- ============================================================================

SELECT 'B1. Series portant un identifiant Criterion (garde <> 0)' AS SECTION;

SELECT COUNT(*)                                                        AS SERIES_TOTAL,
       SUM(ID_CRITERION IS NOT NULL AND ID_CRITERION <> 0)             AS AVEC_CRITERION,
       SUM(ID_CRITERION_SPINE IS NOT NULL AND ID_CRITERION_SPINE <> 0) AS AVEC_SPINE,
       SUM(ID_CRITERION = 0)                                           AS ZEROS_CRITERION,
       SUM(ID_CRITERION_SPINE = 0)                                     AS ZEROS_SPINE
FROM T_WC_T2S_SERIE;

SELECT 'B2. Le temoin : spine 42 doit rendre Fishing with John (61820)' AS SECTION;

SELECT ID_SERIE, SERIE_TITLE, ID_CRITERION, ID_CRITERION_SPINE, ID_WIKIDATA
FROM T_WC_T2S_SERIE
WHERE ID_CRITERION_SPINE = 42;

SELECT 'B3. Toutes les series porteuses, par numero de spine' AS SECTION;

SELECT ID_SERIE, SERIE_TITLE, ID_CRITERION_SPINE, DAT_FIRST_AIR
FROM T_WC_T2S_SERIE
WHERE ID_CRITERION_SPINE IS NOT NULL AND ID_CRITERION_SPINE <> 0
ORDER BY ID_CRITERION_SPINE;

-- ============================================================================
-- C. LE RECOUPEMENT AVEC LA COLLECTION
-- ============================================================================
--
-- La collection Criterion (processus 41, source 'custom') declare son SERIE_COUNT. Les
-- series porteuses d'un spine et les series membres de la collection ne se recouvrent pas
-- forcement : l'appartenance vient de P9584 (identifiant du film sur criterion.com), le
-- spine de P12279. Une serie peut appartenir sans porter de numero. C2 nomme l'ecart au
-- lieu de le laisser deviner.
-- ============================================================================

SELECT 'C1. La collection et ses compteurs' AS SECTION;

SELECT ID_T2S_COLLECTION, COLLECTION_NAME, COLLECTION_NAME_FR, MOVIE_COUNT, SERIE_COUNT
FROM T_WC_T2S_COLLECTION
WHERE COLLECTION_SOURCE = 'custom'
  AND (COLLECTION_NAME LIKE '%Criterion%' OR COLLECTION_NAME_FR LIKE '%Criterion%');

SELECT 'C2. Series membres de la collection, avec ou sans spine' AS SECTION;

SELECT s.ID_SERIE,
       s.SERIE_TITLE,
       s.ID_CRITERION_SPINE,
       CASE WHEN s.ID_CRITERION_SPINE IS NULL OR s.ID_CRITERION_SPINE = 0
            THEN 'SANS SPINE' ELSE 'avec spine' END AS ETAT,
       sc.DISPLAY_ORDER
FROM T_WC_T2S_SERIE_COLLECTION sc
INNER JOIN T_WC_T2S_SERIE s ON s.ID_SERIE = sc.ID_SERIE
INNER JOIN T_WC_T2S_COLLECTION c ON c.ID_T2S_COLLECTION = sc.ID_T2S_COLLECTION
WHERE c.COLLECTION_SOURCE = 'custom'
  AND c.COLLECTION_NAME LIKE '%Criterion%'
ORDER BY sc.DISPLAY_ORDER;

SELECT 'C3. Series a spine NON membres de la collection (doit etre vide)' AS SECTION;

SELECT s.ID_SERIE, s.SERIE_TITLE, s.ID_CRITERION_SPINE
FROM T_WC_T2S_SERIE s
WHERE s.ID_CRITERION_SPINE IS NOT NULL AND s.ID_CRITERION_SPINE <> 0
  AND NOT EXISTS (
      SELECT 1
      FROM T_WC_T2S_SERIE_COLLECTION sc
      INNER JOIN T_WC_T2S_COLLECTION c ON c.ID_T2S_COLLECTION = sc.ID_T2S_COLLECTION
      WHERE sc.ID_SERIE = s.ID_SERIE
        AND c.COLLECTION_SOURCE = 'custom'
        AND c.COLLECTION_NAME LIKE '%Criterion%');

-- ============================================================================
-- D. LA COLLISION DE SPINE ENTRE FILMS ET SERIES
-- ============================================================================
--
-- L'editeur numerote UNE seule serie de spines, films et series confondus : le 42 est une
-- serie, le 100 un film. D1 doit donc rendre ZERO ligne. S'il en rend une, la lecture d'un
-- numero devient ambigue et FASTAPI-TEXT2SQL-237 devra en tenir compte dans sa regle,
-- puisqu'une question sur un numero ne pourrait plus designer une entite unique.
-- ============================================================================

SELECT 'D1. Numeros portes a la fois par un film et une serie (doit etre vide)' AS SECTION;

SELECT m.ID_CRITERION_SPINE AS SPINE,
       m.ID_MOVIE, m.MOVIE_TITLE,
       s.ID_SERIE, s.SERIE_TITLE
FROM T_WC_T2S_MOVIE m
INNER JOIN T_WC_T2S_SERIE s ON s.ID_CRITERION_SPINE = m.ID_CRITERION_SPINE
WHERE m.ID_CRITERION_SPINE IS NOT NULL AND m.ID_CRITERION_SPINE <> 0
  AND s.ID_CRITERION_SPINE IS NOT NULL AND s.ID_CRITERION_SPINE <> 0
ORDER BY m.ID_CRITERION_SPINE;

SELECT 'D2. Etendue des numeros, films et series ensemble' AS SECTION;

SELECT 'movie' AS SOURCE, COUNT(*) AS PORTEURS,
       MIN(ID_CRITERION_SPINE) AS SPINE_MIN, MAX(ID_CRITERION_SPINE) AS SPINE_MAX
FROM T_WC_T2S_MOVIE
WHERE ID_CRITERION_SPINE IS NOT NULL AND ID_CRITERION_SPINE <> 0
UNION ALL
SELECT 'serie', COUNT(*),
       MIN(ID_CRITERION_SPINE), MAX(ID_CRITERION_SPINE)
FROM T_WC_T2S_SERIE
WHERE ID_CRITERION_SPINE IS NOT NULL AND ID_CRITERION_SPINE <> 0;
