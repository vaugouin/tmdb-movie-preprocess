-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-044 : recette de la collection Criterion
-- ============================================================================
--
-- CE QUE CE FICHIER SERT A DECIDER. Le processus 41 fabrique desormais la
-- collection Criterion a partir d'une ligne de T_WC_CUSTOM_LIST, via le 4e
-- mecanisme d'appartenance (identifiant externe lu dans les statements V2). Ce
-- fichier repond a deux questions, dans cet ordre :
--
--   1. La collection est-elle correcte ? Sections A a D.
--   2. Peut-on retirer la regle Criterion des prompts de fastapi-text2sql, et
--      qu'est-ce que cela couterait aux evaluations ? Sections E a G.
--
-- Tant qu'une seule reponse de A a E est rouge, on ne touche a aucun prompt.
--
-- A LANCER APRES le premier passage nocturne qui contient le processus 41. Avant
-- ce passage, les sections C a G ne trouveront rien : ce n'est pas un echec, c'est
-- que la collection n'existe pas encore.
--
-- LE POINT LE MOINS EVIDENT ET LE PLUS IMPORTANT est la section E. Le 4e mecanisme
-- a modifie une ligne PARTAGEE : le mecanisme 2 parse desormais la chaine privee
-- des marqueurs ID: et ORDER:, au lieu de la chaine brute. Sur les 85 autres listes
-- personnalisees, depourvues de marqueurs, le comportement doit etre rigoureusement
-- identique. E1 et E2 se comparent a la veille, E3 se lit seul : toute ligne qu'il
-- rend est une collection qui a cesse d'etre produite.
--
-- ⚠ COLLATION. La premiere instruction n'est pas decorative. Sans elle, le client
-- se connecte en utf8mb4_general_ci pendant que les tables sont en unicode_ci, et
-- toute comparaison passant par un CAST rend ERROR 1267. Lancer avec --force.
--
-- COMMENT PRODUIRE LA SORTIE :
--   mysql --force -t vaugouindb < doc/sql/test-044-collection-criterion.sql \
--       > doc/sql/test-044-collection-criterion-AAAAMMJJ.txt 2>&1
--
-- Proprietes lues : P9584 identifiant du film sur criterion.com (appartenance),
-- P12279 numero de spine (ordre d'affichage). Q1204187 est l'item de la societe.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ============================================================================
-- A. LA LIGNE SOURCE : ce que le processus 41 va lire
-- ============================================================================
--
-- Trois valeurs decident de tout, et une seule qui derape suffit a tout expliquer.
-- WIKIDATA_PROPERTIES doit valoir exactement « ID:P9584 ORDER:P12279 Q1204187 » :
-- sans les marqueurs, le 4e mecanisme ne se declenche pas et la collection reste
-- vide. SORT_BY doit valoir 1, sinon l'ordre ne suit pas les numeros de spine.
-- TMDB_TARGET_RECORD doit etre vide : une valeur non vide basculerait le moteur en
-- mode additif dans une collection TMDb existante, et la purge nocturne effacerait
-- l'en-tete « custom ».
-- ============================================================================

SELECT 'A1. La ligne Criterion dans T_WC_CUSTOM_LIST' AS SECTION;

SELECT ID_CUSTOM_LIST,
       LIST_NAME,
       LIST_NAME_FR,
       WIKIDATA_PROPERTIES,
       SORT_BY,
       TARGET_TABLE,
       DELETED,
       CONCAT('[', COALESCE(TMDB_TARGET_RECORD, 'NULL'), ']') AS TMDB_TARGET_RECORD,
       CONCAT('[', COALESCE(ID_IMDB_LIST, 'NULL'), ']')       AS ID_IMDB_LIST,
       CONCAT('[', COALESCE(TMDB_ELEMENTS, 'NULL'), ']')      AS TMDB_ELEMENTS
FROM T_WC_CUSTOM_LIST
WHERE LIST_NAME LIKE '%Criterion%' OR LIST_NAME_FR LIKE '%Criterion%';

-- ============================================================================
-- B. LE RUN A-T-IL EU LIEU
-- ============================================================================
--
-- La fenetre startdatetime / enddatetime doit couvrir la nuit ecoulee, et
-- totalruntime ne doit pas valoir RUNNING. Un deletedcount anormalement gros sur
-- « collection » n'est pas un detail : il signifierait que le run a supprime des
-- collections existantes, et la section E dira lesquelles.
--
-- Les colonnes de cette table s'appellent VAR_NAME et VAR_VALUE, pas
-- VARIABLE_NAME : c'est la premiere chose qui casse quand on ecrit la requete de
-- memoire.
-- ============================================================================

SELECT 'B1. Telemetrie du processus 41' AS SECTION;

SELECT VAR_NAME, VAR_VALUE
FROM T_WC_SERVER_VARIABLE
WHERE VAR_NAME LIKE 'strtmdbmoviepreprocesscollection%'
   OR VAR_NAME IN ('strtmdbmoviepreprocesstotalruntime',
                   'strtmdbmoviepreprocesstotalruntimeprevious',
                   'strtmdbmoviepreprocessscope',
                   'strtmdbmoviepreprocesscurrentsubprocess')
ORDER BY VAR_NAME;

-- ============================================================================
-- C. L'EN-TETE DE COLLECTION
-- ============================================================================
--
-- COLLECTION_NAME_FR non vide est la condition sans laquelle tout l'exercice
-- tombe : c'est cette colonne que le processus 212 d'embedding-update vectorise
-- pour la recherche en francais, et c'est le francais qui a motive tout le sujet.
--
-- La variable @idcollection sert a toutes les sections suivantes. Si elle reste
-- NULL, la collection n'a pas ete produite : les sections D a G rendront des
-- resultats vides, et c'est la section A ou B qui porte l'explication.
-- ============================================================================

SELECT 'C1. La collection dans T_WC_T2S_COLLECTION' AS SECTION;

SELECT ID_T2S_COLLECTION,
       COLLECTION_NAME,
       COLLECTION_NAME_FR,
       COLLECTION_SOURCE,
       COLLECTION_TYPE,
       ID_RECORD,
       MOVIE_COUNT,
       SERIE_COUNT,
       ID_WIKIDATA,
       WIKIPEDIA_IMAGE_PATH,
       IMDB_RATING
FROM T_WC_T2S_COLLECTION
WHERE COLLECTION_SOURCE = 'custom'
  AND (COLLECTION_NAME LIKE '%Criterion%' OR COLLECTION_NAME_FR LIKE '%Criterion%');

SELECT 'C2. Identifiant retenu pour la suite du fichier' AS SECTION;

SELECT @idcollection := MAX(ID_T2S_COLLECTION) AS ID_COLLECTION_CRITERION
FROM T_WC_T2S_COLLECTION
WHERE COLLECTION_SOURCE = 'custom'
  AND (COLLECTION_NAME LIKE '%Criterion%' OR COLLECTION_NAME_FR LIKE '%Criterion%');

-- ============================================================================
-- D. LES MEMBRES, ET SURTOUT LEUR ORDRE
-- ============================================================================
--
-- DISPLAY_ORDER n'est qu'un compteur qui suit l'ORDER BY du moteur. Si le tri par
-- numero de spine a fonctionne, les vingt premieres lignes de D1 portent les
-- spines 1 a 20 dans l'ordre, en commencant par La Grande Illusion.
--
-- D3 est le controle qui se lit sans reflechir : PREMIER_SANS_SPINE doit etre
-- SUPERIEUR a DERNIER_AVEC_SPINE. Si c'est l'inverse, le tri s'est fait a l'envers
-- et SORT_BY ne vaut pas 1. Il lit le numero DANS V2, comme le tri lui-meme, et la
-- note posee sur D3 explique pourquoi cette precision a coute une fausse alerte.
-- D6 traite l'autre question, l'ecart entre V2 et la colonne T2S.
-- ============================================================================

SELECT 'D1. Les vingt premiers films, par DISPLAY_ORDER' AS SECTION;

SELECT mc.DISPLAY_ORDER,
       m.ID_MOVIE,
       m.MOVIE_TITLE,
       m.ID_CRITERION_SPINE,
       m.ID_CRITERION
FROM T_WC_T2S_MOVIE_COLLECTION mc
INNER JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = mc.ID_MOVIE
WHERE mc.ID_T2S_COLLECTION = @idcollection
ORDER BY mc.DISPLAY_ORDER
LIMIT 20;

SELECT 'D2. Integrite de DISPLAY_ORDER (TROUS doit valoir 0)' AS SECTION;

SELECT COUNT(*)                              AS LIGNES,
       COUNT(DISTINCT ID_MOVIE)              AS FILMS_DISTINCTS,
       MIN(DISPLAY_ORDER)                    AS MIN_ORDRE,
       MAX(DISPLAY_ORDER)                    AS MAX_ORDRE,
       COUNT(DISTINCT DISPLAY_ORDER)         AS ORDRES_DISTINCTS,
       MAX(DISPLAY_ORDER) - COUNT(*)         AS TROUS
FROM T_WC_T2S_MOVIE_COLLECTION
WHERE ID_T2S_COLLECTION = @idcollection;

-- D3 A ETE REECRITE LE 2026-08-30, ET C'EST UNE LECON. La premiere version
-- classait les films selon T_WC_T2S_MOVIE.ID_CRITERION_SPINE, alors que l'ORDRE
-- vient du numero lu dans les statements V2. Ces deux sources ne couvrent pas la
-- meme population : la colonne T2S n'est ecrite que sur les films joignables a
-- T_WC_WIKIDATA_MOVIE_V1, dependance que le processus 4 conserve encore. Un film
-- connu de V2 mais absent de V1 etait donc trie a sa vraie place ET compte comme
-- « sans spine ». Le controle annoncait une inversion du tri qui n'existait pas
-- (PREMIER_SANS_SPINE 438 contre DERNIER_AVEC_SPINE 1229, pour deux films
-- seulement), pendant que D4 et D1 disaient, eux, la verite.
--
-- La regle qu'il faut en retenir : UN CONTROLE DOIT LIRE LA MEME SOURCE QUE CE
-- QU'IL CONTROLE. D3 lit desormais V2, comme le mecanisme. L'ecart entre V2 et la
-- colonne T2S est une vraie question, mais c'est celle de D6, pas celle-ci.
SELECT 'D3. Les films sans spine tombent-ils bien a la fin' AS SECTION;

SELECT SUM(SPINE_V2 IS NOT NULL)                                  AS AVEC_SPINE,
       SUM(SPINE_V2 IS NULL)                                      AS SANS_SPINE,
       MAX(CASE WHEN SPINE_V2 IS NOT NULL THEN DISPLAY_ORDER END) AS DERNIER_AVEC_SPINE,
       MIN(CASE WHEN SPINE_V2 IS NULL THEN DISPLAY_ORDER END)     AS PREMIER_SANS_SPINE
FROM (
    SELECT mc.DISPLAY_ORDER,
           ( SELECT CAST(sv.VALUE_EXTERNAL_ID AS UNSIGNED)
             FROM T_WC_WIKIDATA_STATEMENT ss
             JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE sv ON sv.ID_STATEMENT = ss.ID_STATEMENT
             WHERE ss.ID_WIKIDATA = m.ID_WIKIDATA
               AND ss.ID_PROPERTY = 'P12279'
               AND (ss.`RANK` IS NULL OR ss.`RANK` <> 'deprecated')
               AND sv.VALUE_EXTERNAL_ID REGEXP '^[0-9]+$'
               AND sv.VALUE_EXTERNAL_ID <> '0'
             ORDER BY (ss.`RANK` = 'preferred') DESC, ss.ID_STATEMENT ASC
             LIMIT 1 ) AS SPINE_V2
    FROM T_WC_T2S_MOVIE_COLLECTION mc
    INNER JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = mc.ID_MOVIE
    WHERE mc.ID_T2S_COLLECTION = @idcollection
) classement;

SELECT 'D4. Le tri suit-il vraiment le spine (RUPTURES doit valoir 0)' AS SECTION;

SELECT COUNT(*) AS RUPTURES
FROM (
    SELECT m.ID_CRITERION_SPINE                                             AS SPINE,
           LAG(m.ID_CRITERION_SPINE) OVER (ORDER BY mc.DISPLAY_ORDER)       AS SPINE_PRECEDENT
    FROM T_WC_T2S_MOVIE_COLLECTION mc
    INNER JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = mc.ID_MOVIE
    WHERE mc.ID_T2S_COLLECTION = @idcollection
      AND m.ID_CRITERION_SPINE IS NOT NULL
      AND m.ID_CRITERION_SPINE <> 0
) paires
WHERE SPINE_PRECEDENT IS NOT NULL
  AND SPINE < SPINE_PRECEDENT;

SELECT 'D5. Des series dans la collection' AS SECTION;

SELECT sc.DISPLAY_ORDER, s.ID_SERIE, s.SERIE_TITLE
FROM T_WC_T2S_SERIE_COLLECTION sc
INNER JOIN T_WC_T2S_SERIE s ON s.ID_SERIE = sc.ID_SERIE
WHERE sc.ID_T2S_COLLECTION = @idcollection
ORDER BY sc.DISPLAY_ORDER
LIMIT 20;

-- ============================================================================
-- D6. L'ECART ENTRE V2 ET LA COLONNE T2S : LA JOINTURE V1 EST-ELLE LA CAUSE
-- ============================================================================
--
-- Ajoutee le 2026-08-30, apres que la premiere execution a trouve trois films
-- d'ecart entre la collection et la regle actuelle du prompt. L'hypothese a
-- tester tient en une phrase : la colonne T2S n'est pas ecrite parce que
-- l'UPDATE d'enrichissement du processus 4 jointe encore T_WC_WIKIDATA_MOVIE_V1,
-- dependance connue et documentee sur -043, qui doit tomber avec -042.
--
-- Si PRESENT_DANS_V1 vaut 0 sur ces lignes, l'hypothese est confirmee et les
-- ecarts ne sont pas une regression du 4e mecanisme : ce sont des films que V2
-- connait et que la voie V1 n'atteint pas. La bascule des prompts les GAGNE.
--
-- Si PRESENT_DANS_V1 vaut 1, la cause est ailleurs (ID_IMDB vide, ou la valeur
-- ecartee par une garde) et il faut la chercher avant de basculer.
-- ============================================================================

SELECT 'D6. Spine present en V2, absent de la colonne T2S' AS SECTION;

SELECT m.ID_MOVIE,
       m.MOVIE_TITLE,
       m.ID_WIKIDATA,
       CONCAT('[', COALESCE(m.ID_IMDB, 'NULL'), ']')                       AS ID_IMDB,
       m.ID_CRITERION,
       m.ID_CRITERION_SPINE,
       mc.DISPLAY_ORDER,
       ( SELECT COUNT(*) FROM T_WC_WIKIDATA_MOVIE_V1 w
         WHERE w.ID_WIKIDATA = m.ID_WIKIDATA )                             AS PRESENT_DANS_V1,
       ( SELECT MIN(sv.VALUE_EXTERNAL_ID)
         FROM T_WC_WIKIDATA_STATEMENT ss
         JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE sv ON sv.ID_STATEMENT = ss.ID_STATEMENT
         WHERE ss.ID_WIKIDATA = m.ID_WIKIDATA AND ss.ID_PROPERTY = 'P12279' ) AS SPINE_V2,
       ( SELECT MIN(sv.VALUE_EXTERNAL_ID)
         FROM T_WC_WIKIDATA_STATEMENT ss
         JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE sv ON sv.ID_STATEMENT = ss.ID_STATEMENT
         WHERE ss.ID_WIKIDATA = m.ID_WIKIDATA AND ss.ID_PROPERTY = 'P9584' )  AS CRITERION_V2
FROM T_WC_T2S_MOVIE_COLLECTION mc
INNER JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = mc.ID_MOVIE
WHERE mc.ID_T2S_COLLECTION = @idcollection
  AND (m.ID_CRITERION_SPINE IS NULL OR m.ID_CRITERION_SPINE = 0)
  AND EXISTS ( SELECT 1
               FROM T_WC_WIKIDATA_STATEMENT ss
               JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE sv ON sv.ID_STATEMENT = ss.ID_STATEMENT
               WHERE ss.ID_WIKIDATA = m.ID_WIKIDATA
                 AND ss.ID_PROPERTY = 'P12279'
                 AND sv.VALUE_EXTERNAL_ID REGEXP '^[0-9]+$'
                 AND sv.VALUE_EXTERNAL_ID <> '0' )
ORDER BY mc.DISPLAY_ORDER;

-- ============================================================================
-- E. NON-REGRESSION SUR LES AUTRES LISTES PERSONNALISEES
-- ============================================================================
--
-- La barriere la plus importante, et celle qu'on oublie parce qu'elle ne parle pas
-- de Criterion. E1 et E2 se comparent a la veille : le nombre de collections
-- « custom » doit augmenter d'exactement 1, et la somme des MOVIE_COUNT d'environ
-- 1 677. E3 se lit seul, et toute ligne qu'il rend est une regression : une liste
-- personnalisee active qui ne produit plus de collection.
--
-- E3 laisse volontairement de cote les lignes portant un TMDB_TARGET_RECORD : elles
-- versent leurs membres dans une collection TMDb existante et ne creent pas
-- d'en-tete « custom ». Les compter serait fabriquer de fausses alertes.
-- ============================================================================

SELECT 'E1. Collections par source (a comparer a la veille)' AS SECTION;

SELECT COLLECTION_SOURCE,
       COUNT(*)          AS COLLECTIONS,
       SUM(MOVIE_COUNT)  AS FILMS,
       SUM(SERIE_COUNT)  AS SERIES
FROM T_WC_T2S_COLLECTION
GROUP BY COLLECTION_SOURCE
ORDER BY COLLECTION_SOURCE;

SELECT 'E2. Toutes les collections custom, une ligne chacune' AS SECTION;

SELECT c.ID_RECORD,
       c.COLLECTION_NAME,
       c.MOVIE_COUNT,
       c.SERIE_COUNT,
       cl.SORT_BY,
       cl.WIKIDATA_PROPERTIES
FROM T_WC_T2S_COLLECTION c
LEFT JOIN T_WC_CUSTOM_LIST cl ON cl.ID_CUSTOM_LIST = c.ID_RECORD
WHERE c.COLLECTION_SOURCE = 'custom'
ORDER BY c.ID_RECORD;

SELECT 'E3. Listes actives qui ne produisent PLUS de collection (doit etre vide)' AS SECTION;

SELECT cl.ID_CUSTOM_LIST,
       cl.LIST_NAME,
       cl.SORT_BY,
       cl.WIKIDATA_PROPERTIES,
       CONCAT('[', COALESCE(cl.TMDB_ELEMENTS, 'NULL'), ']') AS TMDB_ELEMENTS,
       CHAR_LENGTH(COALESCE(cl.ID_IMDB_LIST, ''))           AS TAILLE_LISTE_IMDB
FROM T_WC_CUSTOM_LIST cl
LEFT JOIN T_WC_T2S_COLLECTION c
       ON c.COLLECTION_SOURCE = 'custom'
      AND c.ID_RECORD = cl.ID_CUSTOM_LIST
WHERE cl.DELETED = 0
  AND cl.TARGET_TABLE = 2
  AND (cl.TMDB_TARGET_RECORD IS NULL OR cl.TMDB_TARGET_RECORD = '')
  AND c.ID_T2S_COLLECTION IS NULL
ORDER BY cl.ID_CUSTOM_LIST;

-- ============================================================================
-- F. LES TROIS DEFINITIONS DU CATALOGUE, ET LEUR ECART
-- ============================================================================
--
-- Trois definitions coexistent aujourd'hui et ne rendent pas le meme nombre.
-- L'ecart n'est pas un defaut, il est explicable, et il faut l'expliquer AVANT de
-- basculer les prompts, sinon il sera decouvert par une evaluation en echec.
--
--   F1a  le 4e mecanisme tel qu'il est ecrit : tout identifiant P9584 non vide.
--   F1b  le meme, sous la garde numerique du processus 4 (REGEXP + rejet de '0').
--   F1c  la colonne T2S ID_CRITERION, c'est-a-dire la regle actuelle du prompt.
--   F1d  ce que la collection contient reellement.
--
-- Mesure du 2026-08-29 avant le premier run : F1a rendait 1 677, et la recette de
-- -043 avait mesure 1 673 sur la population jointe a V1. F2 nomme les titres qui
-- font la difference, F3 verifie qu'aucun titre n'est perdu dans l'autre sens.
-- ============================================================================

SELECT 'F1. Trois definitions, quatre decomptes' AS SECTION;

SELECT 'F1a. Statement P9584 non vide (mecanisme 4 tel quel)' AS DEFINITION,
       COUNT(DISTINCT t.ID_MOVIE) AS FILMS
FROM T_WC_WIKIDATA_STATEMENT sme
STRAIGHT_JOIN T_WC_T2S_MOVIE t ON t.ID_WIKIDATA = sme.ID_WIKIDATA
INNER JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE smv ON smv.ID_STATEMENT = sme.ID_STATEMENT
WHERE sme.ID_PROPERTY = 'P9584'
  AND (sme.`RANK` IS NULL OR sme.`RANK` <> 'deprecated')
  AND smv.VALUE_EXTERNAL_ID <> ''
  AND t.ADULT = 0

UNION ALL

SELECT 'F1b. Idem, avec la garde numerique du processus 4',
       COUNT(DISTINCT t.ID_MOVIE)
FROM T_WC_WIKIDATA_STATEMENT sme
STRAIGHT_JOIN T_WC_T2S_MOVIE t ON t.ID_WIKIDATA = sme.ID_WIKIDATA
INNER JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE smv ON smv.ID_STATEMENT = sme.ID_STATEMENT
WHERE sme.ID_PROPERTY = 'P9584'
  AND (sme.`RANK` IS NULL OR sme.`RANK` <> 'deprecated')
  AND smv.VALUE_EXTERNAL_ID REGEXP '^[0-9]+$'
  AND smv.VALUE_EXTERNAL_ID <> '0'
  AND t.ADULT = 0

UNION ALL

SELECT 'F1c. Colonne T2S ID_CRITERION (regle actuelle du prompt)',
       COUNT(*)
FROM T_WC_T2S_MOVIE
WHERE ID_CRITERION IS NOT NULL AND ID_CRITERION > 0

UNION ALL

SELECT 'F1d. Membres reels de la collection',
       COUNT(*)
FROM T_WC_T2S_MOVIE_COLLECTION
WHERE ID_T2S_COLLECTION = @idcollection;

SELECT 'F2. Dans la collection, absents de la regle ID_CRITERION' AS SECTION;

SELECT m.ID_MOVIE,
       m.MOVIE_TITLE,
       m.ID_WIKIDATA,
       m.ID_CRITERION,
       m.ID_CRITERION_SPINE,
       mc.DISPLAY_ORDER,
       ( SELECT MIN(smv.VALUE_EXTERNAL_ID)
         FROM T_WC_WIKIDATA_STATEMENT sme
         JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE smv ON smv.ID_STATEMENT = sme.ID_STATEMENT
         WHERE sme.ID_WIKIDATA = m.ID_WIKIDATA AND sme.ID_PROPERTY = 'P9584' ) AS VALEUR_P9584
FROM T_WC_T2S_MOVIE_COLLECTION mc
INNER JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = mc.ID_MOVIE
WHERE mc.ID_T2S_COLLECTION = @idcollection
  AND (m.ID_CRITERION IS NULL OR m.ID_CRITERION = 0)
ORDER BY m.MOVIE_TITLE;

SELECT 'F3. Dans la regle ID_CRITERION, absents de la collection (doit etre vide)' AS SECTION;

SELECT m.ID_MOVIE,
       m.MOVIE_TITLE,
       m.ID_WIKIDATA,
       m.ID_CRITERION,
       m.ID_CRITERION_SPINE,
       m.ADULT,
       ( SELECT COUNT(*) FROM T_WC_WIKIDATA_MOVIE_V1 w
         WHERE w.ID_WIKIDATA = m.ID_WIKIDATA )                              AS PRESENT_DANS_V1,
       ( SELECT COUNT(*)
         FROM T_WC_WIKIDATA_STATEMENT ss
         WHERE ss.ID_WIKIDATA = m.ID_WIKIDATA AND ss.ID_PROPERTY = 'P9584' ) AS P9584_EN_V2
FROM T_WC_T2S_MOVIE m
LEFT JOIN T_WC_T2S_MOVIE_COLLECTION mc
       ON mc.ID_MOVIE = m.ID_MOVIE
      AND mc.ID_T2S_COLLECTION = @idcollection
WHERE m.ID_CRITERION IS NOT NULL
  AND m.ID_CRITERION > 0
  AND mc.ID_MOVIE IS NULL
ORDER BY m.MOVIE_TITLE;

-- ============================================================================
-- G. CE QUE LA BASCULE DES PROMPTS CHANGERAIT
-- ============================================================================
--
-- L'evaluation 44_45_criterion-collection.json fige les VINGT PREMIERS
-- identifiants rendus par l'ancienne regle, triee par numero de spine. Si G1 et G2
-- rendent les memes vingt identifiants dans le meme ordre, cette evaluation
-- survit telle quelle a la bascule, et G3 le dit en un seul chiffre :
-- COMMUNS_SUR_20 doit valoir 20.
--
-- Si le chiffre est inferieur, ce n'est pas necessairement une regression : les
-- titres de F2 peuvent s'inserer legitimement en tete. Il faut alors reecrire
-- l'assertion_refresh_sql de cette evaluation sur le chemin collection, puis
-- relancer le processus 70 (TMDB_PREPROCESS_SCOPE=assertion-refresh).
-- ============================================================================

SELECT 'G1. Les vingt premiers par l ancienne regle (chemin actuel du prompt)' AS SECTION;

SELECT ID_MOVIE, MOVIE_TITLE, ID_CRITERION_SPINE
FROM T_WC_T2S_MOVIE
WHERE ID_CRITERION IS NOT NULL AND ID_CRITERION > 0
ORDER BY CASE WHEN COALESCE(ID_CRITERION_SPINE, 0) = 0 THEN 1 ELSE 0 END,
         ID_CRITERION_SPINE ASC
LIMIT 20;

SELECT 'G2. Les vingt premiers par le chemin collection' AS SECTION;

SELECT m.ID_MOVIE, m.MOVIE_TITLE, m.ID_CRITERION_SPINE, mc.DISPLAY_ORDER
FROM T_WC_T2S_MOVIE_COLLECTION mc
INNER JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = mc.ID_MOVIE
WHERE mc.ID_T2S_COLLECTION = @idcollection
ORDER BY mc.DISPLAY_ORDER
LIMIT 20;

SELECT 'G3. Les deux tetes de liste se recouvrent-elles (attendu : 20)' AS SECTION;

SELECT COUNT(*) AS COMMUNS_SUR_20
FROM ( SELECT ID_MOVIE
       FROM T_WC_T2S_MOVIE
       WHERE ID_CRITERION IS NOT NULL AND ID_CRITERION > 0
       ORDER BY CASE WHEN COALESCE(ID_CRITERION_SPINE, 0) = 0 THEN 1 ELSE 0 END,
                ID_CRITERION_SPINE ASC
       LIMIT 20 ) ancienne
INNER JOIN ( SELECT mc.ID_MOVIE
             FROM T_WC_T2S_MOVIE_COLLECTION mc
             WHERE mc.ID_T2S_COLLECTION = @idcollection
             ORDER BY mc.DISPLAY_ORDER
             LIMIT 20 ) nouvelle ON nouvelle.ID_MOVIE = ancienne.ID_MOVIE;

-- ============================================================================
-- FIN. Ce que la sortie doit dire, en une phrase par section :
--   A1  une ligne, marqueurs exacts, SORT_BY = 1, TMDB_TARGET_RECORD vide
--   B1  une fenetre de run couvrant la nuit, totalruntime different de RUNNING
--   C1  une ligne, COLLECTION_NAME_FR non vide, ID_WIKIDATA = Q1204187
--   D1  spines 1 a 20 dans l'ordre, La Grande Illusion en tete
--   D2  TROUS = 0 et LIGNES = FILMS_DISTINCTS
--   D3  PREMIER_SANS_SPINE > DERNIER_AVEC_SPINE (spine lu en V2, comme le tri)
--   D4  RUPTURES = 0
--   D6  PRESENT_DANS_V1 = 0 partout, sinon chercher ailleurs
--   E1  +1 collection custom et +1 677 films par rapport a la veille
--   E3  aucune ligne
--   F1  quatre chiffres, et F2 nomme ce qui les separe
--   F3  aucune ligne
--   G3  COMMUNS_SUR_20 = 20
-- ============================================================================
