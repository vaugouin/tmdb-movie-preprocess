-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-043 : d'ou viennent les pertes annoncees par la recette
-- ============================================================================
--
-- CE QUE LE PREMIER PASSAGE A DIT. La recette (test-043-coverage.sql, 2026-08-26)
-- annonce quatre colonnes ou V2 couvre MOINS que V1 :
--
--   film      PLEX_MEDIA_KEY   180 012 -> 179 805   perte 234, gain 27
--   film      INSTANCE_OF      248 979 -> 248 583   perte 400, gain 4
--   serie     INSTANCE_OF       50 691 ->  50 615   perte 76,  gain 0
--   personne  INSTANCE_OF      320 101 -> 319 557   perte 1 105, gain 561
--
--   film      ID_CRITERION       1 664 ->   1 669   perte 0, gain 5
--   film      ID_CRITERION_SPINE 1 218 ->   1 222   perte 0, gain 4
--   serie     PLEX_MEDIA_KEY    35 954 ->  36 428   perte 105, gain 579
--
-- Les trois dernieres lignes sont des gains nets et ne posent pas de question. Les
-- quatre premieres si, et le critere d'acceptance ecrit dans le ticket (« superieur
-- ou egal ») n'est PAS tenu pour elles. Avant de le relacher, il faut savoir de quoi
-- ces pertes sont faites.
--
-- LA QUESTION QUE CE FICHIER TRANCHE, et elle n'a qu'un enjeu : est-ce que ces
-- entites sont ABSENTES DE V2, ou est-ce que V2 les connait sans porter la
-- propriete ? Le premier cas n'est pas un defaut de ce ticket : c'est le perimetre
-- d'import, WIKIDATA-CRAWLER-011, la derniere decision ouverte de la migration, et
-- la meme perte frappera tous les consommateurs de la meme facon. Le second cas
-- serait au contraire un defaut de collecte, a corriger cote crawler avant de
-- basculer quoi que ce soit.
--
-- ⚠ COLLATION. Lancer avec --force. Sans la premiere ligne, une comparaison passant
-- par un CAST rend ERROR 1267.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ============================================================================
-- 1. DECOMPOSITION DES PERTES : quatre causes possibles, mutuellement exclusives
-- ============================================================================
--
-- HORS PERIMETRE   l'entite n'a AUCUN statement en V2. Elle n'a pas ete importee.
--                  -> WIKIDATA-CRAWLER-011, pas ce ticket.
-- SANS PROPRIETE   V2 connait l'entite mais ne porte pas cette propriete.
--                  -> soit Wikidata ne l'a pas, soit l'import l'a filtree.
-- RANG DEPRECIE    la propriete existe, mais uniquement en rang 'deprecated', que
--                  le code ecarte volontairement. Wikidata tient la valeur pour
--                  fausse : la perdre est une correction, pas une regression.
-- GARDE            la valeur existe mais le garde l'a ecartee (non numerique, ou
--                  trop longue pour la colonne cible).
-- ============================================================================

SELECT '1A. FILM, les 234 pertes sur PLEX_MEDIA_KEY' AS SECTION;

SELECT CASE
         WHEN NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
                          WHERE s.ID_WIKIDATA = t2s.ID_WIKIDATA)
           THEN 'HORS PERIMETRE'
         WHEN NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
                          WHERE s.ID_WIKIDATA = t2s.ID_WIKIDATA AND s.ID_PROPERTY = 'P11460')
           THEN 'SANS PROPRIETE'
         WHEN NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
                          WHERE s.ID_WIKIDATA = t2s.ID_WIKIDATA AND s.ID_PROPERTY = 'P11460'
                            AND (s.`RANK` IS NULL OR s.`RANK` <> 'deprecated'))
           THEN 'RANG DEPRECIE'
         ELSE 'GARDE'
       END AS CAUSE,
       COUNT(*) AS ENTITES
FROM T_WC_T2S_MOVIE t2s
INNER JOIN T_WC_WIKIDATA_MOVIE_V1 w ON t2s.ID_WIKIDATA = w.ID_WIKIDATA
WHERE t2s.ID_IMDB IS NOT NULL AND t2s.ID_IMDB <> ''
  AND w.PLEX_MEDIA_KEY IS NOT NULL AND w.PLEX_MEDIA_KEY <> ''
  AND NOT EXISTS ( SELECT 1
                   FROM T_WC_WIKIDATA_STATEMENT s
                   JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE ev ON ev.ID_STATEMENT = s.ID_STATEMENT
                   WHERE s.ID_WIKIDATA = t2s.ID_WIKIDATA AND s.ID_PROPERTY = 'P11460'
                     AND (s.`RANK` IS NULL OR s.`RANK` <> 'deprecated')
                     AND CHAR_LENGTH(ev.VALUE_EXTERNAL_ID) <= 50 )
GROUP BY CAUSE
ORDER BY ENTITES DESC;


SELECT '1B. FILM, les 400 pertes sur INSTANCE_OF' AS SECTION;

SELECT CASE
         WHEN NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
                          WHERE s.ID_WIKIDATA = t2s.ID_WIKIDATA)
           THEN 'HORS PERIMETRE'
         WHEN NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
                          WHERE s.ID_WIKIDATA = t2s.ID_WIKIDATA AND s.ID_PROPERTY = 'P31')
           THEN 'SANS PROPRIETE'
         ELSE 'RANG DEPRECIE'
       END AS CAUSE,
       COUNT(*) AS ENTITES
FROM T_WC_T2S_MOVIE t2s
INNER JOIN T_WC_WIKIDATA_MOVIE_V1 w ON t2s.ID_WIKIDATA = w.ID_WIKIDATA
WHERE t2s.ID_IMDB IS NOT NULL AND t2s.ID_IMDB <> ''
  AND w.INSTANCE_OF IS NOT NULL AND w.INSTANCE_OF <> ''
  AND NOT EXISTS ( SELECT 1
                   FROM T_WC_WIKIDATA_STATEMENT s
                   JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = s.ID_STATEMENT
                   WHERE s.ID_WIKIDATA = t2s.ID_WIKIDATA AND s.ID_PROPERTY = 'P31'
                     AND (s.`RANK` IS NULL OR s.`RANK` <> 'deprecated') )
GROUP BY CAUSE
ORDER BY ENTITES DESC;


SELECT '1C. PERSONNE, les 1 105 pertes sur INSTANCE_OF' AS SECTION;

SELECT CASE
         WHEN NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
                          WHERE s.ID_WIKIDATA = t2s.ID_WIKIDATA)
           THEN 'HORS PERIMETRE'
         WHEN NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
                          WHERE s.ID_WIKIDATA = t2s.ID_WIKIDATA AND s.ID_PROPERTY = 'P31')
           THEN 'SANS PROPRIETE'
         ELSE 'RANG DEPRECIE'
       END AS CAUSE,
       COUNT(*) AS ENTITES
FROM T_WC_T2S_PERSON t2s
INNER JOIN T_WC_WIKIDATA_PERSON_V1 w ON t2s.ID_WIKIDATA = w.ID_WIKIDATA
WHERE t2s.ID_IMDB IS NOT NULL AND t2s.ID_IMDB <> ''
  AND t2s.ID_WIKIDATA IS NOT NULL AND t2s.ID_WIKIDATA <> ''
  AND w.INSTANCE_OF IS NOT NULL AND w.INSTANCE_OF <> ''
  AND NOT EXISTS ( SELECT 1
                   FROM T_WC_WIKIDATA_STATEMENT s
                   JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = s.ID_STATEMENT
                   WHERE s.ID_WIKIDATA = t2s.ID_WIKIDATA AND s.ID_PROPERTY = 'P31'
                     AND (s.`RANK` IS NULL OR s.`RANK` <> 'deprecated') )
GROUP BY CAUSE
ORDER BY ENTITES DESC;


SELECT '1D. SERIE, les 76 pertes sur INSTANCE_OF' AS SECTION;

SELECT CASE
         WHEN NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
                          WHERE s.ID_WIKIDATA = t2s.ID_WIKIDATA)
           THEN 'HORS PERIMETRE'
         WHEN NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
                          WHERE s.ID_WIKIDATA = t2s.ID_WIKIDATA AND s.ID_PROPERTY = 'P31')
           THEN 'SANS PROPRIETE'
         ELSE 'RANG DEPRECIE'
       END AS CAUSE,
       COUNT(*) AS ENTITES
FROM T_WC_T2S_SERIE t2s
INNER JOIN T_WC_WIKIDATA_SERIE_V1 w ON t2s.ID_WIKIDATA = w.ID_WIKIDATA
WHERE t2s.ID_IMDB IS NOT NULL AND t2s.ID_IMDB <> ''
  AND w.INSTANCE_OF IS NOT NULL AND w.INSTANCE_OF <> ''
  AND NOT EXISTS ( SELECT 1
                   FROM T_WC_WIKIDATA_STATEMENT s
                   JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = s.ID_STATEMENT
                   WHERE s.ID_WIKIDATA = t2s.ID_WIKIDATA AND s.ID_PROPERTY = 'P31'
                     AND (s.`RANK` IS NULL OR s.`RANK` <> 'deprecated') )
GROUP BY CAUSE
ORDER BY ENTITES DESC;


-- ============================================================================
-- 2. DIX PERDUS A REGARDER, pour mettre des noms sur les chiffres
-- ============================================================================
--
-- Un chiffre de perte ne dit pas si les entites perdues comptent. Dix titres le
-- disent tout de suite : dix films inconnus ne se traitent pas comme dix classiques.
-- ============================================================================

SELECT '2A. FILM, dix titres perdant INSTANCE_OF' AS SECTION;

SELECT t2s.ID_WIKIDATA, w.TITLE, w.INSTANCE_OF AS CLASSE_V1,
       EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
               WHERE s.ID_WIKIDATA = t2s.ID_WIKIDATA) AS CONNU_DE_V2
FROM T_WC_T2S_MOVIE t2s
INNER JOIN T_WC_WIKIDATA_MOVIE_V1 w ON t2s.ID_WIKIDATA = w.ID_WIKIDATA
WHERE t2s.ID_IMDB IS NOT NULL AND t2s.ID_IMDB <> ''
  AND w.INSTANCE_OF IS NOT NULL AND w.INSTANCE_OF <> ''
  AND NOT EXISTS ( SELECT 1
                   FROM T_WC_WIKIDATA_STATEMENT s
                   JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = s.ID_STATEMENT
                   WHERE s.ID_WIKIDATA = t2s.ID_WIKIDATA AND s.ID_PROPERTY = 'P31'
                     AND (s.`RANK` IS NULL OR s.`RANK` <> 'deprecated') )
ORDER BY t2s.ID_MOVIE ASC
LIMIT 10;

SELECT '2B. PERSONNE, dix noms perdant INSTANCE_OF' AS SECTION;

SELECT t2s.ID_WIKIDATA, w.NAME, w.INSTANCE_OF AS CLASSE_V1,
       EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
               WHERE s.ID_WIKIDATA = t2s.ID_WIKIDATA) AS CONNU_DE_V2
FROM T_WC_T2S_PERSON t2s
INNER JOIN T_WC_WIKIDATA_PERSON_V1 w ON t2s.ID_WIKIDATA = w.ID_WIKIDATA
WHERE t2s.ID_IMDB IS NOT NULL AND t2s.ID_IMDB <> ''
  AND t2s.ID_WIKIDATA IS NOT NULL AND t2s.ID_WIKIDATA <> ''
  AND w.INSTANCE_OF IS NOT NULL AND w.INSTANCE_OF <> ''
  AND NOT EXISTS ( SELECT 1
                   FROM T_WC_WIKIDATA_STATEMENT s
                   JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = s.ID_STATEMENT
                   WHERE s.ID_WIKIDATA = t2s.ID_WIKIDATA AND s.ID_PROPERTY = 'P31'
                     AND (s.`RANK` IS NULL OR s.`RANK` <> 'deprecated') )
ORDER BY t2s.ID_PERSON ASC
LIMIT 10;


-- ============================================================================
-- 3. CE QUE LA NOUVELLE REGLE DE CHOIX CHANGE VRAIMENT
-- ============================================================================
--
-- La regle a ete refaite le 2026-08-26 : elle ecarte le rang 'deprecated' et fait
-- passer 'preferred' devant. Cette section dit sur combien d'entites cela change
-- effectivement la valeur choisie. Si le chiffre est nul, la correction est une
-- assurance ; s'il ne l'est pas, elle etait necessaire.
-- ============================================================================

SELECT '3. Entites ou un rang deprecie serait sorti en tete de l ancienne regle' AS SECTION;

SELECT s.ID_PROPERTY AS PROPRIETE, COUNT(*) AS ENTITES
FROM ( SELECT st.ID_PROPERTY, st.ID_WIKIDATA,
              MIN(st.ID_STATEMENT) AS PREMIER,
              MIN(CASE WHEN st.`RANK` IS NULL OR st.`RANK` <> 'deprecated'
                       THEN st.ID_STATEMENT END) AS PREMIER_VALIDE
       FROM T_WC_WIKIDATA_STATEMENT st
       WHERE st.ID_PROPERTY IN ('P11460','P9584','P12279','P31')
       GROUP BY st.ID_PROPERTY, st.ID_WIKIDATA ) s
WHERE s.PREMIER_VALIDE IS NULL OR s.PREMIER <> s.PREMIER_VALIDE
GROUP BY s.ID_PROPERTY
ORDER BY ENTITES DESC;
