-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-043 : recette de l'enrichissement Wikidata V1 vers V2
-- ============================================================================
--
-- CE QUE CE FICHIER SERT A DECIDER. Les trois UPDATE d'enrichissement (processus
-- 4 T2S_MOVIE, 5 T2S_SERIE, 6 T2S_PERSON) lisaient quatre colonnes dans les tables
-- *_V1. Elles les lisent desormais dans les statements V2. Ce fichier repond a une
-- seule question, posee colonne par colonne : est-ce qu'on gagne de la couverture,
-- ou est-ce qu'on en perd ?
--
-- A LANCER DEUX FOIS. Une fois AVANT le passage de nuit : les sections A a D ne
-- lisent que les sources, jamais le resultat, elles PREDISENT donc ce que la nuit
-- va produire. Une fois APRES : la section E verifie que la colonne T2S est bien
-- devenue ce que la prediction annoncait. Si E ne correspond pas a A, le defaut est
-- dans le code, pas dans la donnee, et c'est precisement pour pouvoir faire cette
-- distinction que ce ticket passe en premier dans la migration.
--
-- ⚠ CE QU'IL NE FAUT PAS ATTENDRE DE CE FICHIER : une egalite valeur par valeur sur
-- INSTANCE_OF. V1 gardait la DERNIERE valeur que SPARQL lui avait renvoyee
-- (sparql-crawler.py:1323, une affectation dans une boucle, sans regle de tri) :
-- c'est une valeur arbitraire parmi les P31 de l'entite. Il n'existe donc aucune
-- valeur V1 a retrouver. La section C mesure l'accord pour l'information qu'il
-- porte, pas comme un critere a satisfaire : un desaccord n'y est pas une erreur,
-- c'est le prix, et le benefice, d'une regle de choix explicite.
--
-- ⚠ COLLATION. La premiere ligne n'est pas decorative. Sans elle, le client se
-- connecte en utf8mb4_general_ci pendant que les tables sont en unicode_ci, et
-- toute comparaison passant par un CAST rend ERROR 1267. Lancer avec --force.
--
-- Proprietes lues : P11460 Plex, P9584 Criterion, P12279 numero de collection
-- Criterion, P31 instance de.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ============================================================================
-- A. COUVERTURE CROISEE : ce que la bascule gagne et ce qu'elle perd
-- ============================================================================
--
-- Sur exactement la population que l'UPDATE touche (jointure V1, ID_IMDB rempli),
-- quatre cas par colonne. PERTE est le seul chiffre qui peut bloquer la bascule ;
-- GAIN est le benefice attendu ; COMMUN est la zone ou la section C s'applique.
-- ============================================================================

SELECT 'A1. FILM, couverture par colonne' AS SECTION;

SELECT 'PLEX_MEDIA_KEY (P11460)' AS COLONNE,
       COUNT(*)                                                          AS POPULATION,
       SUM(w.PLEX_MEDIA_KEY IS NOT NULL AND w.PLEX_MEDIA_KEY <> '')      AS V1_REMPLI,
       SUM(v2.ID_WIKIDATA IS NOT NULL)                                   AS V2_REMPLI,
       SUM(  (w.PLEX_MEDIA_KEY IS NOT NULL AND w.PLEX_MEDIA_KEY <> '')
         AND  v2.ID_WIKIDATA IS NOT NULL)                                AS COMMUN,
       SUM(  (w.PLEX_MEDIA_KEY IS NOT NULL AND w.PLEX_MEDIA_KEY <> '')
         AND  v2.ID_WIKIDATA IS NULL)                                    AS PERTE,
       SUM(  (w.PLEX_MEDIA_KEY IS NULL OR w.PLEX_MEDIA_KEY = '')
         AND  v2.ID_WIKIDATA IS NOT NULL)                                AS GAIN
FROM T_WC_T2S_MOVIE t2s
INNER JOIN T_WC_WIKIDATA_MOVIE_V1 w ON t2s.ID_WIKIDATA = w.ID_WIKIDATA
LEFT JOIN ( SELECT DISTINCT st.ID_WIKIDATA
            FROM T_WC_WIKIDATA_STATEMENT st
            JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE ev ON ev.ID_STATEMENT = st.ID_STATEMENT
            WHERE st.ID_PROPERTY = 'P11460'
              AND CHAR_LENGTH(ev.VALUE_EXTERNAL_ID) <= 50 ) v2 ON v2.ID_WIKIDATA = t2s.ID_WIKIDATA
WHERE t2s.ID_IMDB IS NOT NULL AND t2s.ID_IMDB <> ''

UNION ALL

-- ID_CRITERION est un ENTIER. Ecrire seulement IS NOT NULL compterait les zeros et
-- gonflerait V1_REMPLI : c'est l'artefact qui avait fait annoncer « 0 Criterion
-- retrouve sur 19 924 » alors que le vrai chiffre etait 1 673 sur 1 673. D'ou le
-- <> 0, ici et partout ou une colonne entiere est comptee.
SELECT 'ID_CRITERION (P9584)',
       COUNT(*),
       SUM(w.ID_CRITERION IS NOT NULL AND w.ID_CRITERION <> 0),
       SUM(v2.ID_WIKIDATA IS NOT NULL),
       SUM((w.ID_CRITERION IS NOT NULL AND w.ID_CRITERION <> 0) AND v2.ID_WIKIDATA IS NOT NULL),
       SUM((w.ID_CRITERION IS NOT NULL AND w.ID_CRITERION <> 0) AND v2.ID_WIKIDATA IS NULL),
       SUM((w.ID_CRITERION IS NULL OR w.ID_CRITERION = 0) AND v2.ID_WIKIDATA IS NOT NULL)
FROM T_WC_T2S_MOVIE t2s
INNER JOIN T_WC_WIKIDATA_MOVIE_V1 w ON t2s.ID_WIKIDATA = w.ID_WIKIDATA
LEFT JOIN ( SELECT DISTINCT st.ID_WIKIDATA
            FROM T_WC_WIKIDATA_STATEMENT st
            JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE ev ON ev.ID_STATEMENT = st.ID_STATEMENT
            WHERE st.ID_PROPERTY = 'P9584'
              AND ev.VALUE_EXTERNAL_ID REGEXP '^[0-9]+$' ) v2 ON v2.ID_WIKIDATA = t2s.ID_WIKIDATA
WHERE t2s.ID_IMDB IS NOT NULL AND t2s.ID_IMDB <> ''

UNION ALL

SELECT 'ID_CRITERION_SPINE (P12279)',
       COUNT(*),
       SUM(w.ID_CRITERION_SPINE IS NOT NULL AND w.ID_CRITERION_SPINE <> 0),
       SUM(v2.ID_WIKIDATA IS NOT NULL),
       SUM((w.ID_CRITERION_SPINE IS NOT NULL AND w.ID_CRITERION_SPINE <> 0) AND v2.ID_WIKIDATA IS NOT NULL),
       SUM((w.ID_CRITERION_SPINE IS NOT NULL AND w.ID_CRITERION_SPINE <> 0) AND v2.ID_WIKIDATA IS NULL),
       SUM((w.ID_CRITERION_SPINE IS NULL OR w.ID_CRITERION_SPINE = 0) AND v2.ID_WIKIDATA IS NOT NULL)
FROM T_WC_T2S_MOVIE t2s
INNER JOIN T_WC_WIKIDATA_MOVIE_V1 w ON t2s.ID_WIKIDATA = w.ID_WIKIDATA
LEFT JOIN ( SELECT DISTINCT st.ID_WIKIDATA
            FROM T_WC_WIKIDATA_STATEMENT st
            JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE ev ON ev.ID_STATEMENT = st.ID_STATEMENT
            WHERE st.ID_PROPERTY = 'P12279'
              AND ev.VALUE_EXTERNAL_ID REGEXP '^[0-9]+$' ) v2 ON v2.ID_WIKIDATA = t2s.ID_WIKIDATA
WHERE t2s.ID_IMDB IS NOT NULL AND t2s.ID_IMDB <> ''

UNION ALL

SELECT 'INSTANCE_OF (P31)',
       COUNT(*),
       SUM(w.INSTANCE_OF IS NOT NULL AND w.INSTANCE_OF <> ''),
       SUM(v2.ID_WIKIDATA IS NOT NULL),
       SUM((w.INSTANCE_OF IS NOT NULL AND w.INSTANCE_OF <> '') AND v2.ID_WIKIDATA IS NOT NULL),
       SUM((w.INSTANCE_OF IS NOT NULL AND w.INSTANCE_OF <> '') AND v2.ID_WIKIDATA IS NULL),
       SUM((w.INSTANCE_OF IS NULL OR w.INSTANCE_OF = '') AND v2.ID_WIKIDATA IS NOT NULL)
FROM T_WC_T2S_MOVIE t2s
INNER JOIN T_WC_WIKIDATA_MOVIE_V1 w ON t2s.ID_WIKIDATA = w.ID_WIKIDATA
LEFT JOIN ( SELECT DISTINCT st.ID_WIKIDATA
            FROM T_WC_WIKIDATA_STATEMENT st
            JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
            WHERE st.ID_PROPERTY = 'P31' ) v2 ON v2.ID_WIKIDATA = t2s.ID_WIKIDATA
WHERE t2s.ID_IMDB IS NOT NULL AND t2s.ID_IMDB <> '';


SELECT 'A2. SERIE, couverture par colonne' AS SECTION;

SELECT 'PLEX_MEDIA_KEY (P11460)' AS COLONNE,
       COUNT(*)                                                     AS POPULATION,
       SUM(w.PLEX_MEDIA_KEY IS NOT NULL AND w.PLEX_MEDIA_KEY <> '') AS V1_REMPLI,
       SUM(v2.ID_WIKIDATA IS NOT NULL)                              AS V2_REMPLI,
       SUM((w.PLEX_MEDIA_KEY IS NOT NULL AND w.PLEX_MEDIA_KEY <> '') AND v2.ID_WIKIDATA IS NOT NULL) AS COMMUN,
       SUM((w.PLEX_MEDIA_KEY IS NOT NULL AND w.PLEX_MEDIA_KEY <> '') AND v2.ID_WIKIDATA IS NULL)     AS PERTE,
       SUM((w.PLEX_MEDIA_KEY IS NULL OR w.PLEX_MEDIA_KEY = '') AND v2.ID_WIKIDATA IS NOT NULL)       AS GAIN
FROM T_WC_T2S_SERIE t2s
INNER JOIN T_WC_WIKIDATA_SERIE_V1 w ON t2s.ID_WIKIDATA = w.ID_WIKIDATA
LEFT JOIN ( SELECT DISTINCT st.ID_WIKIDATA
            FROM T_WC_WIKIDATA_STATEMENT st
            JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE ev ON ev.ID_STATEMENT = st.ID_STATEMENT
            WHERE st.ID_PROPERTY = 'P11460'
              AND CHAR_LENGTH(ev.VALUE_EXTERNAL_ID) <= 50 ) v2 ON v2.ID_WIKIDATA = t2s.ID_WIKIDATA
WHERE t2s.ID_IMDB IS NOT NULL AND t2s.ID_IMDB <> ''

UNION ALL

SELECT 'INSTANCE_OF (P31)',
       COUNT(*),
       SUM(w.INSTANCE_OF IS NOT NULL AND w.INSTANCE_OF <> ''),
       SUM(v2.ID_WIKIDATA IS NOT NULL),
       SUM((w.INSTANCE_OF IS NOT NULL AND w.INSTANCE_OF <> '') AND v2.ID_WIKIDATA IS NOT NULL),
       SUM((w.INSTANCE_OF IS NOT NULL AND w.INSTANCE_OF <> '') AND v2.ID_WIKIDATA IS NULL),
       SUM((w.INSTANCE_OF IS NULL OR w.INSTANCE_OF = '') AND v2.ID_WIKIDATA IS NOT NULL)
FROM T_WC_T2S_SERIE t2s
INNER JOIN T_WC_WIKIDATA_SERIE_V1 w ON t2s.ID_WIKIDATA = w.ID_WIKIDATA
LEFT JOIN ( SELECT DISTINCT st.ID_WIKIDATA
            FROM T_WC_WIKIDATA_STATEMENT st
            JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
            WHERE st.ID_PROPERTY = 'P31' ) v2 ON v2.ID_WIKIDATA = t2s.ID_WIKIDATA
WHERE t2s.ID_IMDB IS NOT NULL AND t2s.ID_IMDB <> '';


SELECT 'A3. PERSONNE, couverture INSTANCE_OF' AS SECTION;

SELECT 'INSTANCE_OF (P31)' AS COLONNE,
       COUNT(*)                                                AS POPULATION,
       SUM(w.INSTANCE_OF IS NOT NULL AND w.INSTANCE_OF <> '')  AS V1_REMPLI,
       SUM(v2.ID_WIKIDATA IS NOT NULL)                         AS V2_REMPLI,
       SUM((w.INSTANCE_OF IS NOT NULL AND w.INSTANCE_OF <> '') AND v2.ID_WIKIDATA IS NOT NULL) AS COMMUN,
       SUM((w.INSTANCE_OF IS NOT NULL AND w.INSTANCE_OF <> '') AND v2.ID_WIKIDATA IS NULL)     AS PERTE,
       SUM((w.INSTANCE_OF IS NULL OR w.INSTANCE_OF = '') AND v2.ID_WIKIDATA IS NOT NULL)       AS GAIN
FROM T_WC_T2S_PERSON t2s
INNER JOIN T_WC_WIKIDATA_PERSON_V1 w ON t2s.ID_WIKIDATA = w.ID_WIKIDATA
LEFT JOIN ( SELECT DISTINCT st.ID_WIKIDATA
            FROM T_WC_WIKIDATA_STATEMENT st
            JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
            WHERE st.ID_PROPERTY = 'P31' ) v2 ON v2.ID_WIKIDATA = t2s.ID_WIKIDATA
WHERE t2s.ID_IMDB IS NOT NULL AND t2s.ID_IMDB <> ''
  AND t2s.ID_WIKIDATA IS NOT NULL AND t2s.ID_WIKIDATA <> '';


-- ============================================================================
-- B. MULTIPLICITE : combien de fois la regle de choix intervient vraiment
-- ============================================================================
--
-- V1 rangeait une valeur, V2 peut en porter plusieurs. La ou une seule existe, la
-- regle de choix ne change rien et la bascule est mecanique. Ce chiffre dit donc
-- quelle part du resultat depend reellement d'un arbitrage de notre part. S'il est
-- proche de zero sur les identifiants externes, la question du choix est theorique
-- pour eux ; sur P31 il sera eleve, et c'est normal : une entite appartient
-- souvent a plusieurs classes.
-- ============================================================================

SELECT 'B. Entites portant plus d une valeur pour la propriete' AS SECTION;

SELECT st.ID_PROPERTY                                  AS PROPRIETE,
       COUNT(DISTINCT st.ID_WIKIDATA)                  AS ENTITES,
       COUNT(*)                                        AS STATEMENTS,
       SUM(st.IS_BEST_VALUE = 1)                       AS MARQUES_MEILLEURS
FROM T_WC_WIKIDATA_STATEMENT st
WHERE st.ID_PROPERTY IN ('P11460','P9584','P12279','P31')
GROUP BY st.ID_PROPERTY
ORDER BY st.ID_PROPERTY;

SELECT 'B2. Repartition du nombre de valeurs par entite' AS SECTION;

SELECT ID_PROPERTY                      AS PROPRIETE,
       NB_VALEURS                       AS VALEURS_PAR_ENTITE,
       COUNT(*)                         AS ENTITES
FROM ( SELECT st.ID_PROPERTY, st.ID_WIKIDATA, COUNT(*) AS NB_VALEURS
       FROM T_WC_WIKIDATA_STATEMENT st
       WHERE st.ID_PROPERTY IN ('P11460','P9584','P12279')
       GROUP BY st.ID_PROPERTY, st.ID_WIKIDATA ) d
GROUP BY ID_PROPERTY, NB_VALEURS
ORDER BY ID_PROPERTY, NB_VALEURS;


-- ============================================================================
-- C. ACCORD DES VALEURS, sur la zone commune seulement
-- ============================================================================
--
-- Ici la vraie regle de choix est appliquee, celle du code : meilleur rang, puis
-- ordre d'affichage, puis identifiant de statement. Sur les identifiants externes,
-- un desaccord merite un coup d'oeil : deux sources qui donnent deux Criterion
-- differents pour le meme film, c'est soit une erreur de V1, soit une valeur
-- Wikidata qui a change depuis. Sur INSTANCE_OF, relire l'avertissement en tete de
-- fichier : le desaccord y est attendu et n'invalide rien.
--
-- Le sondage Criterion (C1, C2) est PILOTE PAR V1 : il ne regarde que les films qui
-- portent deja un identifiant Criterion, environ 1 673 lignes. Echantillonner 20 000
-- films au hasard ne ramenerait presque rien, puisque Criterion couvre moins de deux
-- pour mille du catalogue. Les sections C3 et C4 prennent au contraire les 20 000
-- premieres lignes DANS L'ORDRE DE LA TABLE : ce n'est pas un tirage aleatoire, et
-- cela suffit pour un ordre de grandeur, pas pour un chiffre a citer.
-- ============================================================================

SELECT 'C1. FILM, accord Criterion sur la zone commune' AS SECTION;

SELECT SUM(V2VAL = w.ID_CRITERION)  AS ACCORD,
       SUM(V2VAL <> w.ID_CRITERION) AS DESACCORD,
       COUNT(*)                     AS ECHANTILLON
FROM ( SELECT t2s.ID_WIKIDATA,
              ( SELECT CAST(scrv.VALUE_EXTERNAL_ID AS UNSIGNED)
                FROM T_WC_WIKIDATA_STATEMENT scr
                JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE scrv ON scrv.ID_STATEMENT = scr.ID_STATEMENT
                WHERE scr.ID_WIKIDATA = t2s.ID_WIKIDATA AND scr.ID_PROPERTY = 'P9584'
                  AND scrv.VALUE_EXTERNAL_ID REGEXP '^[0-9]+$'
                ORDER BY COALESCE(scr.IS_BEST_VALUE, 0) DESC,
                         COALESCE(scr.DISPLAY_ORDER, 2147483647) ASC, scr.ID_STATEMENT ASC
                LIMIT 1 ) AS V2VAL
       FROM T_WC_T2S_MOVIE t2s
       INNER JOIN T_WC_WIKIDATA_MOVIE_V1 wd ON wd.ID_WIKIDATA = t2s.ID_WIKIDATA
       WHERE t2s.ID_IMDB IS NOT NULL AND t2s.ID_IMDB <> ''
         AND wd.ID_CRITERION IS NOT NULL AND wd.ID_CRITERION <> 0 ) e
INNER JOIN T_WC_WIKIDATA_MOVIE_V1 w ON w.ID_WIKIDATA = e.ID_WIKIDATA
WHERE e.V2VAL IS NOT NULL
  AND w.ID_CRITERION IS NOT NULL AND w.ID_CRITERION <> 0;

SELECT 'C2. FILM, dix desaccords Criterion a regarder' AS SECTION;

SELECT e.ID_WIKIDATA, w.TITLE, w.ID_CRITERION AS V1, e.V2VAL AS V2
FROM ( SELECT t2s.ID_WIKIDATA,
              ( SELECT CAST(scrv.VALUE_EXTERNAL_ID AS UNSIGNED)
                FROM T_WC_WIKIDATA_STATEMENT scr
                JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE scrv ON scrv.ID_STATEMENT = scr.ID_STATEMENT
                WHERE scr.ID_WIKIDATA = t2s.ID_WIKIDATA AND scr.ID_PROPERTY = 'P9584'
                  AND scrv.VALUE_EXTERNAL_ID REGEXP '^[0-9]+$'
                ORDER BY COALESCE(scr.IS_BEST_VALUE, 0) DESC,
                         COALESCE(scr.DISPLAY_ORDER, 2147483647) ASC, scr.ID_STATEMENT ASC
                LIMIT 1 ) AS V2VAL
       FROM T_WC_T2S_MOVIE t2s
       INNER JOIN T_WC_WIKIDATA_MOVIE_V1 wd ON wd.ID_WIKIDATA = t2s.ID_WIKIDATA
       WHERE t2s.ID_IMDB IS NOT NULL AND t2s.ID_IMDB <> ''
         AND wd.ID_CRITERION IS NOT NULL AND wd.ID_CRITERION <> 0 ) e
INNER JOIN T_WC_WIKIDATA_MOVIE_V1 w ON w.ID_WIKIDATA = e.ID_WIKIDATA
WHERE e.V2VAL IS NOT NULL
  AND w.ID_CRITERION IS NOT NULL AND w.ID_CRITERION <> 0
  AND e.V2VAL <> w.ID_CRITERION
LIMIT 10;

SELECT 'C3. FILM, accord INSTANCE_OF sur la zone commune (informatif)' AS SECTION;

SELECT SUM(V2VAL = w.INSTANCE_OF)  AS ACCORD,
       SUM(V2VAL <> w.INSTANCE_OF) AS DESACCORD,
       COUNT(*)                    AS ECHANTILLON
FROM ( SELECT t2s.ID_WIKIDATA,
              ( SELECT siov.ID_ITEM
                FROM T_WC_WIKIDATA_STATEMENT sio
                JOIN T_WC_WIKIDATA_ITEM_VALUE siov ON siov.ID_STATEMENT = sio.ID_STATEMENT
                WHERE sio.ID_WIKIDATA = t2s.ID_WIKIDATA AND sio.ID_PROPERTY = 'P31'
                ORDER BY COALESCE(sio.IS_BEST_VALUE, 0) DESC,
                         COALESCE(sio.DISPLAY_ORDER, 2147483647) ASC, sio.ID_STATEMENT ASC
                LIMIT 1 ) AS V2VAL
       FROM T_WC_T2S_MOVIE t2s
       WHERE t2s.ID_IMDB IS NOT NULL AND t2s.ID_IMDB <> ''
         AND t2s.ID_WIKIDATA IS NOT NULL AND t2s.ID_WIKIDATA <> ''
       LIMIT 20000 ) e
INNER JOIN T_WC_WIKIDATA_MOVIE_V1 w ON w.ID_WIKIDATA = e.ID_WIKIDATA
WHERE e.V2VAL IS NOT NULL
  AND w.INSTANCE_OF IS NOT NULL AND w.INSTANCE_OF <> '';

SELECT 'C4. FILM, les classes que V2 choisit le plus souvent' AS SECTION;

SELECT V2VAL AS CLASSE, COUNT(*) AS FILMS
FROM ( SELECT ( SELECT siov.ID_ITEM
                FROM T_WC_WIKIDATA_STATEMENT sio
                JOIN T_WC_WIKIDATA_ITEM_VALUE siov ON siov.ID_STATEMENT = sio.ID_STATEMENT
                WHERE sio.ID_WIKIDATA = t2s.ID_WIKIDATA AND sio.ID_PROPERTY = 'P31'
                ORDER BY COALESCE(sio.IS_BEST_VALUE, 0) DESC,
                         COALESCE(sio.DISPLAY_ORDER, 2147483647) ASC, sio.ID_STATEMENT ASC
                LIMIT 1 ) AS V2VAL
       FROM T_WC_T2S_MOVIE t2s
       WHERE t2s.ID_IMDB IS NOT NULL AND t2s.ID_IMDB <> ''
         AND t2s.ID_WIKIDATA IS NOT NULL AND t2s.ID_WIKIDATA <> ''
       LIMIT 20000 ) e
WHERE V2VAL IS NOT NULL
GROUP BY V2VAL
ORDER BY FILMS DESC
LIMIT 10;


-- ============================================================================
-- D. LE PIEGE DU ZERO, avant et apres
-- ============================================================================
--
-- Les deux colonnes Criterion sont des ENTIERS et la source V2 est du TEXTE. Une
-- conversion sans garde rend 0 pour toute valeur non numerique, et un 0 se lit
-- comme « present » par un IS NOT NULL distrait. Le code pose un garde REGEXP pour
-- que cela n'arrive pas ; cette section verifie qu'il tient. Attendu : zero ligne.
-- ============================================================================

SELECT 'D1. Valeurs Criterion non numeriques dans V2 (ecartees par le garde)' AS SECTION;

SELECT st.ID_PROPERTY, ev.VALUE_EXTERNAL_ID, COUNT(*) AS OCCURRENCES
FROM T_WC_WIKIDATA_STATEMENT st
JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE ev ON ev.ID_STATEMENT = st.ID_STATEMENT
WHERE st.ID_PROPERTY IN ('P9584','P12279')
  AND ev.VALUE_EXTERNAL_ID NOT REGEXP '^[0-9]+$'
GROUP BY st.ID_PROPERTY, ev.VALUE_EXTERNAL_ID
ORDER BY OCCURRENCES DESC
LIMIT 20;

SELECT 'D3. Cles Plex trop longues pour la colonne cible (ecartees par le garde)' AS SECTION;

-- PLEX_MEDIA_KEY est en varchar(50) cote T2S, la valeur externe V2 en varchar(1200).
-- Une valeur trop longue tronquerait en silence, ou ferait avorter le processus de nuit
-- si le serveur est en mode strict. Le code l'ecarte ; cette section dit combien.
-- Attendu : zero ligne. Si ce n'est pas zero, regarder les valeurs avant de conclure :
-- une cle Plex de plus de cinquante caracteres est probablement une donnee douteuse
-- cote Wikidata, pas une colonne trop etroite.
SELECT ev.VALUE_EXTERNAL_ID, CHAR_LENGTH(ev.VALUE_EXTERNAL_ID) AS LONGUEUR, COUNT(*) AS OCCURRENCES
FROM T_WC_WIKIDATA_STATEMENT st
JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE ev ON ev.ID_STATEMENT = st.ID_STATEMENT
WHERE st.ID_PROPERTY = 'P11460'
  AND CHAR_LENGTH(ev.VALUE_EXTERNAL_ID) > 50
GROUP BY ev.VALUE_EXTERNAL_ID
ORDER BY LONGUEUR DESC
LIMIT 20;

SELECT 'D2. Zeros presents dans T2S (attendu : 0 avant comme apres)' AS SECTION;

SELECT SUM(ID_CRITERION = 0)       AS CRITERION_ZERO,
       SUM(ID_CRITERION_SPINE = 0) AS SPINE_ZERO
FROM T_WC_T2S_MOVIE;


-- ============================================================================
-- E. APRES LE PASSAGE : la colonne T2S dit-elle ce que la prediction annoncait ?
-- ============================================================================
--
-- A ne lancer qu'APRES l'execution des processus 4, 5 et 6. Les chiffres de la
-- colonne T2S doivent rejoindre la colonne V2_REMPLI de la section A, aux lignes
-- que la fenetre incrementale n'a pas encore traitees pres. Un ecart qui ne
-- s'explique pas par la fenetre est un defaut de code.
-- ============================================================================

SELECT 'E1. Etat des colonnes T2S apres passage' AS SECTION;

SELECT 'T2S_MOVIE' AS TABLE_T2S,
       COUNT(*)                                                    AS LIGNES,
       SUM(PLEX_MEDIA_KEY IS NOT NULL AND PLEX_MEDIA_KEY <> '')    AS PLEX,
       SUM(ID_CRITERION IS NOT NULL AND ID_CRITERION <> 0)         AS CRITERION,
       SUM(ID_CRITERION_SPINE IS NOT NULL AND ID_CRITERION_SPINE <> 0) AS SPINE,
       SUM(INSTANCE_OF IS NOT NULL AND INSTANCE_OF <> '')          AS INSTANCE_OF
FROM T_WC_T2S_MOVIE
WHERE ID_IMDB IS NOT NULL AND ID_IMDB <> ''

UNION ALL

SELECT 'T2S_SERIE', COUNT(*),
       SUM(PLEX_MEDIA_KEY IS NOT NULL AND PLEX_MEDIA_KEY <> ''),
       NULL, NULL,
       SUM(INSTANCE_OF IS NOT NULL AND INSTANCE_OF <> '')
FROM T_WC_T2S_SERIE
WHERE ID_IMDB IS NOT NULL AND ID_IMDB <> ''

UNION ALL

SELECT 'T2S_PERSON', COUNT(*), NULL, NULL, NULL,
       SUM(INSTANCE_OF IS NOT NULL AND INSTANCE_OF <> '')
FROM T_WC_T2S_PERSON
WHERE ID_IMDB IS NOT NULL AND ID_IMDB <> '';

SELECT 'E2. Lignes ou T2S ne correspond pas a ce que V2 donnerait (attendu : 0)' AS SECTION;

SELECT COUNT(*) AS DIVERGENCES
FROM T_WC_T2S_MOVIE t2s
INNER JOIN T_WC_WIKIDATA_MOVIE_V1 w ON t2s.ID_WIKIDATA = w.ID_WIKIDATA
WHERE t2s.ID_IMDB IS NOT NULL AND t2s.ID_IMDB <> ''
  AND NOT ( t2s.INSTANCE_OF <=> ( SELECT siov.ID_ITEM
              FROM T_WC_WIKIDATA_STATEMENT sio
              JOIN T_WC_WIKIDATA_ITEM_VALUE siov ON siov.ID_STATEMENT = sio.ID_STATEMENT
              WHERE sio.ID_WIKIDATA = t2s.ID_WIKIDATA AND sio.ID_PROPERTY = 'P31'
              ORDER BY COALESCE(sio.IS_BEST_VALUE, 0) DESC,
                       COALESCE(sio.DISPLAY_ORDER, 2147483647) ASC, sio.ID_STATEMENT ASC
              LIMIT 1 ) );
