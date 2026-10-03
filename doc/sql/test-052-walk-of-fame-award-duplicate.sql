-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-052 : deux lignes de T_WC_T2S_AWARD pour l'etoile du
-- Walk of Fame, et la resolution d'entite choisit la mauvaise
-- ============================================================================
--
-- CE QUI A ETE CONSTATE (2026-10-03, sur Green). L'evaluation 2309 filtre sur
-- AWARD_NAME = 'star on Hollywood Walk of Fame'. Reformulee en « People with a
-- star on the Hollywood Walk of Fame », la question passe par l'extraction, qui
-- ne garde que « Hollywood Walk of Fame » ; la resolution trouve une seconde
-- ligne portant exactement ce nom, la prefere, et le resultat change du tout au
-- tout. Ce fichier dit ce qu'est cette seconde ligne.
--
-- CE QUE CHAQUE BLOC TRANCHE.
--   D1 . les lignes « walk of fame » de T_WC_T2S_AWARD, et leurs liens
--   D2 . leur classe Wikidata (P31), et si elle tombe dans le cone de -036
--   D3 . le recouvrement des personnes : doublon de declaration, ou ensemble a part ?
--   D4 . echantillon des personnes rattachees aux lignes autres que « star on »
--   D5 . d'ou vient le lien dans Wikidata : P166 ou une autre propriete ?
--   D6 . d'autres paires du meme type dans la table (« star on X » et « X »,
--        « X Award » et « X »)
--
-- COMMENT LIRE.
--   - D2 dit « dans le cone » pour une ligne qui n'est pas une distinction :
--     pollution, a exclure (liste d'exceptions ou cone resserre).
--   - D3 montre que presque toutes les personnes de la seconde ligne sont aussi
--     sur la premiere : doublon de declaration sur Wikidata, la fusion ou
--     l'exclusion sont indolores.
--   - D3 montre un ensemble a part : ces personnes perdraient leur etoile a
--     l'exclusion ; il faut alors les reporter sur la ligne « star on ».
--   - D6 rend d'autres paires : le ticket ne vise pas un cas isole, la regle
--     doit etre generale.
--
-- LECTURE SEULE. Trois tables temporaires en D6, rien d'autre n'est ecrit.
-- Aucune requete sur information_schema (voir AGENTS.md).
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;


-- ############################################################################
-- D1 . LES LIGNES « WALK OF FAME » ET LEURS LIENS
-- ############################################################################
-- Toutes les lignes, supprimees comprises : une ligne DELETED = 1 encore vue
-- par l'index de resolution serait une autre explication.

SELECT '=== D1 . lignes walk of fame de T_WC_T2S_AWARD ===' AS section;

SELECT a.ID_AWARD,
       a.ID_WIKIDATA,
       a.AWARD_NAME,
       a.AWARD_NAME_FR,
       a.AWARD_SOURCE,
       a.AWARD_TYPE,
       a.DELETED,
       a.PERSON_COUNT,
       (SELECT COUNT(*) FROM T_WC_T2S_PERSON_AWARD pa
         WHERE pa.ID_AWARD = a.ID_AWARD AND (pa.DELETED IS NULL OR pa.DELETED = 0)) AS liens_personnes,
       (SELECT COUNT(*) FROM T_WC_T2S_MOVIE_AWARD ma
         WHERE ma.ID_AWARD = a.ID_AWARD AND (ma.DELETED IS NULL OR ma.DELETED = 0)) AS liens_films,
       (SELECT COUNT(*) FROM T_WC_T2S_SERIE_AWARD sa
         WHERE sa.ID_AWARD = a.ID_AWARD AND (sa.DELETED IS NULL OR sa.DELETED = 0)) AS liens_series,
       LEFT(a.OVERVIEW, 200) AS overview
FROM   T_WC_T2S_AWARD a
WHERE  a.AWARD_NAME LIKE '%walk of fame%'
   OR  a.AWARD_NAME_FR LIKE '%walk of fame%'
ORDER  BY liens_personnes DESC;


-- ############################################################################
-- D2 . CLASSE WIKIDATA ET VERDICT DU CONE
-- ############################################################################
-- Meme cone que -036 (P279 sous Q618779, award), meme regle : « au moins un P31
-- dans le cone ». Le CAST de l'ancre est obligatoire (ERROR 1406 sinon).

SELECT '=== D2 . classes P31 et verdict du cone ===' AS section;

WITH RECURSIVE cone_award (qid) AS (
    SELECT CAST(r.qid AS CHAR(50)) COLLATE utf8mb4_unicode_ci AS qid
    FROM   (SELECT 'Q618779' AS qid) AS r
    UNION
    SELECT sc.ID_CHILD
    FROM   T_WC_WIKIDATA_SUBCLASS sc
    JOIN   cone_award c ON c.qid = sc.ID_PARENT
    WHERE  sc.DELETED = 0
)
SELECT a.ID_AWARD,
       a.ID_WIKIDATA,
       a.AWARD_NAME,
       it.LABEL_EN                                   AS label_wikidata,
       LEFT(it.DESCRIPTION_EN, 150)                  AS description_wikidata,
       iv.ID_ITEM                                    AS p31,
       COALESCE(cl.LABEL_EN, '(classe non cachee)')  AS p31_label,
       CASE WHEN iv.ID_ITEM IN (SELECT qid FROM cone_award)
            THEN 'dans le cone' ELSE 'hors cone' END AS verdict
FROM   T_WC_T2S_AWARD a
LEFT   JOIN T_WC_WIKIDATA_ITEM it       ON it.ID_WIKIDATA = a.ID_WIKIDATA
LEFT   JOIN T_WC_WIKIDATA_STATEMENT st  ON st.ID_WIKIDATA = a.ID_WIKIDATA
                                       AND st.ID_PROPERTY = 'P31'
LEFT   JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
LEFT   JOIN T_WC_WIKIDATA_ITEM cl       ON cl.ID_WIKIDATA = iv.ID_ITEM
WHERE  a.AWARD_NAME LIKE '%walk of fame%'
ORDER  BY a.ID_AWARD, verdict, p31;


-- ############################################################################
-- D3 . RECOUVREMENT DES PERSONNES
-- ############################################################################
-- Chaque personne rattachee a au moins une ligne « walk of fame », regroupee
-- par la combinaison exacte des lignes qui la portent. Une combinaison a un
-- seul ID_AWARD, celui de la seconde ligne, compte les personnes qui perdraient
-- leur etoile si on l'excluait sans report.

SELECT '=== D3 . personnes par combinaison de lignes walk of fame ===' AS section;

SELECT combinaison,
       COUNT(*) AS personnes
FROM  (SELECT pa.ID_PERSON,
              GROUP_CONCAT(DISTINCT CONCAT(a.ID_AWARD, ' ', a.AWARD_NAME)
                           ORDER BY a.ID_AWARD SEPARATOR ' + ') AS combinaison
       FROM   T_WC_T2S_PERSON_AWARD pa
       JOIN   T_WC_T2S_AWARD a ON a.ID_AWARD = pa.ID_AWARD
       WHERE  a.AWARD_NAME LIKE '%walk of fame%'
         AND  (pa.DELETED IS NULL OR pa.DELETED = 0)
       GROUP  BY pa.ID_PERSON) AS p
GROUP  BY combinaison
ORDER  BY personnes DESC;


-- ############################################################################
-- D4 . ECHANTILLON DES PERSONNES HORS « STAR ON »
-- ############################################################################
-- Les quarante plus populaires parmi celles qui sont sur une ligne « walk of
-- fame » sans etre sur une ligne « star on ... » : a regarder une par une sur
-- Wikidata pour juger si la declaration est fautive ou seulement autrement
-- formulee.

SELECT '=== D4 . personnes sur une ligne walk of fame sans star on, 40 premieres ===' AS section;

SELECT p.ID_PERSON,
       p.PERSON_NAME,
       p.ID_WIKIDATA,
       ROUND(p.POPULARITY, 1) AS popularite,
       GROUP_CONCAT(DISTINCT a.ID_AWARD ORDER BY a.ID_AWARD SEPARATOR ', ') AS lignes
FROM   T_WC_T2S_PERSON_AWARD pa
JOIN   T_WC_T2S_AWARD a  ON a.ID_AWARD = pa.ID_AWARD
JOIN   T_WC_T2S_PERSON p ON p.ID_PERSON = pa.ID_PERSON
WHERE  a.AWARD_NAME LIKE '%walk of fame%'
  AND  a.AWARD_NAME NOT LIKE 'star on %'
  AND  (pa.DELETED IS NULL OR pa.DELETED = 0)
  AND  NOT EXISTS (SELECT 1
                   FROM   T_WC_T2S_PERSON_AWARD pa2
                   JOIN   T_WC_T2S_AWARD a2 ON a2.ID_AWARD = pa2.ID_AWARD
                   WHERE  pa2.ID_PERSON = pa.ID_PERSON
                     AND  a2.AWARD_NAME LIKE 'star on %walk of fame%'
                     AND  (pa2.DELETED IS NULL OR pa2.DELETED = 0))
GROUP  BY p.ID_PERSON, p.PERSON_NAME, p.ID_WIKIDATA, p.POPULARITY
ORDER  BY p.POPULARITY DESC
LIMIT  40;


-- ############################################################################
-- D5 . LA PROPRIETE WIKIDATA QUI PORTE LE LIEN
-- ############################################################################
-- Tous les statements dont la valeur est l'une de ces lignes, par propriete.
-- P166 (distinction recue) attendu pour la ligne « star on » ; pour la seconde,
-- une autre propriete (P1411 nomination, P793 evenement...) dirait que le
-- preprocessing l'a ramassee par un autre chemin.

SELECT '=== D5 . statements Wikidata pointant vers ces lignes, par propriete ===' AS section;

SELECT a.ID_AWARD,
       a.ID_WIKIDATA,
       a.AWARD_NAME,
       st.ID_PROPERTY,
       COUNT(*)                        AS statements,
       COUNT(DISTINCT st.ID_WIKIDATA)  AS sujets_distincts
FROM   T_WC_T2S_AWARD a
JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_ITEM = a.ID_WIKIDATA
JOIN   T_WC_WIKIDATA_STATEMENT st  ON st.ID_STATEMENT = iv.ID_STATEMENT
WHERE  a.AWARD_NAME LIKE '%walk of fame%'
GROUP  BY a.ID_AWARD, a.ID_WIKIDATA, a.AWARD_NAME, st.ID_PROPERTY
ORDER  BY a.ID_AWARD, statements DESC;


-- ############################################################################
-- D6 . D'AUTRES PAIRES DU MEME TYPE
-- ############################################################################
-- Deux motifs, cherches par egalite (une recherche « contenu dans » sur 44 000
-- noms croiserait deux milliards de paires) :
--   M1 . « <quelque chose> on X » ou « <quelque chose> of X » face a « X »
--   M2 . « X Award » ou « X Prize » face a « X »
-- Trois tables temporaires (les noms, leur copie, les cles), parce que MariaDB
-- refuse de rouvrir une meme table temporaire dans une requete (ERROR 1137).
-- Seules les paires dont les deux lignes ont au moins un lien sont rendues :
-- ce sont celles qui peuvent faire basculer une resolution.

SELECT '=== D6 . paires de noms du meme type, 50 premieres ===' AS section;

DROP TEMPORARY TABLE IF EXISTS tmp_052_cle;
DROP TEMPORARY TABLE IF EXISTS tmp_052_long;
DROP TEMPORARY TABLE IF EXISTS tmp_052_court;

CREATE TEMPORARY TABLE tmp_052_long AS
SELECT a.ID_AWARD,
       a.ID_WIKIDATA,
       a.AWARD_NAME,
       (SELECT COUNT(*) FROM T_WC_T2S_PERSON_AWARD pa WHERE pa.ID_AWARD = a.ID_AWARD AND (pa.DELETED IS NULL OR pa.DELETED = 0))
     + (SELECT COUNT(*) FROM T_WC_T2S_MOVIE_AWARD  ma WHERE ma.ID_AWARD = a.ID_AWARD AND (ma.DELETED IS NULL OR ma.DELETED = 0))
     + (SELECT COUNT(*) FROM T_WC_T2S_SERIE_AWARD  sa WHERE sa.ID_AWARD = a.ID_AWARD AND (sa.DELETED IS NULL OR sa.DELETED = 0)) AS liens
FROM   T_WC_T2S_AWARD a
WHERE  (a.DELETED IS NULL OR a.DELETED = 0)
  AND  a.AWARD_NAME IS NOT NULL AND a.AWARD_NAME <> '';

DELETE FROM tmp_052_long WHERE liens = 0;
ALTER TABLE tmp_052_long ADD INDEX (AWARD_NAME);

CREATE TEMPORARY TABLE tmp_052_court LIKE tmp_052_long;
INSERT INTO tmp_052_court SELECT * FROM tmp_052_long;

-- Les cles de jointure, un INSERT par motif : une seule ouverture de
-- tmp_052_long par instruction.
CREATE TEMPORARY TABLE tmp_052_cle (
    motif    VARCHAR(10)  NOT NULL,
    ID_AWARD INT          NOT NULL,
    cle      VARCHAR(250) COLLATE utf8mb4_unicode_ci,
    INDEX (cle)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;

INSERT INTO tmp_052_cle SELECT 'M1 on',    ID_AWARD, SUBSTRING(AWARD_NAME, LOCATE(' on ', AWARD_NAME) + 4)
                        FROM tmp_052_long WHERE LOCATE(' on ', AWARD_NAME) > 0;
INSERT INTO tmp_052_cle SELECT 'M1 of',    ID_AWARD, SUBSTRING(AWARD_NAME, LOCATE(' of ', AWARD_NAME) + 4)
                        FROM tmp_052_long WHERE LOCATE(' of ', AWARD_NAME) > 0;
INSERT INTO tmp_052_cle SELECT 'M2 award', ID_AWARD, LEFT(AWARD_NAME, CHAR_LENGTH(AWARD_NAME) - 6)
                        FROM tmp_052_long WHERE AWARD_NAME LIKE '% Award';
INSERT INTO tmp_052_cle SELECT 'M2 prize', ID_AWARD, LEFT(AWARD_NAME, CHAR_LENGTH(AWARD_NAME) - 6)
                        FROM tmp_052_long WHERE AWARD_NAME LIKE '% Prize';

SELECT k.motif,
       l.ID_AWARD    AS id_long,  l.ID_WIKIDATA AS qid_long,  l.AWARD_NAME AS nom_long,  l.liens AS liens_long,
       c.ID_AWARD    AS id_court, c.ID_WIKIDATA AS qid_court, c.AWARD_NAME AS nom_court, c.liens AS liens_court
FROM   tmp_052_cle   k
JOIN   tmp_052_long  l ON l.ID_AWARD = k.ID_AWARD
JOIN   tmp_052_court c ON c.AWARD_NAME = k.cle AND c.ID_AWARD <> l.ID_AWARD
ORDER  BY LEAST(l.liens, c.liens) DESC
LIMIT  50;

SELECT '=== D6 bis . nombre total de paires par motif ===' AS section;

SELECT k.motif, COUNT(*) AS paires
FROM   tmp_052_cle   k
JOIN   tmp_052_court c ON c.AWARD_NAME = k.cle AND c.ID_AWARD <> k.ID_AWARD
GROUP  BY k.motif
ORDER  BY k.motif;

DROP TEMPORARY TABLE IF EXISTS tmp_052_cle;
DROP TEMPORARY TABLE IF EXISTS tmp_052_long;
DROP TEMPORARY TABLE IF EXISTS tmp_052_court;
