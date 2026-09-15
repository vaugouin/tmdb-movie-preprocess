-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-014 : combien de LOCATION_TYPE sont FAUX, et lesquels
-- ============================================================================
--
-- LECTURE SEULE. Ecrit le 2026-09-15, avant toute modification des cones, pour servir
-- de mesure AVANT au retravail du defaut 2.
--
--   mysql --force -t vaugouindb < doc/sql/test-014-location-type-errors.sql \
--       > doc/sql/test-014-location-type-errors-AAAAMMJJ.txt 2>&1
--
-- POURQUOI CE FICHIER. Les 920 lieux sans type du 2026-09-13 sont le seul chiffre que
-- l'on ait, et il ne compte QUE LES ABSENCES. Les erreurs, elles, n'ont jamais ete
-- comptees : le releve du 2026-09-14 sur les dix-huit lignes commencant par « Paris »
-- a trouve une ligne de metro et une ligne de chemin de fer rangees en 'region', un
-- quartier range en 'city', un pont range en 'region'. Toutes ont un type, donc aucune
-- n'entre dans les 7,6 %. Retravailler les cones sans ce compte, c'est se condamner a
-- ne pas savoir si le passage suivant a repare onze lignes ou casse deux cents
-- ailleurs, ce qui est exactement ce qui est arrive le 2026-09-12 quand 'region' volait
-- 274 rues et 162 chateaux.
--
-- ⚠ CE FICHIER NE COMPTE PAS « LES ERREURS », ET IL FAUT LE DIRE AVANT DE LE LIRE.
-- Une erreur de type suppose une verite de reference, et il n'en existe aucune en base :
-- si le cone savait deja quel est le bon type, il ne se tromperait pas. Le fichier fait
-- donc trois choses differentes, qu'il ne faut pas confondre dans la lecture :
--
--   1. IL MESURE CE QUI DECIDE (sections C, D, E). Combien de lieux portent des classes
--      de plusieurs cones, quel type a battu quel autre, et quelles classes portent le
--      verdict. Ce sont des faits exacts, pas des estimations : ils disent la surface
--      exacte qu'un changement d'ordre peut retourner.
--   2. IL ETABLIT UNE BORNE INFERIEURE D'ERREURS (sections F et G). Les cas ou deux
--      sources issues de Wikidata par des chemins differents, le cone P31 d'un cote et
--      la description de l'autre, se contredisent de facon incompatible. Chaque ligne
--      comptee est une erreur reelle ; les erreurs non comptees sont inconnues. Le
--      chiffre se lit « au moins N », jamais « N ».
--   3. IL GELE UN ECHANTILLON A JUGER A LA MAIN (section H). C'est la seule voie vers un
--      TAUX d'erreur. L'echantillon est deterministe, donc le meme avant et apres le
--      retravail, ce que la stabilite d'ID_LOCATION acquise le 2026-09-13 rend possible
--      et qui n'aurait eu aucun sens la semaine derniere.
--
-- ⚠ LE PIEGE DU CONTROLE QUI MESURE SON PROPRE FILTRE. Un chiffre bas ici peut vouloir
-- dire « peu d'erreurs » ou « la table et les cones ne decrivent pas le meme monde ».
-- La section B compare le type recalcule au type stocke : tant que cette section ne rend
-- pas zero, aucun autre chiffre du fichier ne veut rien dire.
--
-- ⚠ L'ORDRE DES TYPES EST RECOPIE ICI, dans les ORDER BY FIELD() des sections B et D2,
-- et il doit rester celui de LOCATION_TYPE_CONES dans tmdb_preprocess_helpers.py :
-- fiction, country, city, island, structure, nature, region. Si le retravail change cet
-- ordre, CHANGER AUSSI CE FICHIER avant de relancer la mesure APRES, sans quoi le
-- controle B1 rendra des desaccords qui ne sont que le decalage entre les deux listes.
--
-- ⚠ COLLATION. Lancer avec --force.
-- ⚠ AUCUNE REQUETE SUR information_schema ICI, et ce n'est pas un oubli : elle
-- basculerait la base courante de phpMyAdmin et ferait echouer tout ce qui suit.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- A. DE QUAND DATE CE QUE JE MESURE
--
--    Se lit en premier et se cite dans le compte rendu. Sans ces lignes, deux releves
--    identiques ne se distinguent pas l'un de l'autre, et l'on ne sait pas si c'est la
--    stabilite ou l'absence de passage qui les rend egaux. Faute payee le 2026-09-14 sur
--    la recette de stabilite d'ID_LOCATION, et le 2026-09-15 sur l'idempotence du
--    processus 216.
-- ---------------------------------------------------------------------------
SELECT 'A. Fraicheur de la matiere' AS SECTION;

SELECT COUNT(*)                                                   AS LIGNES,
       SUM(CASE WHEN COALESCE(DELETED, 0) = 0 THEN 1 ELSE 0 END)  AS VIVANTS,
       SUM(CASE WHEN DELETED = 1 THEN 1 ELSE 0 END)               AS SUPPRIMES,
       MIN(TIM_UPDATED)                                           AS PLUS_ANCIEN,
       MAX(TIM_UPDATED)                                           AS PLUS_RECENT
FROM T_WC_T2S_LOCATION;

-- La table des cones, telle qu'elle est en base a cette seconde. L'ordre attendu est
-- celui de LOCATION_TYPE_CONES : fiction, country, city, island, structure, nature,
-- region. Le compte par type change a chaque retravail, c'est la mesure AVANT.
SELECT lc.LOCATION_TYPE,
       COUNT(*)            AS CLASSES,
       MAX(lc.DAT_CREAT)   AS CONSTRUITE_LE
FROM T_WC_T2S_LOCATION_CLASS lc
GROUP BY lc.LOCATION_TYPE
ORDER BY FIELD(lc.LOCATION_TYPE,
               'fiction', 'country', 'city', 'island', 'structure', 'nature', 'region');

-- ---------------------------------------------------------------------------
-- B. LA MATIERE DE TRAVAIL, ET LE CONTROLE QUI VALIDE TOUT LE RESTE
--
--    TMP_LOC_CANDIDATE porte, pour chaque lieu vivant, TOUS les types que ses classes
--    P31 rendent atteignables. C'est la matiere des sections C a E, materialisee une
--    fois parce que la jointure a quatre tables est la partie couteuse du fichier et
--    qu'elle sert six fois.
--
--    TMP_LOC_VERDICT rejoue ensuite l'arbitrage du processus 72 : le meme ORDER BY
--    FIELD() que l'etape 2 de f_buildlocationtables, sur la meme liste de types. Le
--    gagnant recalcule DOIT etre le type stocke. S'il ne l'est pas, la table servie et
--    la table des cones ne decrivent pas le meme monde, et il faut relancer le scope
--    locations avant de lire quoi que ce soit d'autre ici.
-- ---------------------------------------------------------------------------
SELECT 'B. Construction de la matiere et controle de coherence' AS SECTION;

DROP TEMPORARY TABLE IF EXISTS TMP_LOC_CANDIDATE;
CREATE TEMPORARY TABLE TMP_LOC_CANDIDATE (
  ID_LOCATION   INT         NOT NULL,
  LOCATION_TYPE VARCHAR(20) NOT NULL,
  PRIMARY KEY (ID_LOCATION, LOCATION_TYPE),
  KEY IDX_TYPE (LOCATION_TYPE)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- INSERT IGNORE parce qu'un lieu porte souvent plusieurs classes du MEME cone (Paris en
-- porte quinze en tout) : la cle primaire du couple les replie en une ligne.
INSERT IGNORE INTO TMP_LOC_CANDIDATE (ID_LOCATION, LOCATION_TYPE)
SELECT loc.ID_LOCATION, lc.LOCATION_TYPE
FROM T_WC_T2S_LOCATION loc
INNER JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_WIKIDATA = loc.ID_WIKIDATA
       AND st.ID_PROPERTY = 'P31'
       AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
INNER JOIN T_WC_T2S_LOCATION_CLASS lc ON lc.ID_CLASS = iv.ID_ITEM
WHERE COALESCE(loc.DELETED, 0) = 0;

-- ⚠ UNE COPIE, ET ELLE N'EST PAS UN GASPILLAGE. La section D2 joint la table des
-- candidats a elle-meme, et MariaDB refuse qu'une table TEMPORARY soit citee deux fois
-- dans la meme requete : « Can't reopen table », erreur 1137. Le defaut ne se voit pas a
-- l'ecriture, seulement a l'execution, et il tuerait la section la plus utile du fichier.
DROP TEMPORARY TABLE IF EXISTS TMP_LOC_CANDIDATE_BIS;
CREATE TEMPORARY TABLE TMP_LOC_CANDIDATE_BIS (
  ID_LOCATION   INT         NOT NULL,
  LOCATION_TYPE VARCHAR(20) NOT NULL,
  PRIMARY KEY (ID_LOCATION, LOCATION_TYPE)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO TMP_LOC_CANDIDATE_BIS (ID_LOCATION, LOCATION_TYPE)
SELECT ID_LOCATION, LOCATION_TYPE FROM TMP_LOC_CANDIDATE;

DROP TEMPORARY TABLE IF EXISTS TMP_LOC_VERDICT;
CREATE TEMPORARY TABLE TMP_LOC_VERDICT (
  ID_LOCATION INT          NOT NULL,
  CANDIDATS   INT          NOT NULL,
  LISTE       VARCHAR(255) NOT NULL,
  GAGNANT     VARCHAR(20)  NULL,
  DAUPHIN     VARCHAR(20)  NULL,
  PRIMARY KEY (ID_LOCATION),
  KEY IDX_GAGNANT (GAGNANT),
  KEY IDX_CANDIDATS (CANDIDATS)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO TMP_LOC_VERDICT (ID_LOCATION, CANDIDATS, LISTE)
SELECT c.ID_LOCATION,
       COUNT(*),
       GROUP_CONCAT(c.LOCATION_TYPE
                    ORDER BY FIELD(c.LOCATION_TYPE,
                                   'fiction', 'country', 'city', 'island',
                                   'structure', 'nature', 'region')
                    SEPARATOR ',')
FROM TMP_LOC_CANDIDATE c
GROUP BY c.ID_LOCATION;

UPDATE TMP_LOC_VERDICT
SET GAGNANT = SUBSTRING_INDEX(LISTE, ',', 1),
    DAUPHIN = CASE WHEN CANDIDATS >= 2
                   THEN SUBSTRING_INDEX(SUBSTRING_INDEX(LISTE, ',', 2), ',', -1) END;

-- ⚠ LE CONTROLE QUI COMMANDE LE FICHIER. Attendu : DESACCORDS = 0. Les lieux sans
-- candidat (ceux qui ne sont dans aucun cone) doivent aussi avoir LOCATION_TYPE NULL.
SELECT 'B1. Type recalcule contre type stocke (attendu : 0 desaccord)' AS SECTION;

SELECT COUNT(*)                                                                  AS LIEUX_VIVANTS,
       SUM(CASE WHEN v.GAGNANT IS NULL AND loc.LOCATION_TYPE IS NULL
                THEN 1 ELSE 0 END)                                               AS ACCORD_SANS_TYPE,
       SUM(CASE WHEN v.GAGNANT IS NOT NULL AND v.GAGNANT = loc.LOCATION_TYPE
                THEN 1 ELSE 0 END)                                               AS ACCORD_AVEC_TYPE,
       SUM(CASE WHEN NOT (v.GAGNANT <=> loc.LOCATION_TYPE) THEN 1 ELSE 0 END)    AS DESACCORDS
FROM T_WC_T2S_LOCATION loc
LEFT JOIN TMP_LOC_VERDICT v ON v.ID_LOCATION = loc.ID_LOCATION
WHERE COALESCE(loc.DELETED, 0) = 0;

-- Si la ligne ci-dessus rend autre chose que zero, voici lesquels, pour trancher entre
-- « la table est perimee » et « la lecture de ce fichier est fausse ».
SELECT loc.ID_LOCATION, loc.ID_WIKIDATA, loc.LOCATION_NAME,
       loc.LOCATION_TYPE AS STOCKE, v.GAGNANT AS RECALCULE, v.LISTE
FROM T_WC_T2S_LOCATION loc
LEFT JOIN TMP_LOC_VERDICT v ON v.ID_LOCATION = loc.ID_LOCATION
WHERE COALESCE(loc.DELETED, 0) = 0
  AND NOT (v.GAGNANT <=> loc.LOCATION_TYPE)
LIMIT 20;

-- ---------------------------------------------------------------------------
-- C. LA MESURE AVANT, EN LIGNES ET EN EXPOSITION
--
--    ⚠ LES DEUX COLONNES NE DISENT PAS LA MEME CHOSE, et c'est le point de la section.
--    Les 7,6 % de sans-type comptent des LIGNES : ils mettent Pinewood Studios, cite par
--    4 558 oeuvres, au meme rang qu'un hameau cite une fois. L'exposition (films + series
--    attaches) dit ce qu'un type faux coute reellement, parce que c'est par la que
--    l'utilisateur le rencontre. Un retravail qui ameliore les lignes en degradant
--    l'exposition est une regression, et seule cette colonne le montrerait.
-- ---------------------------------------------------------------------------
SELECT 'C. Repartition des types, lignes et exposition' AS SECTION;

SELECT COALESCE(loc.LOCATION_TYPE, '(sans type)')                       AS LOCATION_TYPE,
       COUNT(*)                                                         AS LIEUX,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)                 AS PCT_LIEUX,
       SUM(COALESCE(loc.MOVIE_COUNT, 0) + COALESCE(loc.SERIE_COUNT, 0)) AS EXPOSITION,
       ROUND(100 * SUM(COALESCE(loc.MOVIE_COUNT, 0) + COALESCE(loc.SERIE_COUNT, 0))
             / SUM(SUM(COALESCE(loc.MOVIE_COUNT, 0) + COALESCE(loc.SERIE_COUNT, 0))) OVER (), 1)
                                                                        AS PCT_EXPOSITION
FROM T_WC_T2S_LOCATION loc
WHERE COALESCE(loc.DELETED, 0) = 0
GROUP BY COALESCE(loc.LOCATION_TYPE, '(sans type)')
ORDER BY EXPOSITION DESC;

-- ---------------------------------------------------------------------------
-- D. LA SURFACE QUE L'ORDRE DECIDE
--
--    Un lieu dont les classes ne touchent qu'un seul cone a un type que l'ordre ne peut
--    pas changer : le retravail ne l'atteindra qu'en deplacant des CLASSES. Un lieu qui
--    touche plusieurs cones, au contraire, bascule au moindre changement de rang. Ces
--    deux populations demandent deux gestes differents, et leur taille respective dit
--    lequel des deux gestes compte ici.
-- ---------------------------------------------------------------------------
SELECT 'D1. Combien de cones chaque lieu touche' AS SECTION;

SELECT v.CANDIDATS                                                      AS CONES_TOUCHES,
       COUNT(*)                                                         AS LIEUX,
       SUM(COALESCE(loc.MOVIE_COUNT, 0) + COALESCE(loc.SERIE_COUNT, 0)) AS EXPOSITION
FROM TMP_LOC_VERDICT v
INNER JOIN T_WC_T2S_LOCATION loc ON loc.ID_LOCATION = v.ID_LOCATION
GROUP BY v.CANDIDATS
ORDER BY v.CANDIDATS;

-- Qui a battu qui, et pour combien de lieux. Se lit ligne a ligne : « GAGNANT a pris
-- LIEUX lieux a PERDANT ». C'est la carte du rayon de souffle : deplacer PERDANT avant
-- GAGNANT retournerait exactement ces lieux-la, ni plus ni moins. La ligne
-- structure / region du 2026-09-12 valait 274 rues et 162 chateaux ; cette section
-- l'aurait donnee avant le passage plutot qu'apres.
SELECT 'D2. Qui a battu qui (carte du rayon de souffle)' AS SECTION;

SELECT a.LOCATION_TYPE                                                  AS GAGNANT,
       b.LOCATION_TYPE                                                  AS PERDANT,
       COUNT(DISTINCT a.ID_LOCATION)                                    AS LIEUX,
       SUM(COALESCE(loc.MOVIE_COUNT, 0) + COALESCE(loc.SERIE_COUNT, 0)) AS EXPOSITION,
       SUBSTRING_INDEX(GROUP_CONCAT(loc.LOCATION_NAME
                       ORDER BY COALESCE(loc.MOVIE_COUNT, 0) + COALESCE(loc.SERIE_COUNT, 0) DESC
                       SEPARATOR ' | '), ' | ', 5)                      AS LES_CINQ_PLUS_EXPOSES
FROM TMP_LOC_CANDIDATE a
INNER JOIN TMP_LOC_CANDIDATE_BIS b ON b.ID_LOCATION = a.ID_LOCATION
       AND FIELD(a.LOCATION_TYPE, 'fiction', 'country', 'city', 'island',
                                  'structure', 'nature', 'region')
         < FIELD(b.LOCATION_TYPE, 'fiction', 'country', 'city', 'island',
                                  'structure', 'nature', 'region')
INNER JOIN T_WC_T2S_LOCATION loc ON loc.ID_LOCATION = a.ID_LOCATION
GROUP BY a.LOCATION_TYPE, b.LOCATION_TYPE
ORDER BY LIEUX DESC;

-- ---------------------------------------------------------------------------
-- E. LES CLASSES QUI PORTENT LE VERDICT
--
--    Un retravail des cones se fait en deplacant ou en ajoutant des CLASSES, pas des
--    lieux. Cette section donne la liste, triee par ce que chaque classe decide, avec
--    son libelle quand la base l'a. Une classe muette (libelle absent) est signalee
--    comme telle plutot que devinee : c'est la regle posee le 2026-08-31, et Q15284,
--    seule classe de Madrid, est precisement dans ce cas.
-- ---------------------------------------------------------------------------
SELECT 'E. Les classes qui decident, par poids de decision' AS SECTION;

-- ⚠ LE DISTINCT DE LA SOUS-REQUETE N'EST PAS DECORATIF. Wikidata autorise deux
-- statements P31 portant la meme classe sur le meme item ; sans le repli, un tel lieu
-- serait compte une fois dans LIEUX_DECIDES (grace au DISTINCT) mais DEUX FOIS dans
-- EXPOSITION, et les deux colonnes de la meme ligne ne parleraient plus de la meme
-- population.
SELECT d.ID_CLASSE,
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.en')),
                NULLIF(wi.LABEL_EN, ''),
                '(classe muette)')                                      AS CLASSE,
       d.TYPE_IMPOSE,
       COUNT(*)                                                         AS LIEUX_DECIDES,
       SUM(COALESCE(loc.MOVIE_COUNT, 0) + COALESCE(loc.SERIE_COUNT, 0)) AS EXPOSITION,
       SUBSTRING_INDEX(GROUP_CONCAT(loc.LOCATION_NAME
                       ORDER BY COALESCE(loc.MOVIE_COUNT, 0) + COALESCE(loc.SERIE_COUNT, 0) DESC
                       SEPARATOR ' | '), ' | ', 4)                      AS EXEMPLES
FROM (
      SELECT DISTINCT loc.ID_LOCATION, iv.ID_ITEM AS ID_CLASSE, lc.LOCATION_TYPE AS TYPE_IMPOSE
      FROM T_WC_T2S_LOCATION loc
      INNER JOIN TMP_LOC_VERDICT v ON v.ID_LOCATION = loc.ID_LOCATION
      INNER JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_WIKIDATA = loc.ID_WIKIDATA
             AND st.ID_PROPERTY = 'P31'
             AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
      INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
      INNER JOIN T_WC_T2S_LOCATION_CLASS lc ON lc.ID_CLASS = iv.ID_ITEM
             AND lc.LOCATION_TYPE = v.GAGNANT
      WHERE COALESCE(loc.DELETED, 0) = 0
     ) d
INNER JOIN T_WC_T2S_LOCATION loc ON loc.ID_LOCATION = d.ID_LOCATION
LEFT JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = d.ID_CLASSE
GROUP BY d.ID_CLASSE, CLASSE, d.TYPE_IMPOSE
ORDER BY LIEUX_DECIDES DESC
LIMIT 40;

-- ---------------------------------------------------------------------------
-- F. LA BORNE INFERIEURE : le cone contre la description Wikidata
--
--    ⚠ CE N'EST PAS UNE VERITE DE REFERENCE, c'est un SECOND TEMOIN. OVERVIEW porte la
--    description anglaise de Wikidata, deposee a la main par un contributeur ; le type
--    vient du graphe P31/P279, construit par d'autres contributeurs. Les deux viennent
--    de Wikidata mais par des chemins independants, et c'est ce qui rend leur desaccord
--    informatif. La description est parfois fausse elle aussi : un desaccord designe une
--    ligne a regarder, il ne designe pas lequel des deux a tort.
--
--    ⚠ LE MOTIF NE VOIT QUE CE QU'IL NOMME, EN ANGLAIS. Les lieux sans description ne
--    sont pas jugeables et sont comptes a part : c'est la couverture de la mesure, et
--    sans elle la borne se lirait comme un taux.
--
--    L'ORDRE DU CASE REJOUE L'ORDRE DES CONES, et pour la meme raison : « fictional
--    town » contient « town », donc fiction doit etre teste avant city, sans quoi toute
--    la fiction serait comptee comme ville.
-- ---------------------------------------------------------------------------
SELECT 'F. Desaccords entre le type et la description Wikidata' AS SECTION;

DROP TEMPORARY TABLE IF EXISTS TMP_LOC_INDICE;
CREATE TEMPORARY TABLE TMP_LOC_INDICE (
  ID_LOCATION   INT         NOT NULL,
  STOCKE        VARCHAR(20) NULL,
  INDICE        VARCHAR(20) NULL,
  INCOMPATIBLE  TINYINT     NOT NULL DEFAULT 0,
  PRIMARY KEY (ID_LOCATION),
  KEY IDX_PAIRE (STOCKE, INDICE),
  KEY IDX_INCOMPATIBLE (INCOMPATIBLE)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO TMP_LOC_INDICE (ID_LOCATION, STOCKE, INDICE)
SELECT loc.ID_LOCATION,
       loc.LOCATION_TYPE,
       CASE
         WHEN loc.OVERVIEW IS NULL OR loc.OVERVIEW = '' THEN NULL
         WHEN loc.OVERVIEW REGEXP '\\b(fictional|fictitious|imaginary)\\b'      THEN 'fiction'
         WHEN loc.OVERVIEW REGEXP '\\b(sovereign state|country)\\b'             THEN 'country'
         WHEN loc.OVERVIEW REGEXP '\\b(island|archipelago|islet)\\b'            THEN 'island'
         WHEN loc.OVERVIEW REGEXP
              '\\b(station|airport|building|museum|castle|palace|theatre|theater|cinema|stadium|arena|prison|jail|bridge|street|avenue|boulevard|church|cathedral|temple|hotel|restaurant|school|university|hospital|tower|studio|factory|lighthouse|monument|railway line|metro line|tram line)\\b'
                                                                               THEN 'structure'
         WHEN loc.OVERVIEW REGEXP
              '\\b(river|mountain|mountains|lake|beach|forest|desert|valley|glacier|volcano|waterfall|canyon|bay|cave|peak|sea|ocean)\\b'
                                                                               THEN 'nature'
         WHEN loc.OVERVIEW REGEXP
              '\\b(city|town|village|municipality|commune|borough|hamlet|capital|settlement|metropolis)\\b'
                                                                               THEN 'city'
         WHEN loc.OVERVIEW REGEXP
              '\\b(region|province|county|district|department|prefecture|canton|state|territory|oblast)\\b'
                                                                               THEN 'region'
         ELSE NULL
       END
FROM T_WC_T2S_LOCATION loc
WHERE COALESCE(loc.DELETED, 0) = 0;

-- LA DEFINITION DE L'INCOMPATIBILITE, POSEE UNE SEULE FOIS. Les sections F3, F4 et I la
-- lisent toutes les trois ; l'ecrire trois fois serait garantir qu'une des trois derive
-- a la premiere correction, ce qui est la raison pour laquelle les deux requetes MOVIE
-- et SERIE du processus 72 sont generees plutot que recopiees.
--
-- SONT RETENUS LES SEULS COUPLES QUI NE PEUVENT PAS ETRE VRAIS TOUS LES DEUX : une
-- riviere n'est pas une region, une gare n'est pas une ville, un lieu de fiction n'est
-- rien d'autre. Les couples DISCUTABLES sont volontairement laisses dehors (city contre
-- region pour une commune, structure contre city pour un quartier) : ils restent
-- visibles dans la matrice F2, et les compter gonflerait une borne dont toute la valeur
-- est qu'on puisse la citer sans la defendre.
UPDATE TMP_LOC_INDICE
SET INCOMPATIBLE = 1
WHERE INDICE IS NOT NULL
  AND STOCKE IS NOT NULL
  AND (   (INDICE = 'fiction'   AND STOCKE <> 'fiction')
       OR (INDICE = 'nature'    AND STOCKE IN ('city', 'region', 'country', 'structure'))
       OR (INDICE = 'structure' AND STOCKE IN ('city', 'region', 'country', 'nature'))
       OR (INDICE = 'island'    AND STOCKE IN ('structure', 'fiction'))
       OR (INDICE = 'country'   AND STOCKE IN ('structure', 'nature', 'fiction'))
       OR (INDICE = 'city'      AND STOCKE IN ('structure', 'nature', 'fiction'))
       OR (INDICE = 'region'    AND STOCKE IN ('structure', 'nature', 'fiction')));

SELECT 'F1. Couverture de la mesure' AS SECTION;

SELECT COUNT(*)                                                       AS LIEUX_VIVANTS,
       SUM(CASE WHEN INDICE IS NOT NULL THEN 1 ELSE 0 END)            AS JUGEABLES,
       SUM(CASE WHEN INDICE IS NULL THEN 1 ELSE 0 END)                AS NON_JUGEABLES,
       ROUND(100 * SUM(CASE WHEN INDICE IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS PCT_COUVERT
FROM TMP_LOC_INDICE;

-- La matrice complete. La diagonale est l'accord, tout le reste est a regarder. Elle
-- est donnee entiere parce qu'une case inattendue en dit plus qu'un total.
SELECT 'F2. Matrice type stocke contre indice de la description' AS SECTION;

SELECT COALESCE(i.STOCKE, '(sans type)')                                AS STOCKE,
       i.INDICE,
       COUNT(*)                                                         AS LIEUX,
       SUM(COALESCE(loc.MOVIE_COUNT, 0) + COALESCE(loc.SERIE_COUNT, 0)) AS EXPOSITION
FROM TMP_LOC_INDICE i
INNER JOIN T_WC_T2S_LOCATION loc ON loc.ID_LOCATION = i.ID_LOCATION
WHERE i.INDICE IS NOT NULL
GROUP BY COALESCE(i.STOCKE, '(sans type)'), i.INDICE
ORDER BY LIEUX DESC;

-- LA BORNE. Le chiffre rendu se cite « au moins N erreurs », jamais « N erreurs ».
SELECT 'F3. Borne inferieure du nombre d erreurs' AS SECTION;

-- ⚠ PAS DE WITH ROLLUP POUR LE TOTAL, deux requetes a la place : MariaDB refuse
-- ORDER BY sur un GROUP BY ... WITH ROLLUP, et un tri par defaut sur un tableau de
-- familles ne se lit pas.
SELECT COUNT(*)                                                         AS ERREURS_CERTAINES,
       SUM(COALESCE(loc.MOVIE_COUNT, 0) + COALESCE(loc.SERIE_COUNT, 0)) AS EXPOSITION_TOUCHEE
FROM TMP_LOC_INDICE i
INNER JOIN T_WC_T2S_LOCATION loc ON loc.ID_LOCATION = i.ID_LOCATION
WHERE i.INCOMPATIBLE = 1;

SELECT i.STOCKE                                                         AS TYPE_FAUX,
       i.INDICE                                                         AS TYPE_PROBABLE,
       COUNT(*)                                                         AS ERREURS_CERTAINES,
       SUM(COALESCE(loc.MOVIE_COUNT, 0) + COALESCE(loc.SERIE_COUNT, 0)) AS EXPOSITION_TOUCHEE
FROM TMP_LOC_INDICE i
INNER JOIN T_WC_T2S_LOCATION loc ON loc.ID_LOCATION = i.ID_LOCATION
WHERE i.INCOMPATIBLE = 1
GROUP BY i.STOCKE, i.INDICE
ORDER BY ERREURS_CERTAINES DESC;

-- Les cent plus exposees de ces erreurs certaines, a lire pour nommer les familles.
SELECT 'F4. Les erreurs certaines les plus exposees' AS SECTION;

SELECT loc.ID_LOCATION, loc.ID_WIKIDATA, loc.LOCATION_NAME,
       i.STOCKE, i.INDICE,
       COALESCE(loc.MOVIE_COUNT, 0) + COALESCE(loc.SERIE_COUNT, 0)      AS EXPOSITION,
       LEFT(loc.OVERVIEW, 90)                                           AS DESCRIPTION
FROM TMP_LOC_INDICE i
INNER JOIN T_WC_T2S_LOCATION loc ON loc.ID_LOCATION = i.ID_LOCATION
WHERE i.INCOMPATIBLE = 1
ORDER BY EXPOSITION DESC, loc.ID_LOCATION
LIMIT 100;

-- ---------------------------------------------------------------------------
-- G. LES FAMILLES REPEREES A LA MAIN LE 2026-09-14
--
--    Le releve sur les dix-huit lignes commencant par « Paris » a donne quatre familles
--    nommees. Elles sont reprises ici en motif de NOM, parce que le nom voit ce que la
--    description manque : « Paris Metro Line 6 » n'a pas toujours de description, et
--    quand elle existe elle ne dit pas toujours « line ». Deux temoins independants
--    valent mieux qu'un, et ils ne se recouvrent pas.
--
--    ⚠ UN MOTIF DE NOM EST UN FILET GROSSIER : il rate tout ce qui est nomme autrement
--    (« Gare du Nord » ne contient ni station ni gare au sens du motif anglais) et
--    attrape des innocents. Les comptes de cette section servent a suivre une famille
--    d'un passage a l'autre, pas a etablir un total.
-- ---------------------------------------------------------------------------
SELECT 'G. Familles nommees, suivies par motif de nom' AS SECTION;

SELECT fam.FAMILLE,
       fam.TYPE_ATTENDU,
       COUNT(*)                                                                    AS LIEUX,
       SUM(CASE WHEN loc.LOCATION_TYPE IS NULL THEN 1 ELSE 0 END)                  AS SANS_TYPE,
       SUM(CASE WHEN loc.LOCATION_TYPE IS NOT NULL
                 AND loc.LOCATION_TYPE <> fam.TYPE_ATTENDU THEN 1 ELSE 0 END)      AS TYPE_AUTRE,
       SUM(CASE WHEN loc.LOCATION_TYPE = fam.TYPE_ATTENDU THEN 1 ELSE 0 END)       AS TYPE_ATTENDU_OK,
       -- ⚠ PAS DE DISTINCT DANS CE GROUP_CONCAT : MariaDB refuse DISTINCT accompagne
       -- d'un ORDER BY portant sur une autre expression. Inutile ici de toute facon, un
       -- lieu n'apparaissant qu'une fois par famille.
       SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(loc.LOCATION_NAME, ' [',
                       COALESCE(loc.LOCATION_TYPE, 'NULL'), ']')
                       ORDER BY COALESCE(loc.MOVIE_COUNT, 0) + COALESCE(loc.SERIE_COUNT, 0) DESC
                       SEPARATOR ' | '), ' | ', 4)                                 AS EXEMPLES
FROM (
      SELECT 'ligne ferroviaire ou de metro' AS FAMILLE, 'structure' AS TYPE_ATTENDU,
             '\\b(metro line|railway|railroad|tram line|subway line|line [0-9])\\b' AS MOTIF
  UNION ALL SELECT 'gare, station, aeroport', 'structure',
             '\\b(station|gare|bahnhof|airport|aeroport|terminal)\\b'
  UNION ALL SELECT 'pont, tunnel, ouvrage',   'structure',
             '\\b(bridge|pont|tunnel|viaduct|aqueduct)\\b'
  UNION ALL SELECT 'universite, ecole',       'structure',
             '\\b(university|universite|college|school|lycee|institute)\\b'
     ) fam
INNER JOIN T_WC_T2S_LOCATION loc
        ON COALESCE(loc.DELETED, 0) = 0
       AND (loc.LOCATION_NAME REGEXP fam.MOTIF OR loc.LOCATION_NAME_FR REGEXP fam.MOTIF)
GROUP BY fam.FAMILLE, fam.TYPE_ATTENDU
ORDER BY TYPE_AUTRE DESC;

-- ---------------------------------------------------------------------------
-- H. L'ECHANTILLON GELE, la seule voie vers un TAUX
--
--    Les sections F et G rendent des bornes ; aucune ne rend un taux, parce qu'aucune ne
--    juge les lieux qu'elle ne sait pas nommer. Un taux demande un jugement humain sur
--    un echantillon, une fois. Deux cents lignes se jugent en une heure et rendent un
--    taux a quelques points pres, ce qui suffit largement pour savoir si un retravail a
--    ameliore ou degrade.
--
--    ⚠ L'ECHANTILLON EST DETERMINISTE, ET C'EST TOUT L'INTERET. Pas de RAND(), pas de
--    LIMIT sur un tri instable : les memes ID_LOCATION ressortiront apres le retravail,
--    donc les memes lignes seront rejugees et le avant/apres portera sur la meme
--    population. Cela n'etait pas possible avant le 2026-09-13, ou ID_LOCATION changeait
--    chaque nuit : la stabilite acquise ce jour-la est ce qui rend cette section
--    utilisable.
--
--    DEUX STRATES, parce qu'un tirage uniforme sur 12 165 lieux ne verrait presque que
--    la longue traine et ne dirait rien de ce que l'utilisateur rencontre :
--      H1, les 100 plus exposes, qui portent l'essentiel des reponses servies ;
--      H2, 100 lieux repartis dans tout le reste, par un tri de hachage stable.
--    Les deux taux se lisent separement et ne se moyennent PAS : ils ne mesurent pas la
--    meme chose.
-- ---------------------------------------------------------------------------
SELECT 'H1. Echantillon des 100 plus exposes, a juger a la main' AS SECTION;

SELECT loc.ID_LOCATION, loc.ID_WIKIDATA,
       loc.LOCATION_NAME, loc.LOCATION_NAME_FR,
       COALESCE(loc.LOCATION_TYPE, '(sans type)')                       AS TYPE_ACTUEL,
       v.LISTE                                                          AS CONES_TOUCHES,
       COALESCE(loc.MOVIE_COUNT, 0) + COALESCE(loc.SERIE_COUNT, 0)      AS EXPOSITION,
       LEFT(loc.OVERVIEW, 80)                                           AS DESCRIPTION
FROM T_WC_T2S_LOCATION loc
LEFT JOIN TMP_LOC_VERDICT v ON v.ID_LOCATION = loc.ID_LOCATION
WHERE COALESCE(loc.DELETED, 0) = 0
ORDER BY COALESCE(loc.MOVIE_COUNT, 0) + COALESCE(loc.SERIE_COUNT, 0) DESC,
         loc.ID_LOCATION
LIMIT 100;

SELECT 'H2. Echantillon de 100 lieux tires dans toute la table, tirage stable' AS SECTION;

SELECT loc.ID_LOCATION, loc.ID_WIKIDATA,
       loc.LOCATION_NAME, loc.LOCATION_NAME_FR,
       COALESCE(loc.LOCATION_TYPE, '(sans type)')                       AS TYPE_ACTUEL,
       v.LISTE                                                          AS CONES_TOUCHES,
       COALESCE(loc.MOVIE_COUNT, 0) + COALESCE(loc.SERIE_COUNT, 0)      AS EXPOSITION,
       LEFT(loc.OVERVIEW, 80)                                           AS DESCRIPTION
FROM T_WC_T2S_LOCATION loc
LEFT JOIN TMP_LOC_VERDICT v ON v.ID_LOCATION = loc.ID_LOCATION
WHERE COALESCE(loc.DELETED, 0) = 0
ORDER BY MD5(CONCAT('t2s-014-type-', loc.ID_LOCATION))
LIMIT 100;

-- ---------------------------------------------------------------------------
-- I. LES CINQ CHIFFRES A RECOPIER DANS LE TICKET
--
--    Ce sont ceux que le passage d'apres devra rendre meilleurs, et le seul moyen de
--    savoir si le retravail a repare ou deplace le probleme. Les recopier tels quels
--    dans TMDB-MOVIE-PREPROCESS-014, avec la date du releve.
-- ---------------------------------------------------------------------------
SELECT 'I. Le bilan en cinq nombres' AS SECTION;

SELECT (SELECT COUNT(*) FROM T_WC_T2S_LOCATION WHERE COALESCE(DELETED, 0) = 0)
                                                                        AS LIEUX_VIVANTS,
       (SELECT COUNT(*) FROM T_WC_T2S_LOCATION
         WHERE COALESCE(DELETED, 0) = 0 AND LOCATION_TYPE IS NULL)      AS SANS_TYPE,
       (SELECT SUM(COALESCE(MOVIE_COUNT, 0) + COALESCE(SERIE_COUNT, 0))
          FROM T_WC_T2S_LOCATION
         WHERE COALESCE(DELETED, 0) = 0 AND LOCATION_TYPE IS NULL)      AS EXPOSITION_SANS_TYPE,
       (SELECT COUNT(*) FROM TMP_LOC_VERDICT WHERE CANDIDATS >= 2)      AS LIEUX_CONTESTES,
       (SELECT COUNT(*) FROM TMP_LOC_INDICE WHERE INCOMPATIBLE = 1)     AS ERREURS_CERTAINES;

DROP TEMPORARY TABLE IF EXISTS TMP_LOC_CANDIDATE;
DROP TEMPORARY TABLE IF EXISTS TMP_LOC_CANDIDATE_BIS;
DROP TEMPORARY TABLE IF EXISTS TMP_LOC_VERDICT;
DROP TEMPORARY TABLE IF EXISTS TMP_LOC_INDICE;
