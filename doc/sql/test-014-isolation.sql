-- ============================================================================
-- Quel correctif d'isolation est applicable, pour l'etape 1c du processus 72
-- ============================================================================
--
-- LECTURE SEULE, deux variables a lire. Aucun privilege particulier.
--
-- LE DEFAUT A CORRIGER. L'etape 1c du processus 72 est un
--   INSERT INTO T_WC_T2S_LOCATION_BUILD ... SELECT ... FROM T_WC_WIKIDATA_STATEMENT
-- et un INSERT ... SELECT ne lit PAS en simple lecture : il pose des verrous PARTAGES
-- sur les lignes lues dans la table source, afin que la replication rejoue le meme
-- resultat. Une lecture ordinaire n'aurait rien bloque grace au MVCC.
--
-- Consequence mesuree le 2026-09-12 : pendant que wikidata-crawler inserait dans
-- T_WC_WIKIDATA_STATEMENT depuis 4 425 secondes, l'etape 1c a attendu puis rendu
-- « 1205 Lock wait timeout exceeded ». La table _BUILD etait creee a 17:22:08 et vide,
-- ce qui localise l'echec sans ambiguite.
--
-- POURQUOI CETTE REQUETE AVANT LE CORRECTIF. La facon de supprimer ces verrous depend
-- du format de journal binaire, et je ne veux pas l'ecrire sur une hypothese.
--
--   binlog_format = ROW    : passer la session en READ COMMITTED suffit, la lecture
--                            redevient un snapshot et ne pose plus de verrou partage.
--                            Correctif d'une ligne.
--   binlog_format = STATEMENT ou MIXED : la lecture DOIT rester verrouillante, sans quoi
--                            le journal ne serait pas rejouable a l'identique. Le serveur
--                            refuse alors READ COMMITTED pour ces instructions, ou les
--                            reverrouille. Il faut alors separer la lecture de l'ecriture :
--                            un SELECT ordinaire cote Python, puis des INSERT par lots.
--                            A 11 922 lignes c'est peu couteux, et c'est la seule voie
--                            qui ne depend d'aucun reglage serveur.
--
-- LA REGLE D'ORDONNANCEMENT VAUT DANS LES DEUX CAS : ne pas reconstruire les lieux
-- pendant un chargement du crawler. Le correctif retire la collision, il ne rend pas la
-- concurrence souhaitable, les deux traitements lisant et ecrivant les memes tables.
--
-- ⚠ COLLATION. Lancer avec --force.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

SELECT '1. Les deux reglages qui decident' AS SECTION;

SELECT @@GLOBAL.binlog_format          AS BINLOG_FORMAT,
       @@GLOBAL.tx_isolation           AS ISOLATION_GLOBALE,
       @@GLOBAL.innodb_lock_wait_timeout AS ATTENTE_VERROU_SECONDES,
       @@GLOBAL.log_bin                AS JOURNAL_BINAIRE_ACTIF;

-- Si JOURNAL_BINAIRE_ACTIF vaut 0, il n'y a pas de replication a preserver et le
-- verrouillage de l'INSERT ... SELECT n'a plus de justification : READ COMMITTED passe
-- alors sans reserve, quel que soit binlog_format.
SELECT '2. Lecture du verdict' AS SECTION;

SELECT CASE
         WHEN @@GLOBAL.log_bin = 0
           THEN 'Journal binaire INACTIF : READ COMMITTED suffit, correctif d une ligne.'
         WHEN @@GLOBAL.binlog_format = 'ROW'
           THEN 'binlog ROW : READ COMMITTED suffit, correctif d une ligne.'
         ELSE CONCAT('binlog ', @@GLOBAL.binlog_format,
                     ' : separer lecture et ecriture, SELECT puis INSERT par lots.')
       END AS VERDICT;
