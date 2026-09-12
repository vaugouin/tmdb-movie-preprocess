-- ============================================================================
-- Qui tient le verrou, quand le processus 72 meurt en 1205
-- ============================================================================
--
-- LECTURE SEULE. A lancer PENDANT ou JUSTE APRES l'echec, pas plus tard : les
-- transactions fautives se ferment, et le coupable disparait avec elles.
--
-- L'ECHEC DU 2026-09-12. « MySQL Error: (1205, Lock wait timeout exceeded) » apres la
-- construction des cones. L'etape n'etait pas nommee dans le journal, ce qui est corrige
-- depuis, mais la cause reste a etablir : une autre session tenait un verrou sur l'une
-- des trois tables des lieux.
--
-- LE SUSPECT LE PLUS PROBABLE est une TRANSACTION OUVERTE ET INACTIVE, typiquement un
-- onglet phpMyAdmin qui a lu T_WC_T2S_LOCATION sans valider. L'etape 5 du processus
-- demande un verrou de metadonnees EXCLUSIF sur les trois tables, pour les renommer :
-- il suffit qu'une session ait touche l'une d'elles dans une transaction non fermee.
--
-- L'AUTRE SUSPECT, et il a un precedent dans ce projet : une requete zombie. Le
-- 2026-09-12, SHOW FULL PROCESSLIST montrait encore la requete 16430,
-- « Retrieve all movies with empty ID_WIKIDATA », en execution depuis 879 621 secondes,
-- soit 10,2 jours. C'etait wikidata-id-movie-fix.sql dans sa version d'avant correction,
-- dont le conteneur avait ete tue le 2026-09-02 sans que la requete s'arrete cote
-- serveur. Une requete tuee cote client continue cote serveur : c'est le KILL qui
-- l'arrete, pas la fermeture du conteneur.
--
-- ⚠ COLLATION. Lancer avec --force.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- 1. Les transactions ouvertes, la plus ancienne d'abord.
--
--    Une transaction en ETAT « RUNNING » depuis des minutes sans requete active est le
--    profil de l'onglet oublie. Sa colonne trx_query est alors NULL : elle ne fait
--    rien, et c'est precisement pour cela qu'elle est dangereuse, personne ne la voit.
-- ---------------------------------------------------------------------------
SELECT '1. Transactions ouvertes' AS SECTION;

SELECT trx_id, trx_state, trx_started,
       TIMESTAMPDIFF(SECOND, trx_started, NOW()) AS AGE_SECONDES,
       trx_mysql_thread_id                       AS ID_SESSION,
       trx_tables_locked, trx_rows_locked, trx_isolation_level,
       LEFT(COALESCE(trx_query, '(aucune requete active)'), 80) AS REQUETE
FROM information_schema.INNODB_TRX
ORDER BY trx_started ASC;

-- ---------------------------------------------------------------------------
-- 2. Les sessions, les plus longues d'abord.
--
--    Chercher deux profils : un Time enorme en « Sending data » ou « Executing », qui
--    est une requete zombie a tuer, et un « Sleep » de plusieurs minutes appartenant a
--    une transaction de la section 1, qui est l'onglet oublie.
-- ---------------------------------------------------------------------------
SELECT '2. Sessions les plus longues' AS SECTION;

SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE,
       LEFT(COALESCE(INFO, ''), 70) AS REQUETE
FROM information_schema.PROCESSLIST
WHERE COMMAND <> 'Daemon'
ORDER BY TIME DESC
LIMIT 20;

-- ---------------------------------------------------------------------------
-- 3. L'etat des tables des lieux : un _OLD ou un _BUILD qui traine dit ou le
--    passage est mort.
--
--    RENAME TABLE est atomique pour l'instruction entiere, donc un _OLD present
--    signifie que la bascule a eu lieu et que seul le menage final a manque. Un _BUILD
--    present sans _OLD signifie que la mort est ANTERIEURE a la bascule, et le
--    prochain passage le detruira de lui-meme : rien a nettoyer a la main.
-- ---------------------------------------------------------------------------
SELECT '3. Etat des tables' AS SECTION;

SHOW TABLE STATUS LIKE '%\_LOCATION';
SHOW TABLE STATUS LIKE '%\_LOCATION\_%';

SELECT 'T_WC_T2S_LOCATION' AS TABLE_NAME, COUNT(*) AS LIGNES FROM T_WC_T2S_LOCATION
UNION ALL SELECT 'T_WC_T2S_MOVIE_LOCATION', COUNT(*) FROM T_WC_T2S_MOVIE_LOCATION
UNION ALL SELECT 'T_WC_T2S_SERIE_LOCATION', COUNT(*) FROM T_WC_T2S_SERIE_LOCATION;
