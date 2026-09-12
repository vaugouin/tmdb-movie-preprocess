-- ============================================================================
-- Qui tient le verrou, quand le processus 72 meurt en 1205
-- ============================================================================
--
-- LECTURE SEULE. A lancer PENDANT ou JUSTE APRES l'echec, pas plus tard : les
-- transactions fautives se ferment, et le coupable disparait avec elles.
--
-- ⚠ L'ORDRE DES SECTIONS N'EST PAS COSMETIQUE, ET J'AI PAYE DEUX FOIS POUR L'APPRENDRE.
-- Les requetes sur information_schema sont EN DERNIER parce que phpMyAdmin retient la
-- derniere base referencee comme base courante : une requete sur information_schema y
-- bascule le contexte, et toute instruction suivante dont les noms de tables ne sont pas
-- qualifies va les chercher la, avec « #1109 Table inconnue dans information_schema ».
--
-- Le meme defaut a casse migration-t2s-location.sql le 2026-09-11, je l'ai documente
-- dans les commentaires de CE fichier-la, puis je l'ai refait ici le 2026-09-12. La
-- lecon n'est pas « faire attention » mais « la consigne etait au mauvais endroit » :
-- un avertissement dans un fichier ne protege que son lecteur. Il est desormais dans
-- AGENTS.md, section SQL, ou l'auteur du prochain fichier le verra.
--
-- Le defaut ne se voit PAS avec le client mariadb en ligne de commande, qui garde la
-- base de la connexion : une recette qui marche d'un cote et casse de l'autre.
--
-- ⚠ LES DEUX DERNIERES SECTIONS DEMANDENT LE PRIVILEGE PROCESS. Le compte applicatif ne
-- l'a pas, et je ne recommande pas de le lui donner : PROCESS laisse voir le texte de
-- toutes les requetes de tous les utilisateurs, donnees comprises. Lancer ce fichier
-- depuis phpMyAdmin en root, ou se contenter de la section 1 ailleurs.
--
-- ⚠ COLLATION. Lancer avec --force.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- 1. L'ETAT DES TABLES, en premier parce que cette section n'a besoin ni de
--    information_schema ni du privilege PROCESS.
--
--    RENAME TABLE est atomique pour l'instruction entiere, donc un _OLD present signifie
--    que la bascule a eu lieu et que seul le menage final a manque. Un _BUILD present
--    sans _OLD signifie que la mort est ANTERIEURE a la bascule, et le prochain passage
--    le detruira de lui-meme : rien a nettoyer a la main.
-- ---------------------------------------------------------------------------
SELECT '1. Lignes servies' AS SECTION;

SELECT 'T_WC_T2S_LOCATION' AS TABLE_NAME, COUNT(*) AS LIGNES FROM T_WC_T2S_LOCATION
UNION ALL SELECT 'T_WC_T2S_MOVIE_LOCATION', COUNT(*) FROM T_WC_T2S_MOVIE_LOCATION
UNION ALL SELECT 'T_WC_T2S_SERIE_LOCATION', COUNT(*) FROM T_WC_T2S_SERIE_LOCATION;

SELECT '1b. Tables presentes, y compris les _BUILD et _OLD residuels' AS SECTION;

SHOW TABLE STATUS LIKE '%\_LOCATION';
SHOW TABLE STATUS LIKE '%\_LOCATION\_%';

-- ---------------------------------------------------------------------------
-- 2. Les transactions ouvertes, la plus ancienne d'abord. PRIVILEGE PROCESS.
--
--    Deux profils a chercher.
--
--    L'ONGLET OUBLIE : etat RUNNING depuis des minutes avec trx_query a NULL. Elle ne
--    fait rien, et c'est precisement pour cela que personne ne la voit. L'etape 5 du
--    processus 72 demande un verrou de metadonnees EXCLUSIF sur les trois tables, pour
--    les renommer : une seule session ayant touche l'une d'elles sans valider suffit.
--
--    LE CRAWLER EN CHARGEMENT, qui est l'hypothese principale pour l'echec du
--    2026-09-12 : SHOW FULL PROCESSLIST montrait alors un INSERT INTO
--    T_WC_WIKIDATA_STATEMENT en cours depuis 1 806 secondes. L'etape 1c du processus 72
--    est un INSERT ... SELECT sur cette meme table, et sous l'isolation par defaut un
--    INSERT ... SELECT ne lit PAS en simple lecture : il pose des verrous partages sur
--    les lignes source. Une lecture ordinaire n'aurait rien bloque grace au MVCC.
--    Les colonnes trx_tables_locked et trx_rows_locked de la transaction du crawler
--    confirment ou infirment cette hypothese.
-- ---------------------------------------------------------------------------
SELECT '2. Transactions ouvertes' AS SECTION;

SELECT trx_id, trx_state, trx_started,
       TIMESTAMPDIFF(SECOND, trx_started, NOW()) AS AGE_SECONDES,
       trx_mysql_thread_id                       AS ID_SESSION,
       trx_tables_locked, trx_rows_locked, trx_isolation_level,
       LEFT(COALESCE(trx_query, '(aucune requete active)'), 80) AS REQUETE
FROM information_schema.INNODB_TRX
ORDER BY trx_started ASC;

-- ---------------------------------------------------------------------------
-- 3. Les sessions, les plus longues d'abord. PRIVILEGE PROCESS.
--
--    Un Time enorme en « Sending data » ou « Executing » est une requete zombie a tuer.
--    Precedent : le 2026-09-12, la requete 16430 tournait depuis 879 621 secondes, soit
--    10,2 jours. C'etait wikidata-id-movie-fix.sql dans sa version d'avant correction,
--    dont le conteneur avait ete tue le 2026-09-02 sans que la requete s'arrete cote
--    serveur. Tuer le conteneur n'arrete pas la requete, seul KILL le fait.
-- ---------------------------------------------------------------------------
SELECT '3. Sessions les plus longues' AS SECTION;

SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE,
       LEFT(COALESCE(INFO, ''), 70) AS REQUETE
FROM information_schema.PROCESSLIST
WHERE COMMAND <> 'Daemon'
ORDER BY TIME DESC
LIMIT 20;
