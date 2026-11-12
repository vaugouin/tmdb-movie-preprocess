-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-075 : LATITUDE et LONGITUDE sur T_WC_T2S_LOCATION
-- ============================================================================
--
-- DDL : a lancer par Philippe (voie 2), le compte agent n'a pas le droit d'ALTER.
-- Une seule fois. Le code 1.1.59 du processus 72 verifie la presence des deux colonnes
-- a chaque passage et saute l'etape 4d tant qu'elles manquent.
--
-- ⚠ ORDRE : deployer le code 1.1.59 AVANT ou EN MEME TEMPS que cette migration. Un code
-- plus ancien reconstruit la table chaque nuit par CREATE ... LIKE (la table de
-- construction aura donc les colonnes) mais ne les remplit pas : la bascule de la nuit
-- remettrait a NULL tout ce que backfill-075-location-coordinates.sql aura ecrit.
--
-- POURQUOI. Decision 3 de l'etude de la phase 3 des representations de resultats
-- (2026-11-12) : la carte des lieux epingle les lignes de T_WC_T2S_LOCATION, et les
-- coordonnees ne sont aujourd'hui que dans les tables Wikidata (P625), que le prompt de
-- l'API n'expose pas. Couverture mesuree le 2026-11-11 : 98 % des liens film-lieu.
--
-- TYPE. DECIMAL(9,6) : six decimales, une dizaine de centimetres, bien au-dela de la
-- precision Wikidata ; une valeur exacte, qui se compare et s'affiche sans les queues
-- binaires d'un DOUBLE. NULL veut dire « pas de coordonnee terrestre dans Wikidata »
-- (lieu de fiction, P625 absent, autre globe).
--
-- PLACE. Apres LOCATION_TYPE, qui dit au consommateur comment dessiner le point (un
-- pays ou une region se colorie, une ville s'epingle).
--
-- ⚠ COLLATION. Lancer avec --force si une comparaison rend ERROR 1267. Voir AGENTS.md.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

ALTER TABLE `T_WC_T2S_LOCATION`
  ADD COLUMN IF NOT EXISTS `LATITUDE`  decimal(9,6) DEFAULT NULL AFTER `LOCATION_TYPE`,
  ADD COLUMN IF NOT EXISTS `LONGITUDE` decimal(9,6) DEFAULT NULL AFTER `LATITUDE`;

-- Verification : les deux colonnes existent, a la bonne place.
SELECT '1. Colonnes' AS SECTION;

SELECT ORDINAL_POSITION, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'T_WC_T2S_LOCATION'
  AND COLUMN_NAME IN ('LOCATION_TYPE', 'LATITUDE', 'LONGITUDE', 'DELETED')
ORDER BY ORDINAL_POSITION;
