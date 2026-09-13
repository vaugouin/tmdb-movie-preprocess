-- migration-049-color-source.sql
-- TMDB-MOVIE-PREPROCESS-049 : la couleur depuis Wikidata (P462) quand la ligne Format
-- francaise manque, la ligne reprenant la main des qu'elle existe. Option A (2026-09-13).
--
-- POURQUOI. Les flags IS_COLOR / IS_BLACK_AND_WHITE ne venaient que de la ligne Format
-- (49 954 films le 2026-09-12) alors que Wikidata porte P462 sur 140 096 films de la base.
-- Le processus 64 remplit les flags depuis Wikidata pour les films sans ligne. Pour que la
-- ligne garde la main, chaque ecriture est signee : COLOR_SOURCE = 'format_line' (processus 1)
-- ou 'wikidata' (processus 64), et TIM_COLOR_SOURCE date la derniere signature.
--
-- ORDRE DES OPERATIONS.
-- 1. Les deux colonnes, sur la table source et sur le read-model.
-- 2. Signer l'existant : tout film qui a une ligne Format non vide et un flag pose est
--    signe 'format_line'. Le processus 64 ne toucherait pas ces films de toute facon (il
--    exclut toute ligne non vide), mais sans signature le rapport par provenance mentirait
--    sur 50 000 films dont les flags viennent bien de la ligne.
-- 3. Optionnel : refleter la signature dans le read-model sans attendre le processus 4
--    (TIM_UPDATED n'est pas avance ici, donc la copie normale ne la porterait qu'a la
--    prochaine modification du film).
--
-- A appliquer UNE FOIS, a la main, AVANT le premier passage du processus 64
-- (./tmdb-movie-preprocess-wikidata-colour.sh). Idempotence : les ALTER echouent
-- bruyamment si les colonnes existent deja, ce qui est le signal attendu ; les UPDATE
-- sont sans effet au second passage.
--
-- ⚠ COLLATION. Lancer avec --force si le client refuse le SET NAMES. Voir AGENTS.md.

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 1. colonnes
ALTER TABLE `T_WC_TMDB_MOVIE`
  ADD COLUMN `COLOR_SOURCE` varchar(20) DEFAULT NULL AFTER `IS_BLACK_AND_WHITE`,
  ADD COLUMN `TIM_COLOR_SOURCE` datetime DEFAULT NULL AFTER `COLOR_SOURCE`,
  ADD KEY `COLOR_SOURCE` (`COLOR_SOURCE`),
  ADD KEY `TIM_COLOR_SOURCE` (`TIM_COLOR_SOURCE`);

ALTER TABLE `T_WC_T2S_MOVIE`
  ADD COLUMN `COLOR_SOURCE` varchar(20) DEFAULT NULL AFTER `IS_BLACK_AND_WHITE`,
  ADD COLUMN `TIM_COLOR_SOURCE` datetime DEFAULT NULL AFTER `COLOR_SOURCE`,
  ADD KEY `COLOR_SOURCE` (`COLOR_SOURCE`);

-- 2. signer l'existant
UPDATE `T_WC_TMDB_MOVIE`
SET COLOR_SOURCE = 'format_line', TIM_COLOR_SOURCE = NOW()
WHERE WIKIPEDIA_FORMAT_LINE IS NOT NULL AND WIKIPEDIA_FORMAT_LINE <> ''
  AND (IS_COLOR IS NOT NULL OR IS_BLACK_AND_WHITE IS NOT NULL)
  AND COLOR_SOURCE IS NULL;

-- 3. optionnel : le read-model suit tout de suite
UPDATE `T_WC_T2S_MOVIE` t
JOIN `T_WC_TMDB_MOVIE` m ON m.ID_MOVIE = t.ID_MOVIE
SET t.COLOR_SOURCE = m.COLOR_SOURCE, t.TIM_COLOR_SOURCE = m.TIM_COLOR_SOURCE
WHERE m.COLOR_SOURCE IS NOT NULL;

-- verification : 'format_line' de l'ordre de 50 000, 'none' le reste, pas encore de 'wikidata'
SELECT COALESCE(COLOR_SOURCE, 'none') AS COLOR_SOURCE, COUNT(*) AS N
FROM `T_WC_TMDB_MOVIE`
GROUP BY COALESCE(COLOR_SOURCE, 'none');
