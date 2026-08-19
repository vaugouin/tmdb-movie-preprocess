-- ============================================================================
-- WIKIDATA-CRAWLER-017 : AWARD_NAME_FR a gagne 4 827 lignes, gain ou degat ?
-- ============================================================================
--
-- LE CONSTAT. Le passage de nuit du 2026-08-19 n'a fait baisser aucune des six
-- colonnes *_FR, et les a presque toutes fait monter. Mais AWARD_NAME_FR est
-- passe de 21 680 a 26 507, soit +4 827, et c'est precisement le seuil qui avait
-- ete pose comme signal d'alarme avant le run : quelques centaines = le gain
-- attendu de V2, plusieurs milliers = la restriction a ITEM a lache et des titres
-- de films s'ecrivent comme des noms de prix.
--
-- LE RISQUE, ET POURQUOI IL EST SILENCIEUX. 7 028 lignes de T_WC_T2S_AWARD ont
-- pour ID_WIKIDATA un film, 9 855 une personne (mesure du 2026-08-19, cone P279,
-- voir award-pollution-impact.sql). Avant -017 le code lisait une seule table et
-- ne trouvait rien pour ces identifiants : la colonne restait vide, et un vide se
-- remarque. Si la resolution va chercher dans MOVIE ou PERSON, elle trouve un
-- titre de film ou un nom d'acteur, l'ecrit comme nom de recompense, et personne
-- ne le voit : une valeur plausible et fausse ne declenche aucune alerte.
--
-- L'HYPOTHESE CONFORTABLE, ET POURQUOI ELLE NE SUFFIT PAS. Le repli $.fr -> $.en
-- ajoute dans V2 le 2026-08-18 remplit toute categorie de prix qui a un libelle
-- anglais et pas de francais. Cela expliquerait +4 827 sans une seule valeur
-- fausse. L'explication colle a tous les chiffres, et c'est justement son defaut :
-- elle n'interdit rien. Une explication qui s'accommode de n'importe quel resultat
-- ne vaut que si on lui fait dire quelque chose de refutable. V4 s'en charge :
-- si le gain vient du repli anglais, alors les lignes gagnees doivent porter un
-- AWARD_NAME_FR IDENTIQUE a l'AWARD_NAME anglais. Sinon l'histoire est fausse.
--
-- CE QUE CHAQUE BLOC TRANCHE.
--   V1 . d'ou viennent les entites qui portent desormais un nom francais ?
--   V2 . LE VERDICT : un titre de film s'affiche-t-il comme nom de prix ?
--   V3 . le meme test sur les personnes, population plus nombreuse que les films
--   V4 . la prediction de l hypothese du repli anglais, vraie ou fausse
--   V5 . les series, population oubliee des deux premieres versions du fichier
--   V6 . fuite ou residu ? le chiffre qui autorise ou non la suite de la migration
--
-- RESULTAT DE LA PREMIERE EXECUTION, 2026-08-20. Le verdict est mixte et il faut
-- le dire dans cet ordre, parce que l'ordre inverse donnerait une fausse alarme.
--
--   LE GAIN EST REEL ET IMPORTANT. Sur 26 507 noms francais, 11 876 (44,8 %) sont
--   de vraies traductions, et elles sont bonnes : « prix Kan-Kikuchi », « docteur
--   honoris causa de l'universite libre d'Amsterdam », « Festival de Cannes 2021 »,
--   « 63e ceremonie des Grammy Awards ». V1 n'en avait pratiquement aucune (85
--   libelles reellement francais sur 100 271). Les 14 631 autres (55,2 %) sont le
--   repli anglais : l'hypothese de V4 est donc a moitie vraie, elle explique la
--   moitie du contenu et pas la totalite.
--
--   LES PERSONNES SONT PROPRES. Zero ligne. Les 9 855 entrees de prix qui pointent
--   une personne ne portent aucun nom francais.
--
--   LA CONTAMINATION EXISTE MAIS ELLE EST PETITE. 155 lignes dont l'entite est un
--   film portent un titre de film comme nom de prix, dont 115 le titre francais :
--   « Les Buddenbrook », « Encanto : La fantastique famille Madrigal », « Happy
--   Feet ». C'est deux ordres de grandeur sous les 7 028 redoutes, mais ce n'est
--   pas zero, et V1 signale 315 series jamais testees, soit le double des films.
--
-- CE QUI DECIDE MAINTENANT, c'est V6 et rien d'autre. Fuite : ne migrer aucune
-- page de plus avant d'avoir revu la fonction. Residu : nettoyer les valeurs
-- anciennes et poursuivre. Les 470 lignes concernees appartenant toutes a la
-- population condamnee de TMDB-MOVIE-PREPROCESS-036, la suppression prevue la
-- reglerait a la racine plutot qu'en surface.
--
-- LECTURE SEULE. Executer avec --force -t.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;


-- ############################################################################
-- V1 . D OU VIENNENT LES ENTITES QUI PORTENT UN NOM FRANCAIS ?
-- ############################################################################
-- Une entite peut figurer dans plusieurs tables, donc les colonnes ne forment pas
-- une partition et leur somme peut depasser le total. Ce qui compte est le
-- rapport : entite_dans_item doit couvrir la quasi-totalite, et les trois autres
-- doivent rester marginales.

SELECT '=== V1 . provenance des entites ayant un AWARD_NAME_FR ===' AS section;

SELECT COUNT(*) AS award_avec_nom_fr,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM   i WHERE i.ID_WIKIDATA = a.ID_WIKIDATA)) AS entite_dans_item,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE  m WHERE m.ID_WIKIDATA = a.ID_WIKIDATA)) AS entite_est_un_film,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON p WHERE p.ID_WIKIDATA = a.ID_WIKIDATA)) AS entite_est_une_personne,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE  s WHERE s.ID_WIKIDATA = a.ID_WIKIDATA)) AS entite_est_une_serie,
       26507 AS repere_20260819
FROM   T_WC_T2S_AWARD a
WHERE  (a.DELETED IS NULL OR a.DELETED = 0)
  AND  a.AWARD_NAME_FR IS NOT NULL AND a.AWARD_NAME_FR <> '';


-- ############################################################################
-- V2 . LE VERDICT : UN TITRE DE FILM COMME NOM DE PRIX ?
-- ############################################################################
-- Le compte d'abord, l'echantillon ensuite. Si la colonne nom_fr_egale_le_titre
-- vaut 0, la restriction a ITEM a tenu meme la ou l'entite est un film, et le
-- reste de ce fichier n'est plus qu'une confirmation.

SELECT '=== V2a . combien de lignes award pointent un film ET ont un nom FR ? ===' AS section;

SELECT COUNT(*) AS lignes_award_sur_un_film_avec_nom_fr,
       SUM(a.AWARD_NAME_FR = m.LABEL_EN)                       AS nom_fr_egale_le_titre_en,
       SUM(a.AWARD_NAME_FR = JSON_UNQUOTE(JSON_EXTRACT(m.LABELS_JSON, '$.fr'))) AS nom_fr_egale_le_titre_fr,
       0 AS attendu
FROM   T_WC_T2S_AWARD a
JOIN   T_WC_WIKIDATA_MOVIE m ON m.ID_WIKIDATA = a.ID_WIKIDATA
WHERE  (a.DELETED IS NULL OR a.DELETED = 0)
  AND  a.AWARD_NAME_FR IS NOT NULL AND a.AWARD_NAME_FR <> '';

SELECT '=== V2b . les vingt premiers cas, a lire a l oeil ===' AS section;
-- Aucune ligne = bonne nouvelle. Des lignes ou AWARD_NAME_FR reprend le titre du
-- film = la bascule ecrit des titres d'oeuvres dans une colonne de recompense.

SELECT a.ID_AWARD,
       a.ID_WIKIDATA,
       COALESCE(NULLIF(a.AWARD_NAME,''), '(vide)') AS award_name_en,
       a.AWARD_NAME_FR,
       m.LABEL_EN                                  AS titre_du_film_en_v2,
       JSON_UNQUOTE(JSON_EXTRACT(m.LABELS_JSON, '$.fr')) AS titre_du_film_fr_en_v2
FROM   T_WC_T2S_AWARD a
JOIN   T_WC_WIKIDATA_MOVIE m ON m.ID_WIKIDATA = a.ID_WIKIDATA
WHERE  (a.DELETED IS NULL OR a.DELETED = 0)
  AND  a.AWARD_NAME_FR IS NOT NULL AND a.AWARD_NAME_FR <> ''
LIMIT  20;


-- ############################################################################
-- V3 . LE MEME TEST SUR LES PERSONNES
-- ############################################################################
-- 9 855 lignes award pointent une personne, contre 7 028 un film : c'est la plus
-- grosse population polluante, et donc le plus gros gisement de faux noms si la
-- resolution y accede. Ne pas s'arreter aux films.

SELECT '=== V3a . lignes award pointant une personne ET portant un nom FR ===' AS section;

SELECT COUNT(*) AS lignes_award_sur_une_personne_avec_nom_fr,
       SUM(a.AWARD_NAME_FR = p.LABEL_EN) AS nom_fr_egale_le_nom_de_la_personne,
       0 AS attendu
FROM   T_WC_T2S_AWARD a
JOIN   T_WC_WIKIDATA_PERSON p ON p.ID_WIKIDATA = a.ID_WIKIDATA
WHERE  (a.DELETED IS NULL OR a.DELETED = 0)
  AND  a.AWARD_NAME_FR IS NOT NULL AND a.AWARD_NAME_FR <> '';

SELECT '=== V3b . les vingt premiers cas ===' AS section;

SELECT a.ID_AWARD,
       a.ID_WIKIDATA,
       COALESCE(NULLIF(a.AWARD_NAME,''), '(vide)') AS award_name_en,
       a.AWARD_NAME_FR,
       p.LABEL_EN                                  AS nom_de_la_personne_en_v2
FROM   T_WC_T2S_AWARD a
JOIN   T_WC_WIKIDATA_PERSON p ON p.ID_WIKIDATA = a.ID_WIKIDATA
WHERE  (a.DELETED IS NULL OR a.DELETED = 0)
  AND  a.AWARD_NAME_FR IS NOT NULL AND a.AWARD_NAME_FR <> ''
LIMIT  20;


-- ############################################################################
-- V4 . L HYPOTHESE DU REPLI ANGLAIS, RENDUE REFUTABLE
-- ############################################################################
-- Si les 4 827 lignes gagnees viennent du repli $.fr -> $.en, elles portent un
-- nom francais IDENTIQUE au nom anglais, puisque c'est litteralement la meme
-- chaine recopiee. La proportion identique/different est donc la signature du
-- mecanisme, et elle est verifiable sans connaitre l'etat d'avant.
--
-- Lecture attendue si l'hypothese tient : une nette majorite d'identiques, et
-- des differents qui sont de vraies traductions (« Oscar du meilleur film »
-- contre « Academy Award for Best Picture »), pas des titres d'oeuvres.

SELECT '=== V4a . nom FR identique au nom EN, ou different ? ===' AS section;

SELECT COUNT(*)                                    AS award_avec_nom_fr,
       SUM(a.AWARD_NAME_FR = a.AWARD_NAME)         AS identique_a_l_anglais,
       SUM(a.AWARD_NAME_FR <> a.AWARD_NAME)        AS vraiment_traduit,
       SUM(a.AWARD_NAME IS NULL OR a.AWARD_NAME = '') AS sans_nom_anglais_a_comparer,
       ROUND(100 * SUM(a.AWARD_NAME_FR = a.AWARD_NAME) / COUNT(*), 1) AS pct_identique
FROM   T_WC_T2S_AWARD a
WHERE  (a.DELETED IS NULL OR a.DELETED = 0)
  AND  a.AWARD_NAME_FR IS NOT NULL AND a.AWARD_NAME_FR <> '';

SELECT '=== V4b . vingt noms reellement traduits, pour juger de leur qualite ===' AS section;
-- Ce sont les seuls vrais libelles francais de la table. S'ils sont bons, la
-- bascule apporte ce qu'on lui demandait. S'ils sont dans une troisieme langue ou
-- reprennent un titre d'oeuvre, il y a autre chose a comprendre.

SELECT a.ID_AWARD, a.ID_WIKIDATA, a.AWARD_NAME AS en, a.AWARD_NAME_FR AS fr
FROM   T_WC_T2S_AWARD a
WHERE  (a.DELETED IS NULL OR a.DELETED = 0)
  AND  a.AWARD_NAME_FR IS NOT NULL AND a.AWARD_NAME_FR <> ''
  AND  a.AWARD_NAME    IS NOT NULL AND a.AWARD_NAME    <> ''
  AND  a.AWARD_NAME_FR <> a.AWARD_NAME
ORDER  BY a.ID_AWARD
LIMIT  20;


-- ############################################################################
-- V5 . LA POPULATION QUE V2 ET V3 AVAIENT OUBLIEE : LES SERIES
-- ############################################################################
-- AJOUTE LE 2026-08-20, APRES LA PREMIERE EXECUTION. V1 a rendu 155 entites film
-- et 0 personne, mais aussi 315 SERIES, et aucun bloc ne les testait. La plus
-- grosse population contaminee etait donc hors du champ du fichier cense trancher.
-- Corrige ici plutot que constate ailleurs.

SELECT '=== V5a . lignes award pointant une serie ET portant un nom FR ===' AS section;

SELECT COUNT(*) AS lignes_award_sur_une_serie_avec_nom_fr,
       SUM(a.AWARD_NAME_FR = s.LABEL_EN) AS nom_fr_egale_le_titre_en,
       SUM(a.AWARD_NAME_FR = JSON_UNQUOTE(JSON_EXTRACT(s.LABELS_JSON, '$.fr'))) AS nom_fr_egale_le_titre_fr,
       315 AS repere_v1_20260820
FROM   T_WC_T2S_AWARD a
JOIN   T_WC_WIKIDATA_SERIE s ON s.ID_WIKIDATA = a.ID_WIKIDATA
WHERE  (a.DELETED IS NULL OR a.DELETED = 0)
  AND  a.AWARD_NAME_FR IS NOT NULL AND a.AWARD_NAME_FR <> '';

SELECT '=== V5b . les vingt premiers cas ===' AS section;

SELECT a.ID_AWARD, a.ID_WIKIDATA,
       COALESCE(NULLIF(a.AWARD_NAME,''), '(vide)') AS award_name_en,
       a.AWARD_NAME_FR,
       s.LABEL_EN AS titre_de_la_serie_en_v2
FROM   T_WC_T2S_AWARD a
JOIN   T_WC_WIKIDATA_SERIE s ON s.ID_WIKIDATA = a.ID_WIKIDATA
WHERE  (a.DELETED IS NULL OR a.DELETED = 0)
  AND  a.AWARD_NAME_FR IS NOT NULL AND a.AWARD_NAME_FR <> ''
LIMIT  20;


-- ############################################################################
-- V6 . LA QUESTION QUI COMMANDE LA SUITE : FUITE OU RESIDU ?
-- ############################################################################
-- 155 films et 315 series portent un titre d'oeuvre comme nom de prix. Deux
-- causes possibles, et elles n'appellent pas du tout la meme decision.
--
--   (a) FUITE. Ces entites sont AUSSI mises en cache dans T_WC_WIKIDATA_ITEM.
--       Restreindre la resolution a ITEM ne protege alors de rien, puisque le
--       titre du film s'y trouve. Les onze pages de tmdb-front restant a migrer
--       sont exposees, et il faut revoir la fonction avant d'y toucher.
--
--   (b) RESIDU. Ces entites ne sont pas dans ITEM, et les valeurs datent d'avant
--       la restriction : l'ecriture ne remplace jamais par du vide, donc une
--       mauvaise valeur ancienne survit. La restriction fonctionne, il ne reste
--       qu'un nettoyage, et la migration des onze pages peut suivre.
--
-- La colonne aussi_dans_item tranche : proche de zero = (b), proche du total =
-- (a). Ne pas migrer une page de plus avant d'avoir lu ce chiffre.

SELECT '=== V6 . les entites contaminees sont-elles aussi dans ITEM ? ===' AS section;

SELECT 'film' AS type_entite,
       COUNT(*) AS lignes_contaminees,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM i WHERE i.ID_WIKIDATA = a.ID_WIKIDATA)) AS aussi_dans_item
FROM   T_WC_T2S_AWARD a
JOIN   T_WC_WIKIDATA_MOVIE m ON m.ID_WIKIDATA = a.ID_WIKIDATA
WHERE  (a.DELETED IS NULL OR a.DELETED = 0)
  AND  a.AWARD_NAME_FR IS NOT NULL AND a.AWARD_NAME_FR <> ''
UNION ALL
SELECT 'serie',
       COUNT(*),
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM i WHERE i.ID_WIKIDATA = a.ID_WIKIDATA))
FROM   T_WC_T2S_AWARD a
JOIN   T_WC_WIKIDATA_SERIE s ON s.ID_WIKIDATA = a.ID_WIKIDATA
WHERE  (a.DELETED IS NULL OR a.DELETED = 0)
  AND  a.AWARD_NAME_FR IS NOT NULL AND a.AWARD_NAME_FR <> '';
