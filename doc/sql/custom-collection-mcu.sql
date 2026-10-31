-- Marvel Cinematic Universe (MCU): replace the id list of custom list 49 (TARGET_TABLE = 2, mechanism 1 of process 41)
--
-- Why. On 2026-10-31 Green answered "The Marvel Cinematic Universe films in release order" with Howard the Duck
-- (1986) first. text2sql-eval/check-mcu-collection-20261031.txt shows the cause: ID_IMDB_LIST of custom list 49
-- held 109 IMDb urls, every one pasted twice, among them 32 films that are Marvel Comics adaptations but not MCU
-- (Fox X-Men and Fantastic Four, Sony Spider-Man, Blade, Elektra, Ghost Rider, the 1986 to 1994 films), so
-- collection 4817 served 73 films from Howard the Duck to Logan. Philippe supplied the MCU proper on 2026-10-31,
-- 100 ids: 34 feature films, 11 shorts and specials (One-Shots, Team Thor, Werewolf by Night, two making-of
-- documentaries), 37 series (ABC, Netflix, Hulu, Disney+, Legends, Assembled), 3 ids TMDb does not know yet.
-- Five undated future films are left out until they carry a release date (Nova tt19037048, the Shang-Chi sequel
-- tt16357306, Black Panther 3 tt37884905, Ghost Rider tt43711959, the untitled X-Men film tt29347085): a NULL
-- date sorts first in an "oldest first" query and would put them ahead of Iron Man. SORT_BY stays 5 (release
-- date ascending). Eval 2600 of text2sql-eval asserts Iron Man (1726) first; it passes after the next process 41
-- run, once the cache entry of that question has been replaced.
--
-- Writes T_WC_CUSTOM_LIST only (one row): voie 1.
--   ssh vps-agent pull tmdb-movie-preprocess
--   ssh vps-agent run tmdb-movie-preprocess/doc/sql/custom-collection-mcu.sql
-- Part 4 reads what the nightly process 41 built: re-run with -f the day after.

-- Part 1. Before: the row and the number of ids it holds (each url holds 'tt' twice, 'https' included)
SELECT ID_CUSTOM_LIST, LIST_NAME, TARGET_TABLE, SORT_BY, WIKIDATA_PROPERTIES, TMDB_TARGET_RECORD,
       (LENGTH(ID_IMDB_LIST) - LENGTH(REPLACE(ID_IMDB_LIST, 'title/tt', ''))) / 8 AS IMDB_ID_COUNT, TIM_UPDATED
FROM T_WC_CUSTOM_LIST WHERE ID_CUSTOM_LIST = 49;

-- Part 2. The new list, 95 urls, one per line, in Philippe's order (films, then series, then shorts and specials)
UPDATE T_WC_CUSTOM_LIST
SET ID_IMDB_LIST = 'https://www.imdb.com/title/tt0371746/
https://www.imdb.com/title/tt0800080/
https://www.imdb.com/title/tt1228705/
https://www.imdb.com/title/tt0800369/
https://www.imdb.com/title/tt0458339/
https://www.imdb.com/title/tt0848228/
https://www.imdb.com/title/tt1300854/
https://www.imdb.com/title/tt1981115/
https://www.imdb.com/title/tt1843866/
https://www.imdb.com/title/tt2015381/
https://www.imdb.com/title/tt2395427/
https://www.imdb.com/title/tt0478970/
https://www.imdb.com/title/tt3498820/
https://www.imdb.com/title/tt1211837/
https://www.imdb.com/title/tt3896198/
https://www.imdb.com/title/tt2250912/
https://www.imdb.com/title/tt3501632/
https://www.imdb.com/title/tt1825683/
https://www.imdb.com/title/tt4154756/
https://www.imdb.com/title/tt5095030/
https://www.imdb.com/title/tt4154664/
https://www.imdb.com/title/tt4154796/
https://www.imdb.com/title/tt6320628/
https://www.imdb.com/title/tt3480822/
https://www.imdb.com/title/tt9376612/
https://www.imdb.com/title/tt9032400/
https://www.imdb.com/title/tt10872600/
https://www.imdb.com/title/tt9419884/
https://www.imdb.com/title/tt10648342/
https://www.imdb.com/title/tt9114286/
https://www.imdb.com/title/tt10676048/
https://www.imdb.com/title/tt10954600/
https://www.imdb.com/title/tt6791350/
https://www.imdb.com/title/tt10676052/
https://www.imdb.com/title/tt2364582/
https://www.imdb.com/title/tt3475734/
https://www.imdb.com/title/tt4154858/
https://www.imdb.com/title/tt3322312/
https://www.imdb.com/title/tt2357547/
https://www.imdb.com/title/tt3322314/
https://www.imdb.com/title/tt3322310/
https://www.imdb.com/title/tt4230076/
https://www.imdb.com/title/tt5675620/
https://www.imdb.com/title/tt1236246/
https://www.imdb.com/title/tt5614844/
https://www.imdb.com/title/tt10266874/
https://www.imdb.com/title/tt9140560/
https://www.imdb.com/title/tt9208876/
https://www.imdb.com/title/tt9140554/
https://www.imdb.com/title/tt10168312/
https://www.imdb.com/title/tt10857164/
https://www.imdb.com/title/tt10160804/
https://www.imdb.com/title/tt10234724/
https://www.imdb.com/title/tt10857160/
https://www.imdb.com/title/tt13623136/
https://www.imdb.com/title/tt13157618/
https://www.imdb.com/title/tt13623126/
https://www.imdb.com/title/tt13623128/
https://www.imdb.com/title/tt13968252/
https://www.imdb.com/title/tt13966962/
https://www.imdb.com/title/tt5296048/
https://www.imdb.com/title/tt6313900/
https://www.imdb.com/title/tt2011118/
https://www.imdb.com/title/tt2011109/
https://www.imdb.com/title/tt2247732/
https://www.imdb.com/title/tt3067038/
https://www.imdb.com/title/tt3438640/
https://www.imdb.com/title/tt6016776/
https://www.imdb.com/title/tt6599818/
https://www.imdb.com/title/tt8023090/
https://www.imdb.com/title/tt13623148/
https://www.imdb.com/title/tt3591568/
https://www.imdb.com/title/tt4128102/
https://www.imdb.com/title/tt11295300/
https://www.imdb.com/title/tt13650480/
https://www.imdb.com/title/tt14094206/
https://www.imdb.com/title/tt10671440/
https://www.imdb.com/title/tt14513804/
https://www.imdb.com/title/tt20969586/
https://www.imdb.com/title/tt21357150/
https://www.imdb.com/title/tt21361444/
https://www.imdb.com/title/tt6263850/
https://www.imdb.com/title/tt15318872/
https://www.imdb.com/title/tt15571732/
https://www.imdb.com/title/tt18923754/
https://www.imdb.com/title/tt16027074/
https://www.imdb.com/title/tt21066182/
https://www.imdb.com/title/tt16027014/
https://www.imdb.com/title/tt22084616/
https://www.imdb.com/title/tt23112594/
https://www.imdb.com/title/tt15450042/
https://www.imdb.com/title/tt36042156/
https://www.imdb.com/title/tt4501242/
https://www.imdb.com/title/tt6243582/
https://www.imdb.com/title/tt48452696/',
    TIM_UPDATED = NOW(), ID_USER_UPDATED = 1
WHERE ID_CUSTOM_LIST = 49 AND TARGET_TABLE = 2 AND LIST_NAME = 'Marvel Cinematic Universe (MCU)';

-- Part 3. After: 95 ids expected
SELECT ID_CUSTOM_LIST, LIST_NAME, SORT_BY,
       (LENGTH(ID_IMDB_LIST) - LENGTH(REPLACE(ID_IMDB_LIST, 'title/tt', ''))) / 8 AS IMDB_ID_COUNT, TIM_UPDATED
FROM T_WC_CUSTOM_LIST WHERE ID_CUSTOM_LIST = 49;

-- Part 4. After the next process 41 run: the collection's counts and its first twelve films
SELECT ID_T2S_COLLECTION, COLLECTION_NAME, MOVIE_COUNT, SERIE_COUNT
FROM T_WC_T2S_COLLECTION WHERE COLLECTION_SOURCE = 'custom' AND ID_RECORD = 49;

SELECT mc.DISPLAY_ORDER, m.ID_MOVIE, m.TITLE, m.DAT_RELEASE, m.IS_SHORT_FILM
FROM T_WC_T2S_COLLECTION c
JOIN T_WC_T2S_MOVIE_COLLECTION mc ON mc.ID_T2S_COLLECTION = c.ID_T2S_COLLECTION
JOIN T_WC_TMDB_MOVIE m ON m.ID_MOVIE = mc.ID_MOVIE
WHERE c.COLLECTION_SOURCE = 'custom' AND c.ID_RECORD = 49 AND mc.DISPLAY_ORDER <= 12
ORDER BY mc.DISPLAY_ORDER;
