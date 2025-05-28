SELECT
    CAST(SPLIT_PART(e."Spiel_Minute", ':', 1) AS INTEGER) + 1 AS "Spielminute", 
    COUNT(e."H4A_Ereignis_ID") AS "Anzahl_Tore"
FROM "Ereignisse" e
JOIN "Spiele" sp ON e."Spiel_ID" = sp."Spiel_ID"
JOIN "Ligen" l ON sp."Liga_ID" = l."Liga_ID"
WHERE e."Referenz_Spieler_ID" = :player_id
  AND e."Typ" = 'Goal'
  AND l."Saison" = :season
  AND e."Spiel_Minute" IS NOT NULL 
  AND STRPOS(e."Spiel_Minute", ':') > 0 
  AND LENGTH(e."Spiel_Minute") >= 3 
GROUP BY "Spielminute"
HAVING CAST(SPLIT_PART(e."Spiel_Minute", ':', 1) AS INTEGER) +1 BETWEEN 1 AND 60
ORDER BY "Spielminute" ASC;