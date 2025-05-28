SELECT 
    s."Vorname" || ' ' || s."Nachname" AS "Spieler",
    SUM(sks."Blaue_Karten") AS "Blaue K.",
    COUNT(DISTINCT sks."Spiel_ID") AS "Spiele_gespielt"
FROM "Spiel_Kader_Statistiken" sks
JOIN "Spieler" s ON sks."Spieler_ID" = s."Spieler_ID"
JOIN "Spiele" sp ON sks."Spiel_ID" = sp."Spiel_ID"
JOIN "Ligen" l ON sp."Liga_ID" = l."Liga_ID"
WHERE sks."Team_ID" = :team_id 
  AND sp."Liga_ID" = :league_id 
  AND l."Saison" = :season 
  AND s."Ist_Offizieller" = 0
GROUP BY s."Spieler_ID", s."Vorname", s."Nachname"
HAVING SUM(sks."Blaue_Karten") > 0
ORDER BY "Blaue K." DESC
LIMIT :limit;