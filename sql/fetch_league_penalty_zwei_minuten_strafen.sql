SELECT 
    s."Vorname" || ' ' || s."Nachname" AS "Spieler",
    t."Name" AS "Team",
    SUM(sks."Zwei_Minuten_Strafen") AS "2-Minuten",
    COUNT(DISTINCT sks."Spiel_ID") AS "Spiele_gespielt"
FROM "Spiel_Kader_Statistiken" sks
JOIN "Spieler" s ON sks."Spieler_ID" = s."Spieler_ID"
JOIN "Teams" t ON sks."Team_ID" = t."Team_ID"
JOIN "Spiele" sp ON sks."Spiel_ID" = sp."Spiel_ID"
JOIN "Ligen" l ON sp."Liga_ID" = l."Liga_ID"
WHERE sp."Liga_ID" = :league_id AND l."Saison" = :season AND s."Ist_Offizieller" = 0
GROUP BY s."Spieler_ID", s."Vorname", s."Nachname", t."Name"
HAVING SUM(sks."Zwei_Minuten_Strafen") > 0
ORDER BY "2-Minuten" DESC
LIMIT :limit;