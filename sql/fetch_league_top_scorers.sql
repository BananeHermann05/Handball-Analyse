SELECT s."Vorname" || ' ' || s."Nachname" AS "Spieler", t."Name" AS "Team",
       SUM(sks."Tore_Gesamt") AS "Gesamttore", COUNT(DISTINCT sks."Spiel_ID") AS "Spiele_gespielt",
       ROUND(SUM(sks."Tore_Gesamt" * 1.0) / NULLIF(COUNT(DISTINCT sks."Spiel_ID"), 0), 2) AS "Tore_pro_Spiel" 
FROM "Spiel_Kader_Statistiken" sks
JOIN "Spieler" s ON sks."Spieler_ID" = s."Spieler_ID"
JOIN "Teams" t ON sks."Team_ID" = t."Team_ID"
JOIN "Spiele" sp ON sks."Spiel_ID" = sp."Spiel_ID"
JOIN "Ligen" l ON sp."Liga_ID" = l."Liga_ID"
WHERE sp."Liga_ID" = :league_id AND l."Saison" = :season AND s."Ist_Offizieller" = 0
GROUP BY s."Spieler_ID", s."Vorname", s."Nachname", t."Name"
HAVING SUM(sks."Tore_Gesamt") > 0
ORDER BY "Gesamttore" DESC
LIMIT :limit;