SELECT
    opp."Name" AS "Gegner_Name",
    COUNT(DISTINCT sp."Spiel_ID") AS "Spiele_gg_Gegner",
    COALESCE(SUM(sks."Tore_Gesamt"), 0) AS "Tore_Gesamt_gg_Gegner",
    COALESCE(SUM(sks."Tore_7m"), 0) AS "Tore_7m_gg_Gegner",
    COALESCE(SUM(sks."Fehlwurf_7m"), 0) AS "Fehlwurf_7m_gg_Gegner",
    ROUND(CASE 
        WHEN (SUM(sks."Tore_7m") + SUM(sks."Fehlwurf_7m")) = 0 THEN NULL
        ELSE (COALESCE(SUM(sks."Tore_7m"), 0) * 100.0 / NULLIF((SUM(sks."Tore_7m") + SUM(sks."Fehlwurf_7m")),0))
    END, 1) AS "7m_Quote_Prozent_gg_Gegner",
    COALESCE(SUM(sks."Gelbe_Karten"), 0) AS "Gelbe_Karten_gg_Gegner",
    COALESCE(SUM(sks."Zwei_Minuten_Strafen"), 0) AS "Zwei_Minuten_gg_Gegner"
FROM "Spiel_Kader_Statistiken" sks
JOIN "Spieler" s ON sks."Spieler_ID" = s."Spieler_ID"
JOIN "Spiele" sp ON sks."Spiel_ID" = sp."Spiel_ID"
JOIN "Ligen" l ON sp."Liga_ID" = l."Liga_ID"
JOIN "Teams" spieler_team ON sks."Team_ID" = spieler_team."Team_ID"
JOIN "Teams" opp ON (CASE 
                    WHEN sks."Team_ID" = sp."Heim_Team_ID" THEN sp."Gast_Team_ID" 
                    ELSE sp."Heim_Team_ID" 
                   END) = opp."Team_ID"
WHERE sks."Spieler_ID" = :player_id
  AND opp."Team_ID" = :opponent_team_id
  AND sks."Team_ID" != opp."Team_ID" 
  AND s."Ist_Offizieller" = 0
  AND (:season IS NULL OR l."Saison" = :season) -- Hinzugefügt für optionalen Saison-Filter
GROUP BY opp."Team_ID", opp."Name";