SELECT DISTINCT
    opp."Team_ID",
    opp."Name"
FROM "Spiel_Kader_Statistiken" sks
JOIN "Spieler" s ON sks."Spieler_ID" = s."Spieler_ID" 
JOIN "Spiele" sp ON sks."Spiel_ID" = sp."Spiel_ID"
JOIN "Ligen" l ON sp."Liga_ID" = l."Liga_ID"
JOIN "Teams" opp ON (CASE 
                    WHEN sks."Team_ID" = sp."Heim_Team_ID" THEN sp."Gast_Team_ID" 
                    ELSE sp."Heim_Team_ID" 
                   END) = opp."Team_ID"
WHERE sks."Spieler_ID" = :player_id
  AND opp."Team_ID" != sks."Team_ID" 
  AND s."Ist_Offizieller" = 0
  AND (:season IS NULL OR l."Saison" = :season) -- Hinzugefügt für optionalen Saison-Filter
ORDER BY opp."Name";