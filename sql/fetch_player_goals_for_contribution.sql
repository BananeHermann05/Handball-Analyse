SELECT COALESCE(SUM(sks."Tore_Gesamt"), 0) AS "Spieler_Tore"
FROM "Spiel_Kader_Statistiken" sks
JOIN "Spiele" sp ON sks."Spiel_ID" = sp."Spiel_ID"
JOIN "Ligen" l ON sp."Liga_ID" = l."Liga_ID"
WHERE sks."Spieler_ID" = :player_id AND sks."Team_ID" = :team_id
  AND sp."Liga_ID" = :league_id AND l."Saison" = :season;