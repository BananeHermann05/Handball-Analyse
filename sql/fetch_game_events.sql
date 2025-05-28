SELECT e."Spiel_Minute",
       CASE e."Team_Seite" WHEN 'Home' THEN sp_ht."Name" WHEN 'Away' THEN sp_gt."Name" ELSE NULL END AS "Team",
       e."Typ" AS "Ereignis Typ", e."Nachricht" AS "Beschreibung",
       e."Score_Heim", e."Score_Gast"  
FROM "Ereignisse" e
JOIN "Spiele" sp ON e."Spiel_ID" = sp."Spiel_ID"
JOIN "Teams" sp_ht ON sp."Heim_Team_ID" = sp_ht."Team_ID"
JOIN "Teams" sp_gt ON sp."Gast_Team_ID" = sp_gt."Team_ID"
WHERE e."Spiel_ID" = :game_id
ORDER BY e."Zeitstempel" ASC;