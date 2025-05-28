SELECT 
    TO_CHAR(TO_TIMESTAMP(sp."Start_Zeit"), 'DD.MM.YYYY HH24:MI') AS "Spieldatum",
    ht."Name" AS "Heimteam",
    gt."Name" AS "Gastteam",
    sp."Tore_Heim" AS "Tore_Heim_Spiel", 
    sp."Tore_Gast" AS "Tore_Gast_Spiel",
    sp."Punkte_Heim_Offiziell" AS "Punkte_Heim",
    sp."Punkte_Gast_Offiziell" AS "Punkte_Gast",
    ht."Team_ID" AS "Heim_Team_ID_H2H" -- Wichtig für die Aggregation in Python
FROM "Spiele" sp
JOIN "Teams" ht ON sp."Heim_Team_ID" = ht."Team_ID"
JOIN "Teams" gt ON sp."Gast_Team_ID" = gt."Team_ID"
JOIN "Ligen" l ON sp."Liga_ID" = l."Liga_ID"
WHERE sp."Status" = 'Post'
  AND ((sp."Heim_Team_ID" = :team1_id AND sp."Gast_Team_ID" = :team2_id) OR (sp."Heim_Team_ID" = :team2_id AND sp."Gast_Team_ID" = :team1_id))
  AND (:league_id IS NULL OR sp."Liga_ID" = :league_id) -- Optionaler Filter
  AND (:season IS NULL OR l."Saison" = :season)       -- Optionaler Filter
ORDER BY sp."Start_Zeit" DESC;