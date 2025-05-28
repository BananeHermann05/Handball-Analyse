SELECT 
    SUM(CASE WHEN sp."Heim_Team_ID" = :team_id THEN sp."Tore_Heim" ELSE sp."Tore_Gast" END) AS "Team_Gesamttore"
FROM "Spiele" sp
JOIN "Ligen" l ON sp."Liga_ID" = l."Liga_ID"
WHERE (sp."Heim_Team_ID" = :team_id OR sp."Gast_Team_ID" = :team_id)
  AND sp."Liga_ID" = :league_id AND l."Saison" = :season AND sp."Status" = 'Post'
  AND sp."Tore_Heim" IS NOT NULL AND sp."Tore_Gast" IS NOT NULL;