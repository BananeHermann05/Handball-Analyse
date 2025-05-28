SELECT
    SUM(CASE WHEN sp."Heim_Team_ID" = :team_id THEN (sp."Tore_Heim_HZ") ELSE (sp."Tore_Gast_HZ") END) AS "Tore_HZ1_Erziehlt",
    SUM(CASE WHEN sp."Heim_Team_ID" = :team_id THEN (sp."Tore_Gast_HZ") ELSE (sp."Tore_Heim_HZ") END) AS "Tore_HZ1_Kassiert",
    SUM(CASE WHEN sp."Heim_Team_ID" = :team_id THEN (sp."Tore_Heim" - sp."Tore_Heim_HZ") ELSE (sp."Tore_Gast" - sp."Tore_Gast_HZ") END) AS "Tore_HZ2_Erziehlt",
    SUM(CASE WHEN sp."Heim_Team_ID" = :team_id THEN (sp."Tore_Gast" - sp."Tore_Gast_HZ") ELSE (sp."Tore_Heim" - sp."Tore_Heim_HZ") END) AS "Tore_HZ2_Kassiert"
FROM "Spiele" sp
JOIN "Ligen" l ON sp."Liga_ID" = l."Liga_ID"
WHERE (sp."Heim_Team_ID" = :team_id OR sp."Gast_Team_ID" = :team_id)
  AND sp."Liga_ID" = :league_id AND l."Saison" = :season AND sp."Status" = 'Post'
  AND sp."Tore_Heim_HZ" IS NOT NULL AND sp."Tore_Gast_HZ" IS NOT NULL 
  AND sp."Tore_Heim" IS NOT NULL AND sp."Tore_Gast" IS NOT NULL;