WITH "GameResults" AS (
    SELECT sp."Spiel_ID", sp."Heim_Team_ID" AS "Team_ID_Calc", 
           sp."Tore_Heim" AS "Tore_Erziehlt", sp."Tore_Gast" AS "Tore_Kassiert",
           sp."Punkte_Heim_Offiziell" AS "Punkte_Erhalten"
    FROM "Spiele" sp JOIN "Ligen" l ON sp."Liga_ID" = l."Liga_ID"
    WHERE sp."Status" = 'Post' AND sp."Punkte_Heim_Offiziell" IS NOT NULL 
      AND sp."Liga_ID" = :league_id AND l."Saison" = :season
    UNION ALL
    SELECT sp."Spiel_ID", sp."Gast_Team_ID" AS "Team_ID_Calc",
           sp."Tore_Gast" AS "Tore_Erziehlt", sp."Tore_Heim" AS "Tore_Kassiert",
           sp."Punkte_Gast_Offiziell" AS "Punkte_Erhalten"
    FROM "Spiele" sp JOIN "Ligen" l ON sp."Liga_ID" = l."Liga_ID"
    WHERE sp."Status" = 'Post' AND sp."Punkte_Gast_Offiziell" IS NOT NULL
      AND sp."Liga_ID" = :league_id AND l."Saison" = :season
)
SELECT ROW_NUMBER() OVER (ORDER BY SUM(COALESCE(gr."Punkte_Erhalten", 0)) DESC, 
                               (SUM(COALESCE(gr."Tore_Erziehlt",0)) - SUM(COALESCE(gr."Tore_Kassiert",0))) DESC, 
                               SUM(COALESCE(gr."Tore_Erziehlt",0)) DESC) AS "Platz",
       t."Name" AS "Team", t."Team_ID", COUNT(DISTINCT gr."Spiel_ID") AS "Spiele",
       SUM(CASE WHEN gr."Punkte_Erhalten" = 2 THEN 1 ELSE 0 END) AS "S",
       SUM(CASE WHEN gr."Punkte_Erhalten" = 1 THEN 1 ELSE 0 END) AS "U",
       SUM(CASE WHEN gr."Punkte_Erhalten" = 0 THEN 1 ELSE 0 END) AS "N",
       SUM(COALESCE(gr."Tore_Erziehlt",0))::TEXT || ':' || SUM(COALESCE(gr."Tore_Kassiert",0))::TEXT AS "Tore",
       SUM(COALESCE(gr."Tore_Erziehlt",0)) - SUM(COALESCE(gr."Tore_Kassiert",0)) AS "Diff",
       SUM(COALESCE(gr."Punkte_Erhalten", 0))::TEXT || ':' || 
       ( (COUNT(DISTINCT gr."Spiel_ID") * 2) - SUM(COALESCE(gr."Punkte_Erhalten", 0)) )::TEXT AS "Punkte"
FROM "GameResults" gr JOIN "Teams" t ON gr."Team_ID_Calc" = t."Team_ID"
GROUP BY t."Team_ID", t."Name" ORDER BY "Platz";