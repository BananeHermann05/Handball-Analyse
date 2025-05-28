SELECT
    AVG("Tore_Heim" + "Tore_Gast") AS "Avg_Gesamttore_pro_Spiel",
    AVG("Tore_Heim") AS "Avg_Heimtore_pro_Spiel",
    AVG("Tore_Gast") AS "Avg_Gasttore_pro_Spiel"
FROM "Spiele" sp
JOIN "Ligen" l ON sp."Liga_ID" = l."Liga_ID"
WHERE sp."Liga_ID" = :league_id AND l."Saison" = :season AND sp."Status" = 'Post'
  AND sp."Tore_Heim" IS NOT NULL AND sp."Tore_Gast" IS NOT NULL;