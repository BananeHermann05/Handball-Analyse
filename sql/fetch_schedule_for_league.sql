SELECT sp."Spiel_ID", TO_CHAR(TO_TIMESTAMP(sp."Start_Zeit"), 'DD.MM.YYYY HH24:MI') AS "Spieldatum",
       ht."Name" AS "Heimteam", ht."Logo_URL" AS "Heim_Logo_URL",
       gt."Name" AS "Gastteam", gt."Logo_URL" AS "Gast_Logo_URL",
       CASE WHEN sp."Status" = 'Post' AND sp."Tore_Heim" IS NOT NULL AND sp."Tore_Gast" IS NOT NULL 
            THEN sp."Tore_Heim"::TEXT || ':' || sp."Tore_Gast"::TEXT
            ELSE 'vs' END AS "Ergebnis",
       h."Name" AS "Halle"
FROM "Spiele" sp
JOIN "Teams" ht ON sp."Heim_Team_ID" = ht."Team_ID"
JOIN "Teams" gt ON sp."Gast_Team_ID" = gt."Team_ID"
JOIN "Ligen" l ON sp."Liga_ID" = l."Liga_ID"
LEFT JOIN "Hallen" h ON sp."Hallen_ID" = h."Hallen_ID"
WHERE sp."Liga_ID" = :league_id AND l."Saison" = :season
ORDER BY sp."Start_Zeit" ASC;