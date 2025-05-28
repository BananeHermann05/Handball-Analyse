WITH "RankedGames" AS (
    SELECT sp."Spiel_ID", sp."Start_Zeit", sp."Heim_Team_ID" AS "Team_ID_Calc", t_heim."Name" AS "Team_Name", COALESCE(sp."Punkte_Heim_Offiziell", 0) AS "Punkte"
    FROM "Spiele" sp JOIN "Teams" t_heim ON sp."Heim_Team_ID" = t_heim."Team_ID" JOIN "Ligen" l ON sp."Liga_ID" = l."Liga_ID"
    WHERE sp."Liga_ID" = :league_id AND l."Saison" = :season AND sp."Status" = 'Post' AND sp."Punkte_Heim_Offiziell" IS NOT NULL
    UNION ALL
    SELECT sp."Spiel_ID", sp."Start_Zeit", sp."Gast_Team_ID" AS "Team_ID_Calc", t_gast."Name" AS "Team_Name", COALESCE(sp."Punkte_Gast_Offiziell", 0) AS "Punkte"
    FROM "Spiele" sp JOIN "Teams" t_gast ON sp."Gast_Team_ID" = t_gast."Team_ID" JOIN "Ligen" l ON sp."Liga_ID" = l."Liga_ID"
    WHERE sp."Liga_ID" = :league_id AND l."Saison" = :season AND sp."Status" = 'Post' AND sp."Punkte_Gast_Offiziell" IS NOT NULL
), "NumberedRankedGames" AS (
    SELECT "Spiel_ID", "Start_Zeit", "Team_ID_Calc", "Team_Name", "Punkte", ROW_NUMBER() OVER (PARTITION BY "Team_ID_Calc" ORDER BY "Start_Zeit", "Spiel_ID") as "Spiel_Nr"
    FROM "RankedGames"
), "CumulativePoints" AS (
    SELECT "Team_ID_Calc", "Team_Name", "Start_Zeit", "Spiel_Nr", "Spiel_ID", SUM("Punkte") OVER (PARTITION BY "Team_ID_Calc" ORDER BY "Start_Zeit", "Spiel_ID") AS "Kumulierte_Punkte"
    FROM "NumberedRankedGames"
)
SELECT TO_CHAR(TO_TIMESTAMP("Start_Zeit"), 'DD.MM.YYYY HH24:MI') AS "Spieldatum", "Team_ID_Calc" AS "Team_ID", "Team_Name", "Spiel_Nr", "Kumulierte_Punkte"
FROM "CumulativePoints" ORDER BY "Team_ID_Calc", "Start_Zeit", "Spiel_ID";