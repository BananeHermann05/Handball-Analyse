SELECT DISTINCT T."Team_ID", T."Name", T."Akronym", T."Logo_URL"
FROM "Teams" T
WHERE T."Team_ID" IN (
    SELECT "Heim_Team_ID" FROM "Spiele" WHERE "Liga_ID" = :league_id
    UNION
    SELECT "Gast_Team_ID" FROM "Spiele" WHERE "Liga_ID" = :league_id
) ORDER BY T."Name";