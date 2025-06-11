SELECT 
    v."Name" AS "Vereinsname_Aggregiert",
    t."Team_ID",
    t."Name" AS "Team_Name",
    l."Liga_ID",
    l."Name" AS "Liga_Name",
    l."Saison",
    l."Altersgruppe"
FROM "Teams" t
JOIN "Vereine" v ON t."Verein_ID" = v."Verein_ID"
-- Dieser JOIN ist komplexer, da die aktuellste Liga pro Team ermittelt werden muss.
-- Eine Möglichkeit ist ein Subquery oder eine Window-Funktion wie im Original.
-- Aber die Haupt-Gruppierungslogik ist weg.
LEFT JOIN (
    -- Finde die letzte Liga für jedes Team
    SELECT 
        sp."Heim_Team_ID" as team_id, 
        sp."Liga_ID" as last_liga_id,
        ROW_NUMBER() OVER(PARTITION BY sp."Heim_Team_ID" ORDER BY sp."Start_Zeit" DESC) as rn
    FROM "Spiele" sp
    UNION
    SELECT 
        sp."Gast_Team_ID" as team_id, 
        sp."Liga_ID" as last_liga_id,
        ROW_NUMBER() OVER(PARTITION BY sp."Gast_Team_ID" ORDER BY sp."Start_Zeit" DESC) as rn
    FROM "Spiele" sp
) last_league ON t."Team_ID" = last_league.team_id AND last_league.rn = 1
LEFT JOIN "Ligen" l ON last_league.last_liga_id = l."Liga_ID"
ORDER BY "Vereinsname_Aggregiert", l."Altersgruppe", "Team_Name";