WITH TeamLigaInfo AS (
    SELECT 
        v."Name" AS "Vereinsname_Aggregiert", -- HOLT DEN NAMEN AUS DER NEUEN VEREINE-TABELLE
        t."Team_ID",
        t."Name" AS "Team_Name",
        l."Liga_ID",
        l."Name" AS "Liga_Name",
        l."Saison",
        l."Altersgruppe",
        -- Finde die aktuellste Liga-Zuweisung für jedes Team, basierend auf der Saison und der Startzeit des Spiels
        ROW_NUMBER() OVER(PARTITION BY t."Team_ID" ORDER BY l."Saison" DESC, sp."Start_Zeit" DESC) as rn
    FROM "Teams" t
    -- Verknüpfe Teams mit den neuen, sauberen Vereinsdaten
    JOIN "Vereine" v ON t."Verein_ID" = v."Verein_ID"
    -- Finde die Spiele und Ligen, um die aktuellste Liga zu bestimmen
    LEFT JOIN "Spiele" sp ON t."Team_ID" = sp."Heim_Team_ID" OR t."Team_ID" = sp."Gast_Team_ID"
    LEFT JOIN "Ligen" l ON sp."Liga_ID" = l."Liga_ID"
    WHERE l."Liga_ID" IS NOT NULL
)
SELECT 
    "Vereinsname_Aggregiert",
    "Team_ID",
    "Team_Name",
    "Liga_ID",
    "Liga_Name",
    "Saison",
    "Altersgruppe"
FROM TeamLigaInfo
WHERE rn = 1 -- Wähle nur die aktuellste Zuordnung pro Team aus
ORDER BY "Vereinsname_Aggregiert", "Altersgruppe", "Team_Name";