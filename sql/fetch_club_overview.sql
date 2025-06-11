-- Erstellt eine temporäre Liste mit der jeweils aktuellsten Liga-ID pro Team
WITH LatestLeagueInfo AS (
    SELECT
        Team_ID,
        Liga_ID,
        -- Nutze ROW_NUMBER, um nur den neuesten Eintrag pro Team zu erhalten
        ROW_NUMBER() OVER(PARTITION BY Team_ID ORDER BY Saison DESC, Start_Zeit DESC) as rn
    FROM (
        -- Sammle alle Spiele (Heim und Gast) mit gültigen Liga-Informationen
        SELECT sp."Heim_Team_ID" AS Team_ID, l."Liga_ID", l."Saison", sp."Start_Zeit"
        FROM "Spiele" sp
        JOIN "Ligen" l ON sp."Liga_ID" = l."Liga_ID"
        WHERE l."Liga_ID" IS NOT NULL AND l."Saison" IS NOT NULL

        UNION ALL

        SELECT sp."Gast_Team_ID" AS Team_ID, l."Liga_ID", l."Saison", sp."Start_Zeit"
        FROM "Spiele" sp
        JOIN "Ligen" l ON sp."Liga_ID" = l."Liga_ID"
        WHERE l."Liga_ID" IS NOT NULL AND l."Saison" IS NOT NULL
    ) AS AllGames
)
-- Hauptabfrage
SELECT
    v."Name" AS "Vereinsname_Aggregiert",
    t."Team_ID",
    t."Name" AS "Team_Name",
    l."Liga_ID",
    l."Name" AS "Liga_Name",
    l."Saison",
    l."Altersgruppe"
FROM "Teams" t
-- Beginne mit der sauberen Liste der Vereine und Teams
JOIN "Vereine" v ON t."Verein_ID" = v."Verein_ID"
-- Verknüpfe die Liga-Infos optional (LEFT JOIN). Teams ohne Liga-Info werden NICHT mehr rausgefiltert.
LEFT JOIN LatestLeagueInfo lli ON t."Team_ID" = lli.Team_ID AND lli.rn = 1
LEFT JOIN "Ligen" l ON lli."Liga_ID" = l."Liga_ID"
-- KEINE WHERE-Klausel hier, die die Ergebnisse einschränkt
ORDER BY "Vereinsname_Aggregiert", "Altersgruppe" DESC, "Team_Name";