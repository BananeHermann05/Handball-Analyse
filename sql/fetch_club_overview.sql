WITH TeamLigaInfo AS (
    SELECT 
        t."Team_ID",
        t."Name" AS "Team_Name",
        l."Liga_ID",
        l."Name" AS "Liga_Name",
        l."Saison",
        l."Altersgruppe",
        -- Versuche, einen Basis-Vereinsnamen zu extrahieren
        REGEXP_REPLACE(t."Name", '\\s+[0-9]+$|\\s+[IVX]+$|\\s+[AaJjBbCcDdEeMmWwGg]$|\\s+\\(A\\)$|\\s+II$|\\s+III$|\\s+IV$', '', 'i') AS "Vereinsname_Aggregiert",
        ROW_NUMBER() OVER (PARTITION BY t."Team_ID" ORDER BY l."Saison" DESC, sp."Start_Zeit" DESC) as rn
    FROM "Teams" t
    LEFT JOIN "Spiele" sp ON (t."Team_ID" = sp."Heim_Team_ID" OR t."Team_ID" = sp."Gast_Team_ID")
    LEFT JOIN "Ligen" l ON sp."Liga_ID" = l."Liga_ID"
    WHERE l."Liga_ID" IS NOT NULL
    GROUP BY t."Team_ID", t."Name", l."Liga_ID", l."Name", l."Saison", l."Altersgruppe", sp."Start_Zeit"
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
WHERE rn = 1 -- Nur die aktuellste Liga-Zuordnung pro Team
ORDER BY "Vereinsname_Aggregiert", "Altersgruppe", "Team_Name";