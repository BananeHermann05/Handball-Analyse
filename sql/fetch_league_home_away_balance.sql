SELECT
    SUM(CASE WHEN "Punkte_Heim_Offiziell" = 2 THEN 1 ELSE 0 END) AS "Heimsiege",
    SUM(CASE WHEN "Punkte_Gast_Offiziell" = 2 THEN 1 ELSE 0 END) AS "Auswärtssiege",
    SUM(CASE WHEN "Punkte_Heim_Offiziell" = 1 AND "Punkte_Gast_Offiziell" = 1 THEN 1 ELSE 0 END) AS "Unentschieden",
    COUNT("Spiel_ID") AS "Gesamtspiele"
FROM "Spiele" sp
JOIN "Ligen" l ON sp."Liga_ID" = l."Liga_ID"
WHERE sp."Liga_ID" = :league_id AND l."Saison" = :season AND sp."Status" = 'Post'
  AND sp."Punkte_Heim_Offiziell" IS NOT NULL AND sp."Punkte_Gast_Offiziell" IS NOT NULL;