SELECT
    sp."Spiel_ID", TO_CHAR(TO_TIMESTAMP(sp."Start_Zeit"), 'DD.MM.YYYY HH24:MI') AS "Datum",
    l."Name" AS "Liga_Name", sp."Phase_ID", h."Name" AS "Halle", h."Stadt" AS "Hallen_Stadt",
    ht."Name" AS "Heimteam", gt."Name" AS "Gastteam",
    sp."Heim_Team_ID", sp."Gast_Team_ID", sp."Tore_Heim", sp."Tore_Gast", 
    sp."Tore_Heim_HZ", sp."Tore_Gast_HZ", sp."Status", sp."PDF_URL",
    sp."Spiel_Nummer", sp."SchiedsrichterInfo", sp."Punkte_Heim_Offiziell", sp."Punkte_Gast_Offiziell"  
FROM "Spiele" sp
JOIN "Teams" ht ON sp."Heim_Team_ID" = ht."Team_ID"
JOIN "Teams" gt ON sp."Gast_Team_ID" = gt."Team_ID"
JOIN "Ligen" l ON sp."Liga_ID" = l."Liga_ID"
LEFT JOIN "Hallen" h ON sp."Hallen_ID" = h."Hallen_ID"
WHERE sp."Spiel_ID" = :game_id;