SELECT TO_CHAR(TO_TIMESTAMP(sp."Start_Zeit"), 'DD.MM.YYYY HH24:MI') AS "Spieldatum",
       ht."Name" AS "Heimteam", gt."Name" AS "Gastteam",
       sp."Tore_Heim"::TEXT || ' : ' || sp."Tore_Gast"::TEXT AS "Endstand",
       sks."Rueckennummer", sks."Tore_Gesamt", sks."Tore_7m", sks."Fehlwurf_7m",
       sks."Gelbe_Karten", sks."Zwei_Minuten_Strafen", sks."Rote_Karten", sks."Blaue_Karten",
       sp."Spiel_ID" 
FROM "Spiel_Kader_Statistiken" sks
JOIN "Spieler" s ON sks."Spieler_ID" = s."Spieler_ID"
JOIN "Spiele" sp ON sks."Spiel_ID" = sp."Spiel_ID"
JOIN "Ligen" l ON sp."Liga_ID" = l."Liga_ID"
JOIN "Teams" ht ON sp."Heim_Team_ID" = ht."Team_ID"
JOIN "Teams" gt ON sp."Gast_Team_ID" = gt."Team_ID"
WHERE s."Spieler_ID" = :player_id AND l."Saison" = :season AND s."Ist_Offizieller" = 0
ORDER BY sp."Start_Zeit" ASC;