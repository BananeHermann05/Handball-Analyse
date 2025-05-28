SELECT sks."Rueckennummer" AS "Nr.", s."Vorname" || ' ' || s."Nachname" AS "Name",
       sks."Tore_Gesamt" AS "Tore",
       sks."Tore_7m"::TEXT || '/' || (sks."Tore_7m" + sks."Fehlwurf_7m")::TEXT AS "7m",
       CASE WHEN sks."Gelbe_Karten" > 0 THEN 'X' ELSE '-' END AS "Gelb",
       sks."Zwei_Minuten_Strafen" AS "2min",
       CASE WHEN sks."Rote_Karten" > 0 OR sks."Blaue_Karten" > 0 THEN 'X' ELSE '-' END AS "Rot",
       s."Spieler_ID" -- Behalte die ID für eventuelle Klicks/Links
FROM "Spiel_Kader_Statistiken" sks
JOIN "Spieler" s ON sks."Spieler_ID" = s."Spieler_ID"
WHERE sks."Spiel_ID" = :game_id AND sks."Team_ID" = :team_id AND s."Ist_Offizieller" = 0
ORDER BY sks."Rueckennummer";