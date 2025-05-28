SELECT DISTINCT s."Spieler_ID", s."Vorname", s."Nachname", s."Position", s."Spitzname", s."Bild_URL"
FROM "Spieler" s
JOIN "Spiel_Kader_Statistiken" sks ON s."Spieler_ID" = sks."Spieler_ID"
WHERE sks."Team_ID" = :team_id AND s."Ist_Offizieller" = 0
ORDER BY s."Nachname", s."Vorname";