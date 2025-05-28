SELECT
    COUNT(DISTINCT sks."Spiel_ID") AS "Spiele",
    COALESCE(SUM(sks."Tore_Gesamt"), 0) AS "Tore_Gesamt",
    COALESCE(SUM(sks."Tore_7m"), 0) AS "Tore_7m",
    COALESCE(SUM(sks."Fehlwurf_7m"), 0) AS "Fehlwurf_7m",
    ROUND(CASE 
        WHEN (SUM(sks."Tore_7m") + SUM(sks."Fehlwurf_7m")) = 0 THEN NULL
        ELSE (COALESCE(SUM(sks."Tore_7m"), 0) * 100.0 / NULLIF((SUM(sks."Tore_7m") + SUM(sks."Fehlwurf_7m")),0))
    END, 1) AS "7m_Quote_Prozent",
    COALESCE(SUM(sks."Gelbe_Karten"), 0) AS "Gelbe_Karten",
    COALESCE(SUM(sks."Zwei_Minuten_Strafen"), 0) AS "Zwei_Minuten",
    COALESCE(SUM(sks."Rote_Karten"), 0) AS "Rote_Karten",
    COALESCE(SUM(sks."Blaue_Karten"), 0) AS "Blaue_Karten"
FROM "Spiel_Kader_Statistiken" sks
JOIN "Spieler" s ON sks."Spieler_ID" = s."Spieler_ID"
WHERE sks."Spieler_ID" = :player_id AND s."Ist_Offizieller" = 0;