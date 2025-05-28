SELECT 
    "Rueckennummer", "Tore_Gesamt", "Tore_7m", "Fehlwurf_7m", 
    "Gelbe_Karten", "Zwei_Minuten_Strafen", "Rote_Karten", "Blaue_Karten"
FROM "Spiel_Kader_Statistiken"
WHERE "Spieler_ID" = :player_id AND "Spiel_ID" = :game_id;