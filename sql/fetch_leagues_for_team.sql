SELECT DISTINCT l."Liga_ID", l."Name", l."Saison", l."Altersgruppe"
FROM "Ligen" l
JOIN "Spiele" sp ON l."Liga_ID" = sp."Liga_ID"
WHERE sp."Heim_Team_ID" = :team_id OR sp."Gast_Team_ID" = :team_id
ORDER BY l."Saison" DESC, l."Name";