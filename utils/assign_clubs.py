import re

def get_base_club_name(team_name):
    # Entfernt übliche Suffixe wie Zahlen, römische Ziffern, Geschlechter, "Jugend" etc.
    # Dies ist ein verbesserter Startpunkt.
    pattern = r'\s+(\d\.?|\(A\)|[IVX]+|\s[mwajcdebgf]|Herren|Damen|Jugend).*$'
    base_name = re.sub(pattern, '', team_name, flags=re.IGNORECASE)
    return base_name.strip()

# Beispiel:
# get_base_club_name("SG Handball Steinfurt 2") -> "SG Handball Steinfurt"
# get_base_club_name("HSG Grönegau-Melle wA-Jugend") -> "HSG Grönegau-Melle"