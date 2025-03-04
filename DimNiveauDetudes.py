import psycopg2
from pymongo import MongoClient

# ✅ Connexion MongoDB
def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "frontusers"

    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    return client, mongo_db, collection

# ✅ Connexion PostgreSQL
def get_postgres_connection():
    return psycopg2.connect(
        dbname="DW_DigitalCook",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

# ✅ Fonction pour éviter les erreurs de conversion
def safe_int(value):
    try:
        return int(value) if value and isinstance(value, (str, int)) and str(value).isdigit() else None
    except ValueError:
        return None

# ✅ Génération d'un `diplome_code` unique avec incrémentation locale
def generate_diplome_code(existing_codes):
    """
    Génère un code de diplôme unique en évitant les doublons lors de l'insertion.
    """
    if existing_codes:
        last_number = max(existing_codes) + 1
    else:
        last_number = 1  # Premier code DIP001

    new_code = f"DIP{str(last_number).zfill(3)}"
    existing_codes.add(last_number)  # Ajoute le numéro utilisé pour éviter les doublons
    return new_code

# ✅ Extraction des niveaux d'études depuis MongoDB
def extract_from_mongodb():
    client, _, collection = get_mongodb_connection()
    mongo_data = collection.find({}, {"_id": 0, "profile.niveauDetudes": 1})

    niveaux_etudes = []
    
    for user in mongo_data:
        if isinstance(user, dict) and "profile" in user and isinstance(user["profile"], dict):
            niveau_list = user["profile"].get("niveauDetudes", [])
            
            if isinstance(niveau_list, list):
                for niveau in niveau_list:
                    if isinstance(niveau, dict):  # ✅ Vérification que le niveau est bien un dictionnaire
                        niveaux_etudes.append({
                            "diplome_code": None,  # Sera généré dynamiquement
                            "label": niveau.get("label"),
                            "universite": niveau.get("universite"),
                            "start_year": safe_int(niveau.get("du", {}).get("year", "")),
                            "start_month": safe_int(niveau.get("du", {}).get("month", "")),
                            "end_year": safe_int(niveau.get("au", {}).get("year", "")),
                            "end_month": safe_int(niveau.get("au", {}).get("month", "")),
                            "nom_diplome": niveau.get("nomDiplome"),
                            "pays": niveau.get("pays"),
                        })

    client.close()
    
    print("✅ Niveaux d'études extraits :", niveaux_etudes)  # Debug
    return niveaux_etudes

# ✅ Chargement : Insérer ou Mettre à jour dans PostgreSQL avec un `diplome_code` unique pour chaque entrée
def load_into_postgres(data):
    conn = get_postgres_connection()
    cur = conn.cursor()

    # Récupérer les numéros existants pour éviter les doublons
    cur.execute("SELECT SUBSTRING(diplome_code, 4)::INTEGER FROM dim_niveau_d_etudes WHERE diplome_code LIKE 'DIP%'")
    existing_codes = {row[0] for row in cur.fetchall()}  # Convertir en ensemble pour éviter les doublons

    insert_query = """
    INSERT INTO dim_niveau_d_etudes (diplome_code, label, universite, start_year, start_month, end_year, end_month, nom_diplome, pays)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (diplome_code) DO UPDATE SET
        label = EXCLUDED.label,
        universite = EXCLUDED.universite,
        start_year = EXCLUDED.start_year,
        start_month = EXCLUDED.start_month,
        end_year = EXCLUDED.end_year,
        end_month = EXCLUDED.end_month,
        nom_diplome = EXCLUDED.nom_diplome,
        pays = EXCLUDED.pays;
    """

    for record in data:
        record["diplome_code"] = generate_diplome_code(existing_codes)  # ✅ Générer un code unique sans répétition
        
        values = (
            record["diplome_code"],
            record["label"],
            record["universite"],
            record["start_year"],
            record["start_month"],
            record["end_year"],
            record["end_month"],
            record["nom_diplome"],
            record["pays"],
        )
        print(f"✅ Insertion / Mise à jour : {values}")  # Debug
        cur.execute(insert_query, values)

    conn.commit()
    cur.close()
    conn.close()

# ✅ Pipeline principal
def main():
    print("--- Extraction et chargement des niveaux d'études ---")
    
    raw_data = extract_from_mongodb()
    
    if raw_data:
        load_into_postgres(raw_data)
        print("✅ Données insérées/mises à jour avec succès dans PostgreSQL.")
    else:
        print("⚠️ Aucune donnée à insérer.")

if __name__ == "__main__":
    main()
