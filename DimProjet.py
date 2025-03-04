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

# ✅ Fonction pour éviter les erreurs de conversion (ex : chaînes vides)
def safe_int(value):
    try:
        return int(value) if value and isinstance(value, (str, int)) and str(value).isdigit() else None
    except ValueError:
        return None

# ✅ Récupérer les codes projets existants pour éviter les doublons
def get_existing_project_codes():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT code_projet FROM dim_projet")
    existing_codes = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return existing_codes

# ✅ Génération d'un code projet unique
def generate_project_code(existing_codes):
    if not existing_codes:
        new_number = 1  # Premier projet
    else:
        last_numbers = [int(code[4:]) for code in existing_codes if code.startswith("PROJ") and code[4:].isdigit()]
        new_number = max(last_numbers) + 1 if last_numbers else 1

    return f"PROJ{str(new_number).zfill(2)}"  # Format PROJ01, PROJ02...

# ✅ Extraction des projets depuis MongoDB
def extract_from_mongodb():
    client, _, collection = get_mongodb_connection()
    mongo_data = collection.find({}, {"_id": 0, "profile.projets": 1})

    projects = []
    
    for user in mongo_data:
        if isinstance(user, dict) and "profile" in user and isinstance(user["profile"], dict):
            projets_list = user["profile"].get("projets", [])
            
            if isinstance(projets_list, list):
                for project in projets_list:
                    if isinstance(project, dict):  # ✅ Vérification que project est bien un dictionnaire
                        projects.append({
                            "nom_projet": project.get("nomProjet"),
                            "year_start": safe_int(project.get("dateDebut", {}).get("year", "")),
                            "month_start": safe_int(project.get("dateDebut", {}).get("month", "")),
                            "year_end": safe_int(project.get("dateFin", {}).get("year", "")),
                            "month_end": safe_int(project.get("dateFin", {}).get("month", "")),
                            "entreprise": project.get("entreprise"),
                            "code_projet": None  # Sera généré pour l'insertion
                        })

    client.close()
    
    print("✅ Projets extraits :", projects)  # Debug
    return projects

# ✅ Transformation : Générer un code unique pour chaque projet
def transform_data(mongo_data):
    unique_projects = {}
    existing_codes = get_existing_project_codes()  # Récupère les codes existants

    for record in mongo_data:
        key = (record["nom_projet"], record["entreprise"])  # Clé unique par projet
        if key not in unique_projects:
            new_code = generate_project_code(existing_codes)
            existing_codes.append(new_code)  # Mise à jour pour éviter les doublons
            record["code_projet"] = new_code
            unique_projects[key] = record

    return list(unique_projects.values())

# ✅ Chargement : Insérer ou Mettre à jour dans PostgreSQL
def load_into_postgres(data):
    conn = get_postgres_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO dim_projet (code_projet, nom_projet, year_start, month_start, year_end, month_end, entreprise)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (code_projet) DO UPDATE SET
        year_start = EXCLUDED.year_start,
        month_start = EXCLUDED.month_start,
        year_end = EXCLUDED.year_end,
        month_end = EXCLUDED.month_end,
        entreprise = EXCLUDED.entreprise
    """

    for record in data:
        values = (
            record["code_projet"],
            record["nom_projet"],
            record["year_start"],
            record["month_start"],
            record["year_end"],
            record["month_end"],
            record["entreprise"],
        )
        print(f"✅ Insertion / Mise à jour : {values}")  # Debug
        cur.execute(insert_query, values)

    conn.commit()
    cur.close()
    conn.close()

# ✅ Pipeline principal
def main():
    print("--- Extraction et chargement des projets ---")
    
    raw_data = extract_from_mongodb()
    transformed_data = transform_data(raw_data)
    
    if transformed_data:
        load_into_postgres(transformed_data)
        print("✅ Données insérées/mises à jour avec succès dans PostgreSQL.")
    else:
        print("⚠️ Aucune donnée à insérer.")

if __name__ == "__main__":
    main()
