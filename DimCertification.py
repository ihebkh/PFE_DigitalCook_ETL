import psycopg2
from pymongo import MongoClient

# ✅ Connexion à MongoDB
def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "frontusers"

    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    return client, mongo_db, collection

# ✅ Connexion à PostgreSQL
def get_postgres_connection():
    return psycopg2.connect(
        dbname="DW_DigitalCook",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

# ✅ Génération d'un code unique pour `certificationCode`
def generate_certification_code(existing_codes):
    if not existing_codes:
        return "CERT001"  # Premier code si la table est vide
    else:
        last_number = max(int(code.replace("CERT", "")) for code in existing_codes)
        new_number = last_number + 1
        return f"CERT{str(new_number).zfill(3)}"

# ✅ Fonction pour valider `year` et `month`
def validate_year_month(year, month):
    # Vérifier si `year` est un nombre à 4 chiffres (ex: "2024")
    if not (year.isdigit() and len(year) == 4):
        year = None  # Mettre `None` si non valide

    # Vérifier si `month` est entre "01" et "12"
    if not (month.isdigit() and 1 <= int(month) <= 12):
        month = None  # Mettre `None` si non valide

    return year, month

# ✅ Extraction des certifications depuis MongoDB
def extract_from_mongodb():
    client, _, collection = get_mongodb_connection()
    mongo_data = collection.find({}, {"_id": 0, "profile.certifications": 1})

    certifications = set()  # Utilisation d'un set pour éviter les doublons

    for user in mongo_data:
        if isinstance(user, dict) and "profile" in user and isinstance(user["profile"], dict):
            user_certifications = user["profile"].get("certifications", [])

            if isinstance(user_certifications, list):
                for certif in user_certifications:
                    if isinstance(certif, dict):
                        nom = certif.get("nomCertification", "").strip()
                        year = certif.get("year", "").strip()
                        month = certif.get("month", "").strip()

                        # Validation des dates
                        year, month = validate_year_month(year, month)

                        if nom:  # Vérifier qu'il y a bien un nom
                            certifications.add((nom, year, month))

    client.close()
    
    print("✅ Certifications extraites :", certifications)  # Debug
    return [{"certificationCode": None, "nom": c[0], "year": c[1], "month": c[2]} for c in certifications]

# ✅ Chargement des données dans PostgreSQL
def load_into_postgres(data):
    conn = get_postgres_connection()
    cur = conn.cursor()

    # Récupérer les `certificationCode` existants pour l'incrémentation
    cur.execute("SELECT certificationCode FROM Dim_certification")
    existing_codes = {row[0] for row in cur.fetchall()}

    insert_query = """
    INSERT INTO Dim_certification (certificationCode, nom, year, month)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (certificationCode) DO NOTHING;
    """

    for record in data:
        if record["certificationCode"] is None:  # Générer un code uniquement si nécessaire
            record["certificationCode"] = generate_certification_code(existing_codes)
            existing_codes.add(record["certificationCode"])  # Ajouter à l'ensemble pour éviter les doublons

        values = (
            record["certificationCode"],
            record["nom"],
            record["year"],  # Déjà validé
            record["month"],  # Déjà validé
        )
        print(f"✅ Insertion / Mise à jour : {values}")  # Debug
        cur.execute(insert_query, values)

    conn.commit()
    cur.close()
    conn.close()

# ✅ Pipeline principal
def main():
    print("--- Extraction et chargement des certifications ---")
    
    raw_data = extract_from_mongodb()
    
    if raw_data:
        load_into_postgres(raw_data)
        print("✅ Données insérées/mises à jour avec succès dans PostgreSQL.")
    else:
        print("⚠️ Aucune donnée à insérer.")

if __name__ == "__main__":
    main()
