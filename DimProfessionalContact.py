import psycopg2
from pymongo import MongoClient

# Connexion MongoDB
def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "frontusers"

    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    return client, mongo_db, collection

# Connexion PostgreSQL
def get_postgres_connection():
    return psycopg2.connect(
        dbname="DW_DigitalCook",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

# Génération d'un contact_code unique
def generate_contact_code():
    conn = get_postgres_connection()
    cur = conn.cursor()

    # Récupérer le dernier contact_code pour incrémenter
    cur.execute("SELECT contact_code FROM dim_professional_contact ORDER BY contact_pk DESC LIMIT 1")
    last_code = cur.fetchone()

    if last_code and last_code[0].startswith("CONTACT"):
        last_number = int(last_code[0][7:])  # Extraire le numéro
        new_number = last_number + 1
    else:
        new_number = 1  # Si aucun code n'existe, commencer par CONTACT01

    conn.close()
    return f"CONTACT{str(new_number).zfill(2)}"  # ✅ Correction ici

# Extraction des contacts professionnels depuis MongoDB
def extract_from_mongodb():
    client, _, collection = get_mongodb_connection()
    mongo_data = collection.find({}, {"_id": 0, "profile.proffessionalContacts": 1})
    
    contacts = []
    
    for user in mongo_data:
        if "profile" in user and "proffessionalContacts" in user["profile"]:
            for contact in user["profile"]["proffessionalContacts"]:
                contacts.append({
                    "firstname": contact.get("firstName"),
                    "lastname": contact.get("lastName"),
                    "company": contact.get("company"),  # ✅ Correction ici
                    "contact_code": None  # Sera généré plus tard
                })

    client.close()
    
    print("✅ Contacts extraits :", contacts)  # Debug
    return contacts

# Transformation des données
def transform_data(mongo_data):
    transformed_contacts = []
    
    conn = get_postgres_connection()
    cur = conn.cursor()

    # Récupérer les contacts existants
    cur.execute("SELECT contact_code, firstname, lastname FROM dim_professional_contact")
    existing_contacts = {f"{row[1]}_{row[2]}": row[0] for row in cur.fetchall()}  # Dictionnaire clé = "prenom_nom", valeur = contact_code
    
    conn.close()

    contact_counter = 1  # Commence par 1 pour le premier enregistrement

    for record in mongo_data:
        contact_key = f"{record['firstname']}_{record['lastname']}"
        
        if contact_key in existing_contacts:
            record["contact_code"] = existing_contacts[contact_key]  # Utilise le contact_code existant
        else:
            record["contact_code"] = f"CONTACT{str(contact_counter).zfill(2)}"  # Génère un nouveau contact_code
            contact_counter += 1
        
        transformed_contacts.append(record)

    return transformed_contacts

# Insertion ou mise à jour des données dans PostgreSQL
def load_into_postgres(data):
    conn = get_postgres_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO dim_professional_contact (contact_code, firstname, lastname, company)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (contact_code) DO UPDATE SET
        firstname = EXCLUDED.firstname,
        lastname = EXCLUDED.lastname,
        company = EXCLUDED.company
    """

    for record in data:
        values = (
            record["contact_code"],
            record["firstname"],
            record["lastname"],
            record["company"],  # ✅ Ajout de `company`
        )
        print(f"✅ Insertion / Mise à jour : {values}")  # Debug
        cur.execute(insert_query, values)

    conn.commit()
    cur.close()
    conn.close()

# Pipeline principal
def main():
    print("--- Extraction et chargement des contacts professionnels ---")
    
    raw_data = extract_from_mongodb()
    transformed_data = transform_data(raw_data)
    
    if transformed_data:
        load_into_postgres(transformed_data)
        print("✅ Données insérées/mises à jour avec succès dans PostgreSQL.")
    else:
        print("⚠️ Aucune donnée à insérer.")

if __name__ == "__main__":
    main()
