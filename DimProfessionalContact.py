import psycopg2
from pymongo import MongoClient

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "frontusers"

    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    return client, mongo_db, collection

def get_postgres_connection():
    return psycopg2.connect(
        dbname="DW_DigitalCook",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

def generate_contact_code():
    conn = get_postgres_connection()
    cur = conn.cursor()

    cur.execute("SELECT contact_code FROM dim_professional_contact ORDER BY contact_pk DESC LIMIT 1")
    last_code = cur.fetchone()

    if last_code and last_code[0].startswith("CONTACT"):
        last_number = int(last_code[0][7:])  
        new_number = last_number + 1
    else:
        new_number = 1 

    conn.close()
    return f"CONTACT{str(new_number).zfill(2)}" 

def extract_from_mongodb():
    client, _, collection = get_mongodb_connection()
    mongo_data = collection.find({}, {"_id": 0, "profile.proffessionalContacts": 1})
    
    contacts = []
    existing_entries = set()  # Ensemble pour éviter les doublons
    
    for user in mongo_data:
        if "profile" in user and "proffessionalContacts" in user["profile"]:
            for contact in user["profile"]["proffessionalContacts"]:
                # Créer une clé unique basée sur (firstname, lastname, company)
                contact_key = (contact.get("firstName"), contact.get("lastName"), contact.get("company"))
                
                # Si la clé n'existe pas déjà, l'ajouter à la liste des contacts
                if contact_key not in existing_entries:
                    contacts.append({
                        "firstname": contact.get("firstName"),
                        "lastname": contact.get("lastName"),
                        "company": contact.get("company"),
                        "contact_code": None
                    })
                    existing_entries.add(contact_key)  # Ajouter la clé à l'ensemble
    
    client.close()
    
    print("Contacts extraits :", contacts)  # Afficher les contacts sans doublons
    return contacts

def transform_data(mongo_data):
    transformed_contacts = []
    
    conn = get_postgres_connection()
    cur = conn.cursor()

    cur.execute("SELECT contact_code, firstname, lastname FROM dim_professional_contact")
    existing_contacts = {f"{row[1]}_{row[2]}": row[0] for row in cur.fetchall()} 
    
    conn.close()

    contact_counter = 1  # Compteur pour générer les codes de contact

    for record in mongo_data:
        contact_key = f"{record['firstname']}_{record['lastname']}"
        
        if contact_key in existing_contacts:
            record["contact_code"] = existing_contacts[contact_key]  # Utiliser le code existant
        else:
            record["contact_code"] = f"CONTACT{str(contact_counter).zfill(2)}"  # Nouveau code de contact
            contact_counter += 1
        
        transformed_contacts.append(record)

    return transformed_contacts

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
            record["company"], 
        )
        print(f"Insertion / Mise à jour : {values}")
        cur.execute(insert_query, values)

    conn.commit()
    cur.close()
    conn.close()

def main():
    print("--- Extraction et chargement des contacts professionnels ---")
    
    raw_data = extract_from_mongodb()  # Extraire les données de MongoDB
    transformed_data = transform_data(raw_data)  # Transformer les données pour correspondre à la structure de PostgreSQL
    
    if transformed_data:
        load_into_postgres(transformed_data)  # Charger les données dans PostgreSQL
        print("Données insérées/mises à jour avec succès dans PostgreSQL.")
    else:
        print("Aucune donnée à insérer.")

if __name__ == "__main__":
    main()
