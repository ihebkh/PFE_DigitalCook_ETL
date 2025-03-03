import os
import json
import psycopg2
from pymongo import MongoClient

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PFE"
    MONGO_COLLECTION = "PowerBi"
    
    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    return client, mongo_db, collection

def get_postgres_connection():
    return psycopg2.connect(dbname="DW_DigitalCook", user='postgres', password='admin', host='localhost', port='5432')

def list_mongodb_content():
    client, _, _ = get_mongodb_connection()
    databases = client.list_database_names()
    print("Bases de données MongoDB:")
    for db in databases:
        print(f"- {db}")
        mongo_db = client[db]
        collections = mongo_db.list_collection_names()
        print("  Collections:")
        for collection in collections:
            print(f"    - {collection}")
    client.close()

def extract_from_mongodb():
    client, _, collection = get_mongodb_connection()
    mongo_data = list(collection.find({}, {"_id": 0}))  # Exclure l'ID MongoDB
    client.close()
    return mongo_data

def transform_data(mongo_data):
    seen_matricules = set()
    transformed_data = []
    for record in mongo_data:
        matricule = record.get("matricule")
        if matricule in seen_matricules:
            continue  # Ignorer les doublons
        seen_matricules.add(matricule)
        transformed_data.append({
            "matricule": matricule,
            "nom": record.get("nom"),
            "prenom": record.get("prenom"),
            "email": record.get("email"),
            "birthdate": record.get("profile", {}).get("birthDate"),
            "nationality": record.get("profile", {}).get("nationality"),
            "adresseDomicile": record.get("profile", {}).get("adresseDomicile"),
            "pays": record.get("profile", {}).get("pays"),
            "situation": record.get("profile", {}).get("situation"),
            "etatCivil": record.get("profile", {}).get("etatCivil")
        })
    return transformed_data

def load_into_postgres(data):
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    insert_query = """
    INSERT INTO dim_client (matricule, nom, prenom, email, birthdate, nationality, adresseDomicile, pays, situation, etatCivil)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (matricule) DO UPDATE SET 
        nom = EXCLUDED.nom,
        prenom = EXCLUDED.prenom,
        email = EXCLUDED.email,
        birthdate = EXCLUDED.birthdate,
        nationality = EXCLUDED.nationality,
        adresseDomicile = EXCLUDED.adresseDomicile,
        pays = EXCLUDED.pays,
        situation = EXCLUDED.situation,
        etatCivil = EXCLUDED.etatCivil
    """
    
    for record in data:
        values = (
            record["matricule"],
            record["nom"],
            record["prenom"],
            record["email"],
            record["birthdate"],
            record["nationality"],
            record["adresseDomicile"],
            record["pays"],
            record["situation"],
            record["etatCivil"]
        )
        cur.execute(insert_query, values)
    
    conn.commit()
    cur.close()
    conn.close()

def main():
    print("--- Listing MongoDB Content ---")
    list_mongodb_content()
    
    raw_data = extract_from_mongodb()
    transformed_data = transform_data(raw_data)
    if transformed_data:
        load_into_postgres(transformed_data)
        print("Données insérées avec succès dans PostgreSQL.")
    else:
        print("Aucune donnée à insérer.")

if __name__ == "__main__":
    main()
