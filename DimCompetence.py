import os
import json
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
    return psycopg2.connect(dbname="DW_DigitalCook", user='postgres', password='admin', host='localhost', port='5432')

def get_existing_competence_count():
    """ Récupère le nombre actuel de compétences déjà stockées. """
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM dim_competence")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count

def extract_from_mongodb():
    client, _, collection = get_mongodb_connection()
    mongo_data = list(collection.find({}, {"_id": 0, "profile.experiences.competances": 1}))  
    client.close()
    return mongo_data

def transform_data(mongo_data):
    existing_count = get_existing_competence_count()
    seen_competences = set()
    transformed_data = []

    for record in mongo_data:
        experiences = record.get("profile", {}).get("experiences", [])
        for experience in experiences:
            competences = experience.get("competances", [])
            for competence in competences:
                competence = competence.strip() if competence else None
                if competence and competence not in seen_competences:
                    seen_competences.add(competence)
                    existing_count += 1  
                    transformed_data.append({
                        "competence_code": f"COMP{str(existing_count).zfill(2)}",
                        "experience_name": competence
                    })
    
    print("Données transformées :", transformed_data)
    return transformed_data

def load_into_postgres(data):
    if not data:
        print("Aucune compétence à insérer.")
        return
    
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    insert_query = """
    INSERT INTO dim_competence (competence_code, experience_name)
    VALUES (%s, %s)
    ON CONFLICT (competence_code) DO UPDATE SET 
        experience_name = EXCLUDED.experience_name
    """
    
    for record in data:
        values = (
            record["competence_code"],
            record["experience_name"]
        )
        cur.execute(insert_query, values)
    
    conn.commit()
    cur.close()
    conn.close()

def main():
    print("--- Extraction et chargement des compétences ---")
    raw_data = extract_from_mongodb()
    transformed_data = transform_data(raw_data)
    if transformed_data:
        load_into_postgres(transformed_data)
        print("Données insérées avec succès dans PostgreSQL.")
    else:
        print("Aucune donnée à insérer.")

if __name__ == "__main__":
    main()
