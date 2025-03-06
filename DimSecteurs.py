import psycopg2
from pymongo import MongoClient

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "secteurdactivities"

    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    return client, mongo_db, collection

def get_postgres_connection():
    return psycopg2.connect(dbname="DW_DigitalCook", user='postgres', password='admin', host='localhost', port='5432')

def get_existing_secteurs():
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT label, secteurcode FROM Dim_secteur;")
    existing_secteurs = {row[0]: row[1] for row in cur.fetchall()}
    
    print("🔍 Secteurs existants dans PostgreSQL:", existing_secteurs)  
    
    cur.close()
    conn.close()
    return existing_secteurs

def extract_from_mongodb():
    client, _, collection = get_mongodb_connection()
    mongo_data = list(collection.find({}, {"_id": 1, "label": 1, "jobs": 1}))
    client.close()
    
    print(f"📦 {len(mongo_data)} secteurs extraits de MongoDB")  
    
    return mongo_data

def transform_data(mongo_data):
    existing_secteurs = get_existing_secteurs() 
    secteur_counter = len(existing_secteurs) + 1
    transformed_data = []
    
    for record in mongo_data:
        secteur_label = record.get("label", "")

        if secteur_label in existing_secteurs:
            secteur_code = existing_secteurs[secteur_label]
        else:
            secteur_code = f"SECT{secteur_counter:04d}"
            existing_secteurs[secteur_label] = secteur_code
            secteur_counter += 1
        
        print(f" Secteur: {secteur_label}, Code Généré: {secteur_code}")

        for job in record.get("jobs", []):
            rome_code = job.get("romeCode", "")
            label_jobs = job.get("label", "")
            main_name = job.get("mainName", "")
            subdomain = job.get("subDomain", "")
            language = job["labels"][0]["language"] if job.get("labels") else ""

            transformed_data.append({
                "secteurcode": secteur_code,
                "label": secteur_label,
                "romecode_jobs": rome_code,
                "label_jobs": label_jobs,
                "mainName_jobs": main_name,
                "subdomain_jobs": subdomain,
                "language_jobs": language
            })
    
    print(f"📊 {len(transformed_data)} métiers transformés")
    return transformed_data

def load_into_postgres(data):
    conn = get_postgres_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO Dim_secteur (secteurcode, label, romecode_jobs, label_jobs, mainName_jobs, subdomain_jobs, language_jobs)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (secteurcode) DO UPDATE SET 
        label = EXCLUDED.label,
        label_jobs = EXCLUDED.label_jobs,
        mainName_jobs = EXCLUDED.mainName_jobs,
        subdomain_jobs = EXCLUDED.subdomain_jobs,
        language_jobs = EXCLUDED.language_jobs;
    """

    for record in data:
        print(" Insertion en cours :", record) 

        values = (
            record["secteurcode"],
            record["label"],
            record["romecode_jobs"],
            record["label_jobs"],
            record["mainName_jobs"],
            record["subdomain_jobs"],
            record["language_jobs"]
        )

        try:
            cur.execute(insert_query, values)
        except Exception as e:
            print(" Erreur lors de l'insertion :", e)

    conn.commit()
    cur.close()
    conn.close()
    print(" Données insérées avec succès dans PostgreSQL.")

def main():
    raw_data = extract_from_mongodb()
    transformed_data = transform_data(raw_data)
    if transformed_data:
        load_into_postgres(transformed_data)
    else:
        print(" Aucune donnée à insérer.")

if __name__ == "__main__":
    main()
