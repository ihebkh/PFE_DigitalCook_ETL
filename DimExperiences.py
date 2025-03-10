from pymongo import MongoClient
import psycopg2

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "frontusers"

    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    return collection

def generate_code_experience(existing_codes):
    # On génère un code basé sur l'incrémentation des codes existants
    if not existing_codes:
        return "CODE0001"
    else:
        last_code = max([int(code[4:]) for code in existing_codes if code.startswith("CODE")], default=0)
        new_code = last_code + 1
        return f"CODE{str(new_code).zfill(4)}"

def extract_experiences_from_mongo():
    collection = get_mongodb_connection()
    
    # Liste pour stocker les expériences extraites
    experiences = []

    # Récupérer tous les utilisateurs
    users = collection.find()

    # Liste pour gérer les codes des expériences déjà affectés
    existing_codes = []

    for user in users:
        # Rechercher des expériences dans le champ "profile"
        if 'profile' in user:
            profile = user['profile']
            if 'experiences' in profile:
                # Générer un code_experience pour cet utilisateur
                code_experience = generate_code_experience(existing_codes)
                existing_codes.append(code_experience)  # Ajouter ce code à la liste des codes déjà utilisés
                
                for experience in profile['experiences']:
                    # Vérification si l'expérience est un dictionnaire ou une chaîne de caractères
                    if isinstance(experience, dict):  # Si c'est un dictionnaire
                        du_year = experience.get('du', {}).get('year', 'N/A')
                        du_month = experience.get('du', {}).get('month', 'N/A')
                        au_year = experience.get('au', {}).get('year', 'N/A')
                        au_month = experience.get('au', {}).get('month', 'N/A')
                        
                        experiences.append({
                            "code_experience": code_experience,  # Ajouter le code d'expérience
                            "role": experience.get("role", "N/A"),
                            "entreprise": experience.get("entreprise", "N/A"),
                            "du_year": du_year,
                            "du_month": du_month,
                            "au_year": au_year,
                            "au_month": au_month,
                        })
                    elif isinstance(experience, str):  # Si c'est une chaîne de caractères
                        experiences.append({
                            "code_experience": code_experience,  # Ajouter le code d'expérience
                            "role": experience,
                            "entreprise": "N/A",  # Pas d'entreprise disponible pour une chaîne de caractères
                            "du_year": "N/A",
                            "du_month": "N/A",
                            "au_year": "N/A",
                            "au_month": "N/A",
                        })
    
    return experiences

def insert_experiences_into_postgres(experiences):
    # Connexion à la base de données PostgreSQL
    conn = psycopg2.connect(
        dbname="DW_DigitalCook",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    # Requête d'insertion SQL avec ON CONFLICT pour mettre à jour si le code d'expérience existe déjà
    insert_query = """
    INSERT INTO dim_experience (codeexperience, role, entreprise)
    VALUES (%s, %s, %s)
    ON CONFLICT (codeexperience) DO UPDATE 
    SET role = EXCLUDED.role,
        entreprise = EXCLUDED.entreprise;
    """

    # Insertion des expériences (en tenant compte des champs nécessaires uniquement)
    for exp in experiences:
        values = (
            exp["code_experience"],  # codeexperience
            exp["role"],              # role
            exp["entreprise"],        # entreprise
        )
        cur.execute(insert_query, values)

    # Commit des modifications et fermeture de la connexion
    conn.commit()
    cur.close()
    conn.close()

# Extraction des expériences depuis MongoDB
experiences = extract_experiences_from_mongo()

# Affichage du nombre d'expériences extraites
print(f"Nombre d'expériences extraites: {len(experiences)}")

# Affichage des détails des expériences
for experience in experiences:
    print(f"Code d'Expérience: {experience['code_experience']}")
    print(f"Rôle: {experience['role']}")
    print(f"Entreprise: {experience['entreprise']}")
    print("-------------------------------------------------")

# Insertion dans PostgreSQL
insert_experiences_into_postgres(experiences)

# Insertion ou mise à jour dans PostgreSQL

