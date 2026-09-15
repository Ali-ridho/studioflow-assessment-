import os 
from dotenv import load_dotenv
import psycopg2

# Load variabel dari file .env ke environment
load_dotenv()

def get_connection():
    """Membuka koneksi baru ke PostgreSQL menggunakan credential dari .env"""
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )
