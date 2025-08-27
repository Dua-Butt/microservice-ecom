# Users service (very basic)
# Endpoints:
#   POST /user/register  {"name":"A","email":"a@x.com","password":"123"}
#   POST /user/login     {"email":"a@x.com","password":"123"}  -> {"token": "...", "user_id": 1}
#   GET  /user/health

import os, time, uuid
from flask import Flask, request, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
import psycopg2
from psycopg2.extras import RealDictCursor

DB_HOST = os.getenv("DB2_HOST", "postgres-db2")
DB_NAME = os.getenv("DB2_NAME", "ecom_db2")
DB_USER = os.getenv("DB2_USER", "ecom_user2")
DB_PASS = os.getenv("DB2_PASS", "password2")
DB_PORT = int(os.getenv("DB2_PORT", "5432"))

app = Flask(__name__)

def conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                            user=DB_USER, password=DB_PASS)

def init_db():
    for _ in range(40):
        try:
            with conn() as c, c.cursor() as cur:
                cur.execute("""
                  CREATE TABLE IF NOT EXISTS users(
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL
                  );
                """)
                cur.execute("""
                  CREATE TABLE IF NOT EXISTS tokens(
                    token TEXT PRIMARY KEY,
                    user_id INT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                  );
                """)
                c.commit()
                print("users initialized")
                return
        except Exception as e:
            print("waiting for db2...", e)
            time.sleep(2)
    raise RuntimeError("users failed to init db2")

@app.route("/user/health")
def health():
    return jsonify({"status":"ok","service":"users"})

@app.route("/user/register", methods=["POST"])
def register():
    data = request.get_json(force=True, silent=True) or {}
    name, email, pwd = data.get("name"), data.get("email"), data.get("password")
    if not all([name, email, pwd]):
        return jsonify({"error":"name, email, password required"}), 400
    with conn() as c, c.cursor(cursor_factory=RealDictCursor) as cur:
        try:
            cur.execute("""INSERT INTO users(name,email,password_hash)
                           VALUES(%s,%s,%s) RETURNING id;""",
                           (name, email, generate_password_hash(pwd)))
            uid = cur.fetchone()["id"]
            c.commit()
            return jsonify({"message":"registered", "user_id":uid})
        except psycopg2.Error as e:
            return jsonify({"error":"email exists?"}), 409

@app.route("/user/login", methods=["POST"])
def login():
    data = request.get_json(force=True, silent=True) or {}
    email, pwd = data.get("email"), data.get("password")
    with conn() as c, c.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id,password_hash FROM users WHERE email=%s;", (email,))
        row = cur.fetchone()
        if not row or not check_password_hash(row["password_hash"], pwd or ""):
            return jsonify({"error":"invalid credentials"}), 401
        token = str(uuid.uuid4())
        cur.execute("INSERT INTO tokens(token,user_id) VALUES(%s,%s);", (token,row["id"]))
        c.commit()
    return jsonify({"token":token, "user_id":row["id"]})

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5003)
