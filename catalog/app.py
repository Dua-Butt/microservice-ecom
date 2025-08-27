# Catalog service
# Endpoints:
#   GET  /catalog/health
#   GET  /catalog/products
#   GET  /catalog/products/<id>

import os, time
from flask import Flask, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor

DB_HOST = os.getenv("DB1_HOST", "postgres-db1")
DB_NAME = os.getenv("DB1_NAME", "ecom_db1")
DB_USER = os.getenv("DB1_USER", "ecom_user1")
DB_PASS = os.getenv("DB1_PASS", "password1")
DB_PORT = int(os.getenv("DB1_PORT", "5432"))

app = Flask(__name__)

def conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                            user=DB_USER, password=DB_PASS)

def init_db():
    for _ in range(40):
        try:
            with conn() as c, c.cursor() as cur:
                cur.execute("""
                  CREATE TABLE IF NOT EXISTS products(
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    price NUMERIC(10,2) NOT NULL,
                    image TEXT
                  );
                """)
                cur.execute("SELECT COUNT(*) FROM products;")
                if cur.fetchone()[0] == 0:
                    cur.execute("""
                      INSERT INTO products(name,price,image) VALUES
                      ('Men T-Shirt', 1199.00, '🧥'),
                      ('Men Hoodie', 2999.00, '🧶'),
                      ('Kids Hoodie', 2399.00, '🧒'),
                      ('Jogger Pants', 1799.00, '👖'),
                      ('Sneakers', 4499.00, '👟');
                    """)
                c.commit()
                print("catalog initialized")
                return
        except Exception as e:
            print("waiting for db1...", e)
            time.sleep(2)
    raise RuntimeError("catalog failed to init db1")

@app.route("/catalog/health")
def health():
    return jsonify({"status":"ok","service":"catalog"})

@app.route("/catalog/products")
def products():
    with conn() as c, c.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id,name,price,image FROM products ORDER BY id;")
        rows = cur.fetchall()
        for r in rows:
            r["name"] = f'{r["name"]} (v2)'
        return jsonify(rows)


@app.route("/catalog/products/<int:pid>")
def product(pid):
    with conn() as c, c.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id,name,price,image FROM products WHERE id=%s;", (pid,))
        row = cur.fetchone()
        if not row:
            return jsonify({"error":"not found"}), 404
        return jsonify(row)

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5001)