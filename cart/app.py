# Cart service
# Endpoints:
#   POST /cart/add    {"user_id":1,"product_id":1,"qty":2}
#   GET  /cart/<user_id>
#   POST /cart/clear  {"user_id":1}
#   GET  /cart/health

import os, time
from flask import Flask, request, jsonify
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
                  CREATE TABLE IF NOT EXISTS carts(
                    user_id INT PRIMARY KEY
                  );
                """)
                cur.execute("""
                  CREATE TABLE IF NOT EXISTS cart_items(
                    id SERIAL PRIMARY KEY,
                    user_id INT NOT NULL,
                    product_id INT NOT NULL,
                    qty INT NOT NULL
                  );
                """)
                c.commit()
                print("cart initialized")
                return
        except Exception as e:
            print("waiting for db2...", e)
            time.sleep(2)
    raise RuntimeError("cart failed to init db2")

@app.route("/cart/health")
def health():
    return jsonify({"status":"ok","service":"cart"})

@app.route("/cart/add", methods=["POST"])
def add():
    data = request.get_json(force=True, silent=True) or {}
    uid, pid, qty = int(data.get("user_id",0)), int(data.get("product_id",0)), int(data.get("qty",1))
    if not (uid and pid):
        return jsonify({"error":"user_id and product_id required"}), 400
    with conn() as c, c.cursor() as cur:
        cur.execute("INSERT INTO carts(user_id) VALUES(%s) ON CONFLICT DO NOTHING;", (uid,))
        cur.execute("""INSERT INTO cart_items(user_id,product_id,qty)
                       VALUES(%s,%s,%s);""", (uid,pid,qty))
        c.commit()
    return jsonify({"message":"added"})

@app.route("/cart/<int:uid>")
def get_cart(uid):
    with conn() as c, c.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""SELECT product_id, qty FROM cart_items
                       WHERE user_id=%s ORDER BY id;""", (uid,))
        items = cur.fetchall()
    return jsonify({"user_id":uid, "items":items})

@app.route("/cart/clear", methods=["POST"])
def clear():
    data = request.get_json(force=True, silent=True) or {}
    uid = int(data.get("user_id",0))
    with conn() as c, c.cursor() as cur:
        cur.execute("DELETE FROM cart_items WHERE user_id=%s;", (uid,))
        c.commit()
    return jsonify({"message":"cleared"})

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5004)
