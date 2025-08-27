# Orders service
# Endpoints:
#   GET  /order/health
#   POST /order/place       {"user_id":1,"items":[{"product_id":1,"qty":2}, ...]}
#   GET  /order/<id>

import os, time, json
from uuid import uuid4
from flask import Flask, request, jsonify
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
                  CREATE TABLE IF NOT EXISTS orders(
                    id SERIAL PRIMARY KEY,
                    user_id INT NOT NULL,
                    total NUMERIC(10,2) NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                  );
                """)
                cur.execute("""
                  CREATE TABLE IF NOT EXISTS order_items(
                    id SERIAL PRIMARY KEY,
                    order_id INT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
                    product_id INT NOT NULL,
                    qty INT NOT NULL,
                    unit_price NUMERIC(10,2) NOT NULL
                  );
                """)
                c.commit()
                print("orders initialized")
                return
        except Exception as e:
            print("waiting for db1...", e)
            time.sleep(2)
    raise RuntimeError("orders failed to init db1")

@app.route("/order/health")
def health():
    return jsonify({"status":"ok","service":"orders"})

@app.route("/order/place", methods=["POST"])
def place():
    data = request.get_json(force=True, silent=True) or {}
    items = data.get("items", [])
    user_id = data.get("user_id", 0)

    if not items:
        return jsonify({"error":"items required"}), 400

    # Calculate total using product prices from DB1.products
    with conn() as c, c.cursor() as cur:
        total = 0
        priced = []
        for it in items:
            pid = int(it.get("product_id"))
            qty = int(it.get("qty",1))
            cur.execute("SELECT price,name FROM products WHERE id=%s;", (pid,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error":f"product {pid} not found"}), 404
            price = float(row[0])
            total += price * qty
            priced.append((pid, qty, price))

        cur.execute("INSERT INTO orders(user_id,total) VALUES(%s,%s) RETURNING id;",
                    (user_id, total))
        oid = cur.fetchone()[0]

        for pid, qty, price in priced:
            cur.execute("""INSERT INTO order_items(order_id,product_id,qty,unit_price)
                           VALUES(%s,%s,%s,%s);""", (oid,pid,qty,price))
        c.commit()

    return jsonify({"message":"order placed","order_id":oid,"total":total})

@app.route("/order/<int:oid>")
def get_order(oid):
    with conn() as c, c.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id,user_id,total,created_at FROM orders WHERE id=%s;", (oid,))
        order = cur.fetchone()
        if not order:
            return jsonify({"error":"not found"}), 404
        cur.execute("""SELECT product_id,qty,unit_price
                       FROM order_items WHERE order_id=%s ORDER BY id;""", (oid,))
        order["items"] = cur.fetchall()
        return jsonify(order)

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5002)
