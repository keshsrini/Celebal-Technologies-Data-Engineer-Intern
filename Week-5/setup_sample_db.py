#!/usr/bin/env python3

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import random

def create_sample_database():
    """Create sample SQLite databases for testing"""
    
    # Create source database
    conn = sqlite3.connect('source.db')
    cursor = conn.cursor()
    
    # Create users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create orders table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            amount REAL NOT NULL,
            order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    ''')
    
    # Create products table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL NOT NULL,
            category TEXT
        )
    ''')
    
    # Insert sample data
    users_data = [
        (1, 'John Doe', 'john@example.com', '2024-01-15'),
        (2, 'Jane Smith', 'jane@example.com', '2024-01-16'),
        (3, 'Bob Johnson', 'bob@example.com', '2024-01-17'),
        (4, 'Alice Brown', 'alice@example.com', '2024-01-18'),
        (5, 'Charlie Wilson', 'charlie@example.com', '2024-01-19')
    ]
    
    cursor.executemany('INSERT OR REPLACE INTO users (id, name, email, created_at) VALUES (?, ?, ?, ?)', users_data)
    
    orders_data = [
        (1, 1, 99.99, '2024-01-20'),
        (2, 2, 149.50, '2024-01-21'),
        (3, 1, 75.25, '2024-01-22'),
        (4, 3, 200.00, '2024-01-23'),
        (5, 2, 50.75, '2024-01-24')
    ]
    
    cursor.executemany('INSERT OR REPLACE INTO orders (order_id, user_id, amount, order_date) VALUES (?, ?, ?, ?)', orders_data)
    
    products_data = [
        (1, 'Laptop', 999.99, 'Electronics'),
        (2, 'Mouse', 29.99, 'Electronics'),
        (3, 'Keyboard', 79.99, 'Electronics'),
        (4, 'Monitor', 299.99, 'Electronics'),
        (5, 'Desk Chair', 199.99, 'Furniture')
    ]
    
    cursor.executemany('INSERT OR REPLACE INTO products (product_id, name, price, category) VALUES (?, ?, ?, ?)', products_data)
    
    conn.commit()
    conn.close()
    
    print("Sample source database created: source.db")
    
    # Create empty target database
    target_conn = sqlite3.connect('target.db')
    target_conn.close()
    print("Empty target database created: target.db")

if __name__ == "__main__":
    create_sample_database()