import pandas as pd
import pytest
from datetime import datetime, timedelta
import random

# ── Helper: generate a small orders DataFrame ──────────────────
def generate_orders(n=100):
    random.seed(42)
    statuses = ['delivered', 'shipped', 'canceled', 'processing']
    orders = []
    for i in range(n):
        purchase = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 364))
        delivered = purchase + timedelta(days=random.randint(3, 30))
        estimated = purchase + timedelta(days=random.randint(7, 25))
        orders.append({
            'order_id': f'ord_{i:05d}',
            'customer_id': f'cust_{random.randint(1, 500):04d}',
            'order_status': random.choice(statuses),
            'order_purchase_timestamp': purchase,
            'order_delivered_timestamp': delivered,
            'order_estimated_delivery_date': estimated,
            'payment_value': round(random.uniform(10, 5000), 2)
        })
    return pd.DataFrame(orders)

# ── Helper: transform orders ───────────────────────────────────
def transform_orders(df):
    df = df.copy()
    df['delivery_days'] = (
        df['order_delivered_timestamp'] - df['order_purchase_timestamp']
    ).dt.days
    df['is_late'] = df['order_delivered_timestamp'] > df['order_estimated_delivery_date']
    df['order_month'] = df['order_purchase_timestamp'].dt.month
    df['order_year'] = df['order_purchase_timestamp'].dt.year

    def categorize(val):
        if val < 100: return 'low'
        elif val < 500: return 'medium'
        else: return 'high'

    df['order_value_category'] = df['payment_value'].apply(categorize)
    return df

# ── Tests ──────────────────────────────────────────────────────
def test_order_count():
    df = generate_orders(100)
    assert len(df) == 100, "Should generate exactly 100 orders"

def test_no_null_order_ids():
    df = generate_orders(100)
    assert df['order_id'].isnull().sum() == 0, "No null order IDs allowed"

def test_no_duplicate_order_ids():
    df = generate_orders(100)
    assert df['order_id'].duplicated().sum() == 0, "No duplicate order IDs allowed"

def test_valid_order_statuses():
    df = generate_orders(100)
    valid = ['delivered', 'shipped', 'canceled', 'processing']
    assert df['order_status'].isin(valid).all(), "All statuses must be valid"

def test_payment_values_positive():
    df = generate_orders(100)
    assert (df['payment_value'] > 0).all(), "All payment values must be positive"

def test_delivery_days_calculated():
    df = generate_orders(100)
    df = transform_orders(df)
    assert 'delivery_days' in df.columns, "delivery_days column must exist"
    assert (df['delivery_days'] >= 0).all(), "Delivery days must be non-negative"

def test_is_late_flag():
    df = generate_orders(100)
    df = transform_orders(df)
    assert 'is_late' in df.columns, "is_late column must exist"
    assert df['is_late'].dtype == bool, "is_late must be boolean"

def test_order_value_categories():
    df = generate_orders(100)
    df = transform_orders(df)
    valid_categories = ['low', 'medium', 'high']
    assert df['order_value_category'].isin(valid_categories).all(), "Invalid category found"

def test_month_year_extracted():
    df = generate_orders(100)
    df = transform_orders(df)
    assert df['order_month'].between(1, 12).all(), "Month must be 1-12"
    assert (df['order_year'] == 2024).all(), "Year must be 2024"
