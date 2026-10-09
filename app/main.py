# streamlit_app.py
import streamlit as st
import requests
import pycountry

API = "http://localhost:8000"

# ---------- helpers ----------
@st.cache_data(ttl=30)
def fetch_list(path: str):
    try:
        r = requests.get(f"{API}{path}", timeout=5)
        r.raise_for_status()
        return r.json(), None
    except requests.RequestException as e:
        return [], str(e)


def refresh_data():
    fetch_list.clear()


# ---------- load reference data ----------
customers, err_c = fetch_list("/api/customers")
products,  err_p = fetch_list("/api/products")

st.title("E-Commerce Transaction API")
st.write("Welcome to the E-Commerce Transaction API interface.")

top_l, top_r = st.columns([6, 1])
with top_r:
    if st.button("🔄 Refresh"):
        refresh_data()
        st.rerun()

if err_c or err_p:
    st.error(f"Backend unavailable: {err_c or err_p}")
    st.stop()

if not customers or not products:
    st.warning("No customers or products available in the database.")
    st.stop()

# ---------- build lookup maps ----------
customer_by_id = {c["id"]: c for c in customers}
product_by_id  = {p["id"]: p for p in products}

def customer_label(cid: int) -> str:
    c = customer_by_id[cid]
    return f"#{cid} · {c['segment']} · {c['country']} · {c['signup_date']}"

def product_label(pid: int) -> str:
    p = product_by_id[pid]
    return f"#{pid} · {p['name']} · {p['price']} €"

country_choices = sorted({c.alpha_2 for c in pycountry.countries})
country_names   = {c.alpha_2: c.name for c in pycountry.countries}


# ---------- form ----------
st.header("Create a Transaction")

customer_id = st.selectbox(
    "Customer",
    options=list(customer_by_id.keys()),
    format_func=customer_label,
)

product_id = st.selectbox(
    "Product",
    options=list(product_by_id.keys()),
    format_func=product_label,
)

# default country from the selected customer
default_country = customer_by_id[customer_id]["country"].upper()
default_idx = country_choices.index(default_country) if default_country in country_choices else 0

country = st.selectbox(
    "Country",
    options=country_choices,
    index=default_idx,
    format_func=lambda cc: f"{cc} — {country_names.get(cc, cc)}",
)

quantity = st.number_input("Quantity", min_value=1, step=1, value=1)

discount_rate = st.number_input(
    "Discount Rate",
    min_value=0.0,
    max_value=1.0,
    step=0.01,
    format="%.2f",
)

payment_method = st.selectbox(
    "Payment Method",
    ["credit_card", "paypal", "bank_transfer"],
)

# ---------- live preview ----------
unit_price = float(product_by_id[product_id]["price"])
preview_amount = round(quantity * unit_price * (1 - discount_rate), 2)

st.caption(
    f"Preview — unit price: **{unit_price:.2f} €** · "
    f"quantity: **{quantity}** · "
    f"discount: **{discount_rate:.0%}** → "
    f"amount: **{preview_amount:.2f} €**"
)


# ---------- submit ----------
if st.button("Create Transaction", type="primary"):
    transaction_data = {
        "customer_id":   int(customer_id),
        "product_id":    int(product_id),
        "quantity":      int(quantity),
        "discount_rate": str(discount_rate),   # avoid float→Decimal artifacts
        "country":       country.upper(),
        "payment_method": payment_method,
    }

    try:
        r = requests.post(
            f"{API}/api/transactions",
            json=transaction_data,
            timeout=5,
        )
        if r.status_code == 201:
            st.success("Transaction created successfully!")
            st.json(r.json())
        elif r.status_code == 422:
            st.error("Validation error from the API:")
            st.json(r.json())
        else:
            st.error(f"Failed ({r.status_code}): {r.text}")
    except requests.RequestException as e:
        st.error(f"Request failed: {e}")
        
        
# run the Streamlit app with  
# streamlit run app/main.py