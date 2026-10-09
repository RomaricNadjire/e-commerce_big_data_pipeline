from fastapi import FastAPI

from app.schemas import TransactionCreate, TransactionEvent, CustomerOut, ProductOut
from app.services.transaction_service import create_transaction


app = FastAPI(
    title="E-Commerce Transaction API",
    version="1.0.0",
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post(
    "/api/transactions",
    response_model=TransactionEvent,
    status_code=201,
)
def create_transaction_endpoint(
    transaction: TransactionCreate,
):
    return create_transaction(transaction)

@app.get(
    "/api/customers",
    response_model=list[CustomerOut],
)
def get_customers_endpoint():
    from app.services.transaction_service import get_customers
    return get_customers()

@app.get(
    "/api/products",
    response_model=list[ProductOut],
)
def get_products_endpoint():
    from app.services.transaction_service import get_products
    return get_products()