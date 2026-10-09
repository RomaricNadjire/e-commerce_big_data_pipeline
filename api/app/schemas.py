from datetime import datetime
from decimal import Decimal

import pycountry
from pydantic import BaseModel, Field, field_validator

ISO_COUNTRIES = {country.alpha_2 for country in pycountry.countries}


class TransactionCreate(BaseModel):
    customer_id: int = Field(gt=0)
    product_id: int = Field(gt=0)
    quantity: int = Field(gt=0)
    discount_rate: Decimal = Field(default=Decimal("0"), ge=0, le=1)
    country: str = Field(min_length=2, max_length=2)
    payment_method: str = Field(min_length=1, max_length=20)
    
    @field_validator("country")
    @classmethod
    def validate_country(cls, v: str) -> str:
        v = v.upper()
        if v not in ISO_COUNTRIES:
            raise ValueError(f"Unknown ISO-3166 country code: {v}")
        return v


class TransactionEvent(TransactionCreate):
    transaction_id: int
    unit_price: Decimal
    amount: Decimal
    status: str
    timestamp: datetime
    source: str = "realtime"
    
# app/schemas.py
class CustomerOut(BaseModel):
    id: int
    country: str
    segment: str
    signup_date: datetime

class ProductOut(BaseModel):
    id: int
    name: str
    price: Decimal