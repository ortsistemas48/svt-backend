# app/pricing.py
"""Única fuente de verdad para zonas y precios de las órdenes de pago.

El frontend NO debe enviar precios: el backend calcula zona, precio unitario
y total a partir de la provincia del taller y la cantidad pedida.
"""
from decimal import Decimal

ZONE_SUR = "SUR"
ZONE_CENTRO = "CENTRO"
ZONE_NORTE = "NORTE"
DEFAULT_ZONE = ZONE_CENTRO

# Precio unitario por revisión, en ARS
UNIT_PRICE_BY_ZONE = {
    ZONE_SUR: Decimal("3500"),
    ZONE_CENTRO: Decimal("1500"),
    ZONE_NORTE: Decimal("1500"),
}

ZONE_BY_PROVINCE = {
    "Santa Cruz": ZONE_SUR,
    "Tierra del Fuego": ZONE_SUR,
    "Chubut": ZONE_SUR,
    "Neuquén": ZONE_SUR,
    "Río Negro": ZONE_SUR,
    "Buenos Aires": ZONE_SUR,
    "CABA": ZONE_SUR,
    "La Pampa": ZONE_CENTRO,
    "Santa Fe": ZONE_CENTRO,
    "Córdoba": ZONE_CENTRO,
    "Mendoza": ZONE_CENTRO,
    "San Luis": ZONE_CENTRO,
    "Entre Ríos": ZONE_CENTRO,
    "La Rioja": ZONE_CENTRO,
    "San Juan": ZONE_CENTRO,
    "Salta": ZONE_NORTE,
    "Jujuy": ZONE_NORTE,
    "Catamarca": ZONE_NORTE,
    "Chaco": ZONE_NORTE,
    "Formosa": ZONE_NORTE,
    "Corrientes": ZONE_NORTE,
    "Misiones": ZONE_NORTE,
    "Santiago del Estero": ZONE_NORTE,
    "Tucumán": ZONE_NORTE,
}

MIN_QUANTITY = 250
MAX_QUANTITY = 20000


def zone_for_province(province: str | None) -> str:
    return ZONE_BY_PROVINCE.get((province or "").strip(), DEFAULT_ZONE)


def unit_price_for_zone(zone: str) -> Decimal:
    return UNIT_PRICE_BY_ZONE[zone]
