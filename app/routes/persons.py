from quart import Blueprint, request, jsonify
from app.db import get_conn_ctx
import re

persons_bp = Blueprint("persons", __name__, url_prefix="/persons")

VALID_DOC_TYPES = {"DNI", "CUIT", "PAS"}

@persons_bp.route("/get-persons-by-dni-or-cuit/<identifier>", methods=["GET"])
async def get_persons_by_dni_or_cuit(identifier: str):
    doc_type = request.args.get("doc_type", "").upper()

    if not doc_type:
        return jsonify({"error": "Se debe proporcionar el parámetro doc_type (DNI, CUIT o PAS)"}), 400

    if doc_type not in VALID_DOC_TYPES:
        return jsonify({"error": f"doc_type inválido. Valores permitidos: {', '.join(VALID_DOC_TYPES)}"}), 400

    if not identifier:
        return jsonify({"error": "Identificador vacío"}), 400

    if doc_type in ("DNI", "CUIT"):
        if not re.fullmatch(r"\d+", identifier):
            return jsonify({"error": "Para DNI o CUIT el identificador debe contener solo dígitos"}), 400
        if doc_type == "DNI" and len(identifier) > 9:
            return jsonify({"error": "El DNI debe tener hasta 9 dígitos"}), 400
        if doc_type == "CUIT" and len(identifier) != 11:
            return jsonify({"error": "El CUIT debe tener exactamente 11 dígitos"}), 400

    if doc_type == "DNI":
        col = "dni"
    elif doc_type == "CUIT":
        col = "cuit"
    else:
        col = "passport_number"

    query = f"""
        SELECT id, first_name, last_name, phone_number, email, province, city, street,
               razon_social, cuit, dni, passport_number
        FROM persons
        WHERE {col} = $1;
    """

    async with get_conn_ctx() as conn:
        rows = await conn.fetch(query, identifier)

    if not rows:
        label = {"DNI": "DNI", "CUIT": "CUIT", "PAS": "Pasaporte"}[doc_type]
        return jsonify({"message": f"No se encontraron personas con ese {label}"}), 404

    return jsonify([dict(r) for r in rows]), 200
