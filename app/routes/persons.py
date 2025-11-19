from quart import Blueprint, request, jsonify
from app.db import get_conn_ctx
import re
persons_bp = Blueprint("persons", __name__, url_prefix="/persons")

@persons_bp.route("/get-persons-by-dni-or-cuit/<identifier>", methods=["GET"])
async def get_persons_by_dni_or_cuit(identifier: str):
    # Validar: solo dígitos, exactamente 9 para DNI o 11 para CUIT
    if not re.fullmatch(r"\d+", identifier):
        return jsonify({"error": "Identificador inválido. Debe contener solo dígitos"}), 400
    
    # Determinar si es DNI o CUIT según la longitud
    is_dni = len(identifier) < 11
    is_cuit = len(identifier) >= 11
    
    if not (is_dni or is_cuit):
        return jsonify({"error": "Identificador inválido. Debe ser DNI (9 dígitos) o CUIT (11 dígitos)"}), 400

    # Construir la query según el tipo de identificador
    if is_dni:
        query = """
            SELECT id, first_name, last_name, phone_number, email, province, city, street, razon_social, cuit
            FROM persons
            WHERE dni = $1;
        """
    else:  # is_cuit
        query = """
            SELECT id, first_name, last_name, phone_number, email, province, city, street, razon_social, cuit
            FROM persons
            WHERE cuit = $1;
        """
    
    async with get_conn_ctx() as conn:
        rows = await conn.fetch(query, identifier)
    print(f"rows={rows}")
    if not rows:
        identifier_type = "DNI" if is_dni else "CUIT"
        return jsonify({"message": f"No se encontraron personas con ese {identifier_type}"}), 404

    return jsonify([dict(r) for r in rows]), 200