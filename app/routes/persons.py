from quart import Blueprint, request, jsonify
from app.db import get_conn_ctx
import re
persons_bp = Blueprint("persons", __name__, url_prefix="/persons")

@persons_bp.route("/get-persons-by-dni/<dni>", methods=["GET"])
async def get_persons_by_dni(dni: str):
    # validar: solo dígitos, 1 a 9 (ajustá si necesitás más)
    if not re.fullmatch(r"\d{1,9}", dni):
        return jsonify({"error": "DNI inválido"}), 400

    query = """
        SELECT id, first_name, last_name, phone_number, email, province, city, street
        FROM persons
        WHERE dni = $1;
    """
    async with get_conn_ctx() as conn:
        rows = await conn.fetch(query, dni)  # dni como str

    if not rows:
        return jsonify({"message": "No se encontraron personas con ese DNI"}), 404

    return jsonify([dict(r) for r in rows]), 200