from quart import Blueprint, request, jsonify, g
from app.db import get_conn_ctx

inspection_validity_bp = Blueprint("inspection_validity", __name__)

# Conjunto de códigos de uso permitidos (alineado con el front)
USAGE_CODES = {
    "A",  # Oficial
    "B",  # Diplomático, Consular u Org. Internacional
    "C",  # Particular
    "D",  # Alquiler / Taxi / Remis
    "E",  # Transporte público de pasajeros
    "E1", # Servicio internacional; larga distancia/urbanos M1-M3
    "E2", # Inter/Jurisdiccional; regulares/turismo M1-M3
    "F",  # Transporte escolar
    "G",  # Cargas / servicios / trabajos vía pública
    "H",  # Emergencia/seguridad/fúnebres/remolque/maquinaria
}


def _parse_int_or_none(v):
    if v is None:
        return None
    try:
        x = int(v)
        return x if 0 <= x <= 120 else None
    except Exception:
        return None


@inspection_validity_bp.route(
    "/<string:province_code>/<string:localidad_key>", methods=["GET"]
)
async def get_inspection_validity(province_code: str, localidad_key: str):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        rows = await conn.fetch(
            """
            SELECT usage_code,
                   up_to_36_months,
                   from_3_to_7_years,
                   over_7_years
            FROM inspection_validity_rules
            WHERE province_code = $1
              AND localidad_key = $2
            """,
            province_code,
            localidad_key,
        )

    out = {
        r["usage_code"]: {
            "up_to_36": r["up_to_36_months"],
            "from_3_to_7": r["from_3_to_7_years"],
            "over_7": r["over_7_years"],
        }
        for r in rows
    }
    return jsonify(out), 200


@inspection_validity_bp.route(
    "/<string:province_code>/<string:localidad_key>", methods=["PUT"]
)
async def upsert_inspection_validity(province_code: str, localidad_key: str):
    """
    Guarda/actualiza la validez para una localidad específica.

    Body esperado (ejemplo):
    {
      "data": {
        "A": { "up_to_36": 12, "from_3_to_7": 6, "over_7": 3 },
        "B": { "up_to_36": 12, "from_3_to_7": 6, "over_7": 3 },
        ...
      }
    }

    - Los valores deben ser enteros entre 0 y 120 o null (para limpiar el valor).
    - Los códigos de uso deben estar dentro de USAGE_CODES.
    - Si falta alguna key de uso, simplemente no se modifica.
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    payload = await request.get_json() or {}
    data = payload.get("data") or {}
    if not isinstance(data, dict):
        return jsonify({"error": "Formato inválido: se espera 'data' como objeto"}), 400

    # normalización y validación
    normalized = {}
    for usage_code, values in data.items():
        if usage_code not in USAGE_CODES:
            return jsonify({"error": f"usage_code inválido: {usage_code}"}), 400
        if not isinstance(values, dict):
            return jsonify({"error": f"Valores inválidos para {usage_code}"}), 400

        up36 = _parse_int_or_none(values.get("up_to_36"))
        a3_7 = _parse_int_or_none(values.get("from_3_to_7"))
        o7 = _parse_int_or_none(values.get("over_7"))
        # Permitimos None explícito para limpiar el valor
        if values.get("up_to_36") is not None and up36 is None:
            return jsonify({"error": f"up_to_36 inválido en {usage_code}"}), 400
        if values.get("from_3_to_7") is not None and a3_7 is None:
            return jsonify({"error": f"from_3_to_7 inválido en {usage_code}"}), 400
        if values.get("over_7") is not None and o7 is None:
            return jsonify({"error": f"over_7 inválido en {usage_code}"}), 400

        normalized[usage_code] = (up36, a3_7, o7)

    if not normalized:
        return jsonify({"message": "Sin cambios"}), 200

    async with get_conn_ctx() as conn:
        async with conn.transaction():
            for usage_code, (up36, a3_7, o7) in normalized.items():
                await conn.execute(
                    """
                    INSERT INTO inspection_validity_rules (
                        province_code, localidad_key, usage_code,
                        up_to_36_months, from_3_to_7_years, over_7_years,
                        updated_by_user_id
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7::uuid)
                    ON CONFLICT (province_code, localidad_key, usage_code)
                    DO UPDATE SET
                        up_to_36_months   = EXCLUDED.up_to_36_months,
                        from_3_to_7_years = EXCLUDED.from_3_to_7_years,
                        over_7_years      = EXCLUDED.over_7_years,
                        updated_by_user_id = EXCLUDED.updated_by_user_id,
                        updated_at         = NOW()
                    """,
                    province_code,
                    localidad_key,
                    usage_code,
                    up36,
                    a3_7,
                    o7,
                    user_id,
                )

    return jsonify({"message": "Validez guardada"}), 200


@inspection_validity_bp.route(
    "/<string:province_code>/bulk", methods=["POST"]
)
async def bulk_apply_inspection_validity(province_code: str):
    """
    Aplica valores por lote a múltiples localidades dentro de una provincia.

    Body esperado:
    {
      "localidad_keys": ["06028010", "06042005", ...],
      "values": { "up_to_36": 12, "from_3_to_7": 6, "over_7": 3 }
    }

    - Sólo los campos presentes en "values" se aplican; los ausentes NO se modifican.
    - Se aplica a TODOS los usage_code definidos en USAGE_CODES.
    - Si no existe fila, se inserta; si existe, se actualiza manteniendo los campos no provistos.
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    payload = await request.get_json() or {}
    loc_keys = payload.get("localidad_keys") or []
    values = payload.get("values") or {}
    usage_codes = payload.get("usage_codes")

    if not isinstance(loc_keys, list) or not all(isinstance(x, str) for x in loc_keys):
        return jsonify({"error": "localidad_keys debe ser string[]"}), 400
    if not loc_keys:
        return jsonify({"error": "Debe indicar al menos una localidad"}), 400

    # Validación de usage_codes opcional: debe ser subset de USAGE_CODES
    if usage_codes is not None:
        if not isinstance(usage_codes, list) or not usage_codes or not all(isinstance(x, str) for x in usage_codes):
            return jsonify({"error": "usage_codes debe ser string[] no vacío"}), 400
        invalid = [x for x in usage_codes if x not in USAGE_CODES]
        if invalid:
            return jsonify({"error": f"usage_codes inválidos: {', '.join(invalid)}"}), 400
        codes_to_apply = set(usage_codes)
    else:
        codes_to_apply = USAGE_CODES

    # Solo aplicamos los campos presentes; los ausentes se dejan como None para usar COALESCE en UPDATE
    apply_up36 = "up_to_36" in values
    apply_a3_7 = "from_3_to_7" in values
    apply_o7 = "over_7" in values

    if not (apply_up36 or apply_a3_7 or apply_o7):
        return jsonify({"error": "No hay campos para aplicar en 'values'"}), 400

    up36_v = _parse_int_or_none(values.get("up_to_36")) if apply_up36 else None
    a3_7_v = _parse_int_or_none(values.get("from_3_to_7")) if apply_a3_7 else None
    o7_v = _parse_int_or_none(values.get("over_7")) if apply_o7 else None

    if apply_up36 and up36_v is None:
        return jsonify({"error": "up_to_36 inválido"}), 400
    if apply_a3_7 and a3_7_v is None:
        return jsonify({"error": "from_3_to_7 inválido"}), 400
    if apply_o7 and o7_v is None:
        return jsonify({"error": "over_7 inválido"}), 400

    async with get_conn_ctx() as conn:
        async with conn.transaction():
            # Optimización: procesar en lotes para evitar que la transacción se quede trabada
            # Usamos lotes de 100 registros para balancear rendimiento y uso de memoria
            BATCH_SIZE = 100
            all_pairs = [(lk, uc) for lk in loc_keys for uc in codes_to_apply]
            
            for batch_start in range(0, len(all_pairs), BATCH_SIZE):
                batch = all_pairs[batch_start:batch_start + BATCH_SIZE]
                
                # Construir query con múltiples VALUES para inserción masiva
                # Esto es mucho más eficiente que ejecutar una query por cada combinación
                values_list = []
                params_list = []
                param_num = 1
                
                for lk, usage_code in batch:
                    values_list.append(
                        f"(${param_num}, ${param_num+1}, ${param_num+2}, ${param_num+3}, ${param_num+4}, ${param_num+5}, ${param_num+6}::uuid)"
                    )
                    params_list.extend([
                        province_code,
                        lk,
                        usage_code,
                        up36_v if apply_up36 else None,
                        a3_7_v if apply_a3_7 else None,
                        o7_v if apply_o7 else None,
                        user_id,
                    ])
                    param_num += 7
                
                values_clause = ", ".join(values_list)
                
                await conn.execute(
                    f"""
                    INSERT INTO inspection_validity_rules (
                        province_code, localidad_key, usage_code,
                        up_to_36_months, from_3_to_7_years, over_7_years,
                        updated_by_user_id
                    ) VALUES {values_clause}
                    ON CONFLICT (province_code, localidad_key, usage_code)
                    DO UPDATE SET
                        up_to_36_months   = COALESCE(EXCLUDED.up_to_36_months, inspection_validity_rules.up_to_36_months),
                        from_3_to_7_years = COALESCE(EXCLUDED.from_3_to_7_years, inspection_validity_rules.from_3_to_7_years),
                        over_7_years      = COALESCE(EXCLUDED.over_7_years,      inspection_validity_rules.over_7_years),
                        updated_by_user_id = EXCLUDED.updated_by_user_id,
                        updated_at         = NOW()
                    """,
                    *params_list,
                )

    return jsonify({"message": f"Aplicado a {len(loc_keys)} localidad(es)"}), 200


