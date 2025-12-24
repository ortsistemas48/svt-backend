from quart import Blueprint, request, jsonify, g
from app.db import get_conn_ctx
from asyncpg.exceptions import UniqueViolationError
from uuid import UUID
import json
from app.email import send_workshop_pending_email, send_workshop_approved_email, send_workshop_suspended_email, send_admin_workshop_registered_email
import logging
import os
import asyncio
import re

log = logging.getLogger(__name__)

workshops_bp = Blueprint("workshops", __name__, url_prefix="/workshops")

FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")

OWNER_ROLE_ID = 2
ENGINEER_ROLE_ID = 3
VALID_PROVINCES = {
    "Buenos Aires","CABA","Catamarca","Chaco","Chubut","Córdoba","Corrientes",
    "Entre Ríos","Formosa","Jujuy","La Pampa","La Rioja","Mendoza","Misiones",
    "Neuquén","Río Negro","Salta","San Juan","San Luis","Santa Cruz",
    "Santa Fe","Santiago del Estero","Tierra del Fuego","Tucumán"
}

DEFAULT_TREE = {
    "Luces reglamentarias": {
        "Luces no reglamentarias": [],
        "Falta luz de freno": ["TD", "TI"],
        "Luces de giro fijas": [],
        "Falta luz marcha atrás": ["TD", "TI"],
        "Retirar agregado de luces no reglamentarias": [],
        "Ajustar óptica": ["DD", "DI", "TD", "TI"],
        "Reparar o reemplazar óptica": ["DD", "DI", "TD", "TI"],
    },

    "Sistema de dirección": {
        "Reemplazar extremo de dirección": ["DD", "DI"],
        "Reemplazar precap": ["DD", "DI"],
        "Reemplazar o ajustar fuelle de precap": ["DD", "DI"],
        "Realizar alineado delantero": [],
        "Pérdida de líquido hidráulico": [],
        "Reemplazar buje de caja de dirección": [],
    },

    "Frenos": {
        "Ajustar frenos delanteros": [],
        "Ajustar frenos traseros": [],
        "Ajustar freno de mano": [],
        "Diferencia de freno delantero": [],
        "Diferencia de freno trasero": [],
        "Diferencia de freno de mano": [],
        "Pérdida de líquido de freno en zona": ["DD", "DI", "TD", "TI"],
        "Reemplazar todas las cañerías de freno": [],
        "Reemplazar cañerías de freno traseras": [],
        "Rectificar discos de freno": ["DD", "DI"],
    },

    "Sistema de suspensión": {
        "Reemplazar amortiguador": ["DD", "DI", "TD", "TI"],
        "Reemplazar cazoleta": ["DD", "DI"],
        "Reemplazar espiral": ["DD", "DI", "TD", "TI"],
        "Reemplazar elástico": [],
        "Ajustar anclaje de amortiguador": [],
        "Reemplazar bujes de parrilla": ["DD", "DI"],
        "Reemplazar rótula": ["DD", "DI"],
        "Reemplazar rótula superior": ["DD", "DI"],
        "Reemplazar bujes de puente trasero": ["TD", "TI"],
        "Reemplazar bujes de barra de torsión": [],
        "Reemplazar bieleta": ["DD", "DI", "TD", "TI"],
    },

    "Bastidor y chasis": {
        "Reparar zócalo": ["LD", "LI"],
        "Reparar guardabarro": ["DD", "DI", "TD", "TI"],
        "Retirar gancho de remolque": [],
    },

    "Llantas": {
        "Reemplazar llanta": ["DD", "DI", "TD", "TI"],
        "Falta bulón en rueda": ["DD", "DI", "TD", "TI"],
        "Reemplazar o ajustar rodamiento": ["DD", "DI", "TD", "TI"],
    },

    "Neumáticos": {
        "Reemplazar cubierta": ["DD", "DI", "TD", "TI"],
        "Neumático no reglamentario": ["DD", "DI", "TD", "TI"],
    },

    "Carrocería": {
        "Reemplazar parabrisas": [],
        "Ajustar correctamente espejo retrovisor": ["LD", "LI"],
        "Reemplazar espejo retrovisor": ["LD", "LI"],
        "Falta bocina": [],
        "Ajustar capot": [],
        "Ajustar paragolpe delantero": [],
        "Ajustar paragolpe trasero": [],
        "Reparar apertura de puerta": ["DD", "DI", "TD", "TI"],
        "Reparar apertura de ventanilla": ["DD", "DI", "TD", "TI"],
        "Sujetar correctamente portaequipaje de techo": [],
        "Retirar butaca no reglamentaria": [],
        "Faltan apoyacabezas": [],
        "Falta cinturón de seguridad": [],
        "Ajustar butaca": [],
    },

    "Accesorios reglamentarios": {
        "Faltan elementos de seguridad": [],
        "Falta matafuego": [],
        "Matafuego vencido": [],
        "Pedir duplicado de patentes": [],
    },

    "Gases": {
        "Exceso de gases": [],
        "Exceso de opacidad de gases": [],
    },

    "Ruidos": {
        "Reparar escape pinchado": [],
        "Reparar o reemplazar precámara de escape": [],
        "Reparar o reemplazar silenciador de escape": [],
        "Reparar o reemplazar flexible de escape": [],
        "Colocar sujeción de escape faltante": [],
        "Exceso de ruidos": [],
        "Colocar silenciador de escape": [],
    },

    "Otros elementos": {
        "Reemplazar homocinética": ["LCD", "LCI", "LRD", "LRI"],
        "Reemplazar fuelle de homocinética": ["LCD", "LCI", "LRD", "LRI"],
        "Ajustar cardan": [],
        "Reemplazar taco de motor": [],
        "Pérdida de aceite por motor": [],
        "Pérdida de líquido refrigerante": [],
    },
}

# DEFAULT_TREE = {
#     "Luces reglamentarias": {
#         "Luces no reglamentarias": [
#             "Adicional techo",
#             "Barra LED frontal",
#             "Faros auxiliares no permitidos",
#         ],
#         "Faltan luces de frenos": [
#             "Stop izquierdo",
#             "Stop derecho",
#             "Stop central",
#         ],
#         "Luces de giro fijas": [
#             "Giro delantero izquierdo fijo",
#             "Giro delantero derecho fijo",
#             "Giro trasero izquierdo fijo",
#             "Giro trasero derecho fijo",
#         ],
#         "Falta luz marcha atrás": [
#             "Marcha atrás izquierda",
#             "Marcha atrás derecha",
#         ],
#         "Retirar agregado de luces no reglamentarias": [
#             "Retirar faro auxiliar agregado",
#             "Retirar barra LED adicional",
#         ],
#         "Ajustar óptica": [
#             "Óptica delantera izquierda desalineada",
#             "Óptica delantera derecha desalineada",
#             "Óptica trasera desalineada",
#         ],
#         "Reparar / reemplazar óptica": [
#             "Mica rota",
#             "Soporte óptica suelto",
#             "Óptica quemada",
#         ],
#     },
#     "Sistema de dirección": {
#         "Reemplazar extremo de dirección": [
#             "Extremo izquierdo con juego",
#             "Extremo derecho con juego",
#         ],
#         "Reemplazar precap": [
#             "Precap lado izquierdo",
#             "Precap lado derecho",
#         ],
#         "Reemplazar / ajustar fuelle de precap": [
#             "Fuelle izquierdo dañado",
#             "Fuelle derecho dañado",
#         ],
#         "Realizar alineado delantero": [
#             "Eje delantero desalineado",
#         ],
#         "Pérdida de líquido hidráulico": [
#             "Caja de dirección con pérdida",
#             "Manguera hidráulica con pérdida",
#         ],
#     },
#     "Frenos": {
#         "Ajustar frenos delanteros": [
#             "Delantero izquierdo",
#             "Delantero derecho",
#         ],
#         "Ajustar frenos traseros": [
#             "Trasero izquierdo",
#             "Trasero derecho",
#         ],
#         "Ajustar freno de mano": [
#             "Palanca freno de mano",
#             "Cables tensores flojos",
#         ],
#         "Diferencia de freno delantero": [
#             "Izquierdo frena más que derecho",
#             "Derecho frena más que izquierdo",
#         ],
#         "Diferencia de freno trasero": [
#             "Izquierdo frena más que derecho",
#             "Derecho frena más que izquierdo",
#         ],
#         "Diferencia de freno de mano": [
#             "Lado izquierdo bajo",
#             "Lado derecho bajo",
#         ],
#         "Pérdida de líquido de freno en zona": [
#             "Bomba de freno con pérdida",
#             "Cilindro de rueda con pérdida",
#             "Flexible de freno fisurado",
#         ],
#         "Reemplazar cañerías de freno": [
#             "Cañería corroída",
#             "Flexible cuarteado",
#         ],
#         "Rectificar discos de freno": [
#             "Disco delantero izquierdo",
#             "Disco delantero derecho",
#         ],
#     },
#     "Sistema de suspensión": {
#         "Reemplazar amortiguador": [
#             "Amortiguador delantero izquierdo con pérdida",
#             "Amortiguador delantero derecho con pérdida",
#             "Amortiguador trasero con fuga",
#         ],
#         "Reemplazar cazoletas": [
#             "Cazoleta superior izquierda",
#             "Cazoleta superior derecha",
#         ],
#         "Reemplazar espiral": [
#             "Espiral delantero fatigado",
#             "Espiral trasero fatigado",
#         ],
#         "Reemplazar elástico": [
#             "Elástico trasero izquierdo",
#             "Elástico trasero derecho",
#         ],
#         "Ajustar anclajes de amortiguadores": [
#             "Tornillería floja delantera",
#             "Tornillería floja trasera",
#         ],
#         "Reemplazar bujes de parrilla": [
#             "Parrilla inferior izquierda con juego",
#             "Parrilla inferior derecha con juego",
#         ],
#         "Reemplazar rótula": [
#             "Rótula inferior izquierda",
#             "Rótula inferior derecha",
#         ],
#         "Reemplazar bujes de puente trasero": [
#             "Buje lado izquierdo desgastado",
#             "Buje lado derecho desgastado",
#         ],
#         "Reemplazar bujes de barra de torsión": [
#             "Buje barra estabilizadora izq.",
#             "Buje barra estabilizadora der.",
#         ],
#         "Reemplazar bieleta": [
#             "Bieleta izquierda con juego",
#             "Bieleta derecha con juego",
#         ],
#     },
#     "Bastidor y chasis": {
#         "Reparar zócalo": [
#             "Zócalo lateral izquierdo dañado",
#             "Zócalo lateral derecho dañado",
#         ],
#         "Reparar guardabarro": [
#             "Guardabarro delantero golpeado",
#             "Guardabarro trasero golpeado",
#         ],
#         "Retirar gancho de remolque": [
#             "Gancho delantero no reglamentario",
#             "Gancho trasero no reglamentario",
#         ],
#     },
#     "Llantas": {
#         "Reemplazar llanta": [
#             "Llanta delantera izquierda ovalada",
#             "Llanta delantera derecha golpeada",
#             "Llanta trasera deformada",
#         ],
#         "Falta bulón en rueda": [
#             "Rueda delantera izquierda",
#             "Rueda delantera derecha",
#             "Rueda trasera sin bulón",
#         ],
#         "Reemplazar rótula": [
#             "Rótula rueda del. izquierda con juego",
#             "Rótula rueda del. derecha con juego",
#         ],
#         "Reemplazar / ajustar rodamiento": [
#             "Rodamiento delantero izq. con ruido",
#             "Rodamiento delantero der. con juego",
#             "Rodamiento trasero con juego",
#         ],
#     },
#     "Neumáticos": {
#         "Reemplazar cubierta": [
#             "Cubierta delantera izquierda lisa",
#             "Cubierta delantera derecha lisa",
#             "Cubierta trasera desgastada",
#         ],
#         "Neumático no reglamentario": [
#             "Medida no permitida",
#             "Desgaste excesivo banda",
#         ],
#     },
#     "Carrocería": {
#         "Reemplazar parabrisas": [
#             "Parabrisas astillado",
#             "Parabrisas rajado en zona de visión",
#         ],
#         "Ajustar correctamente espejo retrovisor": [
#             "Espejo interior suelto",
#             "Espejo exterior flojo",
#         ],
#         "Reemplazar espejo retrovisor": [
#             "Espejo izquierdo faltante",
#             "Espejo derecho dañado",
#         ],
#         "Falta bocina": [
#             "Bocina inoperativa",
#             "Cableado bocina dañado",
#         ],
#         "Ajustar capot": [
#             "Cierre de capot desajustado",
#             "Bisagra capot floja",
#         ],
#         "Ajustar paragolpe delantero": [
#             "Paragolpe delantero suelto lado izq.",
#             "Paragolpe delantero suelto lado der.",
#         ],
#         "Ajustar paragolpe trasero": [
#             "Paragolpe trasero suelto lado izq.",
#             "Paragolpe trasero suelto lado der.",
#         ],
#         "Reparar apertura de puerta": [
#             "Puerta conductor no abre/cierra bien",
#             "Puerta acompañante no abre/cierra bien",
#             "Puerta trasera con traba",
#         ],
#         "Reparar apertura de ventanilla": [
#             "Levantavidrios conductor falla",
#             "Levantavidrios acompañante falla",
#         ],
#         "Sujetar correctamente portaequipaje de techo": [
#             "Portaequipaje flojo",
#             "Abrazaderas sin fijación",
#         ],
#         "Retirar butacas": [
#             "Butaca adicional trasera",
#             "Butaca no original",
#         ],
#         "Faltan apoyacabezas": [
#             "Apoyacabezas delantero faltante",
#             "Apoyacabezas trasero faltante",
#         ],
#         "Falta cinturón de seguridad": [
#             "Cinturón conductor faltante",
#             "Cinturón acompañante faltante",
#             "Cinturón trasero faltante",
#         ],
#         "Ajustar butaca": [
#             "Guías de butaca flojas",
#             "Anclaje de butaca flojo",
#         ],
#     },
#     "Accesorios reglamentarios": {
#         "Faltan elementos de seguridad": [
#             "Balizas faltantes",
#             "Botiquín incompleto",
#         ],
#         "Falta matafuego": [
#             "Sin matafuego a bordo",
#         ],
#         "Matafuego vencido": [
#             "Sin carga",
#             "Manómetro en rojo",
#         ],
#         "Pedir duplicado de patentes": [
#             "Patente delantera ilegible",
#             "Patente trasera ilegible",
#         ],
#     },
#     "Gases": {
#         "Exceso de gases": [
#             "CO/HC fuera de rango",
#         ],
#         "Exceso de opacidad de gases": [
#             "Humo negro excesivo",
#             "Humo azul excesivo",
#         ],
#     },
#     "Ruidos": {
#         "Reparar escape pinchado en zona": [
#             "Tramo intermedio pinchado",
#             "Tramo trasero pinchado",
#         ],
#         "Reparar / reemplazar precámara de escape": [
#             "Precámara fisurada",
#         ],
#         "Reparar / reemplazar silenciador de escape": [
#             "Silenciador trasero roto",
#         ],
#         "Reparar / reemplazar flexible de escape": [
#             "Flexible fisurado",
#         ],
#         "Colocar sujeción de escape en zona": [
#             "Abrazadera de escape floja",
#             "Soporte escape cortado",
#         ],
#         "Exceso de ruidos": [
#             "Escape libre / ruidoso",
#         ],
#         "Colocar silenciador de escape": [
#             "Falta silenciador final",
#         ],
#     },
#     "Otros elementos": {
#         "Reemplazar homocinética": [
#             "Homocinética lado izquierdo con juego",
#             "Homocinética lado derecho con juego",
#         ],
#         "Reemplazar fuelle de homocinética": [
#             "Fuelle lado izquierdo roto",
#             "Fuelle lado derecho roto",
#         ],
#         "Ajustar cardan": [
#             "Cruz cardan con juego",
#             "Soporte intermedio flojo",
#         ],
#         "Reemplazar taco de motor": [
#             "Taco delantero roto",
#             "Taco trasero roto",
#         ],
#         "Pérdida de aceite por motor": [
#             "Pérdida en cárter",
#             "Pérdida en tapa de válvulas",
#         ],
#         "Pérdida de líquido refrigerante": [
#             "Manguera de radiador con pérdida",
#             "Bomba de agua con pérdida",
#         ],
#     }
# }

SUBCAT_NAME = "General"

async def seed_workshop_observations(ws_id: int):
    async with get_conn_ctx() as conn:
        step_rows = await conn.fetch(
            """
            SELECT s.id, s.name
            FROM steps_order so
            JOIN steps s ON s.id = so.step_id
            WHERE so.workshop_id = $1
            ORDER BY so.number ASC
            """,
            ws_id,
        )
        step_name_to_id = {r["name"]: r["id"] for r in step_rows}

        cat_names = set()
        for step_name, cats in DEFAULT_TREE.items():
            if step_name not in step_name_to_id:
                continue
            for cat_name in cats.keys():
                cat_names.add(cat_name)

        if not cat_names:
            return

        cat_names_list = sorted(cat_names)
        ws_ids_arr = [ws_id] * len(cat_names_list)

        inserted_cat_rows = await conn.fetch(
            """
            WITH incoming(ws_id, cat_name) AS (
              SELECT UNNEST($1::bigint[]), UNNEST($2::text[])
            )
            INSERT INTO observation_categories (workshop_id, name)
            SELECT i.ws_id, i.cat_name
            FROM incoming i
            LEFT JOIN observation_categories oc
              ON oc.workshop_id = i.ws_id
             AND oc.name        = i.cat_name
            WHERE oc.id IS NULL
            RETURNING id, name
            """,
            ws_ids_arr,
            cat_names_list,
        )

        existing_cat_rows = await conn.fetch(
            """
            SELECT id, name
            FROM observation_categories
            WHERE workshop_id = $1
              AND name = ANY($2::text[])
            """,
            ws_id,
            cat_names_list,
        )

        cat_id_by_name = {}
        for r in existing_cat_rows:
            cat_id_by_name[r["name"]] = r["id"]
        for r in inserted_cat_rows:
            cat_id_by_name[r["name"]] = r["id"]

        need_subcats_cat_ids = [cat_id_by_name[n] for n in cat_names_list]
        need_subcats_names = [SUBCAT_NAME] * len(need_subcats_cat_ids)

        await conn.execute(
            """
            WITH incoming(category_id, subcat_name) AS (
              SELECT UNNEST($1::bigint[]), UNNEST($2::text[])
            )
            INSERT INTO observation_subcategories (category_id, name)
            SELECT i.category_id, i.subcat_name
            FROM incoming i
            LEFT JOIN observation_subcategories osc
              ON osc.category_id = i.category_id
             AND osc.name        = i.subcat_name
            WHERE osc.id IS NULL
            """,
            need_subcats_cat_ids,
            need_subcats_names,
        )

        subcat_rows = await conn.fetch(
            """
            SELECT id, category_id
            FROM observation_subcategories
            WHERE category_id = ANY($1::bigint[])
              AND name = $2
            """,
            need_subcats_cat_ids,
            SUBCAT_NAME,
        )
        subcat_id_by_cat_id = {r["category_id"]: r["id"] for r in subcat_rows}

        pending = []
        for step_name, cats in DEFAULT_TREE.items():
            step_id = step_name_to_id.get(step_name)
            if not step_id:
                continue
            for cat_name, leaves in cats.items():
                cat_id = cat_id_by_name.get(cat_name)
                if not cat_id:
                    continue
                subcat_id = subcat_id_by_cat_id.get(cat_id)
                if not subcat_id:
                    continue
                for idx, leaf in enumerate(leaves, start=1):
                    pending.append((step_id, subcat_id, leaf, idx))

        if not pending:
            return

        # Preparar arrays para el INSERT masivo
        # No hacemos deduplicación aquí porque necesitamos que se creen todas las observaciones,
        # incluso si tienen la misma descripción en diferentes categorías (diferentes subcat_id)
        ws_arr = []
        step_arr = []
        subcat_arr = []
        desc_arr = []
        sort_arr = []
        for step_id, subcat_id, desc, sort_order in pending:
            ws_arr.append(ws_id)
            step_arr.append(step_id)
            subcat_arr.append(subcat_id)
            desc_arr.append(desc)
            sort_arr.append(sort_order)

        if not ws_arr:
            return

        # Insertar todas las observaciones, evitando duplicados por la combinación de
        # workshop_id, step_id, subcategory_id y description
        await conn.execute(
            """
            WITH incoming(ws_id, step_id, subcat_id, description, sort_order) AS (
              SELECT
                UNNEST($1::bigint[]),
                UNNEST($2::int[]),
                UNNEST($3::bigint[]),
                UNNEST($4::text[]),
                UNNEST($5::int[])
            )
            INSERT INTO observations (
              workshop_id, step_id, subcategory_id, description, is_active, sort_order
            )
            SELECT i.ws_id, i.step_id, i.subcat_id, i.description, TRUE, i.sort_order
            FROM incoming i
            WHERE NOT EXISTS (
              SELECT 1 FROM observations o
              WHERE o.workshop_id = i.ws_id
                AND o.step_id = i.step_id
                AND o.subcategory_id = i.subcat_id
                AND o.description = i.description
            )
            """,
            ws_arr,
            step_arr,
            subcat_arr,
            desc_arr,
            sort_arr,
        )


def _clean_int_or_none(v, field_name: str):
    if v in (None, ""):
        return None
    try:
        n = int(v)
        if n <= 0:
            raise ValueError
        return n
    except Exception:
        raise ValueError(f"El {field_name} debe ser numérico y mayor a cero")
    
    
async def _is_admin(conn, user_id: int) -> bool:
    return await conn.fetchval(
        "SELECT COALESCE(is_admin, false) FROM users WHERE id = $1",
        user_id
    )
 
@workshops_bp.route("/create-unapproved", methods=["POST"])
async def create_workshop_unapproved():
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    data = await request.get_json() or {}
    name = (data.get("name") or "").strip()
    razon_social = (data.get("razonSocial") or "").strip()
    province = (data.get("province") or "").strip()
    city = (data.get("city") or "").strip()
    address = (data.get("address") or "").strip()
    phone = (data.get("phone") or "").strip()
    cuit = (data.get("cuit") or "").strip()
    plant_number_raw = data.get("plantNumber")
    disposition_number = (data.get("dispositionNumber") or "").strip()

    if not disposition_number:
        return jsonify({"error": "Falta el número de disposición"}), 400
    if len(name) < 3:
        return jsonify({"error": "El nombre debe tener al menos 3 caracteres"}), 400
    if len(razon_social) < 3:
        return jsonify({"error": "Ingresá una razón social válida"}), 400
    if province not in VALID_PROVINCES:
        return jsonify({"error": "Provincia inválida"}), 400
    if not city:
        return jsonify({"error": "Falta la localidad"}), 400

    digits_only = re.compile(r"\D+")
    cuit_norm = digits_only.sub("", cuit) if cuit else None
    if cuit_norm and len(cuit_norm) != 11:
        return jsonify({"error": "CUIT inválido, deben ser 11 dígitos"}), 400

    try:
        plant_number = _clean_int_or_none(plant_number_raw, "número de planta")
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    creator_email = None
    creator_name = None

    # vamos a crear el taller y lo mínimo necesario
    async with get_conn_ctx() as conn:
        try:
            async with conn.transaction():
                # datos del usuario creador
                urow = await conn.fetchrow(
                    "SELECT email, first_name, last_name FROM users WHERE id = $1",
                    user_id,
                )
                if urow:
                    creator_email = urow["email"]
                    creator_name = f"{urow['first_name']} {urow['last_name']}"

                # chequeo columnas para address opcional
                cols = await conn.fetch(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'workshop'
                    """
                )
                colset = {r["column_name"] for r in cols}

                if "address" in colset:
                    row = await conn.fetchrow(
                        """
                        INSERT INTO workshop (
                          name,
                          razon_social,
                          province,
                          city,
                          address,
                          phone,
                          cuit,
                          plant_number,
                          disposition_number,
                          is_approved
                        )
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,false)
                        RETURNING
                          id,
                          name,
                          razon_social,
                          province,
                          city,
                          address,
                          phone,
                          cuit,
                          plant_number,
                          disposition_number,
                          is_approved
                        """,
                        name,
                        razon_social,
                        province,
                        city,
                        address,
                        phone,
                        cuit_norm,
                        plant_number,
                        disposition_number,
                    )
                else:
                    row = await conn.fetchrow(
                        """
                        INSERT INTO workshop (
                          name,
                          razon_social,
                          province,
                          city,
                          phone,
                          cuit,
                          plant_number,
                          disposition_number,
                          is_approved
                        )
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,false)
                        RETURNING
                          id,
                          name,
                          razon_social,
                          province,
                          city,
                          phone,
                          cuit,
                          plant_number,
                          disposition_number,
                          is_approved
                        """,
                        name,
                        razon_social,
                        province,
                        city,
                        phone,
                        cuit_norm,
                        plant_number,
                        disposition_number,
                    )

                ws_id = row["id"]

                # relacionar usuario como OWNER
                await conn.execute(
                    """
                    INSERT INTO workshop_users (workshop_id, user_id, user_type_id)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (workshop_id, user_id)
                    DO UPDATE SET user_type_id = EXCLUDED.user_type_id
                    """,
                    ws_id,
                    user_id,
                    OWNER_ROLE_ID,
                )

                # steps_order para este taller
                steps_rows = await conn.fetch(
                    "SELECT id, name FROM steps ORDER BY id ASC"
                )
                if not steps_rows:
                    raise RuntimeError("No hay pasos base en la tabla steps")

                for idx, s in enumerate(steps_rows):
                    await conn.execute(
                        """
                        INSERT INTO steps_order (workshop_id, step_id, number)
                        VALUES ($1, $2, $3)
                        ON CONFLICT DO NOTHING
                        """,
                        ws_id,
                        s["id"],
                        idx + 1,
                    )

        except UniqueViolationError as e:
            # por ejemplo: mismo CUIT
            msg = "Ya existe un taller con ese nombre"
            if "workshop_cuit_uidx" in str(e):
                msg = "Ya existe un taller con ese CUIT"
            return jsonify({"error": msg}), 409
        except RuntimeError as e:
            return jsonify({"error": str(e)}), 500

    # hasta acá COMMIT hecho: el taller existe y el usuario es owner.
    # disparamos el seed en background SIN esperar
    asyncio.create_task(seed_workshop_observations(ws_id))

    # fuego el mail "pendiente de aprobación"
    if creator_email:
        try:
            review_url = f"{FRONTEND_URL}/select-workshop"
            # lo hacemos en background-fire-and-forget también
            asyncio.create_task(
                send_workshop_pending_email(
                    to_email=creator_email,
                    workshop_name=name,
                    review_url=review_url,
                )
            )
        except Exception as e:
            # si falla el schedule del mail no bloqueamos la respuesta
            print(
                "No se pudo encolar email de taller pendiente a %s, error: %s",
                creator_email,
                e,
            )
    else:
        print(
            "No se envió email, creator_email es None para user_id=%s",
            user_id,
        )

    # Notificar a administradores sobre nuevo taller
    try:
        admin_emails = []
        async with get_conn_ctx() as conn:
            rows = await conn.fetch(
                "SELECT email FROM users WHERE COALESCE(is_admin,false) = true AND COALESCE(email,'') <> ''"
            )
            admin_emails = [r["email"] for r in rows]
        for em in admin_emails:
            asyncio.create_task(
                send_admin_workshop_registered_email(
                    to_email=em,
                    workshop_name=name,
                    workshop_id=ws_id,
                )
            )
    except Exception as e:
        log.exception("No se pudieron encolar notificaciones a admins por nuevo taller %s: %s", ws_id, e)

    # armamos respuesta para el front
    out = {
        "id": row["id"],
        "name": row["name"],
        "razonSocial": row["razon_social"],
        "province": row["province"],
        "city": row["city"],
        "phone": row["phone"],
        "cuit": row["cuit"],
        "plant_number": row["plant_number"],
        "disposition_number": row["disposition_number"],
        "is_approved": row["is_approved"],
    }
    if "address" in row.keys():
        out["address"] = row["address"]

    return jsonify(
        {
            "message": "Workshop creado en estado pendiente de aprobación",
            "workshop": out,
            "membership": {
                "user_id": user_id,
                "workshop_id": row["id"],
                "user_type_id": OWNER_ROLE_ID,
            },
        }
    ), 201

    
@workshops_bp.route("/<int:workshop_id>/approve", methods=["POST"])
async def approve_workshop(workshop_id: int):
    # TODO: validar que g.user_id sea admin
    async with get_conn_ctx() as conn:
        async with conn.transaction():
            # 1) Traer datos actuales
            ws = await conn.fetchrow(
                """
                SELECT id, name, is_approved
                FROM workshop
                WHERE id = $1
                """,
                workshop_id
            )
            if not ws:
                return jsonify({"error": "Workshop no encontrado"}), 404

            # 2) Si ya estaba aprobado, no reenviamos mails
            if ws["is_approved"]:
                return jsonify({"ok": True, "workshop_id": ws["id"], "is_approved": True, "already": True}), 200

            # 3) Aprobar
            await conn.execute(
                """
                UPDATE workshop
                SET is_approved = true, updated_at = NOW()
                WHERE id = $1
                """,
                workshop_id
            )

            # 4) Obtener emails de los OWNERS
            owners = await conn.fetch(
                """
                SELECT u.email
                FROM workshop_users wu
                JOIN users u ON u.id = wu.user_id
                WHERE wu.workshop_id = $1 AND wu.user_type_id = $2 AND COALESCE(u.email, '') <> ''
                """,
                workshop_id, OWNER_ROLE_ID
            )
            owner_emails = [r["email"] for r in owners]

            ws_name = ws["name"]

    # 5) Enviar mails fuera de la transacción
    if owner_emails:
        for em in owner_emails:
            try:
                panel_url = f"{FRONTEND_URL}/dashboard/{workshop_id}"
                await send_workshop_approved_email(
                    to_email=em,
                    workshop_name=ws_name,
                    workshop_id=str(workshop_id),
                )
            except Exception as e:
                log.exception("No se pudo enviar email de taller aprobado a %s, error: %s", em, e)

    return jsonify({"ok": True, "workshop_id": workshop_id, "is_approved": True}), 200


@workshops_bp.route("/<int:workshop_id>/suspend", methods=["POST"])
async def suspend_workshop(workshop_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    data = await request.get_json() or {}
    reason = (data.get("reason") or "").strip()

    async with get_conn_ctx() as conn:
        # Validar que el usuario sea admin
        is_admin = await _is_admin(conn, user_id)
        if not is_admin:
            return jsonify({"error": "Requiere admin"}), 403

        async with conn.transaction():
            # 1) Traer datos actuales
            ws = await conn.fetchrow(
                """
                SELECT id, name, is_approved
                FROM workshop
                WHERE id = $1
                """,
                workshop_id
            )
            if not ws:
                return jsonify({"error": "Workshop no encontrado"}), 404

            # 2) Si ya estaba suspendido (is_approved = false), no reenviamos mails
            if not ws["is_approved"]:
                return jsonify({"ok": True, "workshop_id": ws["id"], "is_approved": False, "already": True}), 200

            # 3) Suspender el taller
            await conn.execute(
                """
                UPDATE workshop
                SET is_approved = false, updated_at = NOW()
                WHERE id = $1
                """,
                workshop_id
            )

            # 4) Obtener emails de los OWNERS
            owners = await conn.fetch(
                """
                SELECT u.email
                FROM workshop_users wu
                JOIN users u ON u.id = wu.user_id
                WHERE wu.workshop_id = $1 AND wu.user_type_id = $2 AND COALESCE(u.email, '') <> ''
                """,
                workshop_id, OWNER_ROLE_ID
            )
            owner_emails = [r["email"] for r in owners]

            ws_name = ws["name"]

    # 5) Enviar mails fuera de la transacción
    if owner_emails:
        for em in owner_emails:
            try:
                await send_workshop_suspended_email(
                    to_email=em,
                    workshop_name=ws_name,
                    workshop_id=str(workshop_id),
                    reason=reason if reason else None,
                )
            except Exception as e:
                log.exception("No se pudo enviar email de taller suspendido a %s, error: %s", em, e)

    return jsonify({"ok": True, "workshop_id": workshop_id, "is_approved": False}), 200


@workshops_bp.route("/pending", methods=["GET"])
async def list_pending_workshops():
    async with get_conn_ctx() as conn:
        rows = await conn.fetch(
            """
            SELECT id, name, razon_social , disposition_number, province, city, phone, cuit, plant_number, address
            FROM workshop
            WHERE is_approved = false
            ORDER BY id DESC
            """
        )
    return jsonify([dict(r) for r in rows]), 200


# Crear workshop
@workshops_bp.route("/create", methods=["POST"])
async def create_workshop():
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    data = await request.get_json()

    # Entrada y normalización
    name = (data.get("name") or "").strip()
    razon_social = (data.get("razonSocial") or "").strip()
    province = (data.get("province") or "").strip()
    city = (data.get("city") or "").strip()
    phone = (data.get("phone") or "").strip()
    cuit = (data.get("cuit") or "").strip()
    plant_number_raw = (data.get("plantNumber") or None)
    disposition_number = (data.get("disposition_number") or "").strip()

    # Validaciones
    if len(name) < 3:
        return jsonify({"error": "El nombre debe tener al menos 3 caracteres"}), 400
    if len(razon_social) < 3:
        return jsonify({"error": "Ingresá una razón social válida"}), 400

    VALID_PROVINCES = {
        "Buenos Aires","CABA","Catamarca","Chaco","Chubut","Córdoba","Corrientes",
        "Entre Ríos","Formosa","Jujuy","La Pampa","La Rioja","Mendoza","Misiones",
        "Neuquén","Río Negro","Salta","San Juan","San Luis","Santa Cruz",
        "Santa Fe","Santiago del Estero","Tierra del Fuego","Tucumán"
    }
    if province not in VALID_PROVINCES:
        return jsonify({"error": "Provincia inválida"}), 400
    if not city:
        return jsonify({"error": "Falta la localidad"}), 400

    import re
    digits_only = re.compile(r"\D+")
    phone_norm = phone.strip()
    cuit_norm = digits_only.sub("", cuit) if cuit else None
    if cuit_norm and len(cuit_norm) != 11:
        return jsonify({"error": "CUIT inválido, deben ser 11 dígitos"}), 400

    plant_number = None
    if plant_number_raw not in (None, ""):
        try:
            plant_number = int(plant_number_raw)
            if plant_number <= 0:
                return jsonify({"error": "El número de planta debe ser mayor a cero"}), 400
        except ValueError:
            return jsonify({"error": "El número de planta debe ser numérico"}), 400

    OWNER_ROLE_ID = 2

    from asyncpg import UniqueViolationError
    async with get_conn_ctx() as conn:
        try:
            async with conn.transaction():
                # 1) crear workshop
                row = await conn.fetchrow(
                    """
                    INSERT INTO workshop (name, razon_social, province, city, phone, cuit, plant_number, disposition_number)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    RETURNING id, name, razon_social, province, city, phone, cuit, plant_number, disposition_number
                    """,
                    name, razon_social, province, city, phone_norm, cuit_norm, plant_number, disposition_number
                )
                ws_id = row["id"]

                # 2) dar rol de owner al creador sin ON CONFLICT:
                # primero intento actualizar, si no afectó filas, inserto
                upd_status = await conn.execute(
                    """
                    UPDATE workshop_users
                    SET user_type_id = $3
                    WHERE workshop_id = $1 AND user_id = $2
                    """,
                    ws_id, user_id, OWNER_ROLE_ID
                )
                # asyncpg devuelve "UPDATE <n>"
                if upd_status.split()[-1] == "0":
                    await conn.execute(
                        """
                        INSERT INTO workshop_users (workshop_id, user_id, user_type_id)
                        VALUES ($1, $2, $3)
                        """,
                        ws_id, user_id, OWNER_ROLE_ID
                    )

                # 3) inicializar orden de pasos
                steps = await conn.fetch("SELECT id, name FROM steps ORDER BY id ASC")
                if not steps:
                    raise RuntimeError("No hay pasos base en la tabla steps")

                pairs = [(s["id"], idx + 1) for idx, s in enumerate(steps)]

                # inserto cada fila solo si no existe ya ese (workshop_id, step_id)
                for sid, num in pairs:
                    await conn.execute(
                        """
                        INSERT INTO steps_order (workshop_id, step_id, number)
                        SELECT $1, $2, $3
                        WHERE NOT EXISTS (
                            SELECT 1 FROM steps_order
                            WHERE workshop_id = $1 AND step_id = $2
                        )
                        """,
                        ws_id, sid, num
                    )

                # 4) crear 5 observaciones por defecto para cada step del workshop
                for s in steps:
                    sid = s["id"]
                    sname = (s["name"] or "").strip()
                    defaults = [
                        f"Verificación visual",
                        f"Desgaste o grietas",
                        f"Fijaciones y holguras",
                        f"Funcionamiento general",
                        f"Medidas y tolerancias",
                    ]
                    for desc in defaults:
                        await conn.execute(
                            """
                            INSERT INTO observations (workshop_id, step_id, description)
                            SELECT $1, $2, $3
                            WHERE NOT EXISTS (
                                SELECT 1 FROM observations
                                WHERE workshop_id = $1 AND step_id = $2 AND description = $3
                            )
                            """,
                            ws_id, sid, desc
                        )

        except UniqueViolationError as e:
            msg = "Ya existe un taller con ese nombre"
            if "workshop_cuit_uidx" in str(e):
                msg = "Ya existe un taller con ese CUIT"
            return jsonify({"error": msg}), 409

    # Notificar a administradores sobre nuevo taller
    try:
        admin_emails = []
        async with get_conn_ctx() as conn:
            rows = await conn.fetch(
                "SELECT email FROM users WHERE COALESCE(is_admin,false) = true AND COALESCE(email,'') <> ''"
            )
            admin_emails = [r["email"] for r in rows]
        for em in admin_emails:
            asyncio.create_task(
                send_admin_workshop_registered_email(
                    to_email=em,
                    workshop_name=name,
                    workshop_id=ws_id,
                )
            )
    except Exception as e:
        log.exception("No se pudieron encolar notificaciones a admins por nuevo taller %s: %s", ws_id, e)

    return jsonify({
        "message": "Workshop creado",
        "workshop": dict(row),
        "membership": {
            "user_id": user_id,
            "workshop_id": row["id"],
            "user_type_id": OWNER_ROLE_ID
        }
    }), 201

    # Nota: mantener notificaciones antes del return


# ====== Verificar membresía del usuario en un taller ======
@workshops_bp.route("/<int:workshop_id>/membership", methods=["GET"])
async def check_workshop_membership(workshop_id: int):
    """
    Devuelve si el usuario autenticado (g.user_id) pertenece al taller indicado.
    - 401 si no hay usuario autenticado.
    - 404 si el taller no existe o no está aprobado.
    - 200 con is_member=True/False si el taller existe.
    """
    
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        # 1) verificar que el taller exista y esté aprobado
        workshop = await conn.fetchrow("SELECT id, is_approved FROM workshop WHERE id = $1", workshop_id)
        if not workshop:
            return jsonify({"error": "Workshop no encontrado"}), 404
        
        if not workshop["is_approved"]:
            return jsonify({"error": "Workshop no se encuentra aprobado"}), 404

        # 2) verificar membresía (y devolver rol si existe)
        row = await conn.fetchrow(
            """
            SELECT user_type_id
            FROM workshop_users
            WHERE workshop_id = $1 AND user_id = $2
            """,
            workshop_id, user_id
        )

    if not row:
        return jsonify({
            "workshop_id": workshop_id,
            "user_id": str(user_id),
            "is_member": False
        }), 200

    return jsonify({
        "workshop_id": workshop_id,
        "user_id": str(user_id),
        "is_member": True,
        "user_type_id": row["user_type_id"]  # p.ej. 2 = OWNER
    }), 200

# Cambiar el nombre de un workshop
@workshops_bp.route("/<int:workshop_id>/name", methods=["PUT", "PATCH"])
async def rename_workshop(workshop_id: int):
    data = await request.get_json()
    new_name = (data.get("name") or "").strip()

    if not new_name:
        return jsonify({"error": "Falta el nuevo nombre"}), 400

    async with get_conn_ctx() as conn:
        exists = await conn.fetchval("SELECT 1 FROM workshop WHERE id = $1", workshop_id)
        if not exists:
            return jsonify({"error": "Workshop no encontrado"}), 404

        try:
            row = await conn.fetchrow(
                """
                UPDATE workshop
                SET name = $1, updated_at = CURRENT_TIMESTAMP
                WHERE id = $2
                RETURNING id, name
                """,
                new_name, workshop_id,
            )
        except UniqueViolationError:
            return jsonify({"error": "Ya existe un taller con ese nombre"}), 409

    return jsonify({"message": "Nombre actualizado", "workshop": dict(row)}), 200


# ====== Helpers comunes ======
async def _user_belongs_to_workshop(conn, user_id: int, workshop_id: int) -> bool:
    return await conn.fetchval(
        """
        SELECT EXISTS(
          SELECT 1 FROM workshop_users
          WHERE workshop_id = $1 AND user_id = $2
        )
        """,
        workshop_id, user_id
    )

async def _step_belongs_to_workshop(conn, step_id: int, workshop_id: int) -> bool:
    return await conn.fetchval(
        """
        SELECT EXISTS(
          SELECT 1 FROM steps_order
          WHERE workshop_id = $1 AND step_id = $2
        )
        """,
        workshop_id, step_id
    )

def _camel_ws_row(row) -> dict:
    """Mapea columnas del workshop a las claves esperadas en front."""
    if not row:
        return {}
    return {
        "id": row["id"],
        "name": row["name"],
        "razonSocial": row["razon_social"],
        "phone": row["phone"],
        "cuit": row["cuit"],
        "province": row["province"],
        "city": row["city"],
        # Mantengo snake por compatibilidad, si querés camel cambiá a plantNumber
        "plant_number": row["plant_number"],
        "disposition_number": row["disposition_number"],
        "available_inspections": row["available_inspections"],
    }

# ====== 1) Obtener datos del taller ======
@workshops_bp.route("/<int:workshop_id>", methods=["GET"])
async def get_workshop(workshop_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        is_admin = await _is_admin(conn, user_id)
        # solo bloquear si no es admin y no pertenece
        if not is_admin:
            belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
            if not belongs:
                return jsonify({"error": "No tenés acceso a este taller"}), 403

        row = await conn.fetchrow(
            """
            SELECT id, name, razon_social, province, city, phone, cuit, plant_number, disposition_number, available_inspections
            FROM workshop
            WHERE id = $1
            """,
            workshop_id
        )
        if not row:
            return jsonify({"error": "Workshop no encontrado"}), 404
        
    return jsonify(_camel_ws_row(row)), 200


@workshops_bp.route("/admin/<int:workshop_id>/members", methods=["GET"])
async def admin_list_workshop_members(workshop_id: int):
    # si en tu middleware pones g.user_id, podés usarlo, si no, cambialo
    from quart import g
    admin_id = g.get("user_id")
    if not admin_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        if not await _is_admin(conn, admin_id):
            return jsonify({"error": "Requiere admin"}), 403

        exists = await conn.fetchval("SELECT 1 FROM workshop WHERE id = $1", workshop_id)
        if not exists:
            return jsonify({"error": "Workshop no encontrado"}), 404

        rows = await conn.fetch(
            """
            SELECT
              u.id::text          AS user_id,
              u.first_name,
              u.last_name,
              u.email,
              u.dni,
              u.phone_number,
              u.title_name,
              u.license_number,
              ut.name as role,
              wu.engineer_kind,
              wu.created_at
            FROM workshop_users wu
            JOIN users u      ON u.id = wu.user_id
            LEFT JOIN user_types ut ON ut.id = wu.user_type_id
            WHERE wu.workshop_id = $1
            ORDER BY wu.user_type_id NULLS LAST, u.last_name, u.first_name
            """,
            workshop_id
        )

    return jsonify([dict(r) for r in rows]), 200


@workshops_bp.route("/<int:workshop_id>/members/<uuid:member_user_id>", methods=["DELETE"])
async def owner_unassign_member(workshop_id: int, member_user_id: UUID):
    user_id = g.get("user_id")  # este debe ser UUID, string o UUID
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    # normalizamos actor a string UUID
    actor_uuid = str(user_id)

    async with get_conn_ctx() as conn:
        # 1, validar que el actor es OWNER en ese taller
        is_owner = await conn.fetchval(
            """
            SELECT EXISTS(
              SELECT 1
              FROM workshop_users
              WHERE workshop_id = $1      -- int
                AND user_id      = $2::uuid
                AND user_type_id = $3
            )
            """,
            workshop_id, actor_uuid, OWNER_ROLE_ID
        )
        if not is_owner:
            return jsonify({"error": "Requiere rol OWNER en este taller"}), 403

        # 2, no dejar al taller sin OWNER si el target es el único OWNER
        owner_left = await conn.fetchval(
            """
            SELECT
              (SELECT COUNT(*) FROM workshop_users
                WHERE workshop_id = $1 AND user_type_id = $2) = 1
              AND EXISTS(
                SELECT 1 FROM workshop_users
                WHERE workshop_id = $1
                  AND user_id = $3::uuid
                  AND user_type_id = $2
              )
            """,
            workshop_id, OWNER_ROLE_ID, str(member_user_id)
        )
        if owner_left:
            return jsonify({"error": "No se puede quitar al único OWNER del taller"}), 400

        # 3, transacción, borrar y loguear en forma atómica
        async with conn.transaction():
            deleted_row = await conn.fetchrow(
                """
                DELETE FROM workshop_users
                WHERE workshop_id = $1
                  AND user_id      = $2::uuid
                RETURNING user_type_id
                """,
                workshop_id, str(member_user_id)
            )
            if not deleted_row:
                return jsonify({"error": "Miembro no encontrado en este taller"}), 404

            target_prev_role = deleted_row["user_type_id"]

            meta = {
                "ip": request.headers.get("X-Forwarded-For") or request.remote_addr,
                "user_agent": request.headers.get("User-Agent"),
                "reason": "owner_unassign_member"
            }

            await conn.execute(
                """
                INSERT INTO workshop_user_logs
                  (workshop_id, actor_user_id, target_user_id, target_prev_role, action, meta)
                VALUES ($1, $2::uuid, $3::uuid, $4, 'UNASSIGN', $5::jsonb)
                """,
                workshop_id, actor_uuid, str(member_user_id), target_prev_role, json.dumps(meta)
            )

    return jsonify({"ok": True}), 200


@workshops_bp.route("/admin/<int:workshop_id>/members/<int:member_user_id>", methods=["DELETE"])
async def admin_unassign_member(workshop_id: int, member_user_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        if not await _is_admin(conn, user_id):
            return jsonify({"error": "Requiere admin"}), 403

        # no permitas dejar el taller sin OWNER, opcional:
        owner_left = await conn.fetchval(
            """
            SELECT (SELECT COUNT(*) FROM workshop_users WHERE workshop_id=$1 AND user_type_id=$2) = 1
                   AND EXISTS(SELECT 1 FROM workshop_users WHERE workshop_id=$1 AND user_id=$3 AND user_type_id=$2)
            """,
            workshop_id, OWNER_ROLE_ID, member_user_id
        )
        if owner_left:
            return jsonify({"error": "No se puede quitar al único OWNER del taller"}), 400

        result = await conn.execute(
            """
            DELETE FROM workshop_users
            WHERE workshop_id = $1 AND user_id = $2
            """,
            workshop_id, member_user_id
        )
    return jsonify({"ok": True, "result": result}), 200

def _as_uuid(s: str) -> UUID:
    return UUID(s)  # lanza ValueError si no es UUID


@workshops_bp.route("/admin/<int:workshop_id>/members/<user_id>", methods=["DELETE", "OPTIONS"])
async def admin_unassign_workshop_member(workshop_id: int, user_id: str):
    if request.method == "OPTIONS":
        return ("", 204)

    from quart import g
    admin_id = g.get("user_id")
    if not admin_id:
        return jsonify({"error": "No autorizado"}), 401

    # validar UUID
    try:
        _as_uuid(user_id)
    except ValueError:
        return jsonify({"error": "user_id inválido, debe ser UUID"}), 400

    OWNER_ROLE_ID = 2

    async with get_conn_ctx() as conn:
        if not await _is_admin(conn, admin_id):
            return jsonify({"error": "Requiere admin"}), 403

        exists = await conn.fetchval("SELECT 1 FROM workshop WHERE id = $1", workshop_id)
        if not exists:
            return jsonify({"error": "Workshop no encontrado"}), 404

        # evitar dejar el taller sin owner, opcional pero recomendado
        is_last_owner = await conn.fetchval(
            """
            SELECT
              (SELECT COUNT(*) FROM workshop_users
               WHERE workshop_id = $1 AND user_type_id = $2) = 1
              AND EXISTS(
                SELECT 1 FROM workshop_users
                WHERE workshop_id = $1 AND user_id = $3::uuid AND user_type_id = $2
              )
            """,
            workshop_id, OWNER_ROLE_ID, user_id
        )
        if is_last_owner:
            return jsonify({"error": "No se puede quitar al único OWNER del taller"}), 400

        result = await conn.execute(
            """
            DELETE FROM workshop_users
            WHERE workshop_id = $1 AND user_id = $2::uuid
            """,
            workshop_id, user_id
        )

    # asyncpg devuelve "DELETE n"
    if result.endswith("0"):
        return jsonify({"error": "Usuario no estaba asignado a este taller"}), 404

    return jsonify({"ok": True, "workshop_id": workshop_id, "user_id": user_id}), 200


@workshops_bp.route("/<int:workshop_id>", methods=["PATCH", "PUT"])
async def update_workshop(workshop_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401
    data = await request.get_json() or {}

    # Solo se pueden editar estos campos
    name = (data.get("name") or "").strip()
    razon_social = (data.get("razonSocial") or "").strip()
    phone = (data.get("phone") or "").strip()
    cuit = (data.get("cuit") or "").strip()

    import re
    digits_only = re.compile(r"\D+")
    cuit_norm = digits_only.sub("", cuit) if cuit else None
    if cuit_norm and len(cuit_norm) != 11:
        return jsonify({"error": "CUIT inválido, deben ser 11 dígitos"}), 400

    sets, vals = [], []
    idx = 1
    if name:
        if len(name) < 3:
            return jsonify({"error": "El nombre debe tener al menos 3 caracteres"}), 400
        sets.append(f"name = ${idx}"); vals.append(name); idx += 1
    if razon_social:
        if len(razon_social) < 3:
            return jsonify({"error": "Ingresá una razón social válida"}), 400
        sets.append(f"razon_social = ${idx}"); vals.append(razon_social); idx += 1
    if phone is not None:
        sets.append(f"phone = ${idx}"); vals.append(phone); idx += 1
    if cuit_norm is not None:
        sets.append(f"cuit = ${idx}"); vals.append(cuit_norm); idx += 1

    if not sets:
        return jsonify({"error": "No hay datos para actualizar"}), 400

    async with get_conn_ctx() as conn:
        belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
        if not belongs:
            return jsonify({"error": "No tenés acceso a este taller"}), 403

        try:
            row = await conn.fetchrow(
                f"""
                UPDATE workshop
                SET {", ".join(sets)}, updated_at = CURRENT_TIMESTAMP
                WHERE id = ${idx}
                RETURNING id, name, razon_social, province, city, phone, cuit, plant_number, disposition_number, available_inspections
                """,
                *vals, workshop_id
            )
        except UniqueViolationError:
            return jsonify({"error": "Ya existe un taller con ese nombre o CUIT"}), 409

    return jsonify({"message": "Taller actualizado", "workshop": _camel_ws_row(row)}), 200

# ====== 3) Listar orden de pasos del taller ======
@workshops_bp.route("/<int:workshop_id>/steps-order", methods=["GET"])
async def get_steps_order(workshop_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
        if not belongs:
            return jsonify({"error": "No tenés acceso a este taller"}), 403

        rows = await conn.fetch(
            """
            SELECT so.step_id, s.name, s.description, so.number
            FROM steps_order so
            JOIN steps s ON s.id = so.step_id
            WHERE so.workshop_id = $1
            ORDER BY so.number ASC
            """,
            workshop_id
        )

        if not rows:
            # inicializar desde steps base
            base_steps = await conn.fetch("SELECT id, name, description FROM steps ORDER BY id ASC")
            if not base_steps:
                return jsonify({"error": "No hay pasos base en la tabla steps"}), 500

            async with conn.transaction():
                for idx, s in enumerate(base_steps):
                    await conn.execute(
                        """
                        INSERT INTO steps_order (workshop_id, step_id, number)
                        VALUES ($1, $2, $3)
                        ON CONFLICT DO NOTHING
                        """,
                        workshop_id, s["id"], idx + 1
                    )

            # volver a leer con el orden ya creado
            rows = await conn.fetch(
                """
                SELECT so.step_id, s.name, s.description, so.number
                FROM steps_order so
                JOIN steps s ON s.id = so.step_id
                WHERE so.workshop_id = $1
                ORDER BY so.number ASC
                """,
                workshop_id
            )

    out = [
        {
            "step_id": r["step_id"],
            "name": r["name"],
            "description": r["description"],
            "number": r["number"],
        }
        for r in rows
    ]
    return jsonify(out), 200

# ====== 4) Guardar orden de pasos del taller ======
@workshops_bp.route("/<int:workshop_id>/steps-order", methods=["PUT"])
async def save_steps_order(workshop_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    payload = await request.get_json() or {}
    items = payload.get("items") or []
    if not isinstance(items, list) or not items:
        return jsonify({"error": "Formato inválido, se espera items: [{ step_id, number }]"}), 400

    # Validación mínima
    try:
        step_ids = [int(i["step_id"]) for i in items]
        numbers = [int(i["number"]) for i in items]
    except Exception:
        return jsonify({"error": "step_id y number deben ser numéricos"}), 400

    if len(set(numbers)) != len(numbers):
        return jsonify({"error": "Hay números de orden repetidos"}), 400

    async with get_conn_ctx() as conn:
        belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
        if not belongs:
            return jsonify({"error": "No tenés acceso a este taller"}), 403

        # Verifica que todos los steps pertenezcan al taller
        valid = await conn.fetch(
            """
            SELECT step_id FROM steps_order
            WHERE workshop_id = $1 AND step_id = ANY($2::int[])
            """,
            workshop_id, step_ids
        )
        valid_set = {r["step_id"] for r in valid}
        invalid = [sid for sid in step_ids if sid not in valid_set]
        if invalid:
            return jsonify({"error": f"Paso no pertenece al taller, ids: {invalid}"}), 400

        # Actualización en lote
        async with conn.transaction():
            await conn.execute(
                """
                UPDATE steps_order AS so
                SET number = x.number
                FROM (
                  SELECT unnest($1::int[]) AS step_id, unnest($2::int[]) AS number
                ) AS x
                WHERE so.workshop_id = $3 AND so.step_id = x.step_id
                """,
                step_ids, numbers, workshop_id
            )

    return jsonify({"message": "Orden de pasos guardado"}), 200


@workshops_bp.route("/<int:workshop_id>/members/<uuid:member_user_id>/role", methods=["PUT", "OPTIONS"])
async def set_member_role(workshop_id: int, member_user_id):
    if request.method == "OPTIONS":
        return ("", 204)

    actor_id = g.get("user_id")
    if not actor_id:
        return jsonify({"error": "No autorizado"}), 401

    data = await request.get_json() or {}
    role_name = (data.get("role") or "").strip()
    role_id = data.get("user_type_id")
    
    # Campos adicionales para rol Ingeniero
    title_name = (data.get("title_name") or "").strip()
    licence_number = (data.get("licence_number") or data.get("license_number") or "").strip()
    engineer_kind = (data.get("engineer_kind") or "").strip()

    async with get_conn_ctx() as conn:
        # Permisos, admin o OWNER del mismo taller
        is_admin = await _is_admin(conn, actor_id)
        if not is_admin:
            # actor_id puede ser UUID en tu tabla, normalizo a str
            belongs_as_owner = await conn.fetchval(
                """
                SELECT EXISTS(
                  SELECT 1 FROM workshop_users
                  WHERE workshop_id = $1 AND user_id = $2::uuid AND user_type_id = $3
                )
                """,
                workshop_id, str(actor_id), OWNER_ROLE_ID
            )
            if not belongs_as_owner:
                return jsonify({"error": "Requiere admin o rol OWNER en este taller"}), 403

        # Verificar que el usuario objetivo está asignado al taller
        exists_target = await conn.fetchrow(
            """
            SELECT wu.user_type_id AS current_role
            FROM workshop_users wu
            WHERE wu.workshop_id = $1 AND wu.user_id = $2::uuid
            """,
            workshop_id, str(member_user_id)
        )
        if not exists_target:
            return jsonify({"error": "Miembro no encontrado en este taller"}), 404

        # Resolver role_id por nombre si llega "role"
        if role_id is None:
            if not role_name:
                return jsonify({"error": "Falta role o user_type_id"}), 400
            role_row = await conn.fetchrow(
                "SELECT id FROM user_types WHERE LOWER(name) = LOWER($1)",
                role_name
            )
            if not role_row:
                return jsonify({"error": f"Rol inválido, no existe '{role_name}'"}), 400
            role_id = role_row["id"]
        
        # Validar campos adicionales si el rol es Ingeniero
        is_engineer = int(role_id) == ENGINEER_ROLE_ID
        if is_engineer:
            if not title_name:
                return jsonify({"error": "Para el rol Ingeniero, se requiere title_name"}), 400
            if not licence_number:
                return jsonify({"error": "Para el rol Ingeniero, se requiere licence_number"}), 400
            if engineer_kind not in ("Titular", "Suplente"):
                return jsonify({"error": "engineer_kind debe ser 'Titular' o 'Suplente'"}), 400
            
            # Validar que no haya otro Ingeniero Titular en el taller (excepto el mismo usuario)
            if engineer_kind == "Titular":
                existing_titular = await conn.fetchrow(
                    """
                    SELECT user_id
                    FROM workshop_users
                    WHERE workshop_id = $1
                      AND user_type_id = $2
                      AND engineer_kind = 'Titular'
                      AND user_id != $3::uuid
                    """,
                    workshop_id, ENGINEER_ROLE_ID, str(member_user_id)
                )
                if existing_titular:
                    return jsonify({"error": "Ya existe un Ingeniero Titular asignado a este taller"}), 409

        # Evitar dejar al taller sin OWNER si estamos bajando al último OWNER
        if exists_target["current_role"] == OWNER_ROLE_ID and role_id != OWNER_ROLE_ID:
            is_last_owner = await conn.fetchval(
                """
                SELECT (SELECT COUNT(*) FROM workshop_users WHERE workshop_id = $1 AND user_type_id = $2) = 1
                """,
                workshop_id, OWNER_ROLE_ID
            )
            if is_last_owner:
                return jsonify({"error": "No se puede cambiar el rol del único OWNER del taller"}), 400

        # Guardar cambio y loguear
        async with conn.transaction():
            # Actualizar rol en workshop_users
            if is_engineer:
                await conn.execute(
                    """
                    UPDATE workshop_users
                    SET user_type_id = $3, engineer_kind = $4
                    WHERE workshop_id = $1 AND user_id = $2::uuid
                    """,
                    workshop_id, str(member_user_id), int(role_id), engineer_kind
                )
            else:
                # Si no es ingeniero, limpiar engineer_kind
                await conn.execute(
                    """
                    UPDATE workshop_users
                    SET user_type_id = $3, engineer_kind = NULL
                    WHERE workshop_id = $1 AND user_id = $2::uuid
                    """,
                    workshop_id, str(member_user_id), int(role_id)
                )
            
            # Actualizar campos en tabla users si es Ingeniero
            if is_engineer:
                await conn.execute(
                    """
                    UPDATE users
                    SET title_name = $1, license_number = $2
                    WHERE id = $3::uuid
                    """,
                    title_name, licence_number, str(member_user_id)
                )

            meta = {
                "ip": request.headers.get("X-Forwarded-For") or request.remote_addr,
                "user_agent": request.headers.get("User-Agent"),
                "reason": "set_member_role",
                "new_role_id": int(role_id),
                "new_role_name": role_name or None,
            }
            if is_engineer:
                meta["title_name"] = title_name
                meta["license_number"] = licence_number
                meta["engineer_kind"] = engineer_kind
            
            await conn.execute(
                """
                INSERT INTO workshop_user_logs
                  (workshop_id, actor_user_id, target_user_id, target_prev_role, action, meta)
                VALUES ($1, $2::uuid, $3::uuid, $4, 'SET_ROLE', $5::jsonb)
                """,
                workshop_id, str(actor_id), str(member_user_id), exists_target["current_role"], json.dumps(meta)
            )

    return jsonify({"ok": True, "workshop_id": workshop_id, "user_id": str(member_user_id), "user_type_id": int(role_id)}), 200




# ====== 5) Observaciones por paso ======

# 5.1 Listar observaciones de un paso del taller
@workshops_bp.route("/<int:workshop_id>/steps/<int:step_id>/observations", methods=["GET"])
async def list_step_observations(workshop_id: int, step_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
        if not belongs:
            return jsonify({"error": "No tenés acceso a este taller"}), 403

        ok = await _step_belongs_to_workshop(conn, step_id, workshop_id)
        if not ok:
            return jsonify({"error": "El paso no corresponde al taller"}), 400

        rows = await conn.fetch(
            """
            SELECT id, description
            FROM observations
            WHERE workshop_id = $1 AND step_id = $2
            ORDER BY id
            """,
            workshop_id, step_id
        )
    return jsonify([{"id": r["id"], "description": r["description"]} for r in rows]), 200

# 5.2 Crear observación
@workshops_bp.route("/<int:workshop_id>/steps/<int:step_id>/observations", methods=["POST"])
async def create_step_observation(workshop_id: int, step_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401
    data = await request.get_json() or {}
    desc = (data.get("description") or "").strip()
    if not desc:
        return jsonify({"error": "Falta description"}), 400
    if len(desc) > 300:
        return jsonify({"error": "La descripción no puede superar 300 caracteres"}), 400

    async with get_conn_ctx() as conn:
        belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
        if not belongs:
            return jsonify({"error": "No tenés acceso a este taller"}), 403
        ok = await _step_belongs_to_workshop(conn, step_id, workshop_id)
        if not ok:
            return jsonify({"error": "El paso no corresponde al taller"}), 400

        row = await conn.fetchrow(
            """
            INSERT INTO observations (workshop_id, step_id, description)
            VALUES ($1, $2, $3)
            RETURNING id, description
            """,
            workshop_id, step_id, desc
        )
    return jsonify({"id": row["id"], "description": row["description"]}), 201

# 5.3 Editar observación
@workshops_bp.route("/<int:workshop_id>/steps/<int:step_id>/observations/<int:obs_id>", methods=["PUT", "PATCH"])
async def update_step_observation(workshop_id: int, step_id: int, obs_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401
    data = await request.get_json() or {}
    desc = (data.get("description") or "").strip()
    if not desc:
        return jsonify({"error": "Falta description"}), 400
    if len(desc) > 300:
        return jsonify({"error": "La descripción no puede superar 300 caracteres"}), 400

    async with get_conn_ctx() as conn:
        belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
        if not belongs:
            return jsonify({"error": "No tenés acceso a este taller"}), 403
        ok = await _step_belongs_to_workshop(conn, step_id, workshop_id)
        if not ok:
            return jsonify({"error": "El paso no corresponde al taller"}), 400

        exists = await conn.fetchval(
            """
            SELECT 1 FROM observations
            WHERE id = $1 AND workshop_id = $2 AND step_id = $3
            """,
            obs_id, workshop_id, step_id
        )
        if not exists:
            return jsonify({"error": "Observación no encontrada"}), 404

        row = await conn.fetchrow(
            """
            UPDATE observations
            SET description = $1
            WHERE id = $2
            RETURNING id, description
            """,
            desc, obs_id
        )
    return jsonify({"id": row["id"], "description": row["description"]}), 200

# 5.4 Eliminar observación
@workshops_bp.route("/<int:workshop_id>/steps/<int:step_id>/observations/<int:obs_id>", methods=["DELETE"])
async def delete_step_observation(workshop_id: int, step_id: int, obs_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
        if not belongs:
            return jsonify({"error": "No tenés acceso a este taller"}), 403
        ok = await _step_belongs_to_workshop(conn, step_id, workshop_id)
        if not ok:
            return jsonify({"error": "El paso no corresponde al taller"}), 400

        async with conn.transaction():
            await conn.execute(
                """
                DELETE FROM observation_details
                WHERE observation_id = $1
                """,
                obs_id
            )
            result = await conn.execute(
                """
                DELETE FROM observations
                WHERE id = $1 AND workshop_id = $2 AND step_id = $3
                """,
                obs_id, workshop_id, step_id
            )
    return jsonify({"message": "Observación eliminada"}), 200

    
# ====== 5.a) Categorías de observaciones por paso (workshop scope) ======
@workshops_bp.route("/<int:workshop_id>/steps/<int:step_id>/categories", methods=["GET"])
async def list_step_categories(workshop_id: int, step_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
        if not belongs:
            return jsonify({"error": "No tenés acceso a este taller"}), 403
        ok = await _step_belongs_to_workshop(conn, step_id, workshop_id)
        if not ok:
            return jsonify({"error": "El paso no corresponde al taller"}), 400

        # Obtener el nombre del paso
        step_row = await conn.fetchrow(
            "SELECT name FROM steps WHERE id = $1",
            step_id
        )
        if not step_row:
            return jsonify({"error": "Paso no encontrado"}), 404
        
        step_name = step_row["name"]
        
        # Obtener las categorías definidas en DEFAULT_TREE para este paso (para mantener orden)
        categories_for_step = DEFAULT_TREE.get(step_name, {})
        category_names_for_step = list(categories_for_step.keys()) if categories_for_step else []
        
        # Crear un diccionario para mantener el orden original del DEFAULT_TREE
        category_order = {name: idx for idx, name in enumerate(category_names_for_step)}
        
        # Obtener categorías que:
        # 1. Están en DEFAULT_TREE para este paso, O
        # 2. Tienen observaciones (activas o inactivas) para este paso específico
        #    (las inactivas pueden ser placeholders creados al asociar la categoría al paso)
        rows = await conn.fetch(
            """
            SELECT DISTINCT oc.id AS category_id, oc.name AS category_name
            FROM observation_categories oc
            LEFT JOIN observation_subcategories osc ON osc.category_id = oc.id AND osc.name = $4
            LEFT JOIN observations o ON o.subcategory_id = osc.id AND o.step_id = $2
            WHERE oc.workshop_id = $1
              AND (
                oc.name = ANY($3::text[])  -- Está en DEFAULT_TREE para este paso
                OR o.id IS NOT NULL        -- Tiene observaciones (activas o placeholders) para este paso
              )
            ORDER BY oc.id
            """,
            workshop_id, step_id, category_names_for_step if category_names_for_step else [], SUBCAT_NAME
        )
        
        # Para cada categoría que no tenga subcategoría "General", crearla
        for row in rows:
            subcat_exists = await conn.fetchval(
                """
                SELECT 1 FROM observation_subcategories
                WHERE category_id = $1 AND name = $2
                """,
                row["category_id"], SUBCAT_NAME
            )
            if not subcat_exists:
                await conn.execute(
                    """
                    INSERT INTO observation_subcategories (category_id, name)
                    VALUES ($1, $2)
                    """,
                    row["category_id"], SUBCAT_NAME
                )
        
        # Ordenar: primero las del DEFAULT_TREE (en su orden original), luego las demás (por ID)
        rows_list = list(rows)
        rows_list.sort(key=lambda r: (
            category_order.get(r["category_name"], 999999),  # Las del DEFAULT_TREE primero
            r["category_id"]  # Luego por ID para mantener orden consistente
        ))

    return jsonify([{"category_id": r["category_id"], "name": r["category_name"]} for r in rows_list]), 200


@workshops_bp.route("/<int:workshop_id>/steps/<int:step_id>/categories", methods=["POST"])
async def create_step_category(workshop_id: int, step_id: int):
    """
    Crea una categoría para el taller (scope workshop). No está atada al paso,
    pero se garantiza la existencia de la subcategoría "General" para poder
    asociar observaciones del paso.
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    data = await request.get_json() or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "Falta name"}), 400

    async with get_conn_ctx() as conn:
        belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
        if not belongs:
            return jsonify({"error": "No tenés acceso a este taller"}), 403
        ok = await _step_belongs_to_workshop(conn, step_id, workshop_id)
        if not ok:
            return jsonify({"error": "El paso no corresponde al taller"}), 400

        async with conn.transaction():
            # Crear categoría si no existe
            cat_row = await conn.fetchrow(
                """
                INSERT INTO observation_categories (workshop_id, name)
                VALUES ($1, $2)
                ON CONFLICT (workshop_id, name) DO UPDATE SET name = EXCLUDED.name
                RETURNING id, name
                """,
                workshop_id, name
            )

            # Garantizar subcategoría "General"
            subcat_row = await conn.fetchrow(
                """
                INSERT INTO observation_subcategories (category_id, name)
                VALUES ($1, $2)
                ON CONFLICT (category_id, name) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
                """,
                cat_row["id"], SUBCAT_NAME
            )
            
            # Crear una observación placeholder para asociar la categoría a este paso
            # Esto permite que la categoría aparezca en el GET aunque aún no tenga observaciones reales
            # Verificamos si ya existe una observación para esta combinación
            existing_obs = await conn.fetchval(
                """
                SELECT id FROM observations
                WHERE workshop_id = $1 AND step_id = $2 AND subcategory_id = $3
                LIMIT 1
                """,
                workshop_id, step_id, subcat_row["id"]
            )
            
            if not existing_obs:
                # Crear observación placeholder invisible (is_active = FALSE)
                # Esto asocia la categoría al paso sin mostrarla al usuario
                await conn.execute(
                    """
                    INSERT INTO observations (workshop_id, step_id, subcategory_id, description, is_active)
                    SELECT $1, $2, $3, '', FALSE
                    WHERE NOT EXISTS (
                        SELECT 1 FROM observations
                        WHERE workshop_id = $1 AND step_id = $2 AND subcategory_id = $3
                    )
                    """,
                    workshop_id, step_id, subcat_row["id"]
                )

    return jsonify({"category_id": cat_row["id"], "name": cat_row["name"], "default_subcategory_id": subcat_row["id"]}), 201


@workshops_bp.route("/<int:workshop_id>/steps/<int:step_id>/categories/<int:category_id>", methods=["PUT", "PATCH"])
async def rename_step_category(workshop_id: int, step_id: int, category_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    data = await request.get_json() or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "Falta name"}), 400

    async with get_conn_ctx() as conn:
        belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
        if not belongs:
            return jsonify({"error": "No tenés acceso a este taller"}), 403
        ok = await _step_belongs_to_workshop(conn, step_id, workshop_id)
        if not ok:
            return jsonify({"error": "El paso no corresponde al taller"}), 400

        exists = await conn.fetchval(
            "SELECT 1 FROM observation_categories WHERE id = $1 AND workshop_id = $2",
            category_id, workshop_id
        )
        if not exists:
            return jsonify({"error": "Categoría no encontrada"}), 404

        row = await conn.fetchrow(
            """
            UPDATE observation_categories
            SET name = $1
            WHERE id = $2 AND workshop_id = $3
            RETURNING id, name
            """,
            name, category_id, workshop_id
        )

    return jsonify({"category_id": row["id"], "name": row["name"]}), 200


@workshops_bp.route("/<int:workshop_id>/steps/<int:step_id>/categories/<int:category_id>", methods=["DELETE"])
async def delete_step_category(workshop_id: int, step_id: int, category_id: int):
    """
    Elimina los hijos (observaciones) de esta categoría SOLO para el paso dado.
    Si la categoría queda sin observaciones en todo el taller, también elimina
    la categoría y sus subcategorías.
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
        if not belongs:
            return jsonify({"error": "No tenés acceso a este taller"}), 403

        exists = await conn.fetchval(
            "SELECT 1 FROM observation_categories WHERE id = $1 AND workshop_id = $2",
            category_id, workshop_id
        )
        if not exists:
            return jsonify({"error": "Categoría no encontrada"}), 404

        async with conn.transaction():
            # 1) Borrar detalles de observaciones (para los hijos del paso)
            await conn.execute(
                """
                DELETE FROM observation_details
                WHERE observation_id IN (
                  SELECT o.id
                  FROM observations o
                  JOIN observation_subcategories osc ON osc.id = o.subcategory_id
                  WHERE osc.category_id = $1 AND o.workshop_id = $2 AND o.step_id = $3
                )
                """,
                category_id, workshop_id, step_id
            )

            # 2) Borrar observaciones (hijos) para este paso
            await conn.execute(
                """
                DELETE FROM observations
                WHERE id IN (
                  SELECT o.id
                  FROM observations o
                  JOIN observation_subcategories osc ON osc.id = o.subcategory_id
                  WHERE osc.category_id = $1 AND o.workshop_id = $2 AND o.step_id = $3
                )
                """,
                category_id, workshop_id, step_id
            )

            # 3) ¿Quedaron observaciones en la categoría en otros pasos?
            still_in_use = await conn.fetchval(
                """
                SELECT EXISTS(
                  SELECT 1
                  FROM observations o
                  JOIN observation_subcategories osc ON osc.id = o.subcategory_id
                  WHERE osc.category_id = $1 AND o.workshop_id = $2
                )
                """,
                category_id, workshop_id
            )

            category_deleted = False
            if not still_in_use:
                # 4) Si quedó vacía en todo el taller, eliminar subcategorías y la categoría
                await conn.execute("DELETE FROM observation_subcategories WHERE category_id = $1", category_id)
                await conn.execute("DELETE FROM observation_categories WHERE id = $1 AND workshop_id = $2", category_id, workshop_id)
                category_deleted = True

    return jsonify({"message": "Ok", "category_deleted": bool(category_deleted)}), 200


# ====== 5.b) Observaciones (ítems) por categoría del paso ======
@workshops_bp.route("/<int:workshop_id>/steps/<int:step_id>/categories/<int:category_id>/observations", methods=["GET"])
async def list_category_observations(workshop_id: int, step_id: int, category_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
        if not belongs:
            return jsonify({"error": "No tenés acceso a este taller"}), 403
        ok = await _step_belongs_to_workshop(conn, step_id, workshop_id)
        if not ok:
            return jsonify({"error": "El paso no corresponde al taller"}), 400

        rows = await conn.fetch(
            """
            SELECT o.id, o.description
            FROM observations o
            JOIN observation_subcategories osc ON osc.id = o.subcategory_id
            WHERE o.workshop_id = $1 
              AND o.step_id = $2 
              AND osc.category_id = $3
              AND o.is_active = TRUE
            ORDER BY o.sort_order NULLS LAST, o.id ASC
            """,
            workshop_id, step_id, category_id
        )

    return jsonify([{"id": r["id"], "description": r["description"]} for r in rows]), 200


@workshops_bp.route("/<int:workshop_id>/steps/<int:step_id>/categories/<int:category_id>/observations", methods=["POST"])
async def create_category_observation(workshop_id: int, step_id: int, category_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    data = await request.get_json() or {}
    desc = (data.get("description") or "").strip()
    if not desc:
        return jsonify({"error": "Falta description"}), 400
    if len(desc) > 300:
        return jsonify({"error": "La descripción no puede superar 300 caracteres"}), 400

    async with get_conn_ctx() as conn:
        belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
        if not belongs:
            return jsonify({"error": "No tenés acceso a este taller"}), 403
        ok = await _step_belongs_to_workshop(conn, step_id, workshop_id)
        if not ok:
            return jsonify({"error": "El paso no corresponde al taller"}), 400

        # Garantizar subcategoría General
        subcat = await conn.fetchrow(
            "SELECT id FROM observation_subcategories WHERE category_id = $1 AND name = $2",
            category_id, SUBCAT_NAME
        )
        if not subcat:
            subcat = await conn.fetchrow(
                """
                INSERT INTO observation_subcategories (category_id, name)
                VALUES ($1, $2)
                RETURNING id
                """,
                category_id, SUBCAT_NAME
            )

        row = await conn.fetchrow(
            """
            INSERT INTO observations (workshop_id, step_id, subcategory_id, description)
            VALUES ($1, $2, $3, $4)
            RETURNING id, description
            """,
            workshop_id, step_id, subcat["id"], desc
        )

    return jsonify({"id": row["id"], "description": row["description"]}), 201


@workshops_bp.route("/<int:workshop_id>/steps/<int:step_id>/categories/<int:category_id>/observations/<int:obs_id>", methods=["PUT", "PATCH"])
async def update_category_observation(workshop_id: int, step_id: int, category_id: int, obs_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    data = await request.get_json() or {}
    desc = (data.get("description") or "").strip()
    if not desc:
        return jsonify({"error": "Falta description"}), 400
    if len(desc) > 300:
        return jsonify({"error": "La descripción no puede superar 300 caracteres"}), 400

    async with get_conn_ctx() as conn:
        belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
        if not belongs:
            return jsonify({"error": "No tenés acceso a este taller"}), 403
        ok = await _step_belongs_to_workshop(conn, step_id, workshop_id)
        if not ok:
            return jsonify({"error": "El paso no corresponde al taller"}), 400

        # Validar que el obs pertenezca a la categoría
        exists = await conn.fetchval(
            """
            SELECT 1
            FROM observations o
            JOIN observation_subcategories osc ON osc.id = o.subcategory_id
            WHERE o.id = $1 AND o.workshop_id = $2 AND o.step_id = $3 AND osc.category_id = $4
            """,
            obs_id, workshop_id, step_id, category_id
        )
        if not exists:
            return jsonify({"error": "Observación no encontrada en la categoría"}), 404

        row = await conn.fetchrow(
            """
            UPDATE observations
            SET description = $1
            WHERE id = $2
            RETURNING id, description
            """,
            desc, obs_id
        )

    return jsonify({"id": row["id"], "description": row["description"]}), 200


@workshops_bp.route("/<int:workshop_id>/steps/<int:step_id>/categories/<int:category_id>/observations/<int:obs_id>", methods=["DELETE"])
async def delete_category_observation(workshop_id: int, step_id: int, category_id: int, obs_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
        if not belongs:
            return jsonify({"error": "No tenés acceso a este taller"}), 403
        ok = await _step_belongs_to_workshop(conn, step_id, workshop_id)
        if not ok:
            return jsonify({"error": "El paso no corresponde al taller"}), 400

        # Validar pertenencia a la categoría
        exists = await conn.fetchval(
            """
            SELECT 1
            FROM observations o
            JOIN observation_subcategories osc ON osc.id = o.subcategory_id
            WHERE o.id = $1 AND o.workshop_id = $2 AND o.step_id = $3 AND osc.category_id = $4
            """,
            obs_id, workshop_id, step_id, category_id
        )
        if not exists:
            return jsonify({"error": "Observación no encontrada en la categoría"}), 404

        async with conn.transaction():
            await conn.execute(
                "DELETE FROM observation_details WHERE observation_id = $1",
                obs_id
            )
            await conn.execute(
                "DELETE FROM observations WHERE id = $1",
                obs_id
            )

    return jsonify({"message": "Observación eliminada"}), 200

@workshops_bp.route("/get-all-workshops", methods=["GET"])
async def get_all_workshops():
    async with get_conn_ctx() as conn:
        rows = await conn.fetch("SELECT id, name, province, city, phone, cuit, plant_number, disposition_number, razon_social, address, disposition_number, available_inspections FROM workshop where is_approved = true ORDER BY id ASC")

    result = [{"id": r["id"], "name": r["name"], "province": r["province"], "city": r["city"], "phone": r["phone"], "cuit": r["cuit"], "plant_number": r["plant_number"], "disposition_number": r["disposition_number"], "razon_social": r["razon_social"], "address": r["address"], "available_inspections": r["available_inspections"]} for r in rows]

    return jsonify({"workshops": result}), 200