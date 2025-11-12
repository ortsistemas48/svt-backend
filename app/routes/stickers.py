# stickers_bp.py
from quart import Blueprint, request, jsonify
from app.db import get_conn_ctx
from datetime import date, datetime

stickers_bp = Blueprint("stickers", __name__, url_prefix="/stickers")


def _norm_plate(p: str | None) -> str | None:
    if not p:
        return None
    return p.strip().upper().replace("-", "").replace(" ", "")


@stickers_bp.route("/available", methods=["GET"])
async def list_available_stickers():
    workshop_id = request.args.get("workshop_id", type=int)
    if not workshop_id:
        return jsonify({"error": "workshop_id requerido"}), 400

    async with get_conn_ctx() as conn:
        rows = await conn.fetch(
            """
            SELECT
              s.id,
              s.sticker_number,
              s.expiration_date,
              s.issued_at,
              s.status
            FROM stickers s
            JOIN sticker_orders so ON so.id = s.sticker_order_id
            LEFT JOIN cars c        ON c.sticker_id = s.id
            WHERE
              so.workshop_id = $1
              AND lower(s.status) = 'disponible'
              AND (s.expiration_date IS NULL OR s.expiration_date >= CURRENT_DATE)
              AND c.id IS NULL                    -- no asignada a ningún auto
            ORDER BY s.id ASC
            """,
            workshop_id,
        )

    out = []
    for r in rows:
        d = dict(r)
        for k in ("expiration_date", "issued_at"):
            if d.get(k) is not None and hasattr(d[k], "isoformat"):
                d[k] = d[k].isoformat()
        out.append(d)

    return jsonify(out), 200


@stickers_bp.route("/available-orders", methods=["GET"])
async def list_available_orders():
    workshop_id = request.args.get("workshop_id", type=int)
    if not workshop_id:
        return jsonify({"error": "workshop_id requerido"}), 400

    async with get_conn_ctx() as conn:
        rows = await conn.fetch(
            """
            SELECT
              so.id,
              so.name,
              COUNT(s.id) AS available_count
            FROM stickers s
            JOIN sticker_orders so ON so.id = s.sticker_order_id
            LEFT JOIN cars c        ON c.sticker_id = s.id
            WHERE
              so.workshop_id = $1
              AND lower(s.status) = 'disponible'
              AND (s.expiration_date IS NULL OR s.expiration_date >= CURRENT_DATE)
              AND c.id IS NULL
            GROUP BY so.id, so.name
            ORDER BY so.id DESC
            """,
            workshop_id,
        )

    out = [dict(r) for r in rows]
    print(out)
    return jsonify(out), 200


@stickers_bp.route("/list-orders", methods=["GET"])
async def list_orders():
    workshop_id = request.args.get("workshop_id", type=int)
    if not workshop_id:
        return jsonify({"error": "workshop_id requerido"}), 400

    sql = """
    WITH counts AS (
      SELECT
        so.id,
        COUNT(s.id) FILTER (WHERE s.status = 'Disponible' AND c.id IS NULL) AS available_count
      FROM sticker_orders so
      LEFT JOIN stickers s ON s.sticker_order_id = so.id
      LEFT JOIN cars c     ON c.sticker_id = s.id
      WHERE so.workshop_id = $1
      GROUP BY so.id
    )
    SELECT
      so.*,
      COALESCE(counts.available_count, 0) AS available_count
    FROM sticker_orders so
    LEFT JOIN counts ON counts.id = so.id
    WHERE so.workshop_id = $1
    ORDER BY so.id DESC
    """

    async with get_conn_ctx() as conn:
        rows = await conn.fetch(sql, workshop_id)

    out = [dict(r) for r in rows]
    return jsonify(out), 200


@stickers_bp.route("/orders", methods=["GET"])
async def list_sticker_orders():
    workshop_id = request.args.get("workshop_id", type=int)
    if not workshop_id:
        return jsonify({"error": "workshop_id requerido"}), 400

    async with get_conn_ctx() as conn:
        rows = await conn.fetch(
            """
            SELECT
              so.id,
              so.name,
              so.status,
              so.amount,
              so.created_at,
              COUNT(s.id) AS available_count
            FROM sticker_orders so
            LEFT JOIN stickers s ON s.sticker_order_id = so.id
              AND lower(s.status) = 'disponible'
              AND (s.expiration_date IS NULL OR s.expiration_date >= CURRENT_DATE)
              AND NOT EXISTS (SELECT 1 FROM cars c WHERE c.sticker_id = s.id)
            WHERE so.workshop_id = $1
            GROUP BY so.id, so.name, so.status, so.amount, so.created_at
            ORDER BY so.id DESC
            """,
            workshop_id,
        )

    out = []
    for r in rows:
        d = dict(r)
        if d.get("created_at") is not None and hasattr(d["created_at"], "isoformat"):
            d["created_at"] = d["created_at"].isoformat()
        out.append(d)
    return jsonify(out), 200


@stickers_bp.route("/<int:sticker_id>", methods=["GET"])
async def get_sticker(sticker_id: int):
    async with get_conn_ctx() as conn:
        row = await conn.fetchrow(
            """
            SELECT
              s.id,
              s.sticker_number,
              s.expiration_date,
              s.issued_at,
              s.status,
              s.sticker_order_id,
              so.workshop_id
            FROM stickers s
            JOIN sticker_orders so ON so.id = s.sticker_order_id
            WHERE s.id = $1
            """,
            sticker_id
        )
    return jsonify(dict(row) if row else None), 200


@stickers_bp.route("/assign-to-car", methods=["POST"])
async def assign_sticker_to_car():
    data = await request.get_json()
    license_plate = _norm_plate(data.get("license_plate"))
    sticker_id = data.get("sticker_id")
    workshop_id = data.get("workshop_id", None)
    mark_used = True

    if not license_plate or not sticker_id:
        return jsonify({"error": "license_plate y sticker_id son requeridos"}), 400

    async with get_conn_ctx() as conn:
        async with conn.transaction():
            ok = await conn.fetchval(
                """
                SELECT CASE WHEN COUNT(*)>0 THEN true ELSE false END
                FROM stickers s
                JOIN sticker_orders so ON so.id = s.sticker_order_id
                LEFT JOIN cars c        ON c.sticker_id = s.id
                WHERE s.id = $1
                  AND (c.id IS NULL OR c.license_plate = $2)
                  AND ($3::bigint IS NULL OR so.workshop_id = $3)
                """,
                sticker_id, license_plate, workshop_id
            )
            if not ok:
                return jsonify({"error": "Oblea inválida o ya asignada"}), 400

            car_id = await conn.fetchval(
                """
                INSERT INTO cars (license_plate, sticker_id)
                VALUES ($1, $2)
                ON CONFLICT (license_plate) DO UPDATE
                  SET sticker_id = EXCLUDED.sticker_id
                RETURNING id
                """,
                license_plate, sticker_id
            )

            if mark_used:
                await conn.execute(
                    "UPDATE stickers SET status = 'En Uso' WHERE id = $1",
                    sticker_id
                )

    return jsonify({"ok": True, "car_id": car_id, "sticker_id": sticker_id})


@stickers_bp.route("/unassign-from-car", methods=["POST"])
async def unassign_sticker_from_car():
    data = await request.get_json()
    license_plate = _norm_plate(data.get("license_plate"))
    set_available = bool(data.get("set_available", False))

    if not license_plate:
        return jsonify({"error": "license_plate requerido"}), 400

    async with get_conn_ctx() as conn:
        async with conn.transaction():
            old_sticker = await conn.fetchval(
                "SELECT sticker_id FROM cars WHERE license_plate = $1",
                license_plate
            )

            await conn.execute(
                "UPDATE cars SET sticker_id = NULL WHERE license_plate = $1",
                license_plate
            )

            if set_available and old_sticker:
                await conn.execute(
                    "UPDATE stickers SET status = 'Disponible' WHERE id = $1",
                    old_sticker
                )
            elif set_available == False and old_sticker:
                await conn.execute(
                    "UPDATE stickers SET status = 'No Disponible' WHERE id = $1",
                    old_sticker
                )

    return jsonify({"ok": True})


@stickers_bp.route("/<int:sticker_id>/mark-used", methods=["POST"])
async def mark_sticker_as_used(sticker_id: int):
    async with get_conn_ctx() as conn:
        await conn.execute(
            "UPDATE stickers SET status = 'En Uso' WHERE id = $1",
            sticker_id
        )
        
    return jsonify({"ok": True, "sticker_id": sticker_id, "status": "En Uso"}), 200


@stickers_bp.route("/workshop/<int:workshop_id>", methods=["GET"])
async def get_stickers_by_workshop(workshop_id: int):
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 20, type=int)
    
    if page < 1:
        page = 1
    if per_page < 1:
        per_page = 20
    if per_page > 100:
        per_page = 100
    
    offset = (page - 1) * per_page

    async with get_conn_ctx() as conn:
        total_count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM stickers s
            JOIN sticker_orders so ON so.id = s.sticker_order_id
            WHERE so.workshop_id = $1
            """,
            workshop_id,
        )
        
        rows = await conn.fetch(
            """
            SELECT
              s.id,
              s.sticker_number,
              s.expiration_date,
              s.issued_at,
              s.status,
              s.sticker_order_id,
              so.name as order_name,
              so.workshop_id,
              c.license_plate
            FROM stickers s
            JOIN sticker_orders so ON so.id = s.sticker_order_id
            LEFT JOIN cars c ON c.sticker_id = s.id
            WHERE so.workshop_id = $1
            ORDER BY s.id DESC
            LIMIT $2 OFFSET $3
            """,
            workshop_id, per_page, offset,
        )

    stickers = []
    for r in rows:
        d = dict(r)
        for k in ("expiration_date", "issued_at"):
            if d.get(k) is not None and hasattr(d[k], "isoformat"):
                d[k] = d[k].isoformat()
        stickers.append(d)

    total_pages = (total_count + per_page - 1) // per_page
    has_next = page < total_pages
    has_prev = page > 1

    response = {
        "stickers": stickers,
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total_count,
            "total_pages": total_pages,
            "has_next": has_next,
            "has_prev": has_prev,
            "next_page": page + 1 if has_next else None,
            "prev_page": page - 1 if has_prev else None
        }
    }

    return jsonify(response), 200


# stickers_bp.py

@stickers_bp.route("/<int:sticker_id>/status", methods=["PATCH"])
async def set_sticker_status(sticker_id: int):
    data = await request.get_json(silent=True) or {}
    raw = str(data.get("status", "")).strip().lower()

    #por las dudas esto
    norm_map = {
        "disponible": "Disponible",
        "no disponible": "No Disponible",
        "nodisponible": "No Disponible",
        "en uso": "En Uso",
        "enuso": "En Uso",
    }
    status = norm_map.get(raw)
    if status is None:
        return jsonify({
            "error": "status inválido",
            "allowed": ["Disponible", "No Disponible", "En Uso"]
        }), 400

    async with get_conn_ctx() as conn:
        row = await conn.fetchrow(
            "UPDATE stickers SET status = $1 WHERE id = $2 RETURNING id, status",
            status, sticker_id
        )

    if not row:
        return jsonify({"error": "sticker no encontrado"}), 404

    return jsonify({"ok": True, "sticker_id": row["id"], "status": row["status"]}), 200


@stickers_bp.route("/orders", methods=["POST"])
async def create_sticker_order():
    """
    Crea una orden y hace bulk-insert de obleas.
    Body JSON:
      - workshop_id: int, requerido
      - name: str, opcional (si no se proporciona, se genera automáticamente como "obleas-{orderId}")
      - note: str, opcional
      - stickers: [str], requerido
      - expiration_date: "YYYY-MM-DD" o null, opcional, se aplica a todas
    """
    data = await request.get_json(silent=True) or {}
    workshop_id = data.get("workshop_id")
    name = str(data.get("name", "")).strip()
    note = str(data.get("note", "")).strip() or None
    stickers = data.get("stickers") or []
    exp_raw = data.get("expiration_date", None)

    if not isinstance(workshop_id, int):
        return jsonify({"error": "workshop_id requerido"}), 400
    if not isinstance(stickers, list) or len(stickers) == 0:
        return jsonify({"error": "stickers debe ser lista no vacía"}), 400

    expiration_date = None
    if isinstance(exp_raw, str) and exp_raw.strip():
        try:
            expiration_date = date.fromisoformat(exp_raw.strip())
        except ValueError:
            return jsonify({"error": "expiration_date inválido, formato esperado YYYY-MM-DD"}), 400

    norm_numbers = []
    for s in stickers:
        if isinstance(s, str) and s.strip():
            norm_numbers.append(s.strip())

    if not norm_numbers:
        return jsonify({"error": "stickers no válidos"}), 400

    async with get_conn_ctx() as conn:
        async with conn.transaction():
            order_row = await conn.fetchrow(
                """
                INSERT INTO sticker_orders (workshop_id, name, note, status)
                VALUES ($1, $2, $3, 'Creada')
                RETURNING id, workshop_id, name, note, status, created_at
                """,
                workshop_id, name, note
            )
            order_id = order_row["id"]
            
            # Si el nombre está vacío, generar automáticamente con formato obleas-{orderId}
            if not name:
                auto_name = f"obleas-{order_id}"
                await conn.execute(
                    "UPDATE sticker_orders SET name = $1 WHERE id = $2",
                    auto_name, order_id
                )
                # Actualizar order_row para usar el nombre generado en la respuesta
                order_row = dict(order_row)
                order_row["name"] = auto_name

            existing = await conn.fetch(
                "SELECT sticker_number FROM stickers WHERE sticker_number = ANY($1::text[])",
                norm_numbers
            )
            existing_set = {r["sticker_number"] for r in existing}
            to_insert = [s for s in norm_numbers if s not in existing_set]

            inserted = 0
            BATCH = 1000
            status_disponible = "Disponible"

            for i in range(0, len(to_insert), BATCH):
                batch = to_insert[i:i+BATCH]
                # Insert con expiration_date opcional
                await conn.execute(
                    """
                    INSERT INTO stickers (sticker_order_id, sticker_number, status, expiration_date)
                    SELECT $1, s, $2, $3 FROM UNNEST($4::text[]) AS t(s)
                    """,
                    order_id, status_disponible, expiration_date, batch
                )
                inserted += len(batch)

            await conn.execute(
                "UPDATE sticker_orders SET amount = COALESCE(amount, 0) + $1 WHERE id = $2",
                inserted, order_id
            )

    return jsonify({
        "ok": True,
        "order": {
            "id": order_row["id"],
            "name": order_row["name"],
            "workshop_id": order_row["workshop_id"],
            "note": order_row["note"],
            "status": order_row["status"],
            "created_at": order_row["created_at"].isoformat() if order_row.get("created_at") else None,
            "amount": inserted
        },
        "inserted": inserted,
        "duplicates": sorted(existing_set) if existing_set else []
    }), 201


@stickers_bp.route("/orders/<int:order_id>", methods=["GET"])
async def get_sticker_order(order_id: int):
    async with get_conn_ctx() as conn:
        order = await conn.fetchrow(
            "SELECT id, workshop_id, name, note, status, amount, created_at FROM sticker_orders WHERE id = $1",
            order_id
        )
        if not order:
            return jsonify({"error": "orden no encontrada"}), 404

        rows = await conn.fetch(
            """
            SELECT id, sticker_number, status, expiration_date, issued_at
            FROM stickers
            WHERE sticker_order_id = $1
            ORDER BY id ASC
            """,
            order_id
        )

    stickers = []
    for r in rows:
        d = dict(r)
        for k in ("expiration_date", "issued_at"):
            if d.get(k) is not None and hasattr(d[k], "isoformat"):
                d[k] = d[k].isoformat()
        stickers.append(d)

    out = dict(order)
    if out.get("created_at") is not None and hasattr(out["created_at"], "isoformat"):
        out["created_at"] = out["created_at"].isoformat()

    return jsonify({"order": out, "stickers": stickers}), 200


@stickers_bp.route("/orders/<int:order_id>/add-stickers", methods=["POST"])
async def add_stickers_to_order(order_id: int):
    data = await request.get_json(silent=True) or {}
    stickers = data.get("stickers") or []
    exp_raw = data.get("expiration_date", None)

    if not isinstance(stickers, list) or not stickers:
        return jsonify({"error": "stickers debe ser lista no vacía"}), 400

    expiration_date = None
    if isinstance(exp_raw, str) and exp_raw.strip():
        try:
            expiration_date = date.fromisoformat(exp_raw.strip())
        except ValueError:
            return jsonify({"error": "expiration_date inválido, formato esperado YYYY-MM-DD"}), 400

    norm_numbers = []
    for s in stickers:
        if isinstance(s, str) and s.strip():
            norm_numbers.append(s.strip())

    async with get_conn_ctx() as conn:
        async with conn.transaction():
            exists = await conn.fetchval("SELECT 1 FROM sticker_orders WHERE id = $1", order_id)
            if not exists:
                return jsonify({"error": "orden no encontrada"}), 404

            existing = await conn.fetch(
                "SELECT sticker_number FROM stickers WHERE sticker_number = ANY($1::text[])",
                norm_numbers
            )
            existing_set = {r["sticker_number"] for r in existing}
            to_insert = [s for s in norm_numbers if s not in existing_set]

            inserted = 0
            BATCH = 1000
            status_disponible = "Disponible"
            for i in range(0, len(to_insert), BATCH):
                batch = to_insert[i:i+BATCH]
                await conn.execute(
                    """
                    INSERT INTO stickers (sticker_order_id, sticker_number, status, expiration_date)
                    SELECT $1, s, $2, $3 FROM UNNEST($4::text[]) AS t(s)
                    """,
                    order_id, status_disponible, expiration_date, batch
                )
                inserted += len(batch)

            await conn.execute(
                "UPDATE sticker_orders SET amount = COALESCE(amount, 0) + $1 WHERE id = $2",
                inserted, order_id
            )

    return jsonify({"ok": True, "inserted": inserted, "duplicates": sorted(existing_set)}), 200


@stickers_bp.route("/next-available", methods=["GET"])
async def get_next_available_sticker():
    sticker_order_id = request.args.get("sticker_order_id", type=int)
    if not isinstance(sticker_order_id, int):
        return jsonify({"error": "sticker_order_id requerido"}), 400

    async with get_conn_ctx() as conn:
        row = await conn.fetchrow(
            """
            SELECT s.id, s.sticker_number
            FROM stickers s
            WHERE s.sticker_order_id = $1
              AND lower(s.status) = 'disponible'
              AND (s.expiration_date IS NULL OR s.expiration_date >= CURRENT_DATE)
              AND NOT EXISTS (SELECT 1 FROM cars c WHERE c.sticker_id = s.id)
            ORDER BY s.id ASC
            LIMIT 1
            """,
            sticker_order_id
        )

    if not row:
        return jsonify({"error": "No hay obleas disponibles en esa orden"}), 404

    return jsonify({"id": row["id"], "sticker_number": row["sticker_number"]}), 200


def _norm_sticker_number(raw: str) -> str:
    return str(raw or "").upper().replace(" ", "").replace("-", "").replace("_", "").replace("/", "").replace(".", "")


async def _get_or_create_manual_order(conn, workshop_id: int) -> int:
    today_str = datetime.now().date().isoformat()
    name = f"Manual {today_str}"

    row = await conn.fetchrow(
        "SELECT id FROM sticker_orders WHERE workshop_id = $1 AND name = $2",
        workshop_id, name
    )
    if row:
        return row["id"]

    row = await conn.fetchrow(
        """
        INSERT INTO sticker_orders (workshop_id, name, note, status, amount)
        VALUES ($1, $2, $3, 'Creada', 0)
        RETURNING id
        """,
        workshop_id, name, "Alta manual de obleas"
    )
    return row["id"]


@stickers_bp.route("/assign-by-number", methods=["POST"])
async def assign_by_number():
    data = await request.get_json(silent=True) or {}
    license_plate = _norm_plate(data.get("license_plate"))
    workshop_id = data.get("workshop_id")
    raw_number = data.get("sticker_number")
    mark_used = True
    forced_order_id = data.get("sticker_order_id", None)

    if not license_plate:
        return jsonify({"error": "license_plate requerido"}), 400
    if not isinstance(workshop_id, int):
        return jsonify({"error": "workshop_id requerido"}), 400
    if not raw_number:
        return jsonify({"error": "sticker_number requerido"}), 400

    sticker_number = _norm_sticker_number(raw_number)

    async with get_conn_ctx() as conn:
        async with conn.transaction():
            existing = await conn.fetchrow(
                """
                SELECT s.id, s.status, so.workshop_id, c.id AS car_id
                FROM stickers s
                JOIN sticker_orders so ON so.id = s.sticker_order_id
                LEFT JOIN cars c        ON c.sticker_id = s.id
                WHERE s.sticker_number = $1
                """,
                sticker_number
            )

            sticker_id = None

            if existing:
                if existing["workshop_id"] != workshop_id:
                    return jsonify({"error": "La oblea existe en otro taller, no puede asignarse aquí"}), 409

                if existing["car_id"] is not None:
                    return jsonify({"error": "La oblea ya está asignada a un auto en este taller"}), 409

                if str(existing["status"]).strip().lower() != "disponible":
                    return jsonify({"error": f"Oblea no disponible, estado actual: {existing['status']}"}), 409

                sticker_id = existing["id"]

            else:
                if isinstance(forced_order_id, int):
                    ok = await conn.fetchval(
                        "SELECT 1 FROM sticker_orders WHERE id = $1 AND workshop_id = $2",
                        forced_order_id, workshop_id
                    )
                    if not ok:
                        return jsonify({"error": "sticker_order_id no pertenece a este taller"}), 400
                    order_id = forced_order_id
                else:
                    order_id = await _get_or_create_manual_order(conn, workshop_id)

                row = await conn.fetchrow(
                    """
                    INSERT INTO stickers (sticker_order_id, sticker_number, status, expiration_date)
                    VALUES ($1, $2, 'Disponible', NULL)
                    ON CONFLICT (sticker_number) DO NOTHING
                    RETURNING id
                    """,
                    order_id, sticker_number
                )

                if not row:
                    again = await conn.fetchrow(
                        """
                        SELECT s.id, s.status, so.workshop_id, c.id AS car_id
                        FROM stickers s
                        JOIN sticker_orders so ON so.id = s.sticker_order_id
                        LEFT JOIN cars c        ON c.sticker_id = s.id
                        WHERE s.sticker_number = $1
                        """,
                        sticker_number
                    )
                    if not again:
                        return jsonify({"error": "No se pudo crear la oblea"}), 500
                    if again["workshop_id"] != workshop_id:
                        return jsonify({"error": "La oblea existe en otro taller, no puede asignarse aquí"}), 409
                    if again["car_id"] is not None:
                        return jsonify({"error": "La oblea ya está asignada a un auto en este taller"}), 409
                    if str(again["status"]).strip().lower() != "disponible":
                        return jsonify({"error": f"Oblea no disponible, estado actual: {again['status']}"}), 409
                    sticker_id = again["id"]
                else:
                    sticker_id = row["id"]

                await conn.execute(
                    "UPDATE sticker_orders SET amount = COALESCE(amount, 0) + 1 WHERE id = $1",
                    order_id
                )

            car_id = await conn.fetchval(
                """
                INSERT INTO cars (license_plate, sticker_id)
                VALUES ($1, $2)
                ON CONFLICT (license_plate) DO UPDATE
                  SET sticker_id = EXCLUDED.sticker_id
                RETURNING id
                """,
                license_plate, sticker_id
            )

            if mark_used:
                await conn.execute("UPDATE stickers SET status = 'En Uso' WHERE id = $1", sticker_id)

    return jsonify({
        "ok": True,
        "car_id": car_id,
        "sticker_id": sticker_id,
        "sticker_number": sticker_number
    }), 200
