import pytest
from app import create_app
from app.db import get_conn, init_db
from httpx import AsyncClient, ASGITransport


@pytest.fixture(scope="module")
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope="module")
async def test_client():
    await init_db()
    app = create_app()

    transport = ASGITransport(app=app)  # lifespan="on" puede fallar en algunas versiones
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


# Registro
@pytest.mark.anyio
async def test_register_success(test_client):
    conn = await get_conn()
    await conn.execute("DELETE FROM users WHERE email LIKE 'test_user_%@example.com'")

    response = await test_client.post("/auth/register", json={
        "email": "test_user_1@example.com",
        "password": "123456",
        "confirm_password": "123456",
        "first_name": "Test",
        "last_name": "User",
        "dni": 11111111,
        "phone_number": "3510000000",
        "user_type": 4
    })
    assert response.status_code == 201


@pytest.mark.anyio
async def test_register_existing_email(test_client):
    email = "test_user_2@example.com"
    await test_client.post("/auth/register", json={
        "email": email,
        "password": "123456",
        "confirm_password": "123456",
        "first_name": "Test",
        "last_name": "User",
        "dni": 22222222,
        "phone_number": "3510000001",
        "user_type": 4
    })
    response = await test_client.post("/auth/register", json={
        "email": email,
        "password": "123456",
        "confirm_password": "123456",
        "first_name": "Test",
        "last_name": "Duplicate",
        "dni": 33333333,
        "phone_number": "3510000002",
        "user_type": 4
    })
    assert response.status_code == 400
    assert response.json()["error"] == "El email ya está en uso"


@pytest.mark.anyio
async def test_register_password_mismatch(test_client):
    response = await test_client.post("/auth/register", json={
        "email": "test_user_3@example.com",
        "password": "123456",
        "confirm_password": "654321",
        "first_name": "Mismatch",
        "last_name": "Password",
        "dni": 44444444,
        "phone_number": "3510000003",
        "user_type": 4
    })
    assert response.status_code == 400
    assert response.json()["error"] == "Las contraseñas no coinciden"


@pytest.mark.anyio
async def test_register_missing_fields(test_client):
    response = await test_client.post("/auth/register", json={
        "email": "test_user_4@example.com",
        "password": "123456",
        "confirm_password": "123456"
    })
    assert response.status_code == 400
    assert "obligatorios" in response.json()["error"]


# Login
@pytest.mark.anyio
async def test_login_success(test_client):
    await test_client.post("/auth/register", json={
        "email": "test_user_5@example.com",
        "password": "123456",
        "confirm_password": "123456",
        "first_name": "Login",
        "last_name": "Success",
        "dni": 55555555,
        "phone_number": "3510000004",
        "user_type": 4
    })
    response = await test_client.post("/auth/login", json={
        "email": "test_user_5@example.com",
        "password": "123456"
    })
    assert response.status_code == 200
    assert response.json()["message"] == "Login exitoso"
    assert response.cookies.get("token") is not None


@pytest.mark.anyio
async def test_login_wrong_password(test_client):
    response = await test_client.post("/auth/login", json={
        "email": "test_user_5@example.com",
        "password": "wrongpass"
    })
    assert response.status_code == 401
    assert response.json()["error"] == "Credenciales inválidas"


# Me
@pytest.mark.anyio
async def test_me_success(test_client):
    login_res = await test_client.post("/auth/login", json={
        "email": "test_user_5@example.com",
        "password": "123456"
    })
    token = login_res.cookies.get("token")
    test_client.cookies.set("token", token)

    response = await test_client.get("/auth/me")
    assert response.status_code == 200
    assert response.json()["user"]["email"] == "test_user_5@example.com"


# Logout
@pytest.mark.anyio
async def test_logout(test_client):
    login_res = await test_client.post("/auth/login", json={
        "email": "test_user_5@example.com",
        "password": "123456"
    })
    token = login_res.cookies.get("token")
    test_client.cookies.set("token", token)

    response = await test_client.post("/auth/logout")
    assert response.status_code == 200
    assert response.json()["message"] == "Sesión cerrada correctamente"
