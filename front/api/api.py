import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Depends, HTTPException, status, Query
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from jose import JWTError, jwt
import uvicorn

# --- CONFIGURATION ---
SECRET_KEY = "super-secret-key-instagram-project"  # À changer en prod
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

# Config DB
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "instagram_db")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASS = os.getenv("DB_PASS", "password")
DB_PORT = os.getenv("DB_PORT", "5431")

app = FastAPI(
    title="Instagram Gold API",
    description="API pour les données Gold de l'architecture médaillon",
    version="1.0.0"
)

# Pour gérer l'auth dans le Swagger UI (le bouton cadenas)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login")


# --- MODÈLES PYDANTIC (Validation des données entrantes) ---
class LoginRequest(BaseModel):
    username: str
    password: str


class Token(BaseModel):
    access_token: str
    token_type: str


# --- UTILITAIRES DB ---
def get_db_connection():
    """Crée une connexion à la base de données."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        return conn
    except Exception as e:
        print(f"Erreur de connexion DB: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")


# --- UTILITAIRES AUTHENTIFICATION ---
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def get_current_user(token: str = Depends(oauth2_scheme)):
    """Dépendance pour protéger les routes."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    return username


# --- ENDPOINTS ---

# 1. Login
@app.post("/auth/login", response_model=Token)
def login(login_data: LoginRequest):
    # Simule une authentification basique
    if login_data.username == "admin" and login_data.password == "admin":
        access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = create_access_token(
            data={"sub": login_data.username}, expires_delta=access_token_expires
        )
        return {"access_token": access_token, "token_type": "bearer"}
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Bad username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )


# 2. Statistiques d'Engagement
@app.get("/api/engagement_stats")
def get_engagement_stats(
        country: Optional[str] = None,
        segment: Optional[str] = None,
        page: int = Query(1, ge=1),  # page >= 1
        per_page: int = Query(10, le=100),  # max 100 par page
        current_user: str = Depends(get_current_user)  # Route protégée
):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    offset = (page - 1) * per_page

    # Requête SQL
    query = """
            SELECT country,
                   lifestyle_segment,
                   content_type_preference,
                   preferred_content_theme,
                   avg_engagement,
                   avg_efficiency,
                   total_users
            FROM public.gold_engagement_stats
            WHERE 1 = 1
            """
    params = []

    if country:
        query += " AND country = %s"
        params.append(country)
    if segment:
        query += " AND lifestyle_segment = %s"
        params.append(segment)

    query += " LIMIT %s OFFSET %s"
    params.extend([per_page, offset])

    try:
        cursor.execute(query, tuple(params))
        results = cursor.fetchall()
        return {
            "page": page,
            "per_page": per_page,
            "data": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# 3. Recommandations Utilisateurs
@app.get("/api/recommendations")
def get_recommendations(
        segment: Optional[str] = None,
        max_rank: int = 3,
        page: int = Query(1, ge=1),
        per_page: int = Query(20, le=100),
        current_user: str = Depends(get_current_user)
):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    offset = (page - 1) * per_page

    query = """
            SELECT user_id,
                   country,
                   lifestyle_segment,
                   content_type_preference,
                   preferred_content_theme,
                   engagement_rank
            FROM public.gold_top_recommendations
            WHERE engagement_rank <= %s
            """
    params = [max_rank]

    if segment:
        query += " AND lifestyle_segment = %s"
        params.append(segment)

    query += " LIMIT %s OFFSET %s"
    params.extend([per_page, offset])

    try:
        cursor.execute(query, tuple(params))
        results = cursor.fetchall()
        return {
            "page": page,
            "per_page": per_page,
            "filters": {"max_rank": max_rank, "segment": segment},
            "data": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# 4. Santé & Churn
@app.get("/api/user_health")
def get_user_health(current_user: str = Depends(get_current_user)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    query = """
            SELECT lifestyle_segment,
                   avg_wellbeing_score,
                   avg_days_inactive,
                   potential_churners
            FROM public.gold_user_health
            ORDER BY potential_churners DESC
            """
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        return {"data": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    # Lancement avec Uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)