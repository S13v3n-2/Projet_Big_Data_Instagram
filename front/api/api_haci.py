# -*- coding: utf-8 -*-
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional, List
import psycopg2
from psycopg2.extras import RealDictCursor
import jwt
from datetime import datetime, timedelta
import os

app = FastAPI(
    title="Instagram Gold API",
    description="API pour les datamarts Gold du projet Instagram Engagement Optimizer",
    version="1.0.0"
)
security = HTTPBearer()

# Configuration (en production : variables d'environnement)
SECRET_KEY = os.getenv("JWT_SECRET", "instagram_secret_key_2026")
ALGORITHM = "HS256"

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5431)),
    "database": os.getenv("DB_NAME", "instagram_db"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "password")
}

# ============ MODELS PYDANTIC ============

class Token(BaseModel):
    access_token: str
    token_type: str

class EngagementStats(BaseModel):
    country: str
    lifestyle_segment: str
    content_type_preference: str
    preferred_content_theme: str
    avg_engagement: float
    avg_efficiency: float
    total_users: int

class TopRecommendation(BaseModel):
    user_id: int
    country: str
    lifestyle_segment: str
    content_type_preference: str
    preferred_content_theme: str
    engagement_rank: int

class UserHealth(BaseModel):
    lifestyle_segment: str
    avg_wellbeing_score: float
    avg_days_inactive: float
    potential_churners: int

# ============ FONCTIONS UTILITAIRES ============

def create_token(username: str) -> str:
    expire = datetime.utcnow() + timedelta(hours=24)
    payload = {"sub": username, "exp": expire}
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    try:
        token = credentials.credentials
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload["sub"]
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expire")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token invalide")

def get_db_connection():
    try:
        return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur connexion DB: {str(e)}")

# ============ ENDPOINTS ============

@app.post("/token", response_model=Token, tags=["Authentification"])
def login(username: str, password: str):
    """
    Génère un token JWT pour l'authentification.
    
    Credentials par défaut:
    - username: admin
    - password: admin123
    """
    if username == "admin" and password == "admin123":
        token = create_token(username)
        return {"access_token": token, "token_type": "bearer"}
    raise HTTPException(status_code=401, detail="Identifiants incorrects")


@app.get("/engagement-stats", tags=["Engagement"])
def get_engagement_stats(
    page: int = Query(1, ge=1, description="Numéro de page"),
    page_size: int = Query(20, ge=1, le=100, description="Lignes par page"),
    country: Optional[str] = Query(None, description="Filtrer par pays"),
    lifestyle_segment: Optional[str] = Query(None, description="Filtrer par segment lifestyle"),
    content_type: Optional[str] = Query(None, description="Filtrer par type de contenu"),
    min_users: Optional[int] = Query(None, ge=0, description="Nombre minimum d'utilisateurs"),
    username: str = Depends(verify_token)
):
    """
    Retourne les statistiques d'engagement par segment.
    
    **Tri par défaut** : avg_engagement DESC
    
    **Filtres disponibles** :
    - country : France, United States, Brazil, etc.
    - lifestyle_segment : Fit Relaxed, Workaholic, Balanced, Sleep Deprived
    - content_type : Reels, Stories, Photos, Live, Videos, Mixed
    - min_users : Filtrer les segments avec au moins N utilisateurs
    """
    offset = (page - 1) * page_size
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Construction requête avec filtres
    query = "SELECT * FROM gold_engagement_stats WHERE 1=1"
    params = []
    
    if country:
        query += " AND country = %s"
        params.append(country)
    if lifestyle_segment:
        query += " AND lifestylesegment = %s"
        params.append(lifestyle_segment)
    if content_type:
        query += " AND contenttypepreference = %s"
        params.append(content_type)
    if min_users:
        query += " AND totalusers >= %s"
        params.append(min_users)
    
    # Count total
    count_query = query.replace("SELECT *", "SELECT COUNT(*)")
    cur.execute(count_query, params)
    total = cur.fetchone()["count"]
    
    # Requête principale avec pagination
    query += " ORDER BY avgengagement DESC LIMIT %s OFFSET %s"
    params.extend([page_size, offset])
    cur.execute(query, params)
    
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    data = [
        {
            "country": r["country"],
            "lifestyle_segment": r["lifestylesegment"],
            "content_type_preference": r["contenttypepreference"],
            "preferred_content_theme": r["preferredcontenttheme"],
            "avg_engagement": float(r["avgengagement"]) if r["avgengagement"] else 0,
            "avg_efficiency": float(r["avgefficiency"]) if r["avgefficiency"] else 0,
            "total_users": int(r["totalusers"])
        }
        for r in rows
    ]
    
    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size,
        "data": data
    }


@app.get("/top-recommendations", tags=["Recommendations"])
def get_top_recommendations(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    country: Optional[str] = Query(None),
    lifestyle_segment: Optional[str] = Query(None),
    max_rank: int = Query(10, ge=1, le=10, description="Rank maximum (1-10)"),
    username: str = Depends(verify_token)
):
    """
    Retourne les top utilisateurs par engagement rank.
    
    **Filtres** :
    - country : Limiter à un pays spécifique
    - lifestyle_segment : Filtrer par segment
    - max_rank : Afficher uniquement les N premiers rangs (1-10)
    """
    offset = (page - 1) * page_size
    conn = get_db_connection()
    cur = conn.cursor()
    
    query = "SELECT * FROM gold_top_recommendations WHERE engagementrank <= %s"
    params = [max_rank]
    
    if country:
        query += " AND country = %s"
        params.append(country)
    if lifestyle_segment:
        query += " AND lifestylesegment = %s"
        params.append(lifestyle_segment)
    
    # Count
    count_query = query.replace("SELECT *", "SELECT COUNT(*)")
    cur.execute(count_query, params)
    total = cur.fetchone()["count"]
    
    # Main query
    query += " ORDER BY country, engagementrank LIMIT %s OFFSET %s"
    params.extend([page_size, offset])
    cur.execute(query, params)
    
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    data = [
        {
            "user_id": r["userid"],
            "country": r["country"],
            "lifestyle_segment": r["lifestylesegment"],
            "content_type_preference": r["contenttypepreference"],
            "preferred_content_theme": r["preferredcontenttheme"],
            "engagement_rank": r["engagementrank"]
        }
        for r in rows
    ]
    
    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "data": data
    }


@app.get("/user-recommendations/{user_id}", tags=["Recommendations"])
def get_user_recommendations(
    user_id: int,
    username: str = Depends(verify_token)
):
    """
    Retourne les recommandations pour un utilisateur spécifique.
    
    Retourne 404 si l'utilisateur n'est pas dans le Top 10 de son pays.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT * FROM gold_top_recommendations
        WHERE userid = %s
    """, (user_id,))
    
    row = cur.fetchone()
    cur.close()
    conn.close()
    
    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"Utilisateur {user_id} non trouvé dans les top recommandations"
        )
    
    return {
        "user_id": row["userid"],
        "country": row["country"],
        "lifestyle_segment": row["lifestylesegment"],
        "content_type_preference": row["contenttypepreference"],
        "preferred_content_theme": row["preferredcontenttheme"],
        "engagement_rank": row["engagementrank"],
        "recommendation": f"Prioriser {row['contenttypepreference']} avec thème {row['preferredcontenttheme']}"
    }


@app.get("/user-health", tags=["Wellbeing"])
def get_user_health(
    order_by: str = Query("potential_churners", regex="^(avg_wellbeing_score|avg_days_inactive|potential_churners)$"),
    order_dir: str = Query("desc", regex="^(asc|desc)$"),
    username: str = Depends(verify_token)
):
    """
    Retourne les indicateurs de santé numérique par segment lifestyle.
    
    **Tri disponible** :
    - avg_wellbeing_score : Score de bien-être (0-100)
    - avg_days_inactive : Moyenne jours d'inactivité
    - potential_churners : Nombre d'utilisateurs à risque de churn
    
    **Direction** : asc | desc
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Mapping sécurisé des colonnes
    col_mapping = {
        "avg_wellbeing_score": "avgwellbeingscore",
        "avg_days_inactive": "avgdaysinactive",
        "potential_churners": "potentialchurners"
    }
    
    order_col = col_mapping[order_by]
    query = f"SELECT * FROM gold_user_health ORDER BY {order_col} {order_dir.upper()}"
    
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return {
        "insights": [
            {
                "lifestyle_segment": r["lifestylesegment"],
                "avg_wellbeing_score": float(r["avgwellbeingscore"]) if r["avgwellbeingscore"] else 0,
                "avg_days_inactive": float(r["avgdaysinactive"]) if r["avgdaysinactive"] else 0,
                "potential_churners": int(r["potentialchurners"]),
                "alert": "HIGH RISK" if r["potentialchurners"] > 300000 else "NORMAL"
            }
            for r in rows
        ]
    }


@app.get("/segments-comparison", tags=["Engagement"])
def compare_segments(
    segment1: str = Query(..., description="Premier segment lifestyle"),
    segment2: str = Query(..., description="Second segment lifestyle"),
    country: Optional[str] = Query(None),
    username: str = Depends(verify_token)
):
    """
    Compare deux segments lifestyle sur les métriques d'engagement.
    
    **Segments valides** :
    - Fit Relaxed
    - Workaholic
    - Balanced
    - Sleep Deprived
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    query = """
        SELECT lifestylesegment, 
               AVG(avgengagement) as avg_eng,
               AVG(avgefficiency) as avg_eff,
               SUM(totalusers) as total_users
        FROM gold_engagement_stats
        WHERE lifestylesegment IN (%s, %s)
    """
    params = [segment1, segment2]
    
    if country:
        query += " AND country = %s"
        params.append(country)
    
    query += " GROUP BY lifestylesegment"
    
    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    if len(rows) < 2:
        raise HTTPException(
            status_code=404,
            detail="Un ou plusieurs segments introuvables"
        )
    
    data = {r["lifestylesegment"]: r for r in rows}
    
    return {
        "segment_1": {
            "name": segment1,
            "avg_engagement": float(data[segment1]["avg_eng"]),
            "avg_efficiency": float(data[segment1]["avg_eff"]),
            "total_users": int(data[segment1]["total_users"])
        },
        "segment_2": {
            "name": segment2,
            "avg_engagement": float(data[segment2]["avg_eng"]),
            "avg_efficiency": float(data[segment2]["avg_eff"]),
            "total_users": int(data[segment2]["total_users"])
        },
        "winner": segment1 if data[segment1]["avg_eng"] > data[segment2]["avg_eng"] else segment2
    }


@app.get("/health", tags=["Monitoring"])
def health_check():
    """Vérification de l'état de l'API et de la connexion DB."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) as c FROM gold_engagement_stats")
        count = cur.fetchone()["c"]
        cur.close()
        conn.close()
        
        return {
            "status": "healthy",
            "message": "API Gold Instagram operationnelle",
            "database": "connected",
            "engagement_stats_count": count,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {
            "status": "degraded",
            "message": str(e),
            "database": "disconnected",
            "timestamp": datetime.utcnow().isoformat()
        }


@app.get("/stats/summary", tags=["Monitoring"])
def get_summary_stats(username: str = Depends(verify_token)):
    """Statistiques globales du projet."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Count par table
    cur.execute("SELECT COUNT(*) as c FROM gold_engagement_stats")
    engagement_count = cur.fetchone()["c"]
    
    cur.execute("SELECT COUNT(*) as c FROM gold_top_recommendations")
    reco_count = cur.fetchone()["c"]
    
    cur.execute("SELECT COUNT(*) as c FROM gold_user_health")
    health_count = cur.fetchone()["c"]
    
    # Top segment
    cur.execute("""
        SELECT lifestylesegment, AVG(avgengagement) as avg_eng
        FROM gold_engagement_stats
        GROUP BY lifestylesegment
        ORDER BY avg_eng DESC
        LIMIT 1
    """)
    top_segment = cur.fetchone()
    
    cur.close()
    conn.close()
    
    return {
        "total_segments": engagement_count,
        "total_top_users": reco_count,
        "total_health_records": health_count,
        "best_segment": {
            "name": top_segment["lifestylesegment"],
            "avg_engagement": float(top_segment["avg_eng"])
        }
    }


# Lancement : uvicorn main:app --reload --host 0.0.0.0 --port 8000
