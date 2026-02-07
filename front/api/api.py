from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional
import psycopg2
import jwt
from datetime import datetime, timedelta

app = FastAPI(title="Instagram Gold API")
security = HTTPBearer()

SECRET_KEY = "instagram_secret_key_2026"
ALGORITHM = "HS256"

DB_CONFIG = {
    "host": "localhost",
    "port": 5431,
    "database": "instagram_db",
    "user": "admin",
    "password": "password"
}

class Token(BaseModel):
    access_token: str
    token_type: str

def create_token(username: str):
    expire = datetime.utcnow() + timedelta(hours=24)
    payload = {"sub": username, "exp": expire}
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        token = credentials.credentials
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload["sub"]
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expire")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token invalide")

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

@app.post("/token", response_model=Token)
def login(username: str, password: str):
    if username == "admin" and password == "admin123":
        token = create_token(username)
        return {"access_token": token, "token_type": "bearer"}
    raise HTTPException(status_code=401, detail="Identifiants incorrects")

@app.get("/recommendations")
def get_recommendations(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    username: str = Depends(verify_token)
):
    offset = (page - 1) * page_size
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM engagement_by_content")
    total = cur.fetchone()[0]
    
    cur.execute("""
        SELECT segment_id, country, lifestyle_segment, avg_engagement_score, 
               total_users, churn_rate
        FROM engagement_by_content
        ORDER BY avg_engagement_score DESC
        LIMIT %s OFFSET %s
    """, (page_size, offset))
    
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    data = [
        {
            "segment_id": r[0],
            "country": r[1],
            "lifestyle_segment": r[2],
            "avg_engagement_score": float(r[3]) if r[3] else 0,
            "total_users": r[4],
            "churn_rate": float(r[5]) if r[5] else 0
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

@app.get("/user-profile/{user_id}")
def get_user_profile(
    user_id: int,
    username: str = Depends(verify_token)
):
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT user_id, persona_cluster, persona_name, predicted_engagement,
               top_content_recommendation, churn_probability, lifetime_value_estimate
        FROM user_segmentation
        WHERE user_id = %s
    """, (user_id,))
    
    row = cur.fetchone()
    cur.close()
    conn.close()
    
    if not row:
        raise HTTPException(status_code=404, detail="Utilisateur non trouve")
    
    return {
        "user_id": row[0],
        "persona_cluster": row[1],
        "persona_name": row[2],
        "predicted_engagement": float(row[3]) if row[3] else 0,
        "top_content_recommendation": row[4],
        "churn_probability": float(row[5]) if row[5] else 0,
        "lifetime_value_estimate": float(row[6]) if row[6] else 0
    }

@app.get("/content-stats")
def get_content_stats(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=50),
    username: str = Depends(verify_token)
):
    offset = (page - 1) * page_size
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM content_performance")
    total = cur.fetchone()[0]
    
    cur.execute("""
        SELECT content_type, content_theme, total_users_preferring,
               avg_engagement_score, rank_in_type
        FROM content_performance
        ORDER BY rank_in_type
        LIMIT %s OFFSET %s
    """, (page_size, offset))
    
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    data = [
        {
            "content_type": r[0],
            "content_theme": r[1],
            "total_users": r[2],
            "avg_engagement": float(r[3]) if r[3] else 0,
            "rank": r[4]
        }
        for r in rows
    ]
    
    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "data": data
    }

@app.get("/wellbeing-insights")
def get_wellbeing(
    username: str = Depends(verify_token)
):
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT lifestyle_segment, avg_stress_score, avg_engagement,
               over_usage_pct, total_users
        FROM lifestyle_impact
        ORDER BY over_usage_pct DESC
    """)
    
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return {
        "insights": [
            {
                "lifestyle_segment": r[0],
                "avg_stress_score": float(r[1]) if r[1] else 0,
                "avg_engagement": float(r[2]) if r[2] else 0,
                "over_usage_pct": float(r[3]) if r[3] else 0,
                "total_users": r[4]
            }
            for r in rows
        ]
    }

@app.get("/health")
def health_check():
    return {"status": "ok", "message": "API Gold Instagram operationnelle"}
