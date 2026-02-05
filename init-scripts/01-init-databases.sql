CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS silver.user_profile (
    user_id INT PRIMARY KEY,
    age INT,
    gender VARCHAR(10),
    country VARCHAR(50),
    content_type_preference VARCHAR(20),
    preferred_content_theme VARCHAR(50),
    perceived_stress_score INT,
    weekly_work_hours INT,
    exercise_hours_per_week INT,
    income_level VARCHAR(20),
    education_level VARCHAR(20),
    ingestion_date DATE
);

CREATE TABLE IF NOT EXISTS silver.user_usage (
    user_id INT PRIMARY KEY,
    daily_active_minutes_instagram INT,
    user_engagement_score DECIMAL(5,2),
    notification_response_rate DECIMAL(3,2),
    subscription_status VARCHAR(20),
    time_on_reels_per_day INT,
    time_on_stories_per_day INT,
    last_login_date DATE
);

CREATE INDEX idx_country ON silver.user_profile(country);
CREATE INDEX idx_subscription ON silver.user_usage(subscription_status);

GRANT ALL PRIVILEGES ON SCHEMA silver TO admin;
GRANT ALL PRIVILEGES ON SCHEMA gold TO admin;
