-- Expand only the ETL-owned serving table. All Gainr source tables remain
-- read-only. The publisher validates these columns before it reconciles rows.
ALTER TABLE ads_search_ready
  ADD COLUMN IF NOT EXISTS user_id BIGINT NULL,
  ADD COLUMN IF NOT EXISTS category_type BIGINT NULL,
  ADD COLUMN IF NOT EXISTS parent_id BIGINT NULL,
  ADD COLUMN IF NOT EXISTS slug LONGTEXT NULL,
  ADD COLUMN IF NOT EXISTS category_id BIGINT NULL,
  ADD COLUMN IF NOT EXISTS photos LONGTEXT NULL,
  ADD COLUMN IF NOT EXISTS total_favorite BIGINT NULL,
  ADD COLUMN IF NOT EXISTS total_like BIGINT NULL,
  ADD COLUMN IF NOT EXISTS users_rating_count BIGINT NULL,
  ADD COLUMN IF NOT EXISTS rating_avg DOUBLE NULL,
  ADD COLUMN IF NOT EXISTS service_ad_count BIGINT NULL,
  ADD COLUMN IF NOT EXISTS user_prosper_id LONGTEXT NULL,
  ADD COLUMN IF NOT EXISTS user_name LONGTEXT NULL,
  ADD COLUMN IF NOT EXISTS user_photo LONGTEXT NULL,
  ADD COLUMN IF NOT EXISTS user_is_aadhaar_gst_verified BIGINT NULL,
  ADD COLUMN IF NOT EXISTS user_gender BIGINT NULL,
  ADD COLUMN IF NOT EXISTS ads_attributes_json LONGTEXT NULL;
